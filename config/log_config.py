from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from importlib.metadata import version
from pathlib import PurePosixPath
from typing import Any, BinaryIO

import structlog

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG


def _version_key(version_string: str) -> tuple[int, int, int]:
    parts = version_string.split(".", 2)
    return tuple(int(part) for part in parts[:3])


if (3, 1, 3) <= _version_key(version("apache-airflow")) < (3, 2, 0):
    # Remove this compatibility shim once you upgrade to Airflow 3.2.0 and above.
    REMOTE_TASK_LOG = None
    DEFAULT_REMOTE_CONN_ID = None


SHIP_LOG_FOLDER = "/opt/airflow/log-shipping"
# WARNING: Keep the temp directory on the same filesystem as SHIP_LOG_FOLDER.
# The final handoff uses os.replace(...) for an atomic move, and that will fail
# with a cross-device error if the temp path points to a different mount.
SHIP_LOG_TEMP_FOLDER = "/opt/airflow/log-shipping/.tmp"

SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._=-]+")
ATTEMPT_FILE_RE = re.compile(r"^attempt=(?P<attempt>\d+)\.log(?P<suffix>\..+)?$")
SHIPPING_CONTEXT_KEYS = (
    "dag_id",
    "run_id",
    "task_id",
    "map_index",
    "attempt",
    "log_type",
    "triggerer_job",
    "suffix_extra",
)
TaskLogContext = dict[str, str | int]


def _sanitize_filename_part(value: object) -> str:
    """Convert a value into a filename-safe string.

    The main reason is the log-shipping folder is mounted to your Windows host,
    and values like run_id often contain characters such as : that are valid in
    Linux paths but invalid in Windows filenames. Without _safe_segment,
    filenames like:

    run_id=manual__2026-04-08T21:18:03.884803+00:00

    would break on the host side.

    Args:
        value: Raw value taken from the task log context.

    Returns:
        A sanitized string that is safe to use in shipped filenames.
    """
    return SAFE_NAME_RE.sub("_", str(value))


def _task_context_from_log_path(log_path: str) -> TaskLogContext:
    """Extract task metadata from an Airflow task log path.

    Args:
        log_path: Relative task log path produced by Airflow.

    Returns:
        A context dictionary containing task identifiers and any trigger-related
        suffix fields that can be inferred from the path.
    """
    path = PurePosixPath(log_path)
    context: TaskLogContext = dict(part.split("=", 1) for part in path.parts[:-1] if "=" in part)

    match = ATTEMPT_FILE_RE.match(path.name)
    if not match:
        context["file_name"] = path.name
        return context

    context["attempt"] = int(match.group("attempt"))
    suffix = match.group("suffix")
    if suffix:
        parts = suffix.lstrip(".").split(".")
        for key, value in zip(("log_type", "triggerer_job"), parts):
            if value:
                context[key] = value
        if len(parts) > 2:
            remainder = ".".join(parts[2:])
            if remainder:
                context["suffix_extra"] = remainder

    return context


def _shipping_file_paths(log_path: str) -> tuple[TaskLogContext, str, str]:
    """Build the shipping context and file paths for a task log.

    Args:
        log_path: Relative task log path produced by Airflow.

    Returns:
        A tuple of the parsed context, final shipped file path, and temporary
        partial file path.
    """
    context = _task_context_from_log_path(log_path)
    name_parts = [
        "{0}={1}".format(key, _sanitize_filename_part(context[key]))
        for key in SHIPPING_CONTEXT_KEYS
        if context.get(key) is not None
    ]

    if not name_parts:
        file_name = context.get("file_name", PurePosixPath(log_path).name)
        name_parts.append("file={0}".format(_sanitize_filename_part(file_name)))

    file_name = "__".join(name_parts) + ".log"
    final_path = os.path.join(SHIP_LOG_FOLDER, file_name)
    partial_path = os.path.join(SHIP_LOG_TEMP_FOLDER, file_name + ".partial")
    return context, final_path, partial_path


class TaskLogTeeWriter:
    """Duplicate task log bytes to the GUI log and shipping log.

    Args:
        gui_file: Open file handle for the normal Airflow task log.
        shipping_file: Open file handle for the shipping log.
        shipping_final_path: Final visible path for the shipped log.
        shipping_partial_path: Temporary in-progress path for the shipped log.
        context: Task metadata added to each shipped JSON line.
    """

    def __init__(
        self,
        gui_file: BinaryIO,
        shipping_file: BinaryIO,
        shipping_final_path: str,
        shipping_partial_path: str,
        context: TaskLogContext,
    ) -> None:
        """Initialize the tee writer.

        Args:
            gui_file: Open file handle for the normal Airflow task log.
            shipping_file: Open file handle for the shipping log.
            shipping_final_path: Final visible path for the shipped log.
            shipping_partial_path: Temporary in-progress path for the shipped log.
            context: Task metadata added to each shipped JSON line.
        """
        self._gui_file = gui_file
        self._shipping_file = shipping_file
        self._shipping_final_path = shipping_final_path
        self._shipping_partial_path = shipping_partial_path
        self._context = dict(context)
        self._buffer = bytearray()
        self.name: str | None = getattr(gui_file, "name", None)

    def write(self, data: bytes) -> int:
        """Write bytes to both destinations.

        Args:
            data: Raw bytes emitted by the task logger.

        Returns:
            The number of bytes written to the GUI log file.
        """
        written = self._gui_file.write(data)
        self._buffer.extend(data)

        while True:
            newline_pos = self._buffer.find(b"\n")
            if newline_pos == -1:
                break
            line = bytes(self._buffer[: newline_pos + 1])
            del self._buffer[: newline_pos + 1]
            self._shipping_file.write(self._shipping_line(line))

        return written

    def flush(self) -> None:
        """Flush both the GUI log file and shipping log file."""
        self._gui_file.flush()
        self._shipping_file.flush()

    def fileno(self) -> int:
        """Return the underlying file descriptor for the GUI log file.

        Returns:
            The GUI log file descriptor.
        """
        return self._gui_file.fileno()

    def close(self) -> None:
        """Close both files and finalize the shipped log path.
        
        We need to ensure that any remaining buffered data is flushed to the shipping file, both files are properly closed, 
        and the shipping file is atomically moved to its final destination so that downstream consumers see either the complete log or no log at all.
        """
        if self._buffer:
            self._shipping_file.write(self._shipping_line(bytes(self._buffer)))
            self._buffer.clear()

        try:
            self.flush()
        finally:
            self._gui_file.close()
            self._shipping_file.close()

        if os.path.exists(self._shipping_partial_path):
            os.replace(self._shipping_partial_path, self._shipping_final_path)

    def _shipping_line(self, raw_line: bytes) -> bytes:
        """Convert one raw log line into a shipped JSON line.

        Args:
            raw_line: Raw bytes read from the task log stream.

        Returns:
            JSON-encoded bytes enriched with task metadata.
        """
        stripped = raw_line.rstrip(b"\r\n")
        if not stripped:
            return raw_line

        text = stripped.decode("utf-8", errors="replace")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = {"event": text}

        if not isinstance(payload, dict):
            payload = {"event": payload}

        payload.update(self._context)
        return json.dumps(payload, default=str).encode("utf-8") + b"\n"


def _configure_supervisor_task_logging(log_path: str, client: Any) -> tuple[Any, TaskLogTeeWriter]:
    """Configure Airflow 3 supervisor logging for GUI and shipping outputs.

    Args:
        log_path: Relative Airflow task log path.
        client: Execution API client used by Airflow's supervisor.

    Returns:
        A structlog logger and the tee writer used as the log sink.
    """
    from airflow.sdk.execution_time import supervisor as sdk_supervisor
    from airflow.sdk.log import init_log_file, logging_processors

    gui_file = init_log_file(log_path).open("ab")
    context, shipping_final_path, shipping_partial_path = _shipping_file_paths(log_path)

    os.makedirs(os.path.dirname(shipping_partial_path), exist_ok=True)
    if os.path.exists(shipping_final_path) and not os.path.exists(shipping_partial_path):
        os.replace(shipping_final_path, shipping_partial_path)

    shipping_file = open(shipping_partial_path, "ab")
    tee_writer = TaskLogTeeWriter(
        gui_file=gui_file,
        shipping_file=shipping_file,
        shipping_final_path=shipping_final_path,
        shipping_partial_path=shipping_partial_path,
        context=context,
    )

    with sdk_supervisor._remote_logging_conn(client):
        processors = logging_processors(json_output=True)

    logger = structlog.wrap_logger(
        structlog.BytesLogger(tee_writer),
        processors=processors,
        logger_name="task",
    ).bind()
    return logger, tee_writer


def _patch_airflow3_supervisor_logging() -> None:
    """Patch the Airflow 3 supervisor to use the custom tee writer.
    
    This function monkey-patches the Airflow 3 supervisor's logging configuration and will be removed once the bug is fixed in Airflow. 
    See issue https://github.com/apache/airflow/issues/53442
    """
    try:
        from airflow.sdk.execution_time import supervisor as sdk_supervisor
    except Exception:
        return

    sdk_supervisor._configure_logging = _configure_supervisor_task_logging


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
_patch_airflow3_supervisor_logging()
