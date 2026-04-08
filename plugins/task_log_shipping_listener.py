from __future__ import annotations

import json
import re
from pathlib import Path

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin


SHIP_ROOT = Path("/opt/airflow/log-shipping")
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._=-]+")


def _safe_segment(value: object) -> str:
    return _SAFE_NAME_RE.sub("_", str(value))


def _task_context(task_instance) -> dict[str, object] | None:
    dag_id = getattr(task_instance, "dag_id", None)
    task_id = getattr(task_instance, "task_id", None)
    run_id = getattr(task_instance, "run_id", None)
    try_number = getattr(task_instance, "try_number", None)
    if not all([dag_id, task_id, run_id, try_number]):
        return None

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "attempt": try_number,
    }


def _task_log_source(task_instance) -> Path | None:
    context = _task_context(task_instance)
    if context is None:
        return None

    return (
        Path("/opt/airflow/logs")
        / f"dag_id={context['dag_id']}"
        / f"run_id={context['run_id']}"
        / f"task_id={context['task_id']}"
        / f"attempt={context['attempt']}.log"
    )


def _shipping_target(task_instance) -> Path | None:
    context = _task_context(task_instance)
    if context is None:
        return None

    # Keep shipped logs flat so each task completion becomes a single file
    # directly under the shipping directory.
    filename = (
        f"dag_id={_safe_segment(context['dag_id'])}__"
        f"run_id={_safe_segment(context['run_id'])}__"
        f"task_id={_safe_segment(context['task_id'])}__"
        f"attempt={_safe_segment(context['attempt'])}.log"
    )
    return SHIP_ROOT / filename


def _inject_context_into_line(line: str, context: dict[str, object]) -> str:
    stripped = line.rstrip("\n")
    if not stripped:
        return line

    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        payload = {"message": stripped}

    if not isinstance(payload, dict):
        payload = {"message": payload}

    payload.update(context)
    return json.dumps(payload, default=str) + "\n"


def _ship_task_log(task_instance) -> None:
    source = _task_log_source(task_instance)
    target = _shipping_target(task_instance)
    context = _task_context(task_instance)
    if source is None or target is None:
        return

    if not source.exists():
        return

    target.parent.mkdir(parents=True, exist_ok=True)

    with source.open("r", encoding="utf-8", errors="replace") as src, target.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        for line in src:
            dst.write(_inject_context_into_line(line, context))


@hookimpl
def on_task_instance_success(previous_state, task_instance):
    _ship_task_log(task_instance)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error):
    _ship_task_log(task_instance)


class TaskLogShippingListenerPlugin(AirflowPlugin):
    name = "task_log_shipping_listener"
    listeners = [__import__(__name__)]
