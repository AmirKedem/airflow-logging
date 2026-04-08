import json
import logging
from copy import deepcopy
from datetime import datetime, timezone

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG


class AirflowJsonFormatter(logging.Formatter):
    """Serialize log records as JSON for non-task process logs."""

    RESERVED_ATTRS = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "message": message,
            "module": record.module,
            "filename": record.filename,
            "line": record.lineno,
            "process": record.processName,
            "thread": record.threadName,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        extra = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self.RESERVED_ATTRS and not key.startswith("_")
        }
        if extra:
            payload["extra"] = extra

        return json.dumps(payload, default=str)

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

LOGGING_CONFIG["formatters"]["json"] = {
    "()": "log_config.AirflowJsonFormatter",
}

for handler_name, handler_config in LOGGING_CONFIG["handlers"].items():
    handler_class = handler_config.get("class", "")
    if handler_class in {
        "logging.StreamHandler",
        "airflow.utils.log.logging_mixin.RedirectStdHandler",
    }:
        handler_config["formatter"] = "json"
