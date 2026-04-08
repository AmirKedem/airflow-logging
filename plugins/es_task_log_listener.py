from __future__ import annotations

from pathlib import Path

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin


def _upload_task_log(task_instance) -> None:
    remote_log = None
    try:
        import airflow.logging_config as logging_config

        remote_log = logging_config.get_remote_task_log()
    except Exception:
        return

    if remote_log is None or not getattr(remote_log, "write_to_es", False):
        return

    dag_id = getattr(task_instance, "dag_id", None)
    task_id = getattr(task_instance, "task_id", None)
    run_id = getattr(task_instance, "run_id", None)
    try_number = getattr(task_instance, "try_number", None)
    if not all([dag_id, task_id, run_id, try_number]):
        print("es_task_log_listener: missing task identifiers, skipping upload")
        return

    relative_log_path = getattr(task_instance, "log_path", None)
    if relative_log_path:
        log_path = Path("/opt/airflow/logs") / relative_log_path
    else:
        log_path = (
            Path("/opt/airflow/logs")
            / f"dag_id={dag_id}"
            / f"run_id={run_id}"
            / f"task_id={task_id}"
            / f"attempt={try_number}.log"
        )

    if not log_path.exists():
        print(f"es_task_log_listener: log file not found: {log_path}")
        return

    print(f"es_task_log_listener: uploading {log_path}")
    remote_log.upload(log_path, task_instance)


@hookimpl
def on_task_instance_success(previous_state, task_instance):
    _upload_task_log(task_instance)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, error):
    _upload_task_log(task_instance)


class ElasticsearchTaskLogListenerPlugin(AirflowPlugin):
    name = "elasticsearch_task_log_listener"
    listeners = [__import__(__name__)]
