"""Best-effort Airflow callbacks that preserve task failure semantics."""

from __future__ import annotations

import logging
from typing import Any

from .finalize_audit import finalize_batch
from .notify import notify
from .redact import redact
from .summary import failure_context

log = logging.getLogger(__name__)


def _batch_id(context: dict[str, Any]) -> int | None:
    task_instance = context.get("ti")
    value = task_instance.xcom_pull(task_ids="start_batch") if task_instance else None
    return int(value) if value is not None else None


def on_task_failure(context: dict[str, Any]) -> None:
    """Do not email; scripts remain owners of their task audit rows."""
    try:
        log.warning(
            "task failure callback invoked for %s",
            redact(getattr(context.get("task_instance"), "task_id", "unknown")),
        )
    except Exception:
        log.exception("task failure callback failed")


def on_dag_failure(context: dict[str, Any]) -> None:
    """Finalize one failed batch and send its one non-blocking email."""
    try:
        if (batch_id := _batch_id(context)) is None:
            return
        task_id = getattr(context.get("task_instance"), "task_id", "unknown")
        finalize_batch(batch_id, "FAILED", f"Airflow task failed: {redact(task_id)}")
        notify(
            f"[fleet] FAILED fleet_daily_load at {redact(task_id)} {context.get('logical_date', '')}",
            failure_context(batch_id, task_id),
            failure=True,
        )
    except Exception:
        log.exception("dag failure callback failed")
