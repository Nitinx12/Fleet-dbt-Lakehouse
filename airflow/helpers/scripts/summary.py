"""Build bounded, value-free contexts for notification templates."""

from __future__ import annotations

from . import auidt_reader
from .redact import redact

MAX_ITEMS = 20


def _capped(rows: list[dict[str, object]], key: str) -> list[str]:
    names = [redact(row.get(key, "unknown")) for row in rows]
    extra = len(names) - MAX_ITEMS
    return names[:MAX_ITEMS] + ([f"+{extra} more"] if extra > 0 else [])


def success_context(batch_id: int, airflow_url: str | None = None) -> dict[str, object]:
    """Build the success email context from aggregate audit rows."""
    return {
        "batch": auidt_reader.batch(batch_id) or {},
        "batch_id": batch_id,
        "airflow_url": airflow_url or "",
        "tasks": auidt_reader.task_runs(batch_id),
        "ingestion": auidt_reader.ingestion_rows(batch_id),
        "failed_tests": _capped(auidt_reader.failed_tests(batch_id), "check_name"),
        "reconciliations": auidt_reader.reconciliations(batch_id),
    }


def failure_context(
    batch_id: int, task_id: str | None, airflow_url: str | None = None
) -> dict[str, object]:
    """Build the failure email context with bounded error summaries."""
    return {
        "batch": auidt_reader.batch(batch_id) or {},
        "batch_id": batch_id,
        "task_id": redact(task_id or "unknown"),
        "airflow_url": airflow_url or "",
        "tasks": auidt_reader.task_runs(batch_id),
        "errors": _capped(auidt_reader.open_errors(batch_id), "error_message"),
        "failed_tests": _capped(auidt_reader.failed_tests(batch_id), "check_name"),
    }
