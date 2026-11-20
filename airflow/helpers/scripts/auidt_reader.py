"""Read control and audit evidence for Airflow gates and notifications."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from utils.connection import get_postgres_engine


def _rows(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Run a read-only query and return mapping-shaped rows."""
    with get_postgres_engine().connect() as connection:
        return [dict(row) for row in connection.execute(text(sql), params).mappings()]


def batch(batch_id: int) -> dict[str, Any] | None:
    """Return one batch summary."""
    rows = _rows(
        "SELECT * FROM audit.v_batch_summary WHERE batch_id = :batch_id",
        {"batch_id": batch_id},
    )
    return rows[0] if rows else None


def task_runs(batch_id: int) -> list[dict[str, Any]]:
    """Return aggregate-safe task statuses for a batch."""
    return _rows(
        "SELECT task_id, status, started_at, ended_at, duration_seconds, rows_processed FROM audit.task_run_log WHERE batch_id = :batch_id ORDER BY task_log_id",
        {"batch_id": batch_id},
    )


def ingestion_rows(batch_id: int) -> list[dict[str, Any]]:
    """Return table ingestion task rows without source data."""
    return _rows(
        "SELECT task_id, status, rows_processed FROM audit.task_run_log WHERE batch_id = :batch_id AND task_id LIKE 'ingest_%' ORDER BY task_id",
        {"batch_id": batch_id},
    )


def assert_ingestion_complete(
    batch_id: int, expected_tasks: set[str] | None = None
) -> None:
    """Require all expected ingestion tasks to have succeeded."""
    expected_tasks = expected_tasks or {
        "ingest_mysql__drivers",
        "ingest_mysql__trucks",
        "ingest_mysql__trailers",
        "ingest_mysql__customers",
        "ingest_mysql__routes",
        "ingest_mysql__facilities",
        "ingest_mysql__loads",
        "ingest_mysql__fuel_purchases",
        "ingest_mysql__trips",
        "ingest_mongo__delivery_events",
        "ingest_mongo__maintenance_records",
        "ingest_mongo__safety_incidents",
    }
    succeeded = {
        row["task_id"]
        for row in ingestion_rows(batch_id)
        if row["status"] == "SUCCEEDED"
    }
    missing = sorted(expected_tasks - succeeded)
    if missing:
        raise RuntimeError("ingestion audit incomplete: " + ", ".join(missing))


def failed_tests(batch_id: int) -> list[dict[str, Any]]:
    """Return failed test names and layers."""
    return _rows(
        "SELECT check_name, status, layer FROM audit.data_quality_results WHERE batch_id = :batch_id AND status IN ('FAIL', 'ERROR') ORDER BY dq_result_id",
        {"batch_id": batch_id},
    )


def reconciliations(batch_id: int) -> list[dict[str, Any]]:
    """Return count reconciliation metadata."""
    return _rows(
        "SELECT target_object, source_row_count, target_row_count, row_difference, status FROM audit.reconciliation_results WHERE batch_id = :batch_id ORDER BY recon_id",
        {"batch_id": batch_id},
    )


def open_errors(batch_id: int) -> list[dict[str, Any]]:
    """Return error summaries without details."""
    return _rows(
        "SELECT task_id, error_code, error_message FROM audit.error_log WHERE batch_id = :batch_id AND NOT is_resolved ORDER BY error_id",
        {"batch_id": batch_id},
    )
