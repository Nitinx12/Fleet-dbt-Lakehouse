"""Write ingestion and dbt outcomes to the audit.* tables."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from .connection import get_postgres_engine
from .logger import get_logger

log = get_logger("audit")

_FINAL_TASK_STATUS = frozenset({"SUCCEEDED", "FAILED", "SKIPPED", "UP_FOR_RETRY"})
_DBT_COMMANDS = frozenset({"run", "build", "seed", "snapshot", "test"})
_DBT_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot"})
_DBT_STATUSES = frozenset({"SUCCESS", "ERROR", "SKIPPED"})
_DQ_CHECK_TYPES = frozenset({"DBT_TEST", "FRESHNESS", "ROW_COUNT", "CUSTOM_SQL"})
_DQ_STATUSES = frozenset({"PASS", "FAIL", "WARN", "ERROR", "SKIPPED"})
_DQ_SEVERITIES = frozenset({"WARN", "ERROR"})
_RECON_STATUSES = frozenset({"MATCH", "MISMATCH", "WITHIN_TOLERANCE"})
_ERROR_SEVERITIES = frozenset({"WARNING", "ERROR", "CRITICAL"})
_ERROR_COMPONENTS = frozenset(
    {"AIRFLOW", "DBT", "INGESTION", "DATA_QUALITY", "CONTROL", "OTHER"}
)
_TASK_LAYERS = frozenset({"bronze", "silver", "gold", "audit", "control"})
_DQ_LAYERS = frozenset({"bronze", "silver", "gold"})
_RECON_TARGET_LAYERS = frozenset({"bronze", "silver", "gold"})
_ERROR_LAYERS = frozenset({"bronze", "silver", "gold"})


class AuditError(RuntimeError):
    """Raised when an audit row would violate a table constraint."""


def error_summary(error: BaseException, operation: str) -> str:
    """Return a non-sensitive failure summary suitable for an audit row."""
    return f"{operation} failed ({type(error).__name__})"


@dataclass
class TaskHandle:
    """Mutable: set rows_processed before the with-block ends."""

    task_log_id: int
    rows_processed: int | None = None


def _target(target: Engine | Connection | None) -> Engine | Connection:
    """Fall back to the shared cached engine."""
    return target if target is not None else get_postgres_engine()


def _require(value: str, allowed: frozenset[str], field: str) -> str:
    """Return value unchanged, or raise when it is not an accepted label."""
    if value not in allowed:
        raise AuditError(f"{field} must be one of {sorted(allowed)}, got {value!r}")
    return value


def _field(row: Mapping[str, Any], name: str) -> Any:
    """Fetch a required column, or raise with the column that is missing."""
    value = row.get(name)
    if value is None:
        raise AuditError(f"{name} is required on every audit row")
    return value


def start_task(
    batch_id: int,
    airflow_dag_id: str,
    airflow_run_id: str,
    task_id: str,
    *,
    map_index: int = -1,
    try_number: int = 1,
    layer: str | None = None,
    target: Engine | Connection | None = None,
) -> int:
    """Open a RUNNING task attempt and return its task_log_id (a retry reuses it)."""
    if layer is not None:
        _require(layer, _TASK_LAYERS, "layer")
    sql = text(
        """
        INSERT INTO audit.task_run_log
            (batch_id, airflow_dag_id, airflow_run_id, task_id,
             map_index, try_number, layer, status)
        VALUES
            (:batch_id, :airflow_dag_id, :airflow_run_id, :task_id,
             :map_index, :try_number, :layer, 'RUNNING')
        ON CONFLICT (airflow_dag_id, airflow_run_id, task_id, map_index, try_number)
        DO UPDATE SET status = 'RUNNING',
                      started_at = now(),
                      ended_at = NULL,
                      rows_processed = NULL,
                      error_message = NULL
        RETURNING task_log_id
        """
    )
    params = {
        "batch_id": batch_id,
        "airflow_dag_id": airflow_dag_id,
        "airflow_run_id": airflow_run_id,
        "task_id": task_id,
        "map_index": map_index,
        "try_number": try_number,
        "layer": layer,
    }
    with _as_transaction(target) as connection:
        task_log_id = int(connection.execute(sql, params).scalar_one())

    log.info("audit.task_run_log %s opened for %s", task_log_id, task_id)
    return task_log_id


def finish_task(
    task_log_id: int,
    status: str,
    *,
    rows_processed: int | None = None,
    error_message: str | None = None,
    target: Engine | Connection | None = None,
) -> None:
    """Close a RUNNING attempt; raises when it is already closed or invalid."""
    _require(status, _FINAL_TASK_STATUS, "status")
    sql = text(
        """
        UPDATE audit.task_run_log
           SET status = :status,
               ended_at = now(),
               rows_processed = COALESCE(:rows_processed, rows_processed),
               error_message = :error_message
         WHERE task_log_id = :task_log_id
           AND status = 'RUNNING'
        RETURNING task_log_id
        """
    )
    params = {
        "task_log_id": task_log_id,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
    }
    with _as_transaction(target) as connection:
        closed = connection.execute(sql, params).first()

    if closed is None:
        raise AuditError(f"task_log_id {task_log_id} is not RUNNING")

    log.info("audit.task_run_log %s closed as %s", task_log_id, status)


@contextmanager
def task_run(
    batch_id: int,
    airflow_dag_id: str,
    airflow_run_id: str,
    task_id: str,
    *,
    map_index: int = -1,
    try_number: int = 1,
    layer: str | None = None,
    target: Engine | Connection | None = None,
) -> Iterator[TaskHandle]:
    """Run one block as a task attempt: FAILED on error, SUCCEEDED otherwise."""
    task_log_id = start_task(
        batch_id,
        airflow_dag_id,
        airflow_run_id,
        task_id,
        map_index=map_index,
        try_number=try_number,
        layer=layer,
        target=target,
    )
    handle = TaskHandle(task_log_id)

    try:
        yield handle
    except BaseException as error:
        if isinstance(error, Exception):
            log.error("audit task %s failed (%s)", task_id, type(error).__name__)
        try:
            finish_task(
                task_log_id,
                "FAILED",
                error_message=error_summary(error, "task"),
                target=target,
            )
        except Exception:
            log.exception("could not close audit task %s", task_log_id)
        raise
    else:
        finish_task(
            task_log_id,
            "SUCCEEDED",
            rows_processed=handle.rows_processed,
            target=target,
        )


def log_dbt_results(
    batch_id: int,
    results: Sequence[Mapping[str, Any]],
    *,
    target: Engine | Connection | None = None,
) -> int:
    """Insert one row per dbt node result; returns how many were written."""
    if not results:
        return 0

    rows = []
    for item in results:
        rows.append(
            {
                "batch_id": batch_id,
                "dbt_invocation_id": _field(item, "dbt_invocation_id"),
                "dbt_command": _require(
                    str(_field(item, "dbt_command")), _DBT_COMMANDS, "dbt_command"
                ),
                "node_unique_id": _field(item, "node_unique_id"),
                "node_name": _field(item, "node_name"),
                "resource_type": _require(
                    str(_field(item, "resource_type")),
                    _DBT_RESOURCE_TYPES,
                    "resource_type",
                ),
                "target_schema": item.get("target_schema"),
                "materialization": item.get("materialization"),
                "status": _require(
                    str(_field(item, "status")).upper(), _DBT_STATUSES, "status"
                ),
                "execution_seconds": item.get("execution_seconds"),
                "rows_affected": item.get("rows_affected"),
                "message": item.get("message"),
            }
        )

    sql = text(
        """
        INSERT INTO audit.dbt_run_results
            (batch_id, dbt_invocation_id, dbt_command, node_unique_id, node_name,
             resource_type, target_schema, materialization, status,
             execution_seconds, rows_affected, message)
        VALUES
            (:batch_id, :dbt_invocation_id, :dbt_command, :node_unique_id, :node_name,
             :resource_type, :target_schema, :materialization, :status,
             :execution_seconds, :rows_affected, :message)
        """
    )
    with _as_transaction(target) as connection:
        connection.execute(sql, rows)

    log.info("audit.dbt_run_results: %s rows for batch %s", len(rows), batch_id)
    return len(rows)


def log_data_quality(
    batch_id: int,
    checks: Sequence[Mapping[str, Any]],
    *,
    target: Engine | Connection | None = None,
) -> int:
    """Insert one row per quality check outcome; returns how many were written."""
    if not checks:
        return 0

    rows = []
    for item in checks:
        layer = item.get("layer")
        if layer is not None:
            _require(str(layer), _DQ_LAYERS, "layer")
        rows.append(
            {
                "batch_id": batch_id,
                "dbt_invocation_id": item.get("dbt_invocation_id"),
                "check_name": _field(item, "check_name"),
                "check_type": _require(
                    str(_field(item, "check_type")).upper(),
                    _DQ_CHECK_TYPES,
                    "check_type",
                ),
                "layer": item.get("layer"),
                "target_schema": item.get("target_schema"),
                "target_table": item.get("target_table"),
                "column_name": item.get("column_name"),
                "severity": _require(
                    str(item.get("severity", "ERROR")).upper(),
                    _DQ_SEVERITIES,
                    "severity",
                ),
                "status": _require(
                    str(_field(item, "status")).upper(), _DQ_STATUSES, "status"
                ),
                "failing_rows": item.get("failing_rows"),
                "message": item.get("message"),
            }
        )

    sql = text(
        """
        INSERT INTO audit.data_quality_results
            (batch_id, dbt_invocation_id, check_name, check_type, layer,
             target_schema, target_table, column_name, severity, status,
             failing_rows, message)
        VALUES
            (:batch_id, :dbt_invocation_id, :check_name, :check_type, :layer,
             :target_schema, :target_table, :column_name, :severity, :status,
             :failing_rows, :message)
        """
    )
    with _as_transaction(target) as connection:
        connection.execute(sql, rows)

    log.info("audit.data_quality_results: %s rows for batch %s", len(rows), batch_id)
    return len(rows)


def log_reconciliation(
    batch_id: int,
    comparisons: Sequence[Mapping[str, Any]],
    *,
    target: Engine | Connection | None = None,
) -> int:
    """Insert one row per source-vs-target comparison; returns the count."""
    if not comparisons:
        return 0

    rows = []
    for item in comparisons:
        target_layer = str(_field(item, "target_layer"))
        _require(target_layer, _RECON_TARGET_LAYERS, "target_layer")
        source_row_count = _field(item, "source_row_count")
        target_row_count = _field(item, "target_row_count")
        if source_row_count < 0:
            raise AuditError("source_row_count must be non-negative")
        if target_row_count < 0:
            raise AuditError("target_row_count must be non-negative")
        tolerance_pct = item.get("tolerance_pct", 0)
        if tolerance_pct is not None and float(tolerance_pct) < 0:
            raise AuditError("tolerance_pct must be non-negative")
        rows.append(
            {
                "batch_id": batch_id,
                "source_system": _field(item, "source_system"),
                "source_object": _field(item, "source_object"),
                "target_layer": _field(item, "target_layer"),
                "target_object": _field(item, "target_object"),
                "source_row_count": _field(item, "source_row_count"),
                "target_row_count": _field(item, "target_row_count"),
                "source_checksum": item.get("source_checksum"),
                "target_checksum": item.get("target_checksum"),
                "tolerance_pct": item.get("tolerance_pct", 0),
                "status": _require(
                    str(_field(item, "status")).upper(), _RECON_STATUSES, "status"
                ),
                "message": item.get("message"),
            }
        )

    sql = text(
        """
        INSERT INTO audit.reconciliation_results
            (batch_id, source_system, source_object, target_layer, target_object,
             source_row_count, target_row_count, source_checksum, target_checksum,
             tolerance_pct, status, message)
        VALUES
            (:batch_id, :source_system, :source_object, :target_layer, :target_object,
             :source_row_count, :target_row_count, :source_checksum, :target_checksum,
             :tolerance_pct, :status, :message)
        """
    )
    with _as_transaction(target) as connection:
        connection.execute(sql, rows)

    log.info("audit.reconciliation_results: %s rows for batch %s", len(rows), batch_id)
    return len(rows)


def log_error(
    *,
    component: str,
    error_message: str,
    batch_id: int | None = None,
    layer: str | None = None,
    severity: str = "ERROR",
    error_code: str | None = None,
    error_detail: Mapping[str, Any] | None = None,
    airflow_dag_id: str | None = None,
    task_id: str | None = None,
    target: Engine | Connection | None = None,
) -> int:
    """Insert one pipeline error and return its error_id."""
    _require(severity.upper(), _ERROR_SEVERITIES, "severity")
    _require(component.upper(), _ERROR_COMPONENTS, "component")
    if layer is not None:
        _require(str(layer), _ERROR_LAYERS, "layer")

    sql = text(
        """
        INSERT INTO audit.error_log
            (batch_id, component, layer, severity, airflow_dag_id, task_id,
             error_code, error_message, error_detail)
        VALUES
            (:batch_id, :component, :layer, :severity, :airflow_dag_id, :task_id,
             :error_code, :error_message, :error_detail)
        RETURNING error_id
        """
    )
    params = {
        "batch_id": batch_id,
        "component": component.upper(),
        "layer": layer,
        "severity": severity.upper(),
        "airflow_dag_id": airflow_dag_id,
        "task_id": task_id,
        "error_code": error_code,
        "error_message": error_message,
        "error_detail": json.dumps(error_detail) if error_detail else None,
    }
    with _as_transaction(target) as connection:
        error_id = int(connection.execute(sql, params).scalar_one())

    log.error("audit.error_log %s written for %s", error_id, component.upper())
    return error_id


def resolve_error(
    error_id: int,
    *,
    resolved_by: str,
    note: str,
    target: Engine | Connection | None = None,
) -> int:
    """Mark an error resolved; only the four resolution columns may change."""
    sql = text(
        """
        UPDATE audit.error_log
           SET is_resolved = TRUE,
               resolved_at = now(),
               resolved_by = :resolved_by,
               resolution_note = :note
         WHERE error_id = :error_id
           AND NOT is_resolved
        RETURNING error_id
        """
    )
    params = {"error_id": error_id, "resolved_by": resolved_by, "note": note}
    with _as_transaction(target) as connection:
        resolved = connection.execute(sql, params).first()

    if resolved is None:
        raise AuditError(f"error_log {error_id} is missing or already resolved")

    log.info("audit.error_log %s resolved by %s", error_id, resolved_by)
    return error_id


@contextmanager
def _as_transaction(target: Engine | Connection | None) -> Iterator[Connection]:
    """Commit when given an engine; leave an open connection to its caller."""
    resolved = _target(target)
    if isinstance(resolved, Connection):
        yield resolved
        return

    with resolved.begin() as connection:
        yield connection
