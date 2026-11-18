"""Map dbt run_results.json onto the audit.* tables."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from . import audit
from .connection import get_postgres_engine
from .logger import get_logger

log = get_logger("dbt_audit")

_AUDITED_COMMANDS = frozenset({"run", "build", "seed", "snapshot", "test"})
_RUN_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot"})
_LAYERS = frozenset({"bronze", "silver", "gold"})
_MANUAL_PIPELINE = "fleet_daily"
_MANUAL_DAG_ID = "dbt_local"
_TRIGGERED_BY = "dbt_runner"
_STALE_BATCH_MINUTES = 30


def parse_results(
    results: Sequence[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    *,
    invocation_id: str,
    command: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split dbt results into audit.dbt_run_results and audit.data_quality_results rows."""
    run_rows: list[dict[str, Any]] = []
    dq_rows: list[dict[str, Any]] = []

    for item in results:
        unique_id = str(item.get("unique_id", ""))
        node = _node(manifest, unique_id)
        resource_type = str(node.get("resource_type") or unique_id.split(".", 1)[0])

        if resource_type in _RUN_RESOURCE_TYPES:
            run_rows.append(_run_row(item, node, invocation_id, command, resource_type))
        elif resource_type == "test":
            dq_rows.append(_dq_row(item, node, manifest, invocation_id))
        else:
            log.debug("skipping %s: not an audited resource type", unique_id)

    return run_rows, dq_rows


def invocation_started_at(run_results: Mapping[str, Any]) -> datetime | None:
    """Return when dbt started the invocation, or None when it cannot be read."""
    raw = (run_results.get("metadata") or {}).get("invocation_started_at")
    if not raw:
        return None
    try:
        started = datetime.fromisoformat(str(raw))
    except ValueError:
        log.warning("unreadable invocation_started_at: %r", raw)
        return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return started


def fresh_command(
    run_results: Mapping[str, Any], started_after: datetime
) -> str | None:
    """Return the dbt command to audit, or None for stale or unsupported runs."""
    started = invocation_started_at(run_results)
    if started is None or started < started_after:
        log.debug("run_results.json predates this invocation; skipping audit")
        return None

    command = str((run_results.get("args") or {}).get("which") or "")
    if command not in _AUDITED_COMMANDS:
        log.debug("dbt command %r is not audited; skipping", command)
        return None
    return command


def audit_invocation(
    dbt_dir: Path,
    *,
    started_after: datetime,
    exit_code: int,
    target: Engine | Connection | None = None,
) -> tuple[int, int]:
    """Land one finished dbt invocation in audit.*; returns (run_rows, dq_rows)."""
    artifact = dbt_dir / "target" / "run_results.json"
    run_results = _read_json(artifact)
    if run_results is None:
        log.debug("no %s; nothing to audit", artifact)
        return 0, 0

    command = fresh_command(run_results, started_after)
    if command is None:
        return 0, 0

    manifest = _read_json(dbt_dir / "target" / "manifest.json")
    if manifest is None:
        log.warning("manifest.json missing next to run_results.json; skipping audit")
        return 0, 0

    invocation_id = str((run_results.get("metadata") or {}).get("invocation_id") or "")
    run_rows, dq_rows = parse_results(
        run_results.get("results") or [],
        manifest,
        invocation_id=invocation_id,
        command=command,
    )
    if not run_rows and not dq_rows:
        log.debug("dbt %s produced no auditable nodes", command)
        return 0, 0

    batch_id = resolve_batch(target=target)
    try:
        run_count = audit.log_dbt_results(batch_id, run_rows, target=target)
        dq_count = audit.log_data_quality(batch_id, dq_rows, target=target)
    except Exception:
        _finish_batch(batch_id, "FAILED", "audit load failed; see logs", target)
        raise

    _finish_batch(
        batch_id,
        "SUCCEEDED" if exit_code == 0 else "FAILED",
        None if exit_code == 0 else f"dbt exited with code {exit_code}",
        target,
    )
    log.info(
        "audit batch %s: %s run rows, %s data quality rows from dbt %s",
        batch_id,
        run_count,
        dq_count,
        command,
    )
    return run_count, dq_count


def resolve_batch(target: Engine | Connection | None = None) -> int:
    """Open a manual batch for this dbt run, closing a dead local one first."""
    run_id = f"manual_dbt__{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}"
    with _transaction(target) as connection:
        stale = connection.execute(
            text(
                """
                UPDATE control.batch
                   SET status        = 'FAILED',
                       finished_at   = now(),
                       error_message = :message
                 WHERE airflow_dag_id = :dag_id
                   AND status = 'RUNNING'
                   AND started_at < now() - make_interval(mins => :minutes)
                RETURNING batch_id
                """
            ),
            {
                "dag_id": _MANUAL_DAG_ID,
                "minutes": _STALE_BATCH_MINUTES,
                "message": f"Stale {_MANUAL_DAG_ID} batch closed by the audit loader",
            },
        ).fetchall()
        for (stale_id,) in stale:
            log.warning("closed stale dbt batch %s", stale_id)

        batch_id = int(
            connection.execute(
                text(
                    "SELECT control.start_batch("
                    ":pipeline, :dag_id, :run_id, NULL, :triggered_by)"
                ),
                {
                    "pipeline": _MANUAL_PIPELINE,
                    "dag_id": _MANUAL_DAG_ID,
                    "run_id": run_id,
                    "triggered_by": _TRIGGERED_BY,
                },
            ).scalar_one()
        )

    log.info("audit batch %s opened as %s", batch_id, run_id)
    return batch_id


def _run_row(
    item: Mapping[str, Any],
    node: Mapping[str, Any],
    invocation_id: str,
    command: str,
    resource_type: str,
) -> dict[str, Any]:
    response = item.get("adapter_response") or {}
    return {
        "dbt_invocation_id": invocation_id,
        "dbt_command": command,
        "node_unique_id": item["unique_id"],
        "node_name": str(node.get("name") or item["unique_id"]),
        "resource_type": resource_type,
        "target_schema": node.get("schema"),
        "materialization": (node.get("config") or {}).get("materialized"),
        "status": str(item["status"]).upper(),
        "execution_seconds": item.get("execution_time"),
        "rows_affected": _as_int(response.get("rows_affected")),
        "message": item.get("message"),
    }


def _dq_row(
    item: Mapping[str, Any],
    node: Mapping[str, Any],
    manifest: Mapping[str, Any],
    invocation_id: str,
) -> dict[str, Any]:
    # a test runs in dbt_test__audit, so the target comes from the tested model
    attached = _node(manifest, str(node.get("attached_node") or ""))
    schema = attached.get("schema")
    config = node.get("config") or {}
    return {
        "dbt_invocation_id": invocation_id,
        "check_name": str(node.get("name") or item["unique_id"]),
        "check_type": "DBT_TEST",
        "layer": schema if schema in _LAYERS else None,
        "target_schema": schema,
        "target_table": attached.get("alias") or attached.get("name"),
        "column_name": node.get("column_name"),
        "severity": str(config.get("severity") or "ERROR").upper(),
        "status": str(item["status"]).upper(),
        "failing_rows": _as_int(item.get("failures")),
        "message": item.get("message"),
    }


def _node(manifest: Mapping[str, Any], unique_id: str) -> Mapping[str, Any]:
    nodes = manifest.get("nodes") or {}
    node = nodes.get(unique_id)
    return node if isinstance(node, Mapping) else {}


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        log.warning("not an integer, recorded as NULL: %r", value)
        return None


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    with path.open(encoding="utf-8") as handle:
        loaded = json.load(handle)
    return loaded if isinstance(loaded, dict) else None


def _finish_batch(
    batch_id: int,
    status: str,
    message: str | None,
    target: Engine | Connection | None,
) -> None:
    with _transaction(target) as connection:
        connection.execute(
            text("SELECT control.finish_batch(:batch_id, :status, :message)"),
            {"batch_id": batch_id, "status": status, "message": message},
        )
    log.info("audit batch %s closed as %s", batch_id, status)


@contextmanager
def _transaction(
    target: Engine | Connection | None,
) -> Iterator[Connection]:
    """Commit when given an engine; leave an open connection to its caller."""
    resolved = target if target is not None else get_postgres_engine()
    if isinstance(resolved, Connection):
        yield resolved
        return

    with resolved.begin() as connection:
        yield connection
