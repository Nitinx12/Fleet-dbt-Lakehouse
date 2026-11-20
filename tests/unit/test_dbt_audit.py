"""run_results.json must map cleanly onto the audit.* rows."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from utils import dbt_audit
from utils.audit import AuditError, log_dbt_results

MODEL_RESULT: dict[str, Any] = {
    "unique_id": "model.fleet_dbt.customers",
    "status": "success",
    "execution_time": 1.5,
    "adapter_response": {"code": "CREATE TABLE", "rows_affected": "200"},
    "message": None,
}
TEST_RESULT: dict[str, Any] = {
    "unique_id": "test.fleet_dbt.unique_customers_customer_id.abc123",
    "status": "fail",
    "failures": 3,
    "execution_time": 0.2,
    "message": "Got 3 results, configured to fail if != 0",
}
MANIFEST: dict[str, Any] = {
    "nodes": {
        "model.fleet_dbt.customers": {
            "name": "customers",
            "resource_type": "model",
            "schema": "silver",
            "alias": "customers",
            "config": {"materialized": "incremental"},
        },
        TEST_RESULT["unique_id"]: {
            "name": "unique_customers_customer_id",
            "resource_type": "test",
            "schema": "dbt_test__audit",
            "column_name": "customer_id",
            "attached_node": "model.fleet_dbt.customers",
            "config": {"severity": "ERROR"},
        },
    }
}


def test_model_result_becomes_a_run_row() -> None:
    run_rows, dq_rows = dbt_audit.parse_results(
        [MODEL_RESULT],
        MANIFEST,
        invocation_id="inv-1",
        command="run",
    )

    assert dq_rows == []
    assert run_rows == [
        {
            "dbt_invocation_id": "inv-1",
            "dbt_command": "run",
            "node_unique_id": "model.fleet_dbt.customers",
            "node_name": "customers",
            "resource_type": "model",
            "target_schema": "silver",
            "materialization": "incremental",
            "status": "SUCCESS",
            "execution_seconds": 1.5,
            "rows_affected": 200,
            "message": None,
        }
    ]


def test_test_result_targets_the_model_it_checks() -> None:
    run_rows, dq_rows = dbt_audit.parse_results(
        [TEST_RESULT],
        MANIFEST,
        invocation_id="inv-2",
        command="test",
    )

    assert run_rows == []
    assert dq_rows == [
        {
            "dbt_invocation_id": "inv-2",
            "check_name": "unique_customers_customer_id",
            "check_type": "DBT_TEST",
            "layer": "silver",
            "target_schema": "silver",
            "target_table": "customers",
            "column_name": "customer_id",
            "severity": "ERROR",
            "status": "FAIL",
            "failing_rows": 3,
            "message": TEST_RESULT["message"],
        }
    ]


def test_unaudited_and_missing_nodes_are_handled() -> None:
    run_rows, dq_rows = dbt_audit.parse_results(
        [
            {"unique_id": "unit_test.fleet_dbt.something", "status": "pass"},
            {"unique_id": "model.fleet_dbt.orphan", "status": "skipped"},
        ],
        MANIFEST,
        invocation_id="inv-3",
        command="build",
    )

    assert dq_rows == []
    assert len(run_rows) == 1
    assert run_rows[0]["node_name"] == "model.fleet_dbt.orphan"
    assert run_rows[0]["resource_type"] == "model"
    assert run_rows[0]["status"] == "SKIPPED"
    assert run_rows[0]["target_schema"] is None


def test_unknown_status_is_rejected_before_any_write() -> None:
    run_rows, _ = dbt_audit.parse_results(
        [{**MODEL_RESULT, "status": "partial_success"}],
        MANIFEST,
        invocation_id="inv-4",
        command="run",
    )

    with pytest.raises(AuditError, match="status"):
        log_dbt_results(1, run_rows)


def test_invocation_started_at_is_timezone_aware() -> None:
    started = dbt_audit.invocation_started_at(
        {"metadata": {"invocation_started_at": "2026-11-18T05:55:12.639947Z"}}
    )

    assert started is not None
    assert started.tzinfo is not None
    assert dbt_audit.invocation_started_at({"metadata": {}}) is None


def test_invocation_row_maps_every_field() -> None:
    started = datetime(2026, 11, 18, 5, 55, 12, tzinfo=UTC)
    row = dbt_audit._invocation_row(
        invocation_id="inv-1",
        batch_id=7,
        command="run",
        dbt_target="dev",
        started_at=started,
        exit_code=0,
    )

    assert row == {
        "invocation_id": "inv-1",
        "batch_id": 7,
        "dbt_command": "run",
        "dbt_target": "dev",
        "started_at": started,
        "exit_code": 0,
    }


def test_missing_invocation_id_writes_nothing(tmp_path: Path) -> None:
    _write_artifacts(
        tmp_path,
        invocation_started_at="2026-11-18T05:55:12.639947Z",
        which="run",
        invocation_id="",
    )

    written = dbt_audit.audit_invocation(
        tmp_path,
        started_after=datetime(2026, 1, 1, tzinfo=UTC),
        exit_code=0,
    )

    assert written == (0, 0)


def test_missing_artifacts_write_nothing(tmp_path: Path) -> None:
    written = dbt_audit.audit_invocation(
        tmp_path,
        started_after=datetime(2026, 1, 1, tzinfo=UTC),
        exit_code=0,
    )

    assert written == (0, 0)


def test_stale_artifacts_write_nothing(tmp_path: Path) -> None:
    _write_artifacts(
        tmp_path,
        invocation_started_at="2026-01-01T00:00:00Z",
        which="run",
    )

    written = dbt_audit.audit_invocation(
        tmp_path,
        started_after=datetime(2026, 6, 1, tzinfo=UTC),
        exit_code=0,
    )

    assert written == (0, 0)


def test_non_data_commands_write_nothing(tmp_path: Path) -> None:
    _write_artifacts(
        tmp_path,
        invocation_started_at="2026-11-18T05:55:12.639947Z",
        which="debug",
    )

    written = dbt_audit.audit_invocation(
        tmp_path,
        started_after=datetime(2026, 1, 1, tzinfo=UTC),
        exit_code=0,
    )

    assert written == (0, 0)


def test_provided_batch_is_not_finished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Leave a shared Airflow batch open for the DAG finalizer."""
    _write_artifacts(
        tmp_path,
        invocation_started_at="2026-11-18T05:55:12.639947Z",
        which="run",
    )
    finished: list[tuple[object, ...]] = []
    monkeypatch.setattr(dbt_audit, "_log_invocation", lambda *args, **kwargs: None)
    monkeypatch.setattr(dbt_audit.audit, "log_dbt_results", lambda *args, **kwargs: 1)
    monkeypatch.setattr(dbt_audit.audit, "log_data_quality", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        dbt_audit,
        "_finish_batch",
        lambda *args, **kwargs: finished.append(args),
    )
    monkeypatch.setattr(
        dbt_audit,
        "resolve_batch",
        lambda *args, **kwargs: pytest.fail("shared batch must not be replaced"),
    )

    written = dbt_audit.audit_invocation(
        tmp_path,
        started_after=datetime(2026, 1, 1, tzinfo=UTC),
        exit_code=0,
        batch_id=19,
    )

    assert written == (1, 0)
    assert finished == []


def _write_artifacts(
    dbt_dir: Path,
    *,
    invocation_started_at: str,
    which: str,
    invocation_id: str = "inv-9",
) -> None:
    target = dbt_dir / "target"
    target.mkdir(parents=True)
    (target / "run_results.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "invocation_id": invocation_id,
                    "invocation_started_at": invocation_started_at,
                },
                "args": {"which": which},
                "results": [MODEL_RESULT],
            }
        ),
        encoding="utf-8",
    )
    (target / "manifest.json").write_text(json.dumps(MANIFEST), encoding="utf-8")
