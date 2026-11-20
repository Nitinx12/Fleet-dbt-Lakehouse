"""Verify the dbt runner preserves dbt arguments and Airflow batch context."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts import run_dbt


@pytest.mark.unit
def test_airflow_arguments_are_not_forwarded_to_dbt() -> None:
    """Keep Airflow-only flags out of the dbt command line."""
    context, dbt_argv = run_dbt._arguments(
        [
            "run",
            "--select",
            "path:models/silver",
            "--batch-id",
            "42",
            "--airflow-dag-id",
            "fleet_daily_load",
            "--airflow-run-id",
            "scheduled__2026-01-01",
        ]
    )

    assert context.batch_id == 42
    assert context.airflow_dag_id == "fleet_daily_load"
    assert context.airflow_run_id == "scheduled__2026-01-01"
    assert dbt_argv == ["run", "--select", "path:models/silver"]


@pytest.mark.unit
def test_main_passes_provided_batch_to_the_audit_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A shared Airflow batch must not be replaced by a manual audit batch."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(run_dbt.subprocess, "call", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        run_dbt,
        "audit_invocation",
        lambda started, exit_code, dbt_target, batch_id=None: captured.update(
            {
                "started": started,
                "exit_code": exit_code,
                "target": dbt_target,
                "batch_id": batch_id,
            }
        ),
    )

    assert run_dbt.main(["test", "--batch-id", "19", "--target", "ci"]) == 0
    assert isinstance(captured["started"], datetime)
    assert captured["started"].tzinfo is UTC
    assert captured["exit_code"] == 0
    assert captured["target"] == "ci"
    assert captured["batch_id"] == 19
