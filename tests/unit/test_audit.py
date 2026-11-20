"""utils.audit must validate labels and write only what the audit tables allow."""

from __future__ import annotations

import pytest

from utils.audit import (
    AuditError,
    TaskHandle,
    error_summary,
    finish_task,
    log_data_quality,
    log_dbt_results,
    log_error,
    log_reconciliation,
)


def test_unknown_labels_are_rejected_before_any_write() -> None:
    with pytest.raises(AuditError, match="status"):
        finish_task(1, "DONE")

    with pytest.raises(AuditError, match="dbt_command"):
        log_dbt_results(
            1,
            [
                {
                    "dbt_invocation_id": "i",
                    "dbt_command": "run-models",
                    "node_unique_id": "n",
                    "node_name": "n",
                    "resource_type": "model",
                    "status": "SUCCESS",
                }
            ],
        )

    with pytest.raises(AuditError, match="check_type"):
        log_data_quality(
            1,
            [{"check_name": "c", "check_type": "SQL", "status": "PASS"}],
        )

    with pytest.raises(AuditError, match="status"):
        log_reconciliation(
            1,
            [
                {
                    "source_system": "mysql",
                    "source_object": "loads",
                    "target_layer": "bronze",
                    "target_object": "mysql__loads",
                    "source_row_count": 1,
                    "target_row_count": 1,
                    "status": "OK",
                }
            ],
        )

    with pytest.raises(AuditError, match="component"):
        log_error(component="OTHERWISE", error_message="x")


def test_missing_required_columns_are_named() -> None:
    with pytest.raises(AuditError, match="node_unique_id"):
        log_dbt_results(
            1,
            [
                {
                    "dbt_invocation_id": "i",
                    "dbt_command": "run",
                    "node_name": "n",
                    "resource_type": "model",
                    "status": "SUCCESS",
                }
            ],
        )


def test_task_handle_carries_rows_processed() -> None:
    handle = TaskHandle(task_log_id=7)
    assert handle.rows_processed is None
    handle.rows_processed = 42
    assert handle.task_log_id == 7


def test_error_summary_does_not_persist_exception_text() -> None:
    error = ValueError("source value must not reach audit")

    assert error_summary(error, "ingestion") == "ingestion failed (ValueError)"
