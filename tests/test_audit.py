"""utils.audit must validate labels and write only what the audit tables allow."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from utils.audit import (
    AuditError,
    TaskHandle,
    error_summary,
    finish_task,
    log_data_quality,
    log_dbt_results,
    log_error,
    log_reconciliation,
    resolve_error,
    start_task,
    task_run,
)


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.unit
def test_task_handle_carries_rows_processed() -> None:
    handle = TaskHandle(task_log_id=7)
    assert handle.rows_processed is None
    handle.rows_processed = 42
    assert handle.task_log_id == 7


@pytest.mark.unit
def test_error_summary_does_not_persist_exception_text() -> None:
    error = ValueError("source value must not reach audit")

    assert error_summary(error, "ingestion") == "ingestion failed (ValueError)"


@pytest.mark.integration
def test_round_trip_rolls_back_cleanly() -> None:
    from uuid import uuid4

    from sqlalchemy.exc import IntegrityError

    from utils.connection import get_postgres_engine

    try:
        engine = get_postgres_engine()
    except SQLAlchemyError:
        pytest.skip("Postgres not reachable")

    suffix = uuid4().hex[:12]
    pipeline_name = f"audit_test_{suffix}"
    dag_id = f"dag_audit_{suffix}"

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                text(
                    "INSERT INTO control.pipeline (pipeline_name, airflow_dag_id)"
                    " VALUES (:pipeline_name, :dag_id)"
                ),
                {"pipeline_name": pipeline_name, "dag_id": dag_id},
            )
            batch_id = int(
                connection.execute(
                    text(
                        "SELECT control.start_batch("
                        ":pipeline_name, :dag_id, 'run_ok', NULL, 'test')"
                    ),
                    {"pipeline_name": pipeline_name, "dag_id": dag_id},
                ).scalar_one()
            )

            with task_run(
                batch_id, dag_id, "run_ok", "extract_mysql", target=connection
            ) as task:
                task.rows_processed = 3
            opened = (
                connection.execute(
                    text(
                        "SELECT status, rows_processed FROM audit.task_run_log"
                        " WHERE task_log_id = :id"
                    ),
                    {"id": task.task_log_id},
                )
                .mappings()
                .first()
            )
            assert opened["status"] == "SUCCEEDED"
            assert opened["rows_processed"] == 3

            with (
                pytest.raises(ValueError),
                task_run(
                    batch_id, dag_id, "run_bad", "extract_mongo", target=connection
                ),
            ):
                raise ValueError("boom")
            failed = (
                connection.execute(
                    text(
                        "SELECT status, error_message FROM audit.task_run_log"
                        " WHERE airflow_run_id = 'run_bad'"
                    )
                )
                .mappings()
                .first()
            )
            assert failed["status"] == "FAILED"
            assert failed["error_message"] == "task failed (ValueError)"

            assert (
                start_task(
                    batch_id, dag_id, "run_ok", "extract_mysql", target=connection
                )
                == task.task_log_id
            )  # same attempt is reused, not duplicated

            connection.execute(
                text(
                    "INSERT INTO audit.dbt_invocation"
                    " (invocation_id, batch_id, dbt_command, started_at)"
                    " VALUES ('inv-1', :batch_id, 'build', now())"
                ),
                {"batch_id": batch_id},
            )
            invocation = (
                connection.execute(
                    text(
                        "SELECT batch_id, dbt_command, exit_code"
                        " FROM audit.dbt_invocation WHERE invocation_id = 'inv-1'"
                    )
                )
                .mappings()
                .one()
            )
            assert invocation["batch_id"] == batch_id
            assert invocation["dbt_command"] == "build"
            assert invocation["exit_code"] is None

            written = log_dbt_results(
                batch_id,
                [
                    {
                        "dbt_invocation_id": "inv-1",
                        "dbt_command": "build",
                        "node_unique_id": "model.fleet.stg_mysql__loads",
                        "node_name": "stg_mysql__loads",
                        "resource_type": "model",
                        "status": "success",
                        "execution_seconds": 1.5,
                    }
                ],
                target=connection,
            )
            assert written == 1

            assert (
                log_data_quality(
                    batch_id,
                    [
                        {
                            "dbt_invocation_id": "inv-1",
                            "check_name": "not_null_load_id",
                            "check_type": "dbt_test",
                            "status": "pass",
                        }
                    ],
                    target=connection,
                )
                == 1
            )

            assert (
                log_reconciliation(
                    batch_id,
                    [
                        {
                            "source_system": "mysql",
                            "source_object": "loads",
                            "target_layer": "bronze",
                            "target_object": "mysql__loads",
                            "source_row_count": 85410,
                            "target_row_count": 85410,
                            "status": "match",
                        }
                    ],
                    target=connection,
                )
                == 1
            )

            error_id = log_error(
                component="ingestion",
                error_message="cursor went backwards",
                batch_id=batch_id,
                error_detail={"table": "loads", "cursor": "2024-01-01"},
                target=connection,
            )
            assert (
                resolve_error(
                    error_id, resolved_by="tester", note="replayed", target=connection
                )
                == error_id
            )

            # append-only guard: the history tables reject UPDATE
            with pytest.raises(SQLAlchemyError), connection.begin_nested():
                connection.execute(
                    text("UPDATE audit.dbt_run_results SET message = 'tampered'")
                )
            with pytest.raises(SQLAlchemyError), connection.begin_nested():
                connection.execute(
                    text("UPDATE audit.dbt_invocation SET exit_code = 0")
                )

            # batch_id is a real foreign key: an unknown batch is rejected
            with pytest.raises(IntegrityError), connection.begin_nested():
                connection.execute(
                    text(
                        """
                            INSERT INTO audit.dbt_run_results
                                (batch_id, dbt_invocation_id, dbt_command,
                                 node_unique_id, node_name, resource_type, status)
                            VALUES
                                (999999, 'inv-1', 'run', 'model.x', 'x',
                                 'model', 'SUCCESS')
                            """
                    )
                )
        finally:
            transaction.rollback()
