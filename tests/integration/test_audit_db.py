"""Audit writes against the throwaway Postgres database."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from utils.audit import (
    log_data_quality,
    log_dbt_results,
    log_error,
    log_reconciliation,
    resolve_error,
    start_task,
    task_run,
)


def test_round_trip_rolls_back_cleanly(postgres_engine: Engine) -> None:
    """Audit helpers write only allowed rows and roll back with the batch."""
    suffix = uuid4().hex[:12]
    pipeline_name = f"audit_test_{suffix}"
    dag_id = f"dag_audit_{suffix}"

    with postgres_engine.connect() as connection:
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
            )

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

            with pytest.raises(SQLAlchemyError), connection.begin_nested():
                connection.execute(
                    text("UPDATE audit.dbt_run_results SET message = 'tampered'")
                )
            with pytest.raises(SQLAlchemyError), connection.begin_nested():
                connection.execute(
                    text("UPDATE audit.dbt_invocation SET exit_code = 0")
                )

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
