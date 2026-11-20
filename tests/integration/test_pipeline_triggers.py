"""Failure triggers must write deduplicated audit.error_log evidence."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine


def test_failure_triggers_log_each_failed_state_once(postgres_engine: Engine) -> None:
    suffix = uuid4().hex[:10]
    pipeline_name = f"trg_{suffix}"
    dag_id = f"dag_trg_{suffix}"
    run_id = f"run_{suffix}"

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
                        ":pipeline_name, :dag_id, :run_id, NULL, 'test')"
                    ),
                    {
                        "pipeline_name": pipeline_name,
                        "dag_id": dag_id,
                        "run_id": run_id,
                    },
                ).scalar_one()
            )
            task_log_id = int(
                connection.execute(
                    text(
                        "INSERT INTO audit.task_run_log"
                        " (batch_id, airflow_dag_id, airflow_run_id, task_id, layer)"
                        " VALUES (:batch_id, :dag_id, :run_id, 'extract', 'silver')"
                        " RETURNING task_log_id"
                    ),
                    {"batch_id": batch_id, "dag_id": dag_id, "run_id": run_id},
                ).scalar_one()
            )

            connection.execute(
                text(
                    "UPDATE audit.task_run_log"
                    " SET status = 'FAILED', ended_at = now(), error_message = 'boom'"
                    " WHERE task_log_id = :task_log_id"
                ),
                {"task_log_id": task_log_id},
            )
            connection.execute(
                text(
                    "UPDATE audit.task_run_log SET error_message = 'boom again'"
                    " WHERE task_log_id = :task_log_id"
                ),
                {"task_log_id": task_log_id},
            )

            task_errors = int(
                connection.execute(
                    text(
                        "SELECT count(*) FROM audit.error_log"
                        " WHERE batch_id = :batch_id AND component = 'AIRFLOW'"
                        "   AND error_code = 'FAILED'"
                    ),
                    {"batch_id": batch_id},
                ).scalar_one()
            )
            assert task_errors == 1

            connection.execute(
                text("SELECT control.finish_batch(:batch_id, 'FAILED', 'batch boom')"),
                {"batch_id": batch_id},
            )
            connection.execute(
                text(
                    "UPDATE control.batch SET error_message = 'still boom'"
                    " WHERE batch_id = :batch_id"
                ),
                {"batch_id": batch_id},
            )

            batch_errors = int(
                connection.execute(
                    text(
                        "SELECT count(*) FROM audit.error_log"
                        " WHERE batch_id = :batch_id AND component = 'CONTROL'"
                        "   AND error_code = 'BATCH_FAILED'"
                    ),
                    {"batch_id": batch_id},
                ).scalar_one()
            )
            assert batch_errors == 1
        finally:
            transaction.rollback()
