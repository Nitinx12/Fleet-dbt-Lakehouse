"""Provide the phase-two Fleet watchdog DAG skeleton."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

AIRFLOW_DIRECTORY = Path(__file__).resolve().parents[1]
if str(AIRFLOW_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_DIRECTORY))


@task(
    task_id="watch_control_batches", retries=0, execution_timeout=timedelta(minutes=5)
)
def watch_control_batches() -> None:
    """Reserve phase-two monitoring until its alert de-duplication contract exists."""
    raise NotImplementedError("Watchdog alert de-duplication policy is phase-two work.")


with DAG(
    dag_id="fleet_watchdog",
    description="Phase-two Fleet watchdog skeleton.",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "watchdog"],
    doc_md="""# Fleet watchdog\n\nPhase two: stale batches, failures, watermark regressions, and alert de-duplication.""",
    default_args={"owner": "fleet-data-eng"},
) as dag:
    watch_control_batches()
