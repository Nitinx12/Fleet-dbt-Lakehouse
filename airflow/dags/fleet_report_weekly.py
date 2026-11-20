"""Provide the intentionally payload-free weekly Fleet reporting skeleton."""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

AIRFLOW_DIRECTORY = Path(__file__).resolve().parents[1]
if str(AIRFLOW_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_DIRECTORY))


@task(task_id="await_weekly_payload", retries=0, execution_timeout=timedelta(minutes=5))
def await_weekly_payload() -> None:
    """Fail clearly until the owner defines the weekly report payload."""
    raise NotImplementedError("Weekly report payload requires owner specification.")


with DAG(
    dag_id="fleet_report_weekly",
    description="Weekly Fleet report skeleton awaiting a defined payload.",
    schedule=os.getenv("FLEET_WEEKLY_REPORT_SCHEDULE", "0 9 * * 1"),
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "weekly"],
    doc_md="""# Fleet weekly report\n\nThe report payload has not been specified; this DAG deliberately performs no data work.""",
    default_args={"owner": "fleet-data-eng"},
) as dag:
    await_weekly_payload()
