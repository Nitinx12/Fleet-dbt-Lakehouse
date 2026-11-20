"""Render notification twins with bounded aggregate fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts.notify import render


@pytest.mark.unit
@pytest.mark.parametrize("template", ["summary.txt.j2", "summary.html.j2"])
def test_summary_templates_render(template: str) -> None:
    """Success templates include aggregate run information."""
    output = render(
        template,
        {
            "batch": {"airflow_run_id": "run"},
            "batch_id": 1,
            "ingestion": [],
            "reconciliations": [],
            "airflow_url": "",
        },
    )
    assert "Fleet daily load succeeded" in output


@pytest.mark.unit
@pytest.mark.parametrize("template", ["failure.txt.j2", "failure.html.j2"])
def test_failure_templates_render(template: str) -> None:
    """Failure templates give the safe operator recovery instruction."""
    output = render(
        template,
        {"batch_id": 1, "task_id": "silver_test", "errors": [], "airflow_url": ""},
    )
    assert "clear the failed task" in output
