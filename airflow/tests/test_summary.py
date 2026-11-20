"""Test bounded, redacted notification summary contexts."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import summary


@pytest.mark.unit
def test_summary_caps_long_name_lists() -> None:
    """Keep alert bodies bounded when many tests fail."""
    names = [{"check_name": f"test_{index}"} for index in range(23)]

    capped = summary._capped(names, "check_name")

    assert len(capped) == 21
    assert capped[-1] == "+3 more"


@pytest.mark.unit
def test_failure_summary_uses_aggregate_reader_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pass safe names and counts into the templates."""
    monkeypatch.setattr(
        summary.auidt_reader, "batch", lambda batch_id: {"status": "FAILED"}
    )
    monkeypatch.setattr(summary.auidt_reader, "task_runs", lambda batch_id: [])
    monkeypatch.setattr(summary.auidt_reader, "open_errors", lambda batch_id: [])
    monkeypatch.setattr(summary.auidt_reader, "failed_tests", lambda batch_id: [])

    context = summary.failure_context(2, "silver_test")

    assert context["batch_id"] == 2
    assert context["task_id"] == "silver_test"
