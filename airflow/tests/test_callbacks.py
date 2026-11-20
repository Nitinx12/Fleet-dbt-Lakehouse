"""Test non-raising Airflow notification callbacks."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import callbacks


class _TaskInstance:
    task_id = "silver_test"

    def xcom_pull(self, **kwargs: object) -> int:
        return 9


@pytest.mark.unit
def test_task_failure_never_sends_email(monkeypatch: pytest.MonkeyPatch) -> None:
    """Task callbacks preserve the single DAG-level email policy."""
    notify = Mock()
    monkeypatch.setattr(callbacks, "notify", notify)

    callbacks.on_task_failure({"task_instance": _TaskInstance()})

    notify.assert_not_called()


@pytest.mark.unit
def test_dag_failure_finalizes_then_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failure handling closes the batch once and sends one report."""
    events: list[str] = []
    monkeypatch.setattr(
        callbacks, "finalize_batch", lambda *args: events.append("finalize")
    )
    monkeypatch.setattr(callbacks, "failure_context", lambda *args: {"batch_id": 9})
    monkeypatch.setattr(
        callbacks, "notify", lambda *args, **kwargs: events.append("notify")
    )

    callbacks.on_dag_failure({"ti": _TaskInstance(), "task_instance": _TaskInstance()})

    assert events == ["finalize", "notify"]
