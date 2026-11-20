"""Test idempotent control-batch finalisation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import finalize_audit


@pytest.mark.unit
def test_finalizer_only_calls_control_function(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave append-only audit tables untouched."""
    statements: list[str] = []

    class _Connection:
        def execute(self, statement: object, params: object) -> None:
            statements.append(str(statement))

    class _Engine:
        def begin(self) -> _Context:
            return _Context()

    class _Context:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(finalize_audit, "get_postgres_engine", _Engine)

    assert finalize_audit.finalize_batch(7, "SUCCEEDED") is True
    assert "control.finish_batch" in statements[0]
    assert "audit." not in statements[0].lower()
    assert "update" not in statements[0].lower()
    assert "delete" not in statements[0].lower()


@pytest.mark.unit
def test_start_batch_accepts_airflow_rerun_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleared task can reuse the same DAG run identity."""
    calls: list[dict[str, object]] = []

    class _Result:
        def scalar_one(self) -> int:
            return 12

    class _Connection:
        def execute(self, statement: object, params: dict[str, object]) -> _Result:
            calls.append(params)
            return _Result()

    class _Engine:
        def begin(self) -> _Context:
            return _Context()

    class _Context:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(finalize_audit, "get_postgres_engine", _Engine)

    assert finalize_audit.start_batch("fleet_daily_load", "run-1", None) == 12
    assert calls[0]["run_id"] == "run-1"
