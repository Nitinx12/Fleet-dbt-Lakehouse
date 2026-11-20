"""Test read-only audit queries and ingestion gating."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import auidt_reader


@pytest.mark.unit
def test_reader_queries_are_select_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Summary readers never mutate control or audit evidence."""
    statements: list[str] = []

    def rows(sql: str, params: dict[str, object]) -> list[dict[str, object]]:
        statements.append(sql)
        return []

    monkeypatch.setattr(auidt_reader, "_rows", rows)

    assert auidt_reader.batch(3) is None
    assert auidt_reader.task_runs(3) == []
    assert auidt_reader.failed_tests(3) == []
    assert all(
        statement.lstrip().upper().startswith("SELECT") for statement in statements
    )


@pytest.mark.unit
def test_ingestion_gate_reports_missing_task_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not release silver until every expected source succeeded."""
    monkeypatch.setattr(
        auidt_reader,
        "ingestion_rows",
        lambda batch_id: [{"task_id": "ingest_mysql__drivers", "status": "SUCCEEDED"}],
    )

    with pytest.raises(RuntimeError, match="ingest_mongo__delivery_events"):
        auidt_reader.assert_ingestion_complete(
            3, {"ingest_mysql__drivers", "ingest_mongo__delivery_events"}
        )
