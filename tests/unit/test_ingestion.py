"""Ingestion CLI selection must stay backward compatible and contract-bound."""

from __future__ import annotations

import pytest

from scripts.ingestion import DAG_ID, _arguments, _selected_contracts


def test_airflow_arguments_keep_the_manual_defaults() -> None:
    """Keep a no-argument ingestion run backward compatible."""
    args = _arguments([])

    assert args.source is None
    assert args.table is None
    assert args.batch_id is None
    assert args.airflow_dag_id == DAG_ID
    assert args.airflow_run_id is None


def test_table_selection_limits_the_source_contract() -> None:
    """A per-table Airflow task loads only its requested contract."""
    contracts = [
        {"source": "mysql.fleet_registry.drivers"},
        {"source": "mongodb.fleet_operations.delivery_events"},
    ]

    selected = _selected_contracts(contracts, "mysql", "drivers")

    assert selected == [{"source": "mysql.fleet_registry.drivers"}]


def test_table_selection_rejects_an_unknown_contract() -> None:
    """Reject a task that does not map to a declared source contract."""
    with pytest.raises(ValueError, match="no contract matches"):
        _selected_contracts([], "mongo", "missing")


def test_table_flag_requires_a_source() -> None:
    """Prevent an ambiguous per-table command."""
    with pytest.raises(SystemExit):
        _arguments(["--table", "drivers"])
