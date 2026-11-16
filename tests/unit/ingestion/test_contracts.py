"""Contract loading and dlt schema_contract projection."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ingestion.config import CONTRACTS_DIR, IngestError
from ingestion.contracts import (
    dlt_columns,
    load_contract,
    load_contracts,
    schema_contract_for,
)

pytestmark = pytest.mark.unit


def test_load_contracts_covers_twelve_sources() -> None:
    contracts = load_contracts()
    assert len(contracts) == 12
    assert "mysql__loads" in contracts
    assert "mongo__delivery_events" in contracts


def test_load_contract_parses_mysql_extracts() -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    loads = contracts["mysql__loads"]
    assert loads.system == "mysql"
    assert loads.table == "loads"
    assert loads.primary_key == ("load_id",)
    assert loads.is_incremental
    assert loads.cursor == "load_date"
    assert loads.effective_lookback_days == 14
    assert loads.row_count_baseline == 85410
    assert loads.landing_table == "raw.mysql__loads"


def test_load_contract_parses_mongo_raw_string_columns() -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    events = contracts["mongo__delivery_events"]
    assert events.system == "mongo"
    assert events.is_incremental
    assert events.cursor == "scheduled_datetime"
    # Raw landing keeps every Mongo value as string.
    hints = dlt_columns(events)
    assert all(h["data_type"] == "string" for h in hints.values())
    assert hints["event_id"]["nullable"] is False


def test_schema_contract_freezes_only_in_production() -> None:
    assert schema_contract_for("production") == {
        "tables": "evolve",
        "columns": "freeze",
        "data_type": "freeze",
    }
    assert schema_contract_for("development") == {
        "tables": "evolve",
        "columns": "evolve",
        "data_type": "evolve",
    }


def test_dlt_columns_map_types() -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    hints = dlt_columns(contracts["mysql__loads"])
    assert hints["revenue"]["data_type"] == "decimal(12,2)"
    assert hints["load_date"]["data_type"] == "date"
    assert hints["load_id"]["nullable"] is False


def test_load_contract_missing_keys_raises(tmp_path: Path) -> None:
    bad = tmp_path / "mysql__broken.yml"
    bad.write_text("source: mysql.fleet_registry.broken\n", encoding="utf-8")
    with pytest.raises(IngestError, match="missing required keys"):
        load_contract(bad)


def test_load_contract_invalid_yaml_raises(tmp_path: Path) -> None:
    bad = tmp_path / "mysql__broken.yml"
    bad.write_text("primary_key: [\n", encoding="utf-8")
    with pytest.raises(IngestError, match="invalid contract YAML"):
        load_contract(bad)


def test_load_contracts_missing_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(IngestError, match="contracts directory"):
        load_contracts(tmp_path / "nope")


def test_load_contracts_empty_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(IngestError, match="no contract YAML"):
        load_contracts(tmp_path)


def test_contract_yaml_round_trip_still_valid(tmp_path: Path) -> None:
    """Contract files on disk stay parseable YAML with the required keys."""
    for path in sorted(CONTRACTS_DIR.glob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert "primary_key" in doc, path.name
        assert "columns" in doc, path.name
        assert "extraction" in doc, path.name
