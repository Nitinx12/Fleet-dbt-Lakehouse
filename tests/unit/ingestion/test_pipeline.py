"""dlt resource construction: merge, PK, incremental cursor/lookback, schema_contract."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ingestion.config import SourceMode
from ingestion.contracts import load_contracts, schema_contract_for
from ingestion.pipeline import build_resource, build_resources, run_ingestion

pytestmark = pytest.mark.unit

NOW = datetime(2026, 11, 16, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def contracts() -> dict:
    return load_contracts()


def test_full_replace_resource_has_no_incremental(contracts: dict) -> None:
    resource = build_resource(contracts["mysql__drivers"], SourceMode.CSV, now=NOW)
    assert resource.name == "mysql__drivers"
    assert resource.write_disposition == "merge"
    assert resource.incremental is None
    assert resource.table_name == "mysql__drivers"
    assert resource.columns["driver_id"]["nullable"] is False


def test_incremental_resource_sets_cursor_and_lag(contracts: dict) -> None:
    resource = build_resource(contracts["mysql__loads"], SourceMode.CSV, now=NOW)
    assert resource.write_disposition == "merge"
    incremental = resource.incremental
    assert incremental is not None
    assert incremental.cursor_path == "load_date"
    assert incremental.primary_key == ["load_id"]
    assert incremental.lag == timedelta(days=14)
    # initial_value is now - lookback (first run seeds the window).
    expected = (NOW - timedelta(days=14)).isoformat()
    assert incremental.initial_value == expected


def test_mongo_incremental_uses_contract_cursor(contracts: dict) -> None:
    resource = build_resource(contracts["mongo__delivery_events"], SourceMode.CSV, now=NOW)
    incremental = resource.incremental
    assert incremental is not None
    assert incremental.cursor_path == "scheduled_datetime"
    assert incremental.primary_key == ["event_id"]
    assert incremental.lag == timedelta(days=14)
    # Raw Mongo columns are all strings.
    assert resource.columns["detention_minutes"]["data_type"] == "string"


def test_build_resources_all_twelve(contracts: dict) -> None:
    resources = build_resources(contracts, SourceMode.CSV, now=NOW)
    assert len(resources) == 12
    names = [r.name for r in resources]
    assert names == sorted(names)
    assert set(names) == set(contracts)


def test_dry_run_summary_uses_dev_schema_contract() -> None:
    summary = run_ingestion(mode=SourceMode.CSV, environment="development", dry_run=True)
    assert summary["schema_contract"] == schema_contract_for("development")
    assert summary["contract_count"] == 12
    assert summary["mode"] == "csv"
    assert len(summary["resources"]) == 12


def test_dry_run_summary_freezes_in_production() -> None:
    summary = run_ingestion(mode=SourceMode.CSV, environment="production", dry_run=True)
    assert summary["schema_contract"]["columns"] == "freeze"
    assert summary["schema_contract"]["data_type"] == "freeze"
    assert summary["schema_contract"]["tables"] == "evolve"


def test_dry_run_system_filter(contracts: dict) -> None:
    summary = run_ingestion(
        mode=SourceMode.CSV,
        systems={"mongo"},
        dry_run=True,
    )
    assert summary["resources"] == [
        "mongo__delivery_events",
        "mongo__maintenance_records",
        "mongo__safety_incidents",
    ]
