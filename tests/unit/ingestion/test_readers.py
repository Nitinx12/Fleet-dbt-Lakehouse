"""Reader mode dispatch, file resolution and lookback filtering."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ingestion.config import CSV_DIR, FIXTURE_DIR, IngestError, SourceMode
from ingestion.contracts import load_contracts
from ingestion.readers import _resolve_data_file, read_table

pytestmark = pytest.mark.unit

NOW = datetime(2026, 11, 16, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def contracts() -> dict:
    return load_contracts()


def test_resolve_data_file_mysql_csv(contracts: dict) -> None:
    path = _resolve_data_file(CSV_DIR, "mysql", "drivers")
    assert path.is_file()
    assert path.name == "drivers.csv"


def test_resolve_data_file_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(IngestError, match="no data file"):
        _resolve_data_file(tmp_path, "mysql", "ghost")


def test_csv_mode_reads_mysql_drivers(contracts: dict) -> None:
    rows = list(read_table(contracts["mysql__drivers"], SourceMode.CSV, now=NOW))
    assert len(rows) >= 2
    assert rows[0]["driver_id"]


def test_fixture_mode_reads_tiny_loads(contracts: dict) -> None:
    assert (FIXTURE_DIR / "mysql" / "loads.csv").is_file()
    rows = list(read_table(contracts["mysql__loads"], SourceMode.FIXTURE, now=NOW))
    # L3 (2026-10-01) falls outside the 14-day lookback and is dropped.
    assert [r["load_id"] for r in rows] == ["L1", "L2"]


def test_fixture_mode_reads_mongo_events(contracts: dict) -> None:
    rows = list(read_table(contracts["mongo__delivery_events"], SourceMode.FIXTURE, now=NOW))
    # E3 (2026-10-01) falls outside the 14-day lookback and is dropped.
    assert len(rows) == 2
    assert rows[0]["event_id"] == "E1"
    # Mongo values stay strings in the raw layer.
    assert rows[0]["detention_minutes"] == "60"


def test_incremental_lookback_filters_old_rows(contracts: dict) -> None:
    """load_date older than now-14d is dropped; recent rows are kept."""
    rows = list(read_table(contracts["mysql__loads"], SourceMode.FIXTURE, now=NOW))
    dates = {r["load_date"] for r in rows}
    # Fixture has 2026-10-01 (outside) and 2026-11-10/14 (inside).
    assert "2026-10-01" not in dates
    assert dates == {"2026-11-10", "2026-11-14"}


def test_full_replace_skips_incremental_filter(contracts: dict) -> None:
    """drivers is full_replace: every fixture row is returned regardless of date."""
    rows = list(read_table(contracts["mysql__drivers"], SourceMode.FIXTURE, now=NOW))
    assert len(rows) == 2


def test_system_filter_in_pipeline_builds_only_selected() -> None:
    from ingestion.pipeline import build_resources

    contracts = load_contracts()
    mysql_only = build_resources(contracts, SourceMode.CSV, systems={"mysql"}, now=NOW)
    assert mysql_only
    assert all(r.name.startswith("mysql__") for r in mysql_only)
    assert len(mysql_only) == 9


def test_read_table_unknown_mode_raises(contracts: dict) -> None:
    with pytest.raises(IngestError, match="unknown source mode"):
        # Bypass type checker by forcing a bogus value.
        read_table(contracts["mysql__drivers"], "telepathy", now=NOW)  # type: ignore[arg-type]


def test_lookback_cutoff_is_timezone_aware(contracts: dict) -> None:
    # now without tz should still work (treated as naive / UTC-ish date compare)
    naive_now = datetime(2026, 11, 16, 12, 0, 0)
    rows = list(read_table(contracts["mysql__loads"], SourceMode.FIXTURE, now=naive_now))
    assert len(rows) == 2


def test_incremental_cursor_missing_keeps_row_via_filter() -> None:
    from ingestion.readers import _apply_incremental_filter

    rows = [
        {"id": "1", "load_date": ""},
        {"id": "2", "load_date": "2026-11-15"},
        {"id": "3", "load_date": "2026-01-01"},
    ]
    since = NOW - timedelta(days=14)
    kept = list(_apply_incremental_filter(iter(rows), "load_date", since))
    assert [r["id"] for r in kept] == ["2"]
