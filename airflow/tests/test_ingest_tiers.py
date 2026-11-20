"""Protect the Airflow ingestion tier map against contract drift."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags" / "fleet_dbt_daliy.py"


def _tier_tables() -> dict[str, tuple[tuple[str, ...], ...]]:
    """Load the tier map only when the Airflow Task SDK is installed."""
    if importlib.util.find_spec("airflow.sdk") is None:
        pytest.skip("Airflow Task SDK is installed in the Airflow image, not this venv")
    spec = importlib.util.spec_from_file_location("fleet_daily_dag", DAG_FILE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.INGEST_TIERS


@pytest.mark.unit
def test_every_contract_table_has_one_source_tier() -> None:
    """Require an explicit tier decision whenever a source contract is added."""
    tiers = _tier_tables()
    contract_index = json.loads(
        (ROOT.parent / "contracts" / "sources_index.json").read_text(encoding="utf-8")
    )
    expected = {
        (
            "mongo" if item["source"].startswith("mongodb.") else "mysql",
            item["file"].split("__", 1)[1][:-4],
        )
        for item in contract_index["contracts"]
    }
    actual = {
        (source, table)
        for source, source_tiers in tiers.items()
        for tier in source_tiers
        for table in tier
    }

    assert actual == expected
    assert sum(
        len(tier) for source_tiers in tiers.values() for tier in source_tiers
    ) == len(actual)


@pytest.mark.unit
def test_tiers_are_source_scoped_and_acyclic() -> None:
    """Keep the tier structure strictly forward-moving by source."""
    tiers = _tier_tables()

    assert tuple(tiers) == ("mysql", "mongo")
    assert all(tier for source_tiers in tiers.values() for tier in source_tiers)
    assert all(
        len({table for tier in source_tiers for table in tier})
        == sum(len(tier) for tier in source_tiers)
        for source_tiers in tiers.values()
    )
