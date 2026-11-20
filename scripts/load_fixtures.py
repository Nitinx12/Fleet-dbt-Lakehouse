"""Load contract sample rows into bronze for isolated CI builds."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.ingestion import (
    Source,
    _ensure_bronze,
    _registry_row,
    _write_bronze,
    read_contract,
)
from utils.connection import get_postgres_engine

ROOT = Path(__file__).resolve().parents[1]
SAMPLES_DIR = ROOT / "contracts" / "samples"
SOURCES_DIR = ROOT / "contracts" / "sources"
LOG = logging.getLogger(__name__)
_DECIMAL = re.compile(r"(?:decimal|numeric)\(\d+,\s*\d+\)")
_TYPES = {
    "string": "text",
    "str": "text",
    "int": "bigint",
    "integer": "bigint",
    "bigint": "bigint",
    "date": "date",
    "timestamp": "timestamptz",
    "bool": "boolean",
    "boolean": "boolean",
}


@dataclass
class Fixture:
    """One sample payload ready to land in bronze."""

    spec: Source
    types: dict[str, str]
    contract: dict[str, Any]
    rows: list[dict[str, Any]]


def _require_fixture_flag() -> None:
    """Refuse fixture loads unless the explicit guard is set."""
    if os.getenv("FIXTURE_LOAD") != "1":
        raise RuntimeError("FIXTURE_LOAD=1 is required before loading fixtures")


def _require_fixture_database() -> None:
    """Refuse fixture loads outside fleet_dev or throwaway databases."""
    dbname = os.getenv("PG_DBNAME", "")
    if dbname != "fleet_dev" and not dbname.startswith("fleet_it_"):
        raise RuntimeError(
            "fixture loads require PG_DBNAME=fleet_dev or a throwaway database"
        )


def _pg_type(source_type: str) -> str:
    """Map a contract type to a native Postgres bronze column type."""
    value = source_type.strip().lower()
    if _DECIMAL.fullmatch(value):
        return value
    return _TYPES.get(value, "text")


def _source_spec(contract: dict[str, Any]) -> Source:
    """Build an in-memory bronze source spec from a contract."""
    row = _registry_row(contract)
    return Source(
        source_table_id=0,
        system=str(row["system"]),
        schema=str(row["schema"]),
        table=str(row["table"]),
        target=str(row["target"]),
        load_type=str(row["load_type"]),
        cursor=row["cursor"],
        watermark_type=row["watermark_type"],
        pk=tuple(str(name) for name in row["pk"]),
        columns=tuple(str(name) for name in row["columns"]),
    )


def _bronze_types(contract: dict[str, Any]) -> dict[str, str]:
    """Return Postgres types for every column in a contract."""
    types: dict[str, str] = {}
    for column in contract["columns"]:
        name = str(column["name"])
        source_type = column.get("type") or column.get("raw_type") or "string"
        types[name] = _pg_type(str(source_type))
    return types


def _ensure_bronze_schema(pg: Engine) -> None:
    """Create the landing schema without touching any other layer."""
    with pg.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))


def _sample_rows(sample: dict[str, Any]) -> list[dict[str, Any]]:
    """Read raw sample rows from either MySQL or MongoDB payloads."""
    for key in ("sample_rows", "sample_rows_raw"):
        if key in sample:
            return sample[key]
    raise ValueError("sample payload has no sample_rows or sample_rows_raw")


def _read_fixture(path: Path) -> Fixture:
    """Parse and validate one sample payload against its source contract."""
    sample = json.loads(path.read_text(encoding="utf-8"))
    contract_path = SOURCES_DIR / str(sample["contract"])
    contract = read_contract(contract_path)
    spec = _source_spec(contract)
    rows = _sample_rows(sample)
    expected = set(spec.columns)
    for row in rows:
        if set(row) != expected:
            raise ValueError(
                f"{path.name}: sample columns differ from {contract_path.name}"
            )
    if not rows:
        raise ValueError(f"{path.name}: sample payload has no rows")
    return Fixture(spec, _bronze_types(contract), contract, rows)


def _references(contract: dict[str, Any]) -> list[tuple[str, str]]:
    """Return each foreign-key column and its referenced table key."""
    references: list[tuple[str, str]] = []
    for column in contract["columns"]:
        reference = column.get("references")
        if reference:
            parts = str(reference).split(".")
            references.append((str(column["name"]), f"{parts[-2]}.{parts[-1]}"))
    return references


def _expand_fixtures(fixtures: list[Fixture]) -> None:
    """Copy sample rows so every referenced fixture key exists."""
    by_table = {fixture.spec.table: fixture for fixture in fixtures}
    required: dict[str, dict[str, set[str]]] = {
        table: {fixture.spec.pk[0]: set()} for table, fixture in by_table.items()
    }

    def note(column: str, reference: str, row: dict[str, Any]) -> None:
        table, key = reference.split(".", 1)
        if table in by_table:
            required[table].setdefault(key, set()).add(str(row[column]))

    for fixture in fixtures:
        references = _references(fixture.contract)
        for row in fixture.rows:
            for column, reference in references:
                note(column, reference, row)

    for _ in range(len(fixtures) + 1):
        changed = False
        for table, fixture in by_table.items():
            key = fixture.spec.pk[0]
            template = fixture.rows[0]
            existing = {str(row[key]) for row in fixture.rows}
            references = _references(fixture.contract)
            for value in sorted(required[table].get(key, set()) - existing):
                row = dict(template)
                row[key] = value
                fixture.rows.append(row)
                existing.add(value)
                changed = True
                for column, reference in references:
                    note(column, reference, row)
        if not changed:
            return
    raise RuntimeError("fixture reference closure did not converge")


def _land_fixture(pg: Engine, fixture: Fixture) -> int:
    """Land one expanded fixture through the normal bronze write path."""
    _ensure_bronze(pg, fixture.spec, fixture.types)
    written = _write_bronze(pg, fixture.spec, fixture.rows)
    LOG.info("loaded %s rows into bronze.%s", written, fixture.spec.target)
    return written


def main() -> int:
    """Land every source sample into the configured Postgres database."""
    _require_fixture_flag()
    _require_fixture_database()
    pg = get_postgres_engine()
    try:
        _ensure_bronze_schema(pg)
        fixtures = [
            _read_fixture(path) for path in sorted(SAMPLES_DIR.glob("*.sample.json"))
        ]
        _expand_fixtures(fixtures)
        total = sum(_land_fixture(pg, fixture) for fixture in fixtures)
    finally:
        pg.dispose()
    LOG.info("loaded %s fixture rows", total)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
