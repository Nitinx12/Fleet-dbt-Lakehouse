"""Source readers for live, csv and fixture modes (ARCHITECTURE.md §7.3)."""

from __future__ import annotations

import csv
import gzip
import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ingestion.config import (
    CSV_DIR,
    FIXTURE_DIR,
    IngestError,
    SourceMode,
    mongo_uri,
    mysql_connection_url,
)
from ingestion.contracts import SourceContract

logger = logging.getLogger(__name__)


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def _resolve_data_file(base: Path, system: str, table: str) -> Path:
    """Find table.csv, table.csv.gz under base/<system>/ or base/."""
    candidates = [
        base / system / f"{table}.csv",
        base / system / f"{table}.csv.gz",
        base / f"{table}.csv",
        base / f"{table}.csv.gz",
        base / system / table / "part-0000.csv",
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise IngestError(f"no data file for {system}.{table} under {base}")


def _read_csv_rows(path: Path) -> Iterator[dict[str, Any]]:
    with _open_csv(path) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield dict(row)


def _apply_incremental_filter(
    rows: Iterator[dict[str, Any]],
    cursor: str,
    since: datetime,
) -> Iterator[dict[str, Any]]:
    """Keep rows whose cursor value is >= since (string compare is unsafe; parse dates)."""
    cutoff = since.date() if since.tzinfo is None else since.astimezone(UTC).date()
    kept = 0
    total = 0
    for row in rows:
        total += 1
        raw = row.get(cursor)
        if raw is None or raw == "":
            continue
        # Cursor columns are date or timestamp strings; compare as date.
        try:
            value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            # Date-only strings
            try:
                value = datetime.fromisoformat(str(raw))
            except ValueError:
                logger.warning("unparseable cursor %s=%r; keeping row", cursor, raw)
                yield row
                continue
        row_date = value.date() if value.tzinfo else value.date()
        if row_date >= cutoff:
            kept += 1
            yield row
    logger.debug("incremental filter on %s: kept %d/%d since %s", cursor, kept, total, cutoff)


def read_table(
    contract: SourceContract,
    mode: SourceMode,
    *,
    now: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield raw rows for one contract in the requested source mode."""
    now = now or datetime.now(UTC)
    if mode is SourceMode.CSV:
        path = _resolve_data_file(CSV_DIR, contract.system, contract.table)
        rows: Iterator[dict[str, Any]] = _read_csv_rows(path)
    elif mode is SourceMode.FIXTURE:
        path = _resolve_data_file(FIXTURE_DIR, contract.system, contract.table)
        rows = _read_csv_rows(path)
    elif mode is SourceMode.LIVE:
        rows = _read_live(contract)
    else:  # pragma: no cover - SourceMode is exhaustive
        raise IngestError(f"unknown source mode {mode!r}")

    if contract.is_incremental and contract.cursor:
        since = now - timedelta(days=contract.effective_lookback_days)
        rows = _apply_incremental_filter(rows, contract.cursor, since)
    return rows


def _read_live(contract: SourceContract) -> Iterator[dict[str, Any]]:
    if contract.system == "mysql":
        yield from _read_mysql(contract)
    elif contract.system == "mongo":
        yield from _read_mongo(contract)
    else:  # pragma: no cover
        raise IngestError(f"unknown system {contract.system!r}")


def _read_mysql(contract: SourceContract) -> Iterator[dict[str, Any]]:
    """Read one MySQL table via SQLAlchemy (read-only credentials)."""
    from sqlalchemy import create_engine, text

    url = mysql_connection_url()
    engine = create_engine(url)
    # Identifier comes from our own contract files, not user input.
    stmt = text(f"SELECT * FROM {contract.table}")  # noqa: S608 - trusted identifier
    with engine.connect() as conn:
        result = conn.execute(stmt)
        for row in result.mappings():
            yield dict(row)


def _read_mongo(contract: SourceContract) -> Iterator[dict[str, Any]]:
    """Read one MongoDB collection; every value stays a string (already strings)."""
    from pymongo import MongoClient

    uri = mongo_uri()
    client = MongoClient(uri)
    try:
        database = uri.rsplit("/", 1)[-1].split("?")[0] or "fleet_operations"
        collection = client[database][contract.table]
        for doc in collection.find({}, {"_id": 0}):
            yield dict(doc)
    finally:
        client.close()
