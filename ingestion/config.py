"""Environment and path configuration for ingestion."""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_DIR = ROOT / "contracts" / "sources"
CSV_DIR = ROOT / "data"  # data/mysql/*.csv, data/mongo/*.csv
FIXTURE_DIR = ROOT / "tests" / "fixtures"


class SourceMode(StrEnum):
    """How readers reach the source systems (ARCHITECTURE.md §7.3)."""

    LIVE = "live"
    CSV = "csv"
    FIXTURE = "fixture"


class IngestError(RuntimeError):
    """Raised when ingestion cannot start (bad mode, missing files, no contracts)."""


def source_mode(raw: str | None = None) -> SourceMode:
    """Resolve SOURCE_MODE from the argument or the environment."""
    value = (raw if raw is not None else os.environ.get("SOURCE_MODE", "live")).strip().lower()
    try:
        return SourceMode(value)
    except ValueError as exc:
        raise IngestError(
            f"SOURCE_MODE must be one of {[m.value for m in SourceMode]}, got {value!r}"
        ) from exc


def is_prod(environment: str | None = None) -> bool:
    """True when running against production (freezes the schema contract)."""
    value = environment if environment is not None else os.environ.get("ENVIRONMENT", "development")
    return value.strip().lower() == "production"


def lookback_days(default: int = 14) -> int:
    """Lookback window for incremental extracts; overridable via LOOKBACK_DAYS."""
    raw = os.environ.get("LOOKBACK_DAYS")
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise IngestError(f"LOOKBACK_DAYS must be an integer, got {raw!r}") from exc
    if value < 0:
        raise IngestError(f"LOOKBACK_DAYS must be >= 0, got {value}")
    return value


def mysql_connection_url() -> str:
    """Build a SQLAlchemy URL for the MySQL source (read-only credentials expected)."""
    user = os.environ.get("MYSQL_USER", "")
    password = os.environ.get("MYSQL_PASSWORD", "")
    host = os.environ.get("MYSQL_HOST", "localhost")
    port = os.environ.get("MYSQL_PORT", "3306")
    database = os.environ.get("MYSQL_DATABASE", "fleet_registry")
    if not user:
        raise IngestError("MYSQL_USER is required for SOURCE_MODE=live")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def mongo_uri() -> str:
    """MongoDB connection URI for SOURCE_MODE=live."""
    uri = os.environ.get("MONGO_URI")
    if uri:
        return uri
    host = os.environ.get("MONGO_HOST", "localhost")
    port = os.environ.get("MONGO_PORT", "27017")
    database = os.environ.get("MONGO_DB_NAME", "fleet_operations")
    if not host:
        raise IngestError("MONGO_URI or MONGO_HOST is required for SOURCE_MODE=live")
    return f"mongodb://{host}:{port}/{database}"
