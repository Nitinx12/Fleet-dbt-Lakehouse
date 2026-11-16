"""Load source contracts and project them onto dlt configuration."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ingestion.config import CONTRACTS_DIR, IngestError, is_prod, lookback_days

logger = logging.getLogger(__name__)

# dlt schema_contract per ARCHITECTURE.md §11.3: new tables may appear,
# unexpected columns and type changes are frozen out in production.
_PROD_SCHEMA_CONTRACT: dict[str, str] = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}
_DEV_SCHEMA_CONTRACT: dict[str, str] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "evolve",
}


@dataclass(frozen=True)
class ColumnSpec:
    """One column as declared in the source contract."""

    name: str
    type: str
    nullable: bool = True
    raw_type: str | None = None
    staged_type: str | None = None
    cast: str | None = None


@dataclass(frozen=True)
class ExtractionSpec:
    """How a table is extracted (full vs incremental + lookback)."""

    mode: str  # "full" | "incremental"
    cursor: str | None = None
    lookback_days: int | None = None


@dataclass(frozen=True)
class SourceContract:
    """One contracts/sources/*.yml file, parsed."""

    name: str  # file stem, e.g. mysql__loads
    source: str
    landing_table: str
    primary_key: tuple[str, ...]
    extraction: ExtractionSpec
    columns: tuple[ColumnSpec, ...]
    row_count_baseline: int
    environment_sensitive: bool = False

    @property
    def table(self) -> str:
        """Bare table/collection name without the system prefix."""
        return self.name.split("__", 1)[-1]

    @property
    def system(self) -> str:
        """mysql or mongo."""
        return self.name.split("__", 1)[0]

    @property
    def is_incremental(self) -> bool:
        return self.extraction.mode == "incremental"

    @property
    def cursor(self) -> str | None:
        return self.extraction.cursor

    @property
    def effective_lookback_days(self) -> int:
        if self.extraction.lookback_days is not None:
            return self.extraction.lookback_days
        return lookback_days()


def _parse_column(raw: dict[str, Any]) -> ColumnSpec:
    # Mongo contracts use raw_type/staged_type; MySQL uses a single type.
    col_type = raw.get("type") or raw.get("staged_type") or "string"
    return ColumnSpec(
        name=raw["name"],
        type=str(col_type),
        nullable=bool(raw.get("nullable", True)),
        raw_type=raw.get("raw_type"),
        staged_type=raw.get("staged_type"),
        cast=raw.get("cast"),
    )


def load_contract(path: Path) -> SourceContract:
    """Parse a single contract YAML file."""
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise IngestError(f"invalid contract YAML {path.name}: {exc}") from exc
    if not isinstance(doc, dict) or "primary_key" not in doc or "columns" not in doc:
        raise IngestError(f"contract {path.name} missing required keys")

    extraction_raw = doc.get("extraction") or {}
    extraction = ExtractionSpec(
        mode=str(extraction_raw.get("mode", "full")),
        cursor=extraction_raw.get("cursor"),
        lookback_days=extraction_raw.get("lookback_days"),
    )
    quality = doc.get("quality") or {}
    row_count = quality.get("row_count") or {}
    pk = doc["primary_key"]
    if isinstance(pk, str):
        pk = [pk]
    return SourceContract(
        name=path.stem,
        source=str(doc.get("source", "")),
        landing_table=str(doc.get("landing_table", "")),
        primary_key=tuple(pk),
        extraction=extraction,
        columns=tuple(_parse_column(c) for c in doc["columns"]),
        row_count_baseline=int(row_count.get("baseline", 0)),
    )


def load_contracts(directory: Path | None = None) -> dict[str, SourceContract]:
    """Load every contract under contracts/sources/ keyed by file stem."""
    directory = directory or CONTRACTS_DIR
    if not directory.is_dir():
        raise IngestError(f"contracts directory not found: {directory}")
    paths = sorted(directory.glob("*.yml"))
    if not paths:
        raise IngestError(f"no contract YAML files in {directory}")
    contracts = {p.stem: load_contract(p) for p in paths}
    logger.info("loaded %d source contracts from %s", len(contracts), directory)
    return contracts


def schema_contract_for(environment: str | None = None) -> dict[str, str]:
    """dlt schema_contract: freeze columns/types in production, evolve in dev."""
    if is_prod(environment):
        return dict(_PROD_SCHEMA_CONTRACT)
    return dict(_DEV_SCHEMA_CONTRACT)


def dlt_columns(contract: SourceContract) -> dict[str, Any]:
    """Map contract columns to dlt `columns` hints (names + types only)."""
    hints: dict[str, Any] = {}
    for col in contract.columns:
        # Raw landing keeps source types; Mongo raw is always string.
        type_name = "string" if col.raw_type else col.type
        hints[col.name] = {"data_type": type_name, "nullable": col.nullable}
    return hints


def contract_index(contracts: dict[str, SourceContract]) -> list[dict[str, Any]]:
    """Machine-readable summary used by contracts-check drift detection."""
    rows: list[dict[str, Any]] = []
    for name, c in sorted(contracts.items()):
        rows.append(
            {
                "name": name,
                "source": c.source,
                "landing_table": c.landing_table,
                "primary_key": list(c.primary_key),
                "extraction": {
                    "mode": c.extraction.mode,
                    "cursor": c.extraction.cursor,
                    "lookback_days": c.extraction.lookback_days,
                },
                "columns": [col.name for col in c.columns],
                "row_count_baseline": c.row_count_baseline,
            }
        )
    return rows
