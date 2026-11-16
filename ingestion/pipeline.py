"""Assemble and run dlt pipelines from source contracts."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import dlt
from dlt.extract.incremental import Incremental

from ingestion.config import SourceMode, is_prod, source_mode
from ingestion.contracts import (
    SourceContract,
    dlt_columns,
    load_contracts,
    schema_contract_for,
)
from ingestion.readers import read_table

logger = logging.getLogger(__name__)


def build_resource(contract: SourceContract, mode: SourceMode, now: datetime | None = None) -> Any:
    """One dlt resource per contract: merge on PK, incremental cursor when declared."""
    now = now or datetime.now(UTC)
    columns = dlt_columns(contract)
    pk = list(contract.primary_key)
    resource_name = contract.name  # mysql__loads / mongo__delivery_events

    incremental: Incremental[Any] | None = None
    if contract.is_incremental and contract.cursor:
        since = now - timedelta(days=contract.effective_lookback_days)
        # lag keeps re-reading the lookback window past the stored last_value
        incremental = Incremental(
            cursor_path=contract.cursor,
            initial_value=since.isoformat(),
            primary_key=pk,
            lag=timedelta(days=contract.effective_lookback_days),
            on_cursor_value_missing="include",
        )

    def _generate() -> Iterator[dict[str, Any]]:
        yield from read_table(contract, mode, now=now)

    resource = dlt.resource(
        _generate,
        name=resource_name,
        write_disposition="merge",
        primary_key=pk,
        columns=columns,
        incremental=incremental,
        table_name=contract.landing_table.split(".", 1)[-1],  # raw.mysql__loads -> mysql__loads
    )
    return resource


def build_resources(
    contracts: dict[str, SourceContract],
    mode: SourceMode,
    *,
    now: datetime | None = None,
    systems: set[str] | None = None,
) -> list[Any]:
    """Resources for the selected systems (default: all contracts)."""
    resources = []
    for name in sorted(contracts):
        contract = contracts[name]
        if systems is not None and contract.system not in systems:
            continue
        resources.append(build_resource(contract, mode, now=now))
    return resources


def run_ingestion(
    *,
    mode: SourceMode | None = None,
    environment: str | None = None,
    systems: set[str] | None = None,
    destination: str | None = None,
    now: datetime | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the dlt pipeline for the configured source mode.

    dry_run builds resources and the schema_contract but does not execute a load.
    Returns a summary dict (resources, schema_contract, mode, rows when available).
    """
    mode = mode or source_mode()
    contracts = load_contracts()
    schema_contract = schema_contract_for(environment)
    resources = build_resources(contracts, mode, systems=systems, now=now)

    summary: dict[str, Any] = {
        "mode": str(mode),
        "environment": environment or ("production" if is_prod(environment) else "development"),
        "schema_contract": schema_contract,
        "resources": [r.name for r in resources],
        "contract_count": len(contracts),
    }

    if dry_run:
        logger.info("dry_run: %d resources, schema_contract=%s", len(resources), schema_contract)
        return summary

    dest = destination or "databricks"
    pipeline = dlt.pipeline(
        pipeline_name="fleet_raw",
        destination=dest,
        dataset_name="raw",
        dev_mode=not is_prod(environment),
    )
    # tables=evolve allows a brand-new source table; columns/data_type follow the env.
    info = pipeline.run(resources, schema_contract=schema_contract)
    summary["load_info"] = {
        "pipeline": pipeline.pipeline_name,
        "destination": str(pipeline.destination),
        "dataset": pipeline.dataset_name,
    }
    # rows written per resource when available
    try:
        metrics: dict[str, Any] = {}
        for key, value in (info.metrics or {}).items():  # type: ignore[attr-defined]
            metrics[str(key)] = value
        summary["metrics"] = metrics
    except Exception:  # noqa: BLE001 - metrics shape varies by dlt version
        logger.debug("load info metrics unavailable", exc_info=True)
    return summary
