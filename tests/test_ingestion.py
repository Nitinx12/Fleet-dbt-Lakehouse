"""Ingestion must only upsert against the contract primary key."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from scripts.ingestion import (
    DAG_ID,
    Source,
    _arguments,
    _ensure_bronze_primary_key,
    _selected_contracts,
)


def _source(target: str) -> Source:
    """Return the minimum source specification needed for a key check."""
    return Source(
        source_table_id=1,
        system="mysql",
        schema="fleet_registry",
        table="key_check",
        target=target,
        load_type="INCREMENTAL",
        cursor="updated_at",
        watermark_type="TIMESTAMP",
        pk=("contract_id",),
        columns=("contract_id", "other_id"),
    )


@pytest.mark.integration
def test_bronze_primary_key_must_match_contract() -> None:
    """Add a missing key and reject an incompatible existing key."""
    from utils.connection import get_postgres_engine

    try:
        engine = get_postgres_engine()
    except SQLAlchemyError:
        pytest.skip("Postgres not reachable")

    suffix = uuid4().hex[:12]
    matching = _source(f"audit_key_match_{suffix}")
    mismatched = _source(f"audit_key_mismatch_{suffix}")
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                text(
                    f"CREATE TABLE bronze.{matching.target} "
                    "(contract_id text NOT NULL, other_id text NOT NULL)"
                )
            )
            _ensure_bronze_primary_key(connection, matching)
            key_columns = (
                connection.execute(
                    text(
                        "SELECT a.attname "
                        "FROM pg_index i "
                        "JOIN pg_class c ON c.oid = i.indrelid "
                        "JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, position) ON TRUE "
                        "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum "
                        "WHERE i.indisprimary AND n.nspname = 'bronze' "
                        "AND c.relname = :table "
                        "ORDER BY k.position"
                    ),
                    {"table": matching.target},
                )
                .scalars()
                .all()
            )
            assert key_columns == ["contract_id"]

            connection.execute(
                text(
                    f"CREATE TABLE bronze.{mismatched.target} "
                    "(contract_id text NOT NULL, other_id text PRIMARY KEY)"
                )
            )
            with pytest.raises(ValueError, match="does not match contract key"):
                _ensure_bronze_primary_key(connection, mismatched)
        finally:
            transaction.rollback()


@pytest.mark.unit
def test_airflow_arguments_keep_the_manual_defaults() -> None:
    """Keep a no-argument ingestion run backward compatible."""
    args = _arguments([])

    assert args.source is None
    assert args.table is None
    assert args.batch_id is None
    assert args.airflow_dag_id == DAG_ID
    assert args.airflow_run_id is None


@pytest.mark.unit
def test_table_selection_limits_the_source_contract() -> None:
    """A per-table Airflow task loads only its requested contract."""
    contracts = [
        {"source": "mysql.fleet_registry.drivers"},
        {"source": "mongodb.fleet_operations.delivery_events"},
    ]

    selected = _selected_contracts(contracts, "mysql", "drivers")

    assert selected == [{"source": "mysql.fleet_registry.drivers"}]


@pytest.mark.unit
def test_table_selection_rejects_an_unknown_contract() -> None:
    """Reject a task that does not map to a declared source contract."""
    with pytest.raises(ValueError, match="no contract matches"):
        _selected_contracts([], "mongo", "missing")


@pytest.mark.unit
def test_table_flag_requires_a_source() -> None:
    """Prevent an ambiguous per-table command."""
    with pytest.raises(SystemExit):
        _arguments(["--table", "drivers"])
