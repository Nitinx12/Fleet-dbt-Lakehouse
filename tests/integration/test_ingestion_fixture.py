"""Ingestion writes fixture rows and contract keys into bronze."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts import ingestion
from scripts.ingestion import Source, _ensure_bronze_primary_key


def _source(
    target: str,
    *,
    load_type: str = "INCREMENTAL",
    cursor: str | None = "updated_at",
    watermark_type: str | None = "TIMESTAMP",
    columns: tuple[str, ...] = ("contract_id", "other_id"),
    pk: tuple[str, ...] = ("contract_id",),
) -> Source:
    """Return the minimum source specification for a bronze write."""
    return Source(
        source_table_id=1,
        system="mysql",
        schema="fleet_registry",
        table="key_check",
        target=target,
        load_type=load_type,
        cursor=cursor,
        watermark_type=watermark_type,
        pk=pk,
        columns=columns,
    )


def test_bronze_primary_key_must_match_contract(postgres_engine: Engine) -> None:
    """Add a missing key and reject an incompatible existing key."""
    suffix = uuid4().hex[:12]
    matching = _source(f"audit_key_match_{suffix}")
    mismatched = _source(f"audit_key_mismatch_{suffix}")

    with postgres_engine.connect() as connection:
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


def test_fixture_rows_land_in_bronze(postgres_engine: Engine) -> None:
    """Fixture-mode rows go through the same bronze write path as sources."""
    spec = _source(
        "fixture_drivers",
        load_type="FULL",
        cursor=None,
        watermark_type=None,
        columns=("driver_id", "display_name"),
        pk=("driver_id",),
    )
    rows = [
        {"driver_id": "d1", "display_name": "Ada"},
        {"driver_id": "d2", "display_name": "Lin"},
    ]
    types = {"driver_id": "text", "display_name": "text"}

    ingestion._ensure_bronze(postgres_engine, spec, types)

    assert ingestion._write_bronze(postgres_engine, spec, rows) == 2
    assert ingestion._count_bronze(postgres_engine, spec) == 2
    with postgres_engine.connect() as connection:
        driver_ids = connection.execute(
            text("SELECT driver_id FROM bronze.fixture_drivers ORDER BY driver_id")
        ).scalars()
    assert driver_ids.all() == ["d1", "d2"]
