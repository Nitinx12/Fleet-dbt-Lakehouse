"""Unit tests for the Databricks mart publisher's safety boundaries."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from scripts import publish_marts


def test_publish_list_excludes_the_pii_driver_scorecard() -> None:
    """Keep the driver display-name mart inside Postgres."""
    assert "mart_driver_scorecard_monthly" not in publish_marts.PUBLISHABLE_MARTS
    assert len(publish_marts.PUBLISHABLE_MARTS) == 5


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            {
                "column_name": "amount",
                "data_type": "numeric",
                "numeric_precision": 18,
                "numeric_scale": 2,
            },
            "DECIMAL(18,2)",
        ),
        (
            {
                "column_name": "ratio",
                "data_type": "numeric",
                "numeric_precision": None,
                "numeric_scale": None,
            },
            "DECIMAL(38,18)",
        ),
        (
            {
                "column_name": "loaded_at",
                "data_type": "timestamp with time zone",
                "numeric_precision": None,
                "numeric_scale": None,
            },
            "TIMESTAMP",
        ),
        (
            {
                "column_name": "mart_key",
                "data_type": "character varying",
                "numeric_precision": None,
                "numeric_scale": None,
            },
            "STRING",
        ),
    ],
)
def test_databricks_type_preserves_mart_contract(
    row: dict[str, object], expected: str
) -> None:
    """Map supported Postgres mart types without using floating-point money."""
    assert publish_marts._databricks_type(row) == expected


def test_identifier_rejects_unsafe_sql_fragments() -> None:
    """Limit generated DDL to known snake_case identifiers."""
    with pytest.raises(publish_marts.PublishError, match="snake_case"):
        publish_marts._identifier("marts; drop schema marts", "schema")


def test_http_connection_errors_are_handled() -> None:
    """Close the publisher audit trail when the SQL endpoint is unreachable."""
    assert publish_marts.HTTPError in publish_marts._HANDLED_ERRORS


def test_sql_literal_escapes_and_types() -> None:
    """Render safe Databricks literals for every mart value type."""
    assert publish_marts._sql_literal(None) == "NULL"
    assert publish_marts._sql_literal(True) == "TRUE"
    assert publish_marts._sql_literal(False) == "FALSE"
    assert publish_marts._sql_literal(42) == "42"
    assert publish_marts._sql_literal(Decimal("12.50")) == "12.50"
    assert publish_marts._sql_literal(date(2026, 1, 2)) == "'2026-01-02'"
    assert publish_marts._sql_literal("O'Brien") == "'O''Brien'"
    stamp = publish_marts._sql_literal(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    assert stamp == "'2026-01-02 03:04:05.000000+00:00'"
    # bool is an int subclass: FALSE must not become 0
    assert publish_marts._sql_literal(False) != "0"


def test_insert_rows_emits_one_multi_row_literal_statement() -> None:
    """Batch inserts must not fall back to per-row executemany round-trips."""
    calls: list[str] = []

    class Cursor:
        def execute(self, statement: str, params: list | None = None) -> None:
            assert params is None, "literal batches bind no parameters"
            calls.append(statement)

    columns = [
        publish_marts.Column("mart_key", "character varying", "STRING"),
        publish_marts.Column("amount", "numeric", "DECIMAL(18,2)"),
    ]
    rows = [(f"k{i}", i) for i in range(3)]
    publish_marts._insert_rows(Cursor(), "mart_x__load", columns, rows)

    assert len(calls) == 1
    assert calls[0].count("VALUES") == 1
    assert calls[0].count("('k0', 0), ('k1', 1), ('k2', 2)") == 1


def test_insert_rows_splits_on_the_sql_char_budget() -> None:
    """Keep each literal statement under PUBLISH_MAX_SQL_CHARS."""
    calls: list[str] = []

    class Cursor:
        def execute(self, statement: str, params: list | None = None) -> None:
            calls.append(statement)

    columns = [publish_marts.Column("mart_key", "character varying", "STRING")]
    rows = [(f"key-{i}-{'x' * 40}",) for i in range(10)]
    original = publish_marts.config.PUBLISH_MAX_SQL_CHARS
    publish_marts.config.PUBLISH_MAX_SQL_CHARS = 120
    try:
        publish_marts._insert_rows(Cursor(), "mart_x__load", columns, rows)
    finally:
        publish_marts.config.PUBLISH_MAX_SQL_CHARS = original

    assert len(calls) > 1
    assert all(len(statement) < 200 for statement in calls)
    assert sum(statement.count("('key-") for statement in calls) == 10


def test_worker_count_only_splits_large_loads() -> None:
    """Small tables stay on one connection; large ones use PUBLISH_WORKERS."""
    original_rows = publish_marts.config.PUBLISH_WORKER_MIN_ROWS
    original_workers = publish_marts.config.PUBLISH_WORKERS
    publish_marts.config.PUBLISH_WORKER_MIN_ROWS = 100
    publish_marts.config.PUBLISH_WORKERS = 4
    try:
        assert publish_marts._worker_count(99) == 1
        assert publish_marts._worker_count(100) == 4
    finally:
        publish_marts.config.PUBLISH_WORKER_MIN_ROWS = original_rows
        publish_marts.config.PUBLISH_WORKERS = original_workers


class _Recorder:
    """Capture log.info calls as formatted strings."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def info(self, message: str, *args: object) -> None:
        self.lines.append(message % args if args else str(message))


def test_merge_rows_logs_progress_after_every_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long merge must show liveness instead of one silent gap."""
    statements: list[str] = []

    class Cursor:
        def execute(self, statement: str) -> None:
            statements.append(statement)

    def fake_source(
        *_args: object, **_kwargs: object
    ) -> Iterator[list[tuple[Any, ...]]]:
        yield [(f"k{i}", i) for i in range(3)]
        yield [(f"k{i + 3}", i + 3) for i in range(3)]

    recorder = _Recorder()
    monkeypatch.setattr(publish_marts, "_source_rows", fake_source)
    monkeypatch.setattr(publish_marts, "log", recorder)
    columns = [
        publish_marts.Column("mart_key", "character varying", "STRING"),
        publish_marts.Column("amount", "numeric", "DECIMAL(18,2)"),
    ]
    original = publish_marts.config.PUBLISH_MERGE_MAX_SQL_CHARS
    publish_marts.config.PUBLISH_MERGE_MAX_SQL_CHARS = 120
    try:
        sent = publish_marts._merge_rows(None, Cursor(), "mart_x", columns, None, None)
    finally:
        publish_marts.config.PUBLISH_MERGE_MAX_SQL_CHARS = original

    assert sent == 6
    assert len(statements) > 1
    assert len(recorder.lines) == len(statements)
    assert all(line.startswith("mart_x: merged ") for line in recorder.lines)


def test_load_rows_logs_progress_for_single_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The single-threaded first load must also report row progress."""
    total_rows = [(f"k{i}", i) for i in range(5)]

    def fake_source(
        *_args: object, **_kwargs: object
    ) -> Iterator[list[tuple[Any, ...]]]:
        for start in range(0, len(total_rows), 2):
            yield total_rows[start : start + 2]

    recorder = _Recorder()
    monkeypatch.setattr(publish_marts, "_source_rows", fake_source)
    monkeypatch.setattr(publish_marts, "log", recorder)
    columns = [
        publish_marts.Column("mart_key", "character varying", "STRING"),
        publish_marts.Column("amount", "numeric", "DECIMAL(18,2)"),
    ]

    class Cursor:
        def execute(self, statement: str, params: Any = None) -> None:
            pass

    loaded = publish_marts._load_rows(None, Cursor(), "mart_x", columns, None, 1)

    assert loaded == 5
    assert len(recorder.lines) == 3
    assert recorder.lines[0].startswith("mart_x: inserted 2 rows")
    assert recorder.lines[-1].startswith("mart_x: inserted 5 rows")
