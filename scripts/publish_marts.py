"""Incrementally publish approved Postgres marts to Databricks.

No helper tables are created. Per mart:
  * First run  -> create the Delta table and stream every row straight into it
                  (parallel appends). The table carries a `loading` property until
                  the row count reconciles, so an interrupted first load is
                  detected and redone on the next run.
  * Later runs -> read only rows with mart_loaded_at >= the Databricks watermark,
                  ordered by that timestamp, and MERGE them into the live table
                  with inline VALUES statements. On any failure or count mismatch
                  the table is RESTOREd to the Delta version captured before the run.

Marts are independent, so they are published in parallel. Every run logs
per-phase timings so slow steps are visible.
"""

from __future__ import annotations

import argparse
import math
import queue
import re
import sys
import threading
import time
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from databricks.sql.exc import Error as DatabricksError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib3.exceptions import HTTPError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils import audit
from utils import engine as config
from utils.connection import get_postgres_engine, open_databricks_connection
from utils.logger import get_logger

log = get_logger("publish_marts")

PIPELINE_NAME = "fleet_daily"
LOCAL_DAG_ID = "publish_marts_local"
TASK_ID = "publish_marts"
SOURCE_SCHEMA = "marts"
WATERMARK_COLUMN = "mart_loaded_at"
KEY_COLUMN = "mart_key"
STATUS_PROPERTY = "publish_marts.status"
# Marts published at the same time (each also uses PUBLISH_WORKERS for big first loads).
MART_WORKERS = config.PUBLISH_MART_WORKERS
# Helper tables left behind by earlier versions of this script; dropped if found.
_LEGACY_SUFFIXES = ("__new", "__load", "__prev")

# Driver scorecards carry driver display names and must never leave Postgres.
PUBLISHABLE_MARTS = (
    "mart_sales_daily",
    "mart_equipment_utilization_daily",
    "mart_delivery_performance_daily",
    "mart_customer_service_monthly",
    "mart_truck_cost_monthly",
)
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class PublishError(RuntimeError):
    """Raised when a mart cannot be safely published."""


# Every failure above plus any unexpected bug: one bad mart must still leave
# an audit row and must not block the independent ones. KeyboardInterrupt and
# friends are BaseException, so a user interrupt still stops the run.
_HANDLED_ERRORS = (
    DatabricksError,
    HTTPError,
    PublishError,
    SQLAlchemyError,
    OSError,
    ValueError,
    Exception,
)


@dataclass(frozen=True)
class Column:
    """One Postgres mart column and its Databricks representation."""

    name: str
    postgres_type: str
    databricks_type: str


@dataclass(frozen=True)
class Snapshot:
    """Consistent source counts taken in a single Postgres statement."""

    source_rows: int
    pending_rows: int
    null_watermarks: int
    high: Any | None


@dataclass(frozen=True)
class PublishResult:
    """Counts captured for one mart publication."""

    mart: str
    source_rows: int
    changed_rows: int
    target_rows: int


def _arguments() -> argparse.Namespace:
    """Parse optional Airflow batch context for this publication task."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--airflow-dag-id", default=LOCAL_DAG_ID)
    parser.add_argument("--airflow-run-id")
    return parser.parse_args()


# --------------------------------------------------------------------------- #
# Identifiers
# --------------------------------------------------------------------------- #
def _identifier(value: str, label: str) -> str:
    """Return a safely quoted Databricks SQL identifier."""
    if not _IDENTIFIER.fullmatch(value):
        raise PublishError(f"{label} must be a lowercase snake_case identifier")
    return f"`{value}`"


def _pg_identifier(value: str) -> str:
    """Return a safely quoted Postgres identifier."""
    return '"' + value.replace('"', '""') + '"'


def _schema_name() -> str:
    """Return the fully qualified Databricks schema identifier."""
    schema = config.DATABRICKS_SCHEMA
    if not schema:
        raise PublishError("DATABRICKS_SCHEMA is required to publish marts")
    parts = []
    if config.DATABRICKS_CATALOG:
        parts.append(_identifier(config.DATABRICKS_CATALOG, "DATABRICKS_CATALOG"))
    parts.append(_identifier(schema, "DATABRICKS_SCHEMA"))
    return ".".join(parts)


def _table_name(table: str) -> str:
    """Return a fully qualified Databricks table identifier."""
    return f"{_schema_name()}.{_identifier(table, 'table')}"


# --------------------------------------------------------------------------- #
# Source contract
# --------------------------------------------------------------------------- #
def _databricks_type(row: dict[str, Any]) -> str:
    """Map an approved Postgres type to its exact Databricks table type."""
    data_type = str(row["data_type"]).lower()
    if data_type in {"character varying", "character", "text", "uuid"}:
        return "STRING"
    if data_type == "boolean":
        return "BOOLEAN"
    if data_type == "smallint":
        return "SMALLINT"
    if data_type == "integer":
        return "INT"
    if data_type == "bigint":
        return "BIGINT"
    if data_type == "real":
        return "FLOAT"
    if data_type == "double precision":
        return "DOUBLE"
    if data_type == "date":
        return "DATE"
    if data_type.startswith("timestamp"):
        return "TIMESTAMP"
    if data_type == "numeric":
        # Aggregate typmods are absent.
        precision = int(row["numeric_precision"] or 38)
        scale = int(row["numeric_scale"] if row["numeric_scale"] is not None else 18)
        if not 0 <= scale <= precision <= 38:
            raise PublishError(
                f"{row['column_name']} has unsupported numeric({precision}, {scale})"
            )
        return f"DECIMAL({precision},{scale})"
    raise PublishError(
        f"{row['column_name']} has unsupported Postgres type {data_type}"
    )


def source_columns(pg: Engine, mart: str) -> list[Column]:
    """Return the ordered Postgres schema used as the destination contract."""
    sql = text(
        """
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with pg.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                sql, {"schema": SOURCE_SCHEMA, "table": mart}
            ).mappings()
        ]
    if not rows:
        raise PublishError(f"{SOURCE_SCHEMA}.{mart} does not exist")
    columns = [
        Column(
            name=str(row["column_name"]),
            postgres_type=str(row["data_type"]),
            databricks_type=_databricks_type(row),
        )
        for row in rows
    ]
    by_name = {column.name: column for column in columns}
    missing = {KEY_COLUMN, WATERMARK_COLUMN} - by_name.keys()
    if missing:
        raise PublishError(f"{SOURCE_SCHEMA}.{mart} is missing {sorted(missing)}")
    if not by_name[WATERMARK_COLUMN].postgres_type.startswith("timestamp"):
        raise PublishError(
            f"{SOURCE_SCHEMA}.{mart}.{WATERMARK_COLUMN} must be a timestamp"
        )
    return columns


def _watermark_is_aware(columns: Sequence[Column]) -> bool:
    """Return True when the source watermark is timestamptz."""
    return any(
        column.name == WATERMARK_COLUMN
        and column.postgres_type == "timestamp with time zone"
        for column in columns
    )


def _normalise_watermark(value: Any | None, aware: bool) -> Any | None:
    """Match the Databricks watermark to the Postgres column's timezone semantics.

    Values are written to Databricks as explicit UTC instants, so a naive value
    read back is treated as UTC.
    """
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(UTC)
    return value if aware else value.replace(tzinfo=None)


def _snapshot(pg: Engine, mart: str, watermark: Any | None) -> Snapshot:
    """Take every source count and the upper watermark in ONE statement.

    A single statement is one MVCC snapshot, so concurrent upstream writes can
    never make the later reconciliation disagree with what was loaded.
    """
    wm = _pg_identifier(WATERMARK_COLUMN)
    pending = (
        f"count({wm})"
        if watermark is None
        else f"count(*) FILTER (WHERE {wm} >= :watermark)"
    )
    sql = text(
        f"SELECT count(*) FILTER (WHERE {wm} IS NULL), count({wm}), {pending}, max({wm}) "
        f"FROM {SOURCE_SCHEMA}.{mart}"
    )
    params = {} if watermark is None else {"watermark": watermark}
    with pg.connect() as connection:
        nulls, total, pending_rows, high = connection.execute(sql, params).one()
    snap = Snapshot(int(total), int(pending_rows), int(nulls), high)
    if snap.null_watermarks:
        raise PublishError(
            f"{SOURCE_SCHEMA}.{mart} has {snap.null_watermarks} rows with NULL "
            f"{WATERMARK_COLUMN}"
        )
    return snap


def _source_rows(
    pg: Engine,
    mart: str,
    columns: Sequence[Column],
    watermark: Any | None,
    high: Any | None,
    ordered: bool,
) -> Iterator[list[tuple[Any, ...]]]:
    """Stream pending Postgres rows in batches from one server-side cursor.

    `ordered` sorts by watermark then key. Incremental merges need it: if a run
    dies halfway, the committed watermark only ever moves forward, so the next
    run re-reads everything that was not applied.
    """
    names = ", ".join(_pg_identifier(column.name) for column in columns)
    wm = _pg_identifier(WATERMARK_COLUMN)
    conditions = [f"{wm} <= :high"]
    params: dict[str, Any] = {"high": high}
    if watermark is not None:
        # >= makes timestamp ties safe to retry; MERGE keeps it idempotent.
        conditions.append(f"{wm} >= :watermark")
        params["watermark"] = watermark
    order = f" ORDER BY {wm}, {_pg_identifier(KEY_COLUMN)}" if ordered else ""
    sql = text(
        f"SELECT {names} FROM {SOURCE_SCHEMA}.{mart} "
        f"WHERE {' AND '.join(conditions)}{order}"
    )
    with pg.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(sql, params)
        while rows := result.fetchmany(config.PUBLISH_FETCH_SIZE):
            yield [tuple(row) for row in rows]


# --------------------------------------------------------------------------- #
# Databricks target
# --------------------------------------------------------------------------- #
def _normalise_type(value: Any) -> str:
    """Normalise Databricks DESCRIBE output for a strict contract comparison."""
    return re.sub(r"\s+", "", str(value).upper()).replace("VARCHAR", "STRING")


def _inspect_target(cursor: Any, mart: str, columns: Sequence[Column]) -> bool:
    """Validate the existing table against the source contract in ONE statement.

    Raises on schema drift. Returns True when an earlier first load never
    finished (the `loading` property is still set), so it must be redone.
    """
    cursor.execute(f"DESCRIBE TABLE EXTENDED {_table_name(mart)}")
    actual: dict[str, str] = {}
    in_columns = True
    interrupted = False
    for row in cursor.fetchall():
        name = str(row[0]).strip() if row and row[0] is not None else ""
        if in_columns:
            if not name or name.startswith("#"):
                in_columns = False
                continue
            actual[name] = _normalise_type(row[1])
        elif name == "Table Properties":
            flattened = re.sub(r"\s+", "", str(row[1]))
            interrupted = f"{STATUS_PROPERTY}=loading" in flattened
    expected = {
        column.name: _normalise_type(column.databricks_type) for column in columns
    }
    if actual != expected:
        raise PublishError(f"Databricks schema drift detected for {mart}")
    return interrupted


def _count(cursor: Any, table: str) -> int:
    """Return a Databricks table row count."""
    cursor.execute(f"SELECT count(*) FROM {_table_name(table)}")
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _table_version(cursor: Any, table: str) -> int:
    """Return the current Delta version, used as the rollback point."""
    cursor.execute(f"DESCRIBE HISTORY {_table_name(table)} LIMIT 1")
    row = cursor.fetchone()
    if not row:
        raise PublishError(f"no Delta history for {table}")
    return int(row[0])


def _rollback(cursor: Any, table: str, version: int) -> None:
    """Restore a Delta table to a captured version; never masks the real error."""
    try:
        cursor.execute(
            f"RESTORE TABLE {_table_name(table)} TO VERSION AS OF {int(version)}"
        )
        log.warning("%s: restored to Delta version %s", table, version)
    except Exception:
        log.exception("%s: could not restore to Delta version %s", table, version)


def _drop(cursor: Any, table: str) -> None:
    """Drop a table; never raises so it is safe in cleanup paths."""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {_table_name(table)}")
    except Exception:
        log.exception("could not drop %s", table)


def _prepare_schema() -> set[str]:
    """Create the schema once, drop legacy helper tables, return existing marts."""
    connection = open_databricks_connection()
    try:
        with connection.cursor() as cursor:
            schema = _schema_name()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cursor.execute(f"SHOW TABLES IN {schema}")
            names: set[str] = set()
            for row in cursor.fetchall():
                names.update(str(value) for value in tuple(row))
            for mart in PUBLISHABLE_MARTS:
                for suffix in _LEGACY_SUFFIXES:
                    if f"{mart}{suffix}" in names:
                        log.info("dropping legacy helper table %s%s", mart, suffix)
                        _drop(cursor, f"{mart}{suffix}")
            return {mart for mart in PUBLISHABLE_MARTS if mart in names}
    finally:
        connection.close()


# --------------------------------------------------------------------------- #
# Row rendering and statements
# --------------------------------------------------------------------------- #
def _render_str(value: str) -> str:
    # Databricks treats backslash as an escape inside string literals.
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _render_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return f"'{value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00'"


def _render_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise PublishError("non-finite Decimal cannot be published")
    return format(value, "f")


def _render_float(value: float) -> str:
    if math.isfinite(value):
        return repr(value)
    if math.isnan(value):
        return "CAST('NaN' AS DOUBLE)"
    return "CAST('Infinity' AS DOUBLE)" if value > 0 else "CAST('-Infinity' AS DOUBLE)"


_RENDERERS = {
    bool: lambda value: "TRUE" if value else "FALSE",
    int: str,
    float: _render_float,
    Decimal: _render_decimal,
    datetime: _render_datetime,
    date: lambda value: f"'{value.isoformat()}'",
    str: _render_str,
}


def _sql_literal(value: Any) -> str:
    """Render one Python value as a Databricks SQL literal (always escaped)."""
    if value is None:
        return "NULL"
    render = _RENDERERS.get(type(value))
    if render is None:
        raise PublishError(f"unsupported value type {type(value).__name__}")
    return render(value)


def _values_chunks(
    rows: Iterator[tuple[Any, ...]] | Sequence[tuple[Any, ...]],
    overhead: int,
    max_chars: int | None = None,
) -> Iterator[tuple[str, int]]:
    """Yield `((...), (...), row_count)` chunks, each full up to `max_chars`.

    `max_chars` defaults to PUBLISH_MAX_SQL_CHARS. `overhead` is the length of the
    statement text wrapped around the values. The connector's executemany issues
    one request per row and bound parameters cap at 10k, so literals keep a large
    load to a few statements. The row count lets callers log what was really sent.
    """
    cap = config.PUBLISH_MAX_SQL_CHARS if max_chars is None else max_chars
    limit = max(cap - overhead, 1)
    batch: list[str] = []
    chars = 0
    for row in rows:
        rendered = "(" + ", ".join(map(_sql_literal, row)) + ")"
        if batch and chars + len(rendered) + 2 > limit:
            yield ", ".join(batch), len(batch)
            batch = []
            chars = 0
        batch.append(rendered)
        chars += len(rendered) + 2
    if batch:
        yield ", ".join(batch), len(batch)


def _insert_rows(
    cursor: Any, table: str, columns: Sequence[Column], rows: Sequence[tuple[Any, ...]]
) -> None:
    """Append rows with as few INSERT ... VALUES statements as the size limit allows."""
    if not rows:
        return
    names = ", ".join(_identifier(column.name, "column") for column in columns)
    prefix = f"INSERT INTO {_table_name(table)} ({names}) VALUES "
    for values, _ in _values_chunks(rows, len(prefix)):
        cursor.execute(prefix + values)


def _merge_statement_parts(mart: str, columns: Sequence[Column]) -> tuple[str, str]:
    """Return the text before and after the VALUES list of an inline-source MERGE."""
    quoted = {column.name: _identifier(column.name, "column") for column in columns}
    names = ", ".join(quoted.values())
    casts = ", ".join(
        f"CAST({quoted[column.name]} AS {column.databricks_type}) AS {quoted[column.name]}"
        for column in columns
    )
    assignments = ", ".join(
        f"target.{quoted[column.name]} = source.{quoted[column.name]}"
        for column in columns
        if column.name != KEY_COLUMN
    )
    values = ", ".join(f"source.{name}" for name in quoted.values())
    key = quoted[KEY_COLUMN]
    prefix = (
        f"MERGE INTO {_table_name(mart)} AS target USING (SELECT {casts} FROM VALUES "
    )
    suffix = (
        f" AS v({names})) AS source ON target.{key} = source.{key} "
        f"WHEN MATCHED THEN UPDATE SET {assignments} "
        f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
    )
    return prefix, suffix


def _merge_rows(
    pg: Engine,
    cursor: Any,
    mart: str,
    columns: Sequence[Column],
    watermark: Any | None,
    high: Any | None,
) -> int:
    """MERGE pending rows into the live table, oldest watermark first.

    Statements are packed up to PUBLISH_MERGE_MAX_SQL_CHARS: each MERGE has a
    large fixed cost, so fewer, fuller statements are much faster than many
    small ones.
    """
    prefix, suffix = _merge_statement_parts(mart, columns)
    batches = _source_rows(pg, mart, columns, watermark, high, ordered=True)
    merged = 0
    rows = (row for batch in batches for row in batch)

    try:
        started = time.perf_counter()
        chunks = _values_chunks(
            rows,
            len(prefix) + len(suffix),
            max_chars=config.PUBLISH_MERGE_MAX_SQL_CHARS,
        )
        for statements, (values, count) in enumerate(chunks, start=1):
            statement_started = time.perf_counter()
            cursor.execute(prefix + values + suffix)
            merged += count
            # A MERGE can run for minutes; log after each one so a long run
            # never looks stuck. `merged` counts rows actually executed.
            log.info(
                "%s: merged %s rows in %s statements (%.1fs; last: %s rows, %.1fs)",
                mart,
                merged,
                statements,
                time.perf_counter() - started,
                count,
                time.perf_counter() - statement_started,
            )
    finally:
        batches.close()
    return merged


def _worker_count(rows_to_load: int) -> int:
    """Split large loads across PUBLISH_WORKERS; small ones stay single-threaded."""
    if rows_to_load < config.PUBLISH_WORKER_MIN_ROWS:
        return 1
    return max(1, config.PUBLISH_WORKERS)


def _load_rows(
    pg: Engine,
    cursor: Any,
    mart: str,
    columns: Sequence[Column],
    high: Any | None,
    workers: int,
) -> int:
    """Append every source row to the new table (first load only).

    One Postgres scan feeds a bounded queue; each writer thread owns its own
    Databricks connection (opened lazily). Concurrent appends to a Delta table
    never conflict. The first writer error stops the producer and is re-raised.
    """
    batches = _source_rows(pg, mart, columns, None, high, ordered=False)
    if workers <= 1:
        total = 0
        started = time.perf_counter()
        try:
            for rows in batches:
                _insert_rows(cursor, mart, columns, rows)
                total += len(rows)
                log.info(
                    "%s: inserted %s rows (%.1fs)",
                    mart,
                    total,
                    time.perf_counter() - started,
                )
        finally:
            batches.close()
        return total

    work: queue.Queue[list[tuple[Any, ...]] | None] = queue.Queue(maxsize=workers * 2)
    failed = threading.Event()
    errors: list[BaseException] = []
    counts = [0] * workers

    def consume(index: int) -> None:
        connection = None
        writer = None
        try:
            while (rows := work.get()) is not None:
                if failed.is_set():
                    continue  # keep draining so the producer never blocks
                try:
                    if writer is None:
                        connection = open_databricks_connection()
                        writer = connection.cursor()
                    _insert_rows(writer, mart, columns, rows)
                    counts[index] += len(rows)
                except BaseException as error:  # noqa: BLE001 - re-raised by caller
                    errors.append(error)
                    failed.set()
        finally:
            if connection is not None:
                connection.close()

    log.info("%s: loading with %s workers", mart, workers)
    threads = [
        threading.Thread(target=consume, args=(index,), name=f"publish-{mart}-{index}")
        for index in range(workers)
    ]
    for thread in threads:
        thread.start()
    queued = 0
    try:
        for rows in batches:
            if failed.is_set():
                break
            work.put(rows)
            queued += len(rows)
            log.info("%s: queued %s rows for the load workers", mart, queued)
    except BaseException:
        failed.set()
        raise
    finally:
        # Release the writers first so a failure while closing the source
        # cursor can never leave them blocked on the queue.
        for _ in threads:
            work.put(None)
        for thread in threads:
            thread.join()
        batches.close()
    if errors:
        raise errors[0]
    log.info("%s: worker row counts %s", mart, counts)
    return sum(counts)


# --------------------------------------------------------------------------- #
# Publication
# --------------------------------------------------------------------------- #
def _incremental(
    pg: Engine, cursor: Any, mart: str, columns: Sequence[Column]
) -> tuple[Snapshot, int, int]:
    """MERGE changed rows into the existing table; returns (snapshot, changed, target)."""
    cursor.execute(
        f"SELECT max({_identifier(WATERMARK_COLUMN, 'watermark')}), count(*) "
        f"FROM {_table_name(mart)}"
    )
    row = cursor.fetchone()
    watermark = _normalise_watermark(
        row[0] if row else None, _watermark_is_aware(columns)
    )
    target_before = int(row[1]) if row else 0

    snap = _snapshot(pg, mart, watermark)
    log.info(
        "%s: %s source rows, %s pending (watermark=%s)",
        mart,
        snap.source_rows,
        snap.pending_rows,
        watermark,
    )

    version: int | None = None
    changed = 0
    try:
        if snap.pending_rows:
            version = _table_version(cursor, mart)
            started = time.perf_counter()
            changed = _merge_rows(pg, cursor, mart, columns, watermark, snap.high)
            log.info(
                "%s: merged %s rows in %.1fs",
                mart,
                changed,
                time.perf_counter() - started,
            )
            if changed != snap.pending_rows:
                raise PublishError(
                    f"{mart} merged {changed} rows, expected {snap.pending_rows}"
                )
            target_rows = _count(cursor, mart)
        else:
            target_rows = target_before
        if target_rows != snap.source_rows:
            raise PublishError(
                f"{mart} row count mismatch: source={snap.source_rows}, "
                f"target={target_rows}"
            )
    except BaseException:
        if version is not None:
            _rollback(cursor, mart, version)
        raise
    return snap, changed, target_rows


def _initial_load(
    pg: Engine, cursor: Any, mart: str, columns: Sequence[Column]
) -> tuple[Snapshot, int, int]:
    """Create the table and load every row; returns (snapshot, changed, target)."""
    snap = _snapshot(pg, mart, None)
    log.info("%s: first load of %s rows", mart, snap.source_rows)
    definition = ", ".join(
        f"{_identifier(column.name, 'column')} {column.databricks_type}"
        for column in columns
    )
    cursor.execute(
        f"CREATE OR REPLACE TABLE {_table_name(mart)} ({definition}) "
        f"TBLPROPERTIES ('{STATUS_PROPERTY}' = 'loading')"
    )
    try:
        started = time.perf_counter()
        changed = (
            _load_rows(
                pg, cursor, mart, columns, snap.high, _worker_count(snap.pending_rows)
            )
            if snap.pending_rows
            else 0
        )
        log.info(
            "%s: loaded %s rows in %.1fs", mart, changed, time.perf_counter() - started
        )
        target_rows = _count(cursor, mart)
        if target_rows != snap.source_rows:
            raise PublishError(
                f"{mart} row count mismatch: source={snap.source_rows}, "
                f"target={target_rows}"
            )
        cursor.execute(
            f"ALTER TABLE {_table_name(mart)} "
            f"SET TBLPROPERTIES ('{STATUS_PROPERTY}' = 'ready')"
        )
    except BaseException:
        _drop(cursor, mart)
        raise
    return snap, changed, target_rows


def publish_one(pg: Engine, mart: str, exists: bool) -> PublishResult:
    """Load, reconcile and publish one mart; safe to run in its own thread."""
    started = time.perf_counter()
    columns = source_columns(pg, mart)
    connection = open_databricks_connection()
    try:
        with connection.cursor() as cursor:
            interrupted = _inspect_target(cursor, mart, columns) if exists else False
            if interrupted:
                log.warning("%s: previous first load never finished; reloading", mart)
            if exists and not interrupted:
                snap, changed, target_rows = _incremental(pg, cursor, mart, columns)
            else:
                snap, changed, target_rows = _initial_load(pg, cursor, mart, columns)
    finally:
        connection.close()
    log.info(
        "%s: published %s rows (%s changed) in %.1fs",
        mart,
        target_rows,
        changed,
        time.perf_counter() - started,
    )
    return PublishResult(mart, snap.source_rows, changed, target_rows)


# --------------------------------------------------------------------------- #
# Batch control and audit
# --------------------------------------------------------------------------- #
def _start_batch(pg: Engine, dag_id: str, run_id: str) -> int:
    """Open a standalone publish batch through the standard control function."""
    with pg.begin() as connection:
        return int(
            connection.execute(
                text(
                    "SELECT control.start_batch(:pipeline, :dag_id, :run_id, NULL, "
                    "'publish_marts')"
                ),
                {"pipeline": PIPELINE_NAME, "dag_id": dag_id, "run_id": run_id},
            ).scalar_one()
        )


def _resume_batch(pg: Engine, dag_id: str) -> tuple[int, str] | None:
    """Return the one active local publisher batch, if a retry left it open."""
    with pg.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT batch_id, airflow_run_id
                FROM control.batch
                WHERE status = 'RUNNING'
                  AND airflow_dag_id = :dag_id
                  AND triggered_by = 'publish_marts'
                ORDER BY started_at DESC
                """
            ),
            {"dag_id": dag_id},
        ).all()
    if len(rows) != 1:
        return None
    return int(rows[0][0]), str(rows[0][1])


def _finish_batch(
    pg: Engine, batch_id: int, status: str, message: str | None = None
) -> None:
    """Close only the standalone batch that this script opened."""
    with pg.begin() as connection:
        connection.execute(
            text("SELECT control.finish_batch(:batch_id, :status, :message)"),
            {"batch_id": batch_id, "status": status, "message": message},
        )


def _reconcile(pg: Engine, batch_id: int, result: PublishResult) -> None:
    """Append the successful source-to-Databricks row-count comparison."""
    audit.log_reconciliation(
        batch_id,
        [
            {
                "source_system": "postgres",
                "source_object": f"{SOURCE_SCHEMA}.{result.mart}",
                "target_layer": "gold",
                "target_object": f"{config.DATABRICKS_SCHEMA}.{result.mart}",
                "source_row_count": result.source_rows,
                "target_row_count": result.target_rows,
                "status": "MATCH",
                "message": f"incremental_rows={result.changed_rows}",
            }
        ],
        target=pg,
    )


def main() -> int:
    """Publish approved marts and write all task, reconciliation and error audit rows."""
    args = _arguments()
    pg = get_postgres_engine()
    run_id = (
        args.airflow_run_id or f"manual_publish__{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    )
    owns_batch = args.batch_id is None
    batch_id: int | None = args.batch_id
    processed = 0
    final_status = "FAILED"
    final_message = "publish did not complete"
    exit_code = 1
    try:
        if batch_id is None:
            resumed = _resume_batch(pg, args.airflow_dag_id)
            if resumed:
                batch_id, run_id = resumed
                log.info("resuming local publisher batch %s", batch_id)
            else:
                batch_id = _start_batch(pg, args.airflow_dag_id, run_id)
        try:
            with audit.task_run(
                batch_id,
                args.airflow_dag_id,
                run_id,
                TASK_ID,
                layer="gold",
                target=pg,
            ) as task:
                started = time.perf_counter()
                existing = _prepare_schema()
                failures: list[str] = []
                workers = max(1, min(MART_WORKERS, len(PUBLISHABLE_MARTS)))
                with ThreadPoolExecutor(
                    max_workers=workers, thread_name_prefix="mart"
                ) as pool:
                    futures = {
                        pool.submit(publish_one, pg, mart, mart in existing): mart
                        for mart in PUBLISHABLE_MARTS
                    }
                    # Audit rows are written here, on the main thread, as each mart
                    # finishes. One bad mart must not block the independent ones.
                    for future in as_completed(futures):
                        mart = futures[future]
                        try:
                            result = future.result()
                        except _HANDLED_ERRORS as error:
                            log.error(
                                "%s: publish failed (%s)", mart, type(error).__name__
                            )
                            audit.log_error(
                                component="OTHER",
                                layer="gold",
                                batch_id=batch_id,
                                airflow_dag_id=args.airflow_dag_id,
                                task_id=TASK_ID,
                                error_code="MART_PUBLISH_FAILED",
                                error_message=audit.error_summary(
                                    error, f"publication of {mart}"
                                ),
                                target=pg,
                            )
                            failures.append(mart)
                            continue
                        _reconcile(pg, batch_id, result)
                        processed += result.changed_rows
                task.rows_processed = processed
                log.info("publish finished in %.1fs", time.perf_counter() - started)
                if failures:
                    raise PublishError(
                        f"{len(failures)} of {len(PUBLISHABLE_MARTS)} marts failed: "
                        f"{', '.join(sorted(failures))}"
                    )
            final_status = "SUCCEEDED"
            final_message = None
            exit_code = 0
            log.info(
                "published %s incremental rows across %s marts",
                processed,
                len(PUBLISHABLE_MARTS),
            )
        except _HANDLED_ERRORS as error:
            log.error("mart publish failed (%s)", type(error).__name__)
            final_message = audit.error_summary(error, "mart publication")
            audit.log_error(
                component="OTHER",
                layer="gold",
                batch_id=batch_id,
                airflow_dag_id=args.airflow_dag_id,
                task_id=TASK_ID,
                error_code="MART_PUBLISH_FAILED",
                error_message=final_message,
                target=pg,
            )
        return exit_code
    finally:
        if owns_batch and batch_id is not None:
            try:
                _finish_batch(pg, batch_id, final_status, final_message)
            except Exception:
                log.exception("could not close batch %s", batch_id)
        pg.dispose()


if __name__ == "__main__":
    sys.exit(main())
