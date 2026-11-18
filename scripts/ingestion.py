"""Extract MySQL and MongoDB sources into bronze, with watermarks and audit rows."""

from __future__ import annotations

import logging
import re
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pymongo.errors import PyMongoError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # direct script run

from utils import audit
from utils.connection import (
    get_databricks_connection,
    get_mongo_db,
    get_mysql_engine,
    get_postgres_engine,
)
from utils.logger import get_logger

log = get_logger("ingestion", console_level=logging.INFO)

CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "sources"
PIPELINE_NAME = "fleet_daily"
DAG_ID = "fleet_daily_load"
CHUNK_SIZE = 5000
_CONTRACT_KEYS = ("source", "landing_table", "primary_key", "extraction", "columns")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MYSQL_TO_PG = {
    "varchar": "text",
    "char": "text",
    "text": "text",
    "mediumtext": "text",
    "longtext": "text",
    "tinytext": "text",
    "enum": "text",
    "set": "text",
    "bigint": "bigint",
    "int": "integer",
    "integer": "integer",
    "mediumint": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "double": "double precision",
    "float": "double precision",
    "decimal": "numeric",
    "numeric": "numeric",
    "date": "date",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "time": "text",
    "year": "text",
    "json": "text",
}


@dataclass(frozen=True)
class Source:
    """One registered source object: where it lives and how it loads."""

    source_table_id: int
    system: str
    schema: str
    table: str
    target: str
    load_type: str
    cursor: str | None
    watermark_type: str | None
    pk: tuple[str, ...]
    columns: tuple[str, ...]


@dataclass(frozen=True)
class Stats:
    """What one table load produced, for the end-of-run report."""

    table: str
    mode: str
    extracted: int
    source_rows: int
    bronze_rows: int
    watermark: str | None


def _scalar(value: str) -> Any:
    """Unquote a YAML scalar; plain digits become ints."""
    value = value.strip().strip("'\"")
    return int(value) if value.isdigit() else value


def read_contract(path: Path) -> dict[str, Any]:
    """Parse the subset of the generated contract YAML ingestion needs."""
    contract: dict[str, Any] = {}
    key: str | None = None
    item: dict[str, Any] | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        body = line.strip()

        if indent == 0:
            item = None
            if body.startswith("- "):
                if contract.get(key) is None:
                    contract[key] = []
                if key == "columns":
                    item = {}
                    name, sep, value = body[2:].partition(":")
                    if sep:
                        item[name.strip()] = _scalar(value)
                    contract[key].append(item)
                else:
                    contract[key].append(_scalar(body[2:]))
                continue
            name, sep, value = body.partition(":")
            if not sep:
                continue
            key = name.strip()
            if key in _CONTRACT_KEYS:
                contract[key] = _scalar(value) if value.strip() else None
            continue

        if key not in _CONTRACT_KEYS:
            continue
        if key == "columns":
            if item is not None:
                name, sep, value = body.partition(":")
                if sep:
                    item[name.strip()] = _scalar(value)
            continue
        if contract.get(key) is None:
            contract[key] = {}
        name, sep, value = body.partition(":")
        if sep:
            contract[key][name.strip()] = _scalar(value)

    missing = [k for k in _CONTRACT_KEYS if contract.get(k) in (None, [])]
    if missing:
        raise ValueError(f"{path.name}: contract is missing {', '.join(missing)}")
    if isinstance(contract["primary_key"], str):
        contract["primary_key"] = [contract["primary_key"]]
    return contract


def _ident(name: str, where: str) -> str:
    """Reject anything that is not a plain SQL identifier."""
    if not _IDENT.match(name):
        raise ValueError(f"{where}: unsafe identifier {name!r}")
    return name


def _registry_row(contract: dict[str, Any]) -> dict[str, Any]:
    """Map a contract onto one control.source_table row."""
    system, schema, table = contract["source"].split(".")
    target_schema, target = contract["landing_table"].split(".")
    _ident(schema, contract["source"])
    _ident(table, contract["source"])
    _ident(target, contract["landing_table"])
    if target_schema != "bronze":
        raise ValueError(f"{contract['source']}: landing schema must be bronze")

    mode = contract["extraction"].get("mode")
    if mode not in {"incremental", "full_replace"}:
        raise ValueError(f"{contract['source']}: unknown extraction mode {mode!r}")
    incremental = mode == "incremental"
    cursor = contract["extraction"].get("cursor") if incremental else None
    if incremental and not cursor:
        raise ValueError(f"{contract['source']}: incremental needs a cursor")

    watermark_type = None
    if incremental:
        column = next(c for c in contract["columns"] if c["name"] == cursor)
        staged = column.get("staged_type") or column.get("type")
        watermark_type = "DATE" if staged == "date" else "TIMESTAMP"

    return {
        "system": system,
        "schema": schema,
        "table": table,
        "target": target,
        "load_type": "INCREMENTAL" if incremental else "FULL",
        "cursor": cursor,
        "watermark_type": watermark_type,
        "pk": [_ident(k, contract["source"]) for k in contract["primary_key"]],
        "columns": [_ident(c["name"], contract["source"]) for c in contract["columns"]],
    }


def _ensure_registry(pg: Engine, contracts: list[dict[str, Any]]) -> list[Source]:
    """Upsert the pipeline and every contract into control; return load-ordered sources."""
    rows = [_registry_row(c) for c in contracts]
    rows.sort(key=lambda r: (r["load_type"] != "FULL", r["table"]))
    full_index = 0
    inc_index = 0
    for row in rows:
        if row["load_type"] == "FULL":
            full_index += 1
            row["load_order"] = full_index * 10
        else:
            inc_index += 1
            row["load_order"] = 100 + inc_index * 10

    sources: list[Source] = []
    with pg.begin() as conn:
        pipeline_id = conn.execute(
            text(
                """
                INSERT INTO control.pipeline
                    (pipeline_name, airflow_dag_id, description, schedule_cron, owner)
                VALUES
                    (:name, :dag, 'MySQL and MongoDB sources into bronze',
                     '0 2 * * *', 'data-engineering')
                ON CONFLICT (pipeline_name) DO UPDATE
                   SET airflow_dag_id = EXCLUDED.airflow_dag_id,
                       is_active = TRUE
                RETURNING pipeline_id
                """
            ),
            {"name": PIPELINE_NAME, "dag": DAG_ID},
        ).scalar_one()

        for row in rows:
            source_table_id = conn.execute(
                text(
                    """
                    INSERT INTO control.source_table
                        (pipeline_id, source_system, source_schema, source_table,
                         target_schema, target_table, load_type, watermark_column,
                         watermark_data_type, primary_key_columns, load_order)
                    VALUES
                        (:pipeline_id, :system, :schema, :table, 'bronze', :target,
                         :load_type, :cursor, :watermark_type, :pk, :load_order)
                    ON CONFLICT (source_system, source_schema, source_table) DO UPDATE
                       SET target_table = EXCLUDED.target_table,
                           load_type = EXCLUDED.load_type,
                           watermark_column = EXCLUDED.watermark_column,
                           watermark_data_type = EXCLUDED.watermark_data_type,
                           primary_key_columns = EXCLUDED.primary_key_columns,
                           load_order = EXCLUDED.load_order,
                           is_active = TRUE
                    RETURNING source_table_id
                    """
                ),
                {"pipeline_id": pipeline_id, **row},
            ).scalar_one()
            sources.append(
                Source(
                    source_table_id=source_table_id,
                    system=row["system"],
                    schema=row["schema"],
                    table=row["table"],
                    target=row["target"],
                    load_type=row["load_type"],
                    cursor=row["cursor"],
                    watermark_type=row["watermark_type"],
                    pk=tuple(row["pk"]),
                    columns=tuple(row["columns"]),
                )
            )
    log.info("control registry ready: %s source tables", len(sources))
    return sources


def _start_batch(pg: Engine, run_id: str) -> int:
    """Open the control batch for this script run."""
    with pg.begin() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT control.start_batch(
                        :pipeline_name, :dag_id, :run_id, :logical_date,
                        'ingestion_script')
                    """
                ),
                {
                    "pipeline_name": PIPELINE_NAME,
                    "dag_id": DAG_ID,
                    "run_id": run_id,
                    "logical_date": datetime.now(UTC),
                },
            ).scalar_one()
        )


def _finish_batch(
    pg: Engine, batch_id: int, status: str, message: str | None = None
) -> None:
    """Close the batch; FAILED also lands in audit.pipeline_run_log."""
    with pg.begin() as conn:
        conn.execute(
            text("SELECT control.finish_batch(:batch_id, :status, :message)"),
            {"batch_id": batch_id, "status": status, "message": message},
        )


def _watermark(pg: Engine, spec: Source) -> str | None:
    """Current watermark for an incremental source; NULL means never loaded."""
    if spec.load_type != "INCREMENTAL":
        return None
    with pg.connect() as conn:
        return conn.execute(
            text("SELECT control.get_watermark(:source_table_id)"),
            {"source_table_id": spec.source_table_id},
        ).scalar()


def _advance_watermark(pg: Engine, spec: Source, batch_id: int, value: str) -> None:
    """Move the watermark forward only after the bronze write succeeded."""
    with pg.begin() as conn:
        conn.execute(
            text(
                "SELECT control.advance_watermark(:source_table_id, :batch_id, :value)"
            ),
            {
                "source_table_id": spec.source_table_id,
                "batch_id": batch_id,
                "value": value,
            },
        )


def _lower_as(lower: str, watermark_type: str | None) -> Any:
    """Type a stored watermark so MySQL can compare it to its cursor column."""
    if watermark_type == "DATE":
        return date.fromisoformat(lower[:10])
    return datetime.fromisoformat(lower)


def _watermark_text(value: Any) -> str:
    """Format a cursor value the way control.watermark stores it."""
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _pg_type(mysql_type: str) -> str:
    """Map a MySQL column type to its Postgres landing type."""
    return _MYSQL_TO_PG.get(mysql_type, "text")


def _column_types(mysql: Engine, spec: Source) -> dict[str, str]:
    """Read the source column types; raise when a contract column is absent."""
    with mysql.connect() as conn:
        found = {
            name: _pg_type(mysql_type)
            for name, mysql_type in conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table"
                ),
                {"schema": spec.schema, "table": spec.table},
            ).fetchall()
        }
    missing = [c for c in spec.columns if c not in found]
    if missing:
        raise ValueError(
            f"{spec.schema}.{spec.table}: missing source columns {missing}"
        )
    return {column: found[column] for column in spec.columns}


def _ensure_bronze(pg: Engine, spec: Source, types: dict[str, str]) -> None:
    """Create the bronze table with its primary key; add new contract columns."""
    columns = ",\n    ".join(f"{name} {types[name]}" for name in spec.columns)
    primary_key = ", ".join(spec.pk)
    with pg.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS bronze.{spec.target} (\n"
                f"    {columns},\n"
                "    _ingested_at timestamptz NOT NULL DEFAULT now(),\n"
                f"    PRIMARY KEY ({primary_key})\n"
                ")"
            )
        )
        existing = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'bronze' AND table_name = :table"
                ),
                {"table": spec.target},
            )
        }
        for name in spec.columns:
            if name not in existing:
                conn.execute(
                    text(
                        f"ALTER TABLE bronze.{spec.target} "
                        f"ADD COLUMN {name} {types[name]}"
                    )
                )
        _ensure_bronze_primary_key(conn, spec)


def _ensure_bronze_primary_key(connection: Any, spec: Source) -> None:
    """Ensure the bronze key agrees with the contract used for upserts."""
    actual = tuple(
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
            {"table": spec.target},
        ).scalars()
    )
    if not actual:
        primary_key = ", ".join(spec.pk)
        connection.execute(
            text(f"ALTER TABLE bronze.{spec.target} ADD PRIMARY KEY ({primary_key})")
        )
        return
    if actual != spec.pk:
        raise ValueError(
            f"bronze.{spec.target}: primary key {actual} does not match "
            f"contract key {spec.pk}"
        )


def _extract_mysql(
    mysql: Engine, spec: Source, lower: str | None
) -> tuple[Any, list[dict[str, Any]]]:
    """Return (cursor upper bound, rows); FULL loads ignore the watermark."""
    columns = ", ".join(f"`{c}`" for c in spec.columns)
    source = f"`{spec.schema}`.`{spec.table}`"
    with mysql.connect() as conn:
        if spec.load_type != "INCREMENTAL":
            rows = conn.execute(text(f"SELECT {columns} FROM {source}")).mappings()
            return None, [dict(row) for row in rows]

        upper = conn.execute(
            text(f"SELECT MAX(`{spec.cursor}`) FROM {source}")
        ).scalar()
        if upper is None:
            return None, []

        where = f"`{spec.cursor}` <= :upper"
        params: dict[str, Any] = {"upper": upper}
        if lower is not None:
            where = f"`{spec.cursor}` > :lower AND `{spec.cursor}` <= :upper"
            params["lower"] = _lower_as(lower, spec.watermark_type)
        rows = conn.execute(
            text(f"SELECT {columns} FROM {source} WHERE {where}"), params
        ).mappings()
        return upper, [dict(row) for row in rows]


def _extract_mongo(
    mongo: Any, spec: Source, lower: str | None
) -> tuple[Any, list[dict[str, Any]]]:
    """Return (cursor upper bound, rows) from a collection; raw values stay strings."""
    collection = mongo.client[spec.schema][spec.table]
    projection = dict.fromkeys(spec.columns, 1)
    projection["_id"] = 0

    def _rows(documents: Any) -> list[dict[str, Any]]:
        return [{c: doc.get(c) for c in spec.columns} for doc in documents]

    if spec.load_type != "INCREMENTAL":
        return None, _rows(collection.find({}, projection))

    last = list(
        collection.find({}, {spec.cursor: 1, "_id": 0}).sort(spec.cursor, -1).limit(1)
    )
    if not last or last[0].get(spec.cursor) is None:
        return None, []
    upper = last[0][spec.cursor]
    bounds: dict[str, Any] = {spec.cursor: {"$lte": upper}}
    if lower is not None:
        bounds[spec.cursor] = {"$gt": lower, "$lte": upper}
    return upper, _rows(collection.find(bounds, projection))


def _chunks(rows: list[dict[str, Any]], size: int) -> Any:
    """Yield insert-sized slices so one statement never carries a whole table."""
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


def _write_bronze(pg: Engine, spec: Source, rows: list[dict[str, Any]]) -> int:
    """FULL replaces the table; INCREMENTAL upserts on the business key."""
    column_list = ", ".join(spec.columns)
    placeholders = ", ".join(f":{c}" for c in spec.columns)
    insert = f"INSERT INTO bronze.{spec.target} ({column_list}) VALUES ({placeholders})"
    if spec.load_type == "INCREMENTAL":
        conflict = ", ".join(spec.pk)
        updatable = [c for c in spec.columns if c not in spec.pk]
        if updatable:
            assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
            insert += (
                f" ON CONFLICT ({conflict}) DO UPDATE SET {assignments}, "
                "_ingested_at = now()"
            )
        else:
            insert += f" ON CONFLICT ({conflict}) DO NOTHING"

    with pg.begin() as conn:
        if spec.load_type == "FULL":
            conn.execute(text(f"DELETE FROM bronze.{spec.target}"))
        for chunk in _chunks(rows, CHUNK_SIZE):
            conn.execute(text(insert), chunk)
    return len(rows)


def _count_bronze(pg: Engine, spec: Source) -> int:
    """Rows currently in the bronze table."""
    with pg.connect() as conn:
        return int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM bronze.{spec.target}")
            ).scalar_one()
        )


def _count_mysql(mysql: Engine, spec: Source) -> int:
    """Rows currently in the source table."""
    with mysql.connect() as conn:
        return int(
            conn.execute(
                text(f"SELECT COUNT(*) FROM `{spec.schema}`.`{spec.table}`")
            ).scalar_one()
        )


def _log_recon(
    pg: Engine,
    batch_id: int,
    spec: Source,
    source_rows: int,
    bronze_rows: int,
    extracted: int,
    watermark: str | None,
) -> None:
    """Record source-vs-bronze counts for this table in this batch."""
    audit.log_reconciliation(
        batch_id,
        [
            {
                "source_system": spec.system,
                "source_object": f"{spec.schema}.{spec.table}",
                "target_layer": "bronze",
                "target_object": spec.target,
                "source_row_count": source_rows,
                "target_row_count": bronze_rows,
                "tolerance_pct": 0,
                "status": "MATCH" if source_rows == bronze_rows else "MISMATCH",
                "message": f"extracted={extracted}, watermark={watermark or '-'}",
            }
        ],
    )


def _ingest_one(
    pg: Engine,
    mysql: Engine,
    mongo: Any,
    spec: Source,
    batch_id: int,
    run_id: str,
) -> Stats:
    """Extract one source into bronze and write its control/audit rows."""
    with audit.task_run(
        batch_id, DAG_ID, run_id, f"ingest_{spec.target}", layer="bronze"
    ) as task:
        lower = _watermark(pg, spec)
        if spec.system == "mysql":
            upper, rows = _extract_mysql(mysql, spec, lower)
            types = _column_types(mysql, spec)
            source_rows = _count_mysql(mysql, spec)
        else:
            upper, rows = _extract_mongo(mongo, spec, lower)
            types = {column: "text" for column in spec.columns}
            source_rows = int(mongo.client[spec.schema][spec.table].count_documents({}))

        _ensure_bronze(pg, spec, types)
        extracted = _write_bronze(pg, spec, rows)

        watermark = None
        if spec.load_type == "INCREMENTAL" and upper is not None:
            watermark = _watermark_text(upper)
            _advance_watermark(pg, spec, batch_id, watermark)

        bronze_rows = _count_bronze(pg, spec)
        _log_recon(pg, batch_id, spec, source_rows, bronze_rows, extracted, watermark)
        task.rows_processed = extracted

    return Stats(
        table=spec.target,
        mode=spec.load_type,
        extracted=extracted,
        source_rows=source_rows,
        bronze_rows=bronze_rows,
        watermark=watermark,
    )


def _databricks_status() -> str:
    """Read-only ping; reported but never able to fail the batch."""
    try:
        connection = get_databricks_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except Exception as error:  # noqa: BLE001 - diagnostic only
        return f"failed ({type(error).__name__})"
    return "ok"


def _report(pg: Engine, batch_id: int, stats: list[Stats]) -> None:
    """Print per-table results plus what landed in audit and control."""
    for item in stats:
        log.info(
            "%-30s %-11s extracted=%-7d source=%-7d bronze=%-7d watermark=%s",
            item.table,
            item.mode,
            item.extracted,
            item.source_rows,
            item.bronze_rows,
            item.watermark or "-",
        )

    with pg.connect() as conn:
        tasks = conn.execute(
            text(
                "SELECT status, count(*) FROM audit.task_run_log "
                "WHERE batch_id = :batch_id GROUP BY status ORDER BY status"
            ),
            {"batch_id": batch_id},
        ).fetchall()
        recon = conn.execute(
            text(
                "SELECT status, count(*) FROM audit.reconciliation_results "
                "WHERE batch_id = :batch_id GROUP BY status ORDER BY status"
            ),
            {"batch_id": batch_id},
        ).fetchall()
        events = conn.execute(
            text(
                "SELECT event_type FROM audit.pipeline_run_log "
                "WHERE batch_id = :batch_id ORDER BY log_id"
            ),
            {"batch_id": batch_id},
        ).scalars()
        watermarks = conn.execute(
            text(
                "SELECT s.target_table, w.watermark_value "
                "FROM control.watermark w "
                "JOIN control.source_table s USING (source_table_id) "
                "ORDER BY s.target_table"
            )
        ).fetchall()

    for status, count in tasks:
        log.info("audit.task_run_log %s: %s", status, count)
    for status, count in recon:
        log.info("audit.reconciliation_results %s: %s", status, count)
    for event in events:
        log.info("audit.pipeline_run_log: %s", event)
    for table, value in watermarks:
        log.info("control.watermark %s = %s", table, value)
    log.info("databricks connectivity: %s", _databricks_status())


def main() -> int:
    """Run one full ingestion pass; 0 on success, 1 on a failed batch."""
    contracts = [read_contract(path) for path in sorted(CONTRACTS_DIR.glob("*.yml"))]
    if not contracts:
        log.error("no contracts found in %s", CONTRACTS_DIR)
        return 1

    pg = get_postgres_engine()
    mysql = get_mysql_engine()
    mongo = get_mongo_db()
    log.info(
        "connected: mysql %s:%s/%s, mongo db %s, postgres %s:%s/%s",
        mysql.url.host,
        mysql.url.port,
        mysql.url.database,
        mongo.name,
        pg.url.host,
        pg.url.port,
        pg.url.database,
    )

    sources = _ensure_registry(pg, contracts)
    run_id = f"manual__{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    batch_id = _start_batch(pg, run_id)
    log.info("batch %s started (run %s)", batch_id, run_id)

    stats: list[Stats] = []
    try:
        for spec in sources:
            stats.append(_ingest_one(pg, mysql, mongo, spec, batch_id, run_id))
        _finish_batch(pg, batch_id, "SUCCEEDED")
        _report(pg, batch_id, stats)
        log.info("batch %s SUCCEEDED", batch_id)
        return 0
    except (OSError, PyMongoError, SQLAlchemyError, TypeError, ValueError) as error:
        message = audit.error_summary(error, "ingestion")
        log.error("ingestion failed (%s)", type(error).__name__)
        audit.log_error(
            component="INGESTION",
            layer="bronze",
            batch_id=batch_id,
            airflow_dag_id=DAG_ID,
            task_id="ingest_bronze",
            error_message=message,
        )
        _finish_batch(pg, batch_id, "FAILED", message)
        return 1
    finally:
        mongo.client.close()
        pg.dispose()
        mysql.dispose()


if __name__ == "__main__":
    sys.exit(main())
