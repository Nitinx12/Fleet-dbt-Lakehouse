"""Cached connections for the four systems this pipeline moves data between."""

import logging
from typing import Any

from databricks import sql as databricks_sql
from databricks.sql.client import Connection
from databricks.sql.exc import Error as DatabricksError
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pymysql.err import MySQLError
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError

from . import engine as config
from .logger import get_logger

# INFO stays in the log file; the terminal only gets WARNING and above
log = get_logger("connection", console_level=logging.WARNING)

_mongo_client: MongoClient | None = None
_postgres_engine: Engine | None = None
_mysql_engine: Engine | None = None
_databricks_connection: Connection | None = None


def get_mongo_db() -> Database:
    """Return a cached Mongo database handle, creating the client on first use."""
    global _mongo_client

    if _mongo_client is None:
        log.info(f"Opening MongoDB connection to database '{config.MONGO_DB_NAME}'")
        try:
            _mongo_client = MongoClient(config.MONGO_URI)
            # ping now so the error names this call, not a later query
            _mongo_client.admin.command("ping")
        except PyMongoError:
            _mongo_client = None
            log.exception("Failed to connect to MongoDB")
            raise

    return _mongo_client[config.MONGO_DB_NAME]


def get_postgres_engine() -> Engine:
    """Return a cached SQLAlchemy engine, creating it on first use."""
    global _postgres_engine

    if _postgres_engine is None:
        log.info(
            f"Opening Postgres connection to "
            f"{config.PG_HOST}:{config.PG_PORT}/{config.PG_DBNAME}"
        )
        url = URL.create(
            "postgresql+psycopg2",
            username=config.PG_USER,
            password=config.PG_PASSWORD,
            host=config.PG_HOST,
            port=config.PG_PORT,
            database=config.PG_DBNAME,
            # unset SSL options are dropped so plain local Postgres still works
            query={
                name: value
                for name, value in {
                    "sslmode": config.PG_SSLMODE,
                    "channel_binding": config.PG_CHANNEL_BINDING,
                }.items()
                if value
            },
        )
        try:
            _postgres_engine = create_engine(url)
            # ping now so the error names this call, not a later query
            with _postgres_engine.connect():
                pass
        except SQLAlchemyError:
            _postgres_engine = None
            log.exception("Failed to connect to Postgres")
            raise

    return _postgres_engine


def get_mysql_engine() -> Engine:
    """Return a cached SQLAlchemy engine for MySQL, creating it on first use."""
    global _mysql_engine

    if _mysql_engine is None:
        log.info(
            f"Opening MySQL connection to "
            f"{config.MYSQL_HOST}:{config.MYSQL_PORT}/{config.MYSQL_DATABASE}"
        )
        url = URL.create(
            "mysql+pymysql",
            username=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD,
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            database=config.MYSQL_DATABASE,
            query={"charset": "utf8mb4"},
        )
        try:
            _mysql_engine = create_engine(
                url,
                # only local servers without TLS set this to DISABLED
                connect_args=(
                    {"ssl_disabled": True}
                    if config.MYSQL_SSL_MODE == "DISABLED"
                    else {}
                ),
            )
            event.listen(_mysql_engine, "connect", _set_mysql_session_timezone)
            # ping now so the error names this call, not a later query
            with _mysql_engine.connect() as connection:
                connection.exec_driver_sql("SELECT 1")
        except SQLAlchemyError:
            _mysql_engine = None
            log.exception("Failed to connect to MySQL")
            raise

    return _mysql_engine


def _set_mysql_session_timezone(dbapi_connection: Any, _record: Any) -> None:
    """Match the session to the zone the source rows were written in."""
    if not config.MYSQL_SERVER_TZ:
        return

    # named zones need mysql_tzinfo_to_sql loaded; UTC is only an offset
    zone = (
        "+00:00"
        if config.MYSQL_SERVER_TZ.upper() in {"UTC", "Z"}
        else config.MYSQL_SERVER_TZ
    )
    try:
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SET time_zone = %s", (zone,))
    except MySQLError:
        # no tz tables on this server: keep the connection usable
        log.warning(
            "Could not set the MySQL session time zone to %s", config.MYSQL_SERVER_TZ
        )


def get_databricks_connection() -> Connection:
    """Return a cached Databricks SQL connection, creating it on first use."""
    global _databricks_connection

    if _databricks_connection is None:
        log.info(f"Opening Databricks connection to '{config.DATABRICKS_HOST}'")

        connect_kwargs = {
            "server_hostname": config.DATABRICKS_HOST,
            "http_path": config.DATABRICKS_HTTP_PATH,
            "access_token": config.DATABRICKS_TOKEN,
        }
        # unset means the warehouse default, so omit rather than pass None
        if config.DATABRICKS_CATALOG:
            connect_kwargs["catalog"] = config.DATABRICKS_CATALOG
        if config.DATABRICKS_SCHEMA:
            connect_kwargs["schema"] = config.DATABRICKS_SCHEMA

        try:
            _databricks_connection = databricks_sql.connect(**connect_kwargs)
            # ping now so the error names this call, not a later query
            with _databricks_connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        except DatabricksError:
            _databricks_connection = None
            log.exception("Failed to connect to Databricks")
            raise

    return _databricks_connection
