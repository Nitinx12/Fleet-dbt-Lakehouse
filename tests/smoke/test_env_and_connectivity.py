"""Smoke checks for environment settings and Postgres reachability."""

from __future__ import annotations

from sqlalchemy import text


def test_required_environment_is_loaded() -> None:
    """The core connection settings must be present after startup."""
    from utils import engine

    required = (
        "PG_HOST",
        "PG_PORT",
        "PG_DBNAME",
        "PG_USER",
        "PG_PASSWORD",
        "MONGO_URI",
        "MONGO_DB_NAME",
        "MYSQL_HOST",
        "MYSQL_PORT",
        "MYSQL_USER",
        "MYSQL_PASSWORD",
        "MYSQL_DATABASE",
        "DATABRICKS_HOST",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_TOKEN",
    )
    assert [name for name in required if not getattr(engine, name)] == []


def test_postgres_is_reachable() -> None:
    """The warehouse must answer a trivial query."""
    from utils.connection import get_postgres_engine

    with get_postgres_engine().connect() as connection:
        assert connection.execute(text("SELECT 1")).scalar_one() == 1
