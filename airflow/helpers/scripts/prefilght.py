"""Check configuration and service reachability before a warehouse batch opens."""

from __future__ import annotations

import os
import subprocess

from databricks.sql.exc import Error as DatabricksError
from pymongo.errors import PyMongoError
from pymysql.err import MySQLError
from sqlalchemy.exc import SQLAlchemyError


class PreflightError(RuntimeError):
    """Raised with check names only, never settings or connection values."""


def _required_names() -> tuple[str, ...]:
    """Read the application-owned configuration declaration."""
    from utils import engine

    return tuple(engine.AIRFLOW_REQUIRED_ENV)


def run_preflight() -> None:
    """Validate configuration, dependencies, and the contracts check."""
    try:
        required = _required_names()
    except OSError as error:
        required = ()
        failed = [
            name.strip() for name in str(error).split(":", maxsplit=1)[-1].split(",")
        ]
    else:
        failed = [name for name in required if not os.getenv(name)]
    if not failed:
        from utils.connection import (
            get_mongo_db,
            get_mysql_engine,
            get_postgres_engine,
            open_databricks_connection,
        )

        checks = (
            ("postgres", lambda: get_postgres_engine().connect().close()),
            ("mysql", lambda: get_mysql_engine().connect().close()),
            ("mongodb", lambda: get_mongo_db().command("ping")),
            ("databricks", lambda: open_databricks_connection().close()),
        )
        for name, check in checks:
            try:
                check()
            except (
                SQLAlchemyError,
                PyMongoError,
                MySQLError,
                DatabricksError,
                OSError,
            ):
                failed.append(name)
        result = subprocess.run(
            ["make", "contracts-check"],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode:
            failed.append("contracts")
    if failed:
        raise PreflightError("preflight failed: " + ", ".join(sorted(set(failed))))
