"""utils.engine must accept the env names .env actually ships."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]

# Every required setting, so a child process always gets a loadable config.
_DEFAULTS = {
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_DBNAME": "fleet_warehouse",
    "PG_USER": "postgres",
    "PG_PASSWORD": "admin",
    "MONGO_URI": "mongodb://localhost:27017/fleet_operations",
    "MONGO_DB_NAME": "fleet_operations",
    "MYSQL_HOST": "localhost",
    "MYSQL_PORT": "3306",
    "MYSQL_USER": "root",
    "MYSQL_PASSWORD": "admin",
    "MYSQL_DATABASE": "fleet_registry",
    "DATABRICKS_HOST": "https://workspace.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
    "DATABRICKS_TOKEN": "dapi-test",
}


def _import_engine(overrides: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Import utils.engine in a child process and print the resolved config."""
    env = dict(os.environ)
    env.update(_DEFAULTS)
    env.update(overrides)  # "" beats .env and forces a fallback path
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "import utils.engine as e; print(e.PG_HOST, e.PG_PORT, e.PG_DBNAME)",
        ],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


_PG_BLANK = {
    "PG_HOST": "",
    "PG_PORT": "",
    "PG_DBNAME": "",
    "PG_USER": "",
    "PG_PASSWORD": "",
}


@pytest.mark.unit
def test_pg_names_from_the_env_file_are_accepted() -> None:
    result = _import_engine(
        {
            **_PG_BLANK,
            "PG_HOST": "pg-host",
            "PG_PORT": "6543",
            "PG_DBNAME": "warehouse",
            "PG_USER": "fleet",
            "PG_PASSWORD": "secret",
        }
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "pg-host 6543 warehouse"


@pytest.mark.unit
def test_databricks_is_required_not_optional() -> None:
    result = _import_engine({"DATABRICKS_TOKEN": ""})
    assert result.returncode != 0, "engine imported without DATABRICKS_TOKEN"
    assert "DATABRICKS_TOKEN" in result.stderr


@pytest.mark.unit
def test_mysql_is_required_not_optional() -> None:
    result = _import_engine({"MYSQL_HOST": ""})
    assert result.returncode != 0, "engine imported without MYSQL_HOST"
    assert "MYSQL_HOST" in result.stderr


@pytest.mark.unit
def test_ports_are_cast_to_int() -> None:
    result = _import_engine({})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "localhost 5432 fleet_warehouse"


@pytest.mark.unit
def test_required_settings_are_loaded_in_process() -> None:
    from utils import engine

    assert isinstance(engine.PG_PORT, int)
    assert isinstance(engine.MYSQL_PORT, int)
    assert engine.DATABRICKS_MAX_ATTEMPTS > 0
    assert engine.DATABRICKS_SOCKET_TIMEOUT > 0
    assert engine.PUBLISH_FETCH_SIZE > 0
    assert engine.PUBLISH_MAX_SQL_CHARS > 0
    assert engine.PUBLISH_MERGE_MAX_SQL_CHARS > 0
    assert engine.PUBLISH_WORKERS > 0
    assert engine.PUBLISH_WORKER_MIN_ROWS > 0
    assert engine.PUBLISH_MART_WORKERS > 0
    assert (
        engine.DATABRICKS_RETRY_BASE_SECONDS is None
        or engine.DATABRICKS_RETRY_BASE_SECONDS > 0
    )
    assert engine.MONGO_DB_NAME
    assert engine.MYSQL_DATABASE
    assert (
        engine.DATABRICKS_HOST
        and engine.DATABRICKS_HTTP_PATH
        and engine.DATABRICKS_TOKEN
    )
