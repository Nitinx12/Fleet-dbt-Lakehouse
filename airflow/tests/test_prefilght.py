"""Test preflight failure reporting without contacting services."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import prefilght


@pytest.mark.unit
def test_missing_setting_reports_its_name_not_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid exposing configuration values in preflight errors."""
    monkeypatch.setattr(prefilght, "_required_names", lambda: ("MISSING_TOKEN",))
    monkeypatch.delenv("MISSING_TOKEN", raising=False)

    with pytest.raises(prefilght.PreflightError, match="MISSING_TOKEN") as error:
        prefilght.run_preflight()

    assert "TOKEN=" not in str(error.value)


@pytest.mark.unit
def test_contract_failure_is_named(monkeypatch: pytest.MonkeyPatch) -> None:
    """Surface the contracts check name rather than command output."""
    monkeypatch.setattr(prefilght, "_required_names", lambda: ("PG_HOST",))
    monkeypatch.setenv("PG_HOST", "configured")
    monkeypatch.setattr(
        prefilght.subprocess,
        "run",
        lambda *args, **kwargs: type("R", (), {"returncode": 1})(),
    )

    class _Engine:
        def connect(self) -> _Connection:
            return _Connection()

    class _Connection:
        def close(self) -> None:
            return None

    class _Mongo:
        def command(self, name: str) -> None:
            return None

    from utils import connection

    monkeypatch.setattr(connection, "get_postgres_engine", _Engine)
    monkeypatch.setattr(connection, "get_mysql_engine", _Engine)
    monkeypatch.setattr(connection, "get_mongo_db", _Mongo)
    monkeypatch.setattr(connection, "open_databricks_connection", _Connection)

    with pytest.raises(prefilght.PreflightError, match="contracts"):
        prefilght.run_preflight()
