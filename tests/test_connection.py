"""utils.connection must expose a getter for each of the four systems."""

from __future__ import annotations

from typing import Any, Self

import pytest


@pytest.mark.unit
def test_connection_exposes_all_four_getters() -> None:
    from utils import connection

    assert callable(connection.get_mongo_db)
    assert callable(connection.get_postgres_engine)
    assert callable(connection.get_mysql_engine)
    assert callable(connection.get_databricks_connection)
    assert callable(connection.open_databricks_connection)


@pytest.mark.unit
def test_connection_logger_stays_quiet_on_console() -> None:
    import logging
    from logging.handlers import RotatingFileHandler

    from utils import connection

    consoles = [
        handler
        for handler in connection.log.handlers
        if isinstance(handler, logging.StreamHandler)
        and not isinstance(handler, RotatingFileHandler)
    ]
    assert connection.log.name == "fleet.connection"
    assert (
        connection.log.propagate is False
    )  # else records reach the root stdout handler
    assert consoles and consoles[0].level == logging.WARNING


@pytest.mark.unit
def test_databricks_connection_forwards_retry_base_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured backoff base must reach the connector's retry policy."""
    from utils import connection, engine

    captured: dict[str, Any] = {}

    class _Cursor:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def execute(self, _statement: str) -> None:
            pass

    class _Connection:
        def cursor(self) -> _Cursor:
            return _Cursor()

    def _connect(**kwargs: Any) -> _Connection:
        captured.update(kwargs)
        return _Connection()

    monkeypatch.setattr(connection.databricks_sql, "connect", _connect)

    monkeypatch.setattr(engine, "DATABRICKS_RETRY_BASE_SECONDS", 5)
    connection.open_databricks_connection()
    assert captured["_retry_delay_min"] == 5
    assert captured["_retry_delay_max"] == 60

    captured.clear()
    monkeypatch.setattr(engine, "DATABRICKS_RETRY_BASE_SECONDS", None)
    connection.open_databricks_connection()
    assert "_retry_delay_min" not in captured
    assert "_retry_delay_max" not in captured
