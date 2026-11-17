"""utils.connection must expose a getter for each of the four systems."""

from __future__ import annotations

import pytest


@pytest.mark.unit
def test_connection_exposes_all_four_getters() -> None:
    from utils import connection

    assert callable(connection.get_mongo_db)
    assert callable(connection.get_postgres_engine)
    assert callable(connection.get_mysql_engine)
    assert callable(connection.get_databricks_connection)


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
