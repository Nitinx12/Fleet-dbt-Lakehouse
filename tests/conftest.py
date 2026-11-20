"""Give every required env var a value so utils imports without a real .env."""

import os

import pytest

_REQUIRED = {
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
    "DATABRICKS_HOST": "https://localhost",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/test",
    "DATABRICKS_TOKEN": "test-token",
}

for _name, _value in _REQUIRED.items():
    os.environ.setdefault(_name, _value)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Apply suite markers from the folder path."""
    for item in items:
        for marker in ("unit", "integration", "smoke"):
            if marker in item.path.parts and item.get_closest_marker(marker) is None:
                item.add_marker(getattr(pytest.mark, marker))
