"""Load .env once and validate the settings every connection in utils needs."""

import os
import warnings

from dotenv import load_dotenv

if not load_dotenv():
    warnings.warn("No .env file found, relying on system environment", stacklevel=2)

# Names mirror .env / .env.example, so that file stays the single source of truth.
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DBNAME = os.getenv("PG_DBNAME")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
# managed providers only; plain local Postgres leaves both unset
PG_SSLMODE = os.getenv("PG_SSLMODE")
PG_CHANNEL_BINDING = os.getenv("PG_CHANNEL_BINDING")

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
# source rows are written in this zone, so the session must match
MYSQL_SERVER_TZ = os.getenv("MYSQL_SERVER_TZ")
MYSQL_SSL_MODE = os.getenv("MYSQL_SSL_MODE")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
# unset means the warehouse default
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA")

# Airflow-only settings remain optional to command-line pipeline scripts.
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = os.getenv("SMTP_PORT")
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")
MYSQL_POOL_SLOTS = os.getenv("MYSQL_POOL_SLOTS", "4")
MONGO_POOL_SLOTS = os.getenv("MONGO_POOL_SLOTS", "3")
AIRFLOW_DB_PASSWORD = os.getenv("AIRFLOW_DB_PASSWORD")
AIRFLOW_REQUIRED_ENV = (
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
    "SMTP_HOST",
    "SMTP_PORT",
    "ALERT_EMAIL_FROM",
    "ALERT_EMAIL_TO",
    "DBT_TARGET",
)


def _optional_positive_int(name: str) -> int | None:
    """Read a positive integer environment variable; None when unset."""
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise OSError(f"{name} must be an integer, got: {value!r}") from exc
    if parsed <= 0:
        raise OSError(f"{name} must be greater than zero, got: {value!r}")
    return parsed


def _positive_int(name: str, default: int) -> int:
    """Read a positive integer environment variable or return its default."""
    parsed = _optional_positive_int(name)
    return default if parsed is None else parsed


PUBLISH_MAX_PARAMS = _positive_int("PUBLISH_MAX_PARAMS", 10_000)
PUBLISH_FETCH_SIZE = _positive_int("PUBLISH_FETCH_SIZE", 1_000)
PUBLISH_MAX_SQL_CHARS = _positive_int("PUBLISH_MAX_SQL_CHARS", 400_000)
# MERGE pays a fixed cost per statement, so it is packed far fuller than INSERT;
# 4M chars stays under Databricks' 16 MiB limit even at 4 bytes per character.
PUBLISH_MERGE_MAX_SQL_CHARS = _positive_int(
    "PUBLISH_MERGE_MAX_SQL_CHARS", max(4_000_000, PUBLISH_MAX_SQL_CHARS)
)
PUBLISH_WORKERS = _positive_int("PUBLISH_WORKERS", 1)
PUBLISH_WORKER_MIN_ROWS = _positive_int("PUBLISH_WORKER_MIN_ROWS", 50_000)
PUBLISH_MART_WORKERS = _positive_int("PUBLISH_MART_WORKERS", 3)
DATABRICKS_MAX_ATTEMPTS = _positive_int("DATABRICKS_MAX_ATTEMPTS", 3)
DATABRICKS_SOCKET_TIMEOUT = _positive_int("DATABRICKS_SOCKET_TIMEOUT", 30)
# unset keeps the connector's built-in backoff schedule (1s .. 10s)
DATABRICKS_RETRY_BASE_SECONDS = _optional_positive_int("DATABRICKS_RETRY_BASE_SECONDS")

# cast now: a driver must not receive a string
if PG_PORT is not None:
    try:
        PG_PORT = int(PG_PORT)
    except ValueError as exc:
        raise OSError(f"PG_PORT must be an integer, got: {PG_PORT!r}") from exc

if MYSQL_PORT is not None:
    try:
        MYSQL_PORT = int(MYSQL_PORT)
    except ValueError as exc:
        raise OSError(f"MYSQL_PORT must be an integer, got: {MYSQL_PORT!r}") from exc

# all four systems are load-bearing: nothing runs until every one is set
_required = {
    "PG_HOST": PG_HOST,
    "PG_PORT": PG_PORT,
    "PG_DBNAME": PG_DBNAME,
    "PG_USER": PG_USER,
    "PG_PASSWORD": PG_PASSWORD,
    "MONGO_URI": MONGO_URI,
    "MONGO_DB_NAME": MONGO_DB_NAME,
    "MYSQL_HOST": MYSQL_HOST,
    "MYSQL_PORT": MYSQL_PORT,
    "MYSQL_USER": MYSQL_USER,
    "MYSQL_PASSWORD": MYSQL_PASSWORD,
    "MYSQL_DATABASE": MYSQL_DATABASE,
    "DATABRICKS_HOST": DATABRICKS_HOST,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
}

_missing = [name for name, value in _required.items() if not value]

if _missing:
    raise OSError(f"Missing required environment variables: {', '.join(_missing)}")
