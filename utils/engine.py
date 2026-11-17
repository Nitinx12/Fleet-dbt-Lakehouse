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
