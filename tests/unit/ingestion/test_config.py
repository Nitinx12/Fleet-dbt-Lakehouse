"""Source mode, lookback and environment resolution."""

from __future__ import annotations

import pytest

from ingestion.config import (
    IngestError,
    SourceMode,
    is_prod,
    lookback_days,
    mongo_uri,
    mysql_connection_url,
    source_mode,
)

pytestmark = pytest.mark.unit


def test_source_mode_parses_values() -> None:
    assert source_mode("live") is SourceMode.LIVE
    assert source_mode("CSV") is SourceMode.CSV
    assert source_mode(" fixture ") is SourceMode.FIXTURE


def test_source_mode_rejects_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_MODE", "bogus")
    with pytest.raises(IngestError, match="SOURCE_MODE"):
        source_mode()


def test_source_mode_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_MODE", "fixture")
    assert source_mode() is SourceMode.FIXTURE


def test_is_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    assert is_prod("production") is True
    assert is_prod("development") is False
    monkeypatch.setenv("ENVIRONMENT", "PRODUCTION")
    assert is_prod() is True


def test_lookback_days_default_and_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOKBACK_DAYS", raising=False)
    assert lookback_days() == 14
    monkeypatch.setenv("LOOKBACK_DAYS", "7")
    assert lookback_days() == 7


def test_lookback_days_rejects_bad_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOKBACK_DAYS", "soon")
    with pytest.raises(IngestError, match="LOOKBACK_DAYS"):
        lookback_days()
    monkeypatch.setenv("LOOKBACK_DAYS", "-1")
    with pytest.raises(IngestError, match="LOOKBACK_DAYS"):
        lookback_days()


def test_mysql_connection_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MYSQL_USER", "reader")
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")
    monkeypatch.setenv("MYSQL_HOST", "mysql")
    monkeypatch.setenv("MYSQL_PORT", "13306")
    monkeypatch.setenv("MYSQL_DATABASE", "fleet_registry")
    url = mysql_connection_url()
    assert url.startswith("mysql+pymysql://reader:secret@mysql:13306/fleet_registry")


def test_mysql_connection_url_requires_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MYSQL_USER", raising=False)
    with pytest.raises(IngestError, match="MYSQL_USER"):
        mysql_connection_url()


def test_mongo_uri_prefers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MONGO_URI", "mongodb://mongo:27017/fleet_operations")
    assert mongo_uri() == "mongodb://mongo:27017/fleet_operations"


def test_mongo_uri_builds_from_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.setenv("MONGO_HOST", "mongo")
    monkeypatch.setenv("MONGO_PORT", "27017")
    monkeypatch.setenv("MONGO_DB_NAME", "fleet_operations")
    assert mongo_uri() == "mongodb://mongo:27017/fleet_operations"
