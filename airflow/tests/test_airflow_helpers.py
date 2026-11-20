"""Exercise pure Airflow helper boundaries without external services."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_ROOT = ROOT / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts.commads import build_dbt_command, build_ingest_command
from helpers.scripts.redact import redact


@pytest.mark.unit
def test_redact_removes_environment_and_url_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Never expose a configured secret or credential-bearing URL."""
    monkeypatch.setenv("TEST_TOKEN", "very-secret")

    actual = redact("Bearer very-secret https://user:pass@example.test/path")

    assert "very-secret" not in actual
    assert "user:pass" not in actual
    assert "[REDACTED]" in actual


@pytest.mark.unit
def test_command_builders_use_argv_and_airflow_identifiers() -> None:
    """Commands contain the batch and Airflow run linkage without a shell."""
    ingest = build_ingest_command("mysql", "drivers", 7, "fleet_daily_load", "run-1")
    dbt = build_dbt_command("test", ("dim_*",), 7, "fleet_daily_load", "run-1")

    assert "--batch-id" in ingest and ingest[ingest.index("--batch-id") + 1] == "7"
    assert "--airflow-run-id" in ingest
    assert "--target" in dbt
    assert not any(item in {"|", ";", "&&"} for item in [*ingest, *dbt])


@pytest.mark.unit
def test_invalid_table_identifier_is_rejected() -> None:
    """A table cannot introduce shell syntax into a constructed argv."""
    with pytest.raises(ValueError, match="invalid table"):
        build_ingest_command("mysql", "drivers; echo unsafe", 7, "dag", "run")
