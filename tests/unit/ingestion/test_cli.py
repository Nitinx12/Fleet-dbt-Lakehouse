"""CLI entrypoint: dry-run and error paths (no warehouse)."""

from __future__ import annotations

import json

import pytest

from ingestion.__main__ import main

pytestmark = pytest.mark.unit


def test_cli_dry_run_fixture(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--dry-run", "--mode", "fixture"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "fixture"
    assert payload["contract_count"] == 12
    assert payload["schema_contract"]["tables"] == "evolve"


def test_cli_dry_run_system_filter(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--dry-run", "--mode", "csv", "--system", "mysql", "--system", "mongo"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["resources"]) == 12


def test_cli_bad_mode_exits_nonzero() -> None:
    # argparse rejects before IngestError
    with pytest.raises(SystemExit) as exc:
        main(["--mode", "telepathy"])
    assert exc.value.code == 2
