"""Keep phase-two watchdog scope explicit until its policy is approved."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
def test_watchdog_is_a_phase_two_skeleton() -> None:
    """Do not invent stale-alert de-duplication behaviour before approval."""
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "fleet_watchdog.py"
    ).read_text(encoding="utf-8")

    assert "Phase two" in source
    assert "NotImplementedError" in source
