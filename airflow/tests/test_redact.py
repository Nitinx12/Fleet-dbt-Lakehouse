"""Verify notification redaction removes secret-bearing forms."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts.redact import redact


@pytest.mark.unit
@pytest.mark.parametrize("text", ["Bearer abc.def", "postgres://user:pass@host/db"])
def test_redact_common_credentials(text: str) -> None:
    """Bearer and URL credentials are replaced independent of environment."""
    assert "[REDACTED]" in redact(text)
