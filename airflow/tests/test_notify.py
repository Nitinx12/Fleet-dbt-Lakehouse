"""Test non-blocking SMTP notifications without a network connection."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

AIRFLOW_ROOT = Path(__file__).resolve().parents[1] / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from helpers.scripts import notify


@pytest.mark.unit
def test_send_email_uses_multipart_starttls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SMTP delivery uses an explicit timeout, TLS, and multipart content."""
    client = Mock()
    smtp = MagicMock()
    smtp.return_value.__enter__.return_value = client
    monkeypatch.setenv("SMTP_HOST", "mail.test")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_STARTTLS", "true")
    monkeypatch.setenv("ALERT_EMAIL_FROM", "from@test")
    monkeypatch.setenv("ALERT_EMAIL_TO", "to@test")
    monkeypatch.setattr(notify.smtplib, "SMTP", smtp)

    notify.send_email("subject", "text", "<p>html</p>")

    assert smtp.call_args.kwargs["timeout"] == 20
    client.starttls.assert_called_once()
    message = client.send_message.call_args.args[0]
    assert message.is_multipart()


@pytest.mark.unit
def test_notify_delivery_error_is_non_blocking(monkeypatch: pytest.MonkeyPatch) -> None:
    """A delivery outage returns false instead of failing the DAG's data result."""
    monkeypatch.setattr(notify, "send_email", Mock(side_effect=OSError("offline")))

    context = {
        "batch": {},
        "batch_id": 1,
        "ingestion": [],
        "reconciliations": [],
        "airflow_url": "",
    }
    assert notify.notify("subject", context, failure=False) is False
