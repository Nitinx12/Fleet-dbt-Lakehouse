"""Render and dispatch redacted multipart notification email."""

from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .redact import redact

log = logging.getLogger(__name__)
TEMPLATES = Path(__file__).resolve().parents[1] / "template"


def _truth(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


def render(template: str, context: dict[str, object]) -> str:
    """Render a local text or HTML notification template."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATES), autoescape=select_autoescape(("html", "j2"))
    )
    return redact(env.get_template(template).render(**context))


def send_email(subject: str, text_body: str, html_body: str) -> None:
    """Send one multipart email with bounded SMTP operations."""
    recipients = [
        item.strip()
        for item in os.getenv("ALERT_EMAIL_TO", "").split(",")
        if item.strip()
    ]
    if not recipients:
        raise ValueError("ALERT_EMAIL_TO is required for notifications")
    message = EmailMessage()
    message["Subject"] = redact(subject)
    message["From"] = os.getenv("ALERT_EMAIL_FROM", "")
    message["To"] = ", ".join(recipients)
    message.set_content(redact(text_body))
    message.add_alternative(redact(html_body), subtype="html")
    with smtplib.SMTP(
        os.environ["SMTP_HOST"], int(os.getenv("SMTP_PORT", "587")), timeout=20
    ) as client:
        client.ehlo()
        if _truth(os.getenv("SMTP_STARTTLS", "true")):
            client.starttls()
            client.ehlo()
        if user := os.getenv("SMTP_USER"):
            client.login(user, os.getenv("SMTP_PASSWORD", ""))
        client.send_message(message)


def send_webhook(payload: str) -> None:
    """Send an optional redacted webhook payload with a short timeout."""
    if not (url := os.getenv("ALERT_WEBHOOK_URL")):
        return
    request = Request(
        url,
        data=redact(payload).encode(),
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    with urlopen(request, timeout=20):
        pass


def notify(subject: str, context: dict[str, object], failure: bool = False) -> bool:
    """Try notifications without changing the data pipeline outcome."""
    prefix = "failure" if failure else "summary"
    try:
        text_body = render(f"{prefix}.txt.j2", context)
        html_body = render(f"{prefix}.html.j2", context)
        send_email(subject, text_body, html_body)
        send_webhook(text_body)
    except (
        OSError,
        smtplib.SMTPException,
        TimeoutError,
        URLError,
        ValueError,
    ) as error:
        log.warning("notification delivery failed (%s)", redact(type(error).__name__))
        return False
    return True


def send_run_summary(
    batch_id: int, dag_id: str, run_id: str, logical_date: object
) -> bool:
    """Deliver the one success message after its batch has been closed."""
    from .summary import success_context

    subject = f"[fleet] SUCCESS {redact(dag_id)} {logical_date}"
    context = success_context(batch_id, os.getenv("AIRFLOW_BASE_URL", ""))
    context["run_id"] = redact(run_id)
    return notify(subject, context)
