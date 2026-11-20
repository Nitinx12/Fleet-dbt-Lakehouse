"""Remove credentials and sensitive configuration from Airflow output."""

from __future__ import annotations

import os
import re

_URL_CREDENTIALS = re.compile(
    r"([a-z][a-z0-9+.-]*://)[^\s/@:]+:[^\s/@]+@", re.IGNORECASE
)
_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)


def redact(value: object) -> str:
    """Return text with configured secrets and common credential forms removed."""
    text = str(value)
    secrets = [
        item
        for name, item in os.environ.items()
        if any(
            token in name.upper()
            for token in ("PASSWORD", "TOKEN", "SECRET", "KEY", "SALT", "FERNET")
        )
        and item
    ]
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, "[REDACTED]")
    text = _URL_CREDENTIALS.sub(r"\1[REDACTED]@", text)
    return _BEARER.sub("Bearer [REDACTED]", text)
