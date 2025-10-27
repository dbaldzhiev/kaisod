"""Time-related helpers."""
from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return the current UTC time as a naive ``datetime``."""
    return datetime.now(UTC).replace(tzinfo=None)
