from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def to_epoch_ms(value: Any) -> int:
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        try:
            return int(float(timestamp()) * 1000)
        except Exception:
            pass

    return int(value)
