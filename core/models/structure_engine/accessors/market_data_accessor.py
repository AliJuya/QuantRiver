from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DefaultAccessor:
    ts_keys: tuple[str, ...] = (
        "close_time",
        "close_ts",
        "ts",
        "t",
        "time",
        "timestamp",
        "open_time",
    )
