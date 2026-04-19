from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class OpenPosition:
    side: str
    entry_time: datetime
    entry_price: float
    size: float = 1.0
    strategy_id: str = ""
    tf: str = "1s"
    sl_price: float = 0.0
    tp_price: float = 0.0
    entry_atr: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClosedTrade:
    side: str
    entry_time: datetime
    exit_time: datetime
    entry_price: float
    exit_price: float
    size: float = 1.0
    strategy_id: str = ""
    tf: str = "1s"
    reason: str | None = None
    pnl: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)
