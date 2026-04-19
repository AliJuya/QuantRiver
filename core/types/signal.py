from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Signal:
    action: str
    tf: str
    strategy_id: str = ""
    reason: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    close_only: bool = False
    force_close_all: bool = False
    flip: bool = False

