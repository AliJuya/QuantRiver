from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque

from core.data_engine.candle_river import Candle, CandleRiver
from core.data_engine.tick_river import TickRiver


@dataclass
class MarketState:
    """
    Shared SSOT view over the DataEngine rivers.
    Holds references to the same deques the DataEngine owns.
    """
    rivers_by_tf: dict[str, CandleRiver]
    tick_river: TickRiver | None = None

    def __post_init__(self) -> None:
        self.candles: dict[str, Deque[Candle]] = {
            tf: river.deque_ref()
            for tf, river in self.rivers_by_tf.items()
        }
        self.is_warm: bool = False

    def set_warm(self, value: bool = True) -> None:
        self.is_warm = bool(value)

    def last_candle(self, tf: str) -> Candle | None:
        buf = self.candles.get(tf)
        if not buf:
            return None
        return buf[-1]

    def candle_count(self, tf: str) -> int:
        buf = self.candles.get(tf)
        return len(buf) if buf is not None else 0

    def get_candle(self, tf: str, open_time: datetime) -> Candle | None:
        buf = self.candles.get(tf)
        if not buf:
            return None

        for candle in reversed(buf):
            if candle.open_time == open_time:
                return candle
            if candle.open_time < open_time:
                break
        return None

    def window(self, tf: str, size: int | None = None) -> list[Candle]:
        buf = self.candles.get(tf)
        if not buf:
            return []
        if size is None or size <= 0:
            return list(buf)
        return list(deque(buf, maxlen=size))
