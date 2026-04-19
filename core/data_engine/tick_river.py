from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from threading import Condition
from typing import Deque, Optional


@dataclass(frozen=True)
class Tick:
    ts: datetime
    price: float
    qty: float
    side: str  # "BUY"/"SELL" or similar
    trade_id: int | None = None


class TickRiver:
    """
    Bounded river of raw ticks (drop-oldest).
    Producer: WS / backtest tick replay
    Consumer: CandleBuilder1s
    """

    def __init__(self, maxlen: int = 200_000):
        if maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self._maxlen = maxlen
        self._buf: Deque[Tick] = deque()
        self._cv = Condition()
        self._dropped = 0
        self._pushes = 0
        self._pops = 0

    def push(self, tick: Tick) -> None:
        with self._cv:
            if len(self._buf) >= self._maxlen:
                self._buf.popleft()
                self._dropped += 1
            self._buf.append(tick)
            self._pushes += 1
            self._cv.notify()

    def pop(self, timeout: Optional[float] = None) -> Optional[Tick]:
        with self._cv:
            if not self._buf:
                if timeout == 0:
                    return None
                self._cv.wait(timeout=timeout)

            if not self._buf:
                return None

            self._pops += 1
            return self._buf.popleft()

    def try_pop(self) -> Optional[Tick]:
        return self.pop(timeout=0)

    def __len__(self) -> int:
        with self._cv:
            return len(self._buf)

    def stats(self) -> dict:
        with self._cv:
            return {
                "maxlen": self._maxlen,
                "size": len(self._buf),
                "dropped": self._dropped,
                "pushes": self._pushes,
                "pops": self._pops,
            }
            
    def drop_before(self, ts: datetime) -> int:
        """
        Drop ticks with tick.ts < ts (pre-anchor cleanup).
        Returns number removed.
        """
        removed = 0
        with self._cv:
            while self._buf and self._buf[0].ts < ts:
                self._buf.popleft()
                removed += 1
        return removed