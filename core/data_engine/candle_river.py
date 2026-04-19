# core/data_engine/candle_river.py
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from queue import SimpleQueue, Empty
from threading import Condition, Event, Thread
from typing import Callable, Deque, List, Optional


@dataclass
class Candle:
    tf: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    indicators: dict[str, float] = field(default_factory=dict)


CloseCallback = Callable[[str, Candle], None]


class CandleRiver:
    """
    ✅ SSOT storage for CLOSED candles (append-only, bounded, drop-oldest)
    ✅ subscribe_on_close(cb): non-blocking notifications (async dispatch)
    ❌ NO pop() (this is not a consuming queue)
    """

    def __init__(self, tf: str, maxlen: int = 50_000):
        if maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self.tf = tf
        self._maxlen = maxlen
        self._buf: Deque[Candle] = deque()

        self._cv = Condition()
        self._subs: List[CloseCallback] = []

        self._dropped = 0
        self._pushes = 0

        # async notifier so push_closed never blocks on subscribers
        self._notify_q: SimpleQueue[tuple[str, Candle]] = SimpleQueue()
        self._notify_stop = Event()
        self._notify_thread: Optional[Thread] = None

    # ---------- subscribe ----------
    def subscribe_on_close(self, cb: CloseCallback) -> None:
        self._subs.append(cb)
        self._ensure_notifier()

    def _ensure_notifier(self) -> None:
        if self._notify_thread and self._notify_thread.is_alive():
            return
        self._notify_stop.clear()
        self._notify_thread = Thread(
            target=self._notifier_loop,
            name=f"CandleRiverNotifier[{self.tf}]",
            daemon=True,
        )
        self._notify_thread.start()

    def _notifier_loop(self) -> None:
        while not self._notify_stop.is_set():
            try:
                tf, candle = self._notify_q.get(timeout=0.25)
            except Empty:
                continue

            # best-effort dispatch; never let a subscriber kill the river
            for cb in list(self._subs):
                try:
                    cb(tf, candle)
                except Exception:
                    pass

    def stop_notifier(self) -> None:
        self._notify_stop.set()
        thread = self._notify_thread
        if thread and thread.is_alive():
            thread.join(timeout=1.0)

    # ---------- storage ----------
    def push_closed(self, candle: Candle) -> None:
        if candle.tf != self.tf:
            raise ValueError(f"tf mismatch: river={self.tf} candle={candle.tf}")

        with self._cv:
            if len(self._buf) >= self._maxlen:
                self._buf.popleft()
                self._dropped += 1
            self._buf.append(candle)
            self._pushes += 1
            self._cv.notify_all()

        # notify subscribers asynchronously (non-blocking)
        if self._subs:
            self._notify_q.put((self.tf, candle))

    def deque_ref(self) -> Deque[Candle]:
        """SSOT reference (read-only usage outside DataEngine)."""
        return self._buf

    def last(self) -> Optional[Candle]:
        with self._cv:
            return self._buf[-1] if self._buf else None

    def __len__(self) -> int:
        with self._cv:
            return len(self._buf)

    def stats(self) -> dict:
        with self._cv:
            return {
                "tf": self.tf,
                "maxlen": self._maxlen,
                "size": len(self._buf),
                "dropped": self._dropped,
                "pushes": self._pushes,
            }

    # ---------- boot helpers (anchor cleanup) ----------
    def drop_before(self, ts: datetime) -> int:
        """
        Drop candles with open_time < ts (used for pre-anchor cleanup).
        Returns number removed.
        """
        removed = 0
        with self._cv:
            while self._buf and self._buf[0].open_time < ts:
                self._buf.popleft()
                removed += 1
        return removed

    def appendleft_many(self, candles: list[Candle]) -> None:
        """
        Insert historical candles strictly BEFORE current left edge.
        Caller must ensure monotonic order and non-overlap.
        Implementation expects `candles` is in chronological order (oldest->newest).
        """
        if not candles:
            return
        with self._cv:
            # appendleft must be done reverse to preserve chronological order
            for c in reversed(candles):
                if c.tf != self.tf:
                    raise ValueError(f"tf mismatch in appendleft_many: river={self.tf} candle={c.tf}")
                self._buf.appendleft(c)
                # enforce maxlen by dropping from right (newest) would be wrong here,
                # so we drop from left (oldest) only if we exceed. But since we're
                # adding older history, exceeding means we should drop even older,
                # which is also left. So popleft is consistent.
                if len(self._buf) > self._maxlen:
                    self._buf.popleft()
                    self._dropped += 1
            self._cv.notify_all()
