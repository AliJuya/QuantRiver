from __future__ import annotations

from dataclasses import dataclass, field
from queue import Full

from core.data_engine.candle_river import Candle, CandleRiver
from core.state.events import EngineEventQueue, TFClosedEvent


@dataclass
class EventBridge:
    """
    Ultra-light bridge from CandleRiver close notifications into the engine queue.
    """
    rivers_by_tf: dict[str, CandleRiver]
    out_q: EngineEventQueue
    tfs: tuple[str, ...] | None = None
    _attached_tfs: set[str] = field(default_factory=set, init=False)
    _dropped: int = field(default=0, init=False)

    def attach(self) -> None:
        targets = self.tfs or tuple(self.rivers_by_tf.keys())
        for tf in targets:
            if tf in self._attached_tfs:
                continue

            river = self.rivers_by_tf.get(tf)
            if river is None:
                continue

            river.subscribe_on_close(self._on_candle_close)
            self._attached_tfs.add(tf)

    def stats(self) -> dict:
        return {
            "attached_tfs": tuple(sorted(self._attached_tfs)),
            "dropped": self._dropped,
        }

    def _on_candle_close(self, tf: str, candle: Candle) -> None:
        event = TFClosedEvent(tf=tf, candle_open_time=candle.open_time)
        try:
            self.out_q.put_nowait(event)
        except Full:
            # Keep the bridge non-blocking even if a bounded queue is used.
            self._dropped += 1
