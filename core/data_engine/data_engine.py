from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Thread
from typing import Dict, Optional, Protocol

from .tick_river import TickRiver
from .candle_river import CandleRiver
from .candle_builder_1s import CandleBuilder1s
from .candle_aggregator_tf import CandleAggregatorTF, tf_to_seconds


class TickSource(Protocol):
    def start(self, *, on_tick) -> None: ...
    def stop(self) -> None: ...


class CandleSource(Protocol):
    def start(self, *, on_candle_close) -> None: ...
    def stop(self) -> None: ...


class Candle1sSource(Protocol):
    def start(self, *, on_candle_1s) -> None: ...
    def stop(self) -> None: ...


@dataclass
class DataEngineConfig:
    input_mode: str  # "ticks", "candles", or legacy "1s"
    source_tf: str = "1s"
    tick_river_maxlen: int = 200_000
    candle_river_maxlen: int = 50_000
    tfs: tuple[str, ...] = ("1m", "5m", "15m")


class DataEngine:
    """
    DataEngine = Rivers + Builders. Independent market-data layer.

    Supported base inputs:
    - ticks -> build closed 1s candles first, then aggregate upward
    - closed candles at any source timeframe -> aggregate upward only
    """

    def __init__(
        self,
        *,
        config: DataEngineConfig,
        tick_source: Optional[TickSource] = None,
        candle_source: Optional[CandleSource] = None,
        candle1s_source: Optional[Candle1sSource] = None,
    ):
        self.cfg = config
        self.tick_source = tick_source
        self.candle_source = candle_source or candle1s_source
        self.candle1s_source = candle1s_source

        mode = str(self.cfg.input_mode).strip().lower()
        if mode == "1s":
            mode = "candles"
            self.cfg.source_tf = "1s"
        if mode not in ("ticks", "candles"):
            raise ValueError("input_mode must be 'ticks', 'candles', or legacy '1s'")

        self.cfg.input_mode = mode
        self.cfg.source_tf = str(self.cfg.source_tf).strip().lower()
        tf_to_seconds(self.cfg.source_tf)

        if self.cfg.input_mode == "ticks":
            if self.tick_source is None:
                raise ValueError("ticks mode requires tick_source")
            self.cfg.source_tf = "1s"
        else:
            if self.candle_source is None:
                raise ValueError("candles mode requires candle_source or candle1s_source")

        self.tick_river = TickRiver(maxlen=self.cfg.tick_river_maxlen)

        self.base_river = CandleRiver(
            self.cfg.source_tf,
            maxlen=self.cfg.candle_river_maxlen,
        )
        self.rivers_by_tf: Dict[str, CandleRiver] = {self.cfg.source_tf: self.base_river}
        for raw_tf in self.cfg.tfs:
            tf = str(raw_tf).strip().lower()
            tf_to_seconds(tf)
            if tf == self.cfg.source_tf:
                continue
            self.rivers_by_tf[tf] = CandleRiver(tf, maxlen=self.cfg.candle_river_maxlen)

        # Backward-compatible alias for the old 1s-centric code path.
        self.river_1s = self.rivers_by_tf.get("1s")

        self.builder_1s: Optional[CandleBuilder1s] = None
        if self.cfg.input_mode == "ticks":
            self.builder_1s = CandleBuilder1s(
                tick_river=self.tick_river,
                out_1s=self.base_river,
            )

        self.agg_tf = CandleAggregatorTF(
            in_base=self.base_river,
            out_by_tf={
                tf: river
                for tf, river in self.rivers_by_tf.items()
                if tf != self.cfg.source_tf
            },
        )

        self._stop = Event()
        self._threads: list[Thread] = []

    def get_candle_river(self, tf: str) -> CandleRiver:
        return self.rivers_by_tf[str(tf).strip().lower()]

    def start(self) -> None:
        self._stop.clear()
        self.agg_tf.attach()

        if self.cfg.input_mode == "ticks":
            self.tick_source.start(on_tick=self.tick_river.push)
            t1 = Thread(
                target=self.builder_1s.run_forever,
                args=(self._stop,),
                name="Builder1s",
                daemon=True,
            )
            t1.start()
            self._threads.append(t1)
            return

        try:
            self.candle_source.start(on_candle_close=self.base_river.push_closed)
        except TypeError:
            if self.cfg.source_tf != "1s":
                raise
            self.candle_source.start(on_candle_1s=self.base_river.push_closed)

    def stop(self) -> None:
        self._stop.set()

        try:
            if self.tick_source:
                self.tick_source.stop()
        except Exception:
            pass

        try:
            if self.candle_source:
                self.candle_source.stop()
        except Exception:
            pass

        for river in self.rivers_by_tf.values():
            try:
                river.stop_notifier()
            except Exception:
                pass

        try:
            self.agg_tf.stop()
        except Exception:
            pass

    def stats(self) -> dict:
        return {
            "input_mode": self.cfg.input_mode,
            "source_tf": self.cfg.source_tf,
            "tick_river": self.tick_river.stats(),
            "candle_rivers": {tf: river.stats() for tf, river in self.rivers_by_tf.items()},
        }
