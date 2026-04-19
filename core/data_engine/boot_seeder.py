from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict

from .candle_river import CandleRiver, Candle
from .tick_river import TickRiver


@dataclass
class BootSeeder:
    rivers_by_tf: Dict[str, CandleRiver]
    tick_river: TickRiver

    def wait_anchor_1m(self, timeout_sec: float = 180.0) -> datetime:
        """
        Wait for the first CLOSED 1m candle (FULL bucket, since aggregator suppresses partial).
        Returns ANCHOR = candle.open_time
        """
        r1m = self.rivers_by_tf["1m"]
        t0 = time.time()

        while True:
            c = r1m.last()
            if c is not None:
                # first emitted 1m candle is your anchor boundary
                return c.open_time

            if time.time() - t0 > timeout_sec:
                raise TimeoutError("Timeout waiting for first 1m closed candle (anchor).")

            # tiny sleep to avoid busy loop
            time.sleep(0.05)

    def cleanup_pre_anchor(self, anchor: datetime) -> dict:
        """
        Drop all ticks and 1s candles strictly before ANCHOR.
        This deletes the pre-anchor noise (partial minute buildup).
        """
        removed_ticks = self.tick_river.drop_before(anchor)
        removed_1s = self.rivers_by_tf["1s"].drop_before(anchor)

        return {"removed_ticks": removed_ticks, "removed_1s": removed_1s}

    def seed_history_before_anchor(self, anchor: datetime, history_by_tf: Dict[str, list[Candle]]) -> None:
        """
        Insert historical candles strictly before ANCHOR using appendleft_many.
        Caller must ensure:
          - candles are CLOSED
          - all have open_time < ANCHOR
          - chronological order oldest->newest
          - no duplicates vs existing live candles
        """
        for tf, candles in history_by_tf.items():
            if tf not in self.rivers_by_tf:
                continue
            if not candles:
                continue

            # enforce strict boundary
            if candles[-1].open_time >= anchor:
                raise ValueError(f"History for {tf} overlaps anchor (>= ANCHOR). Last={candles[-1].open_time}, ANCHOR={anchor}")

            self.rivers_by_tf[tf].appendleft_many(candles)

    def fetch_and_seed_history_before_anchor(
        self,
        anchor: datetime,
        warmup_by_tf: Dict[str, int],
        fetcher: Callable[[str, datetime, int], list[Candle]],
    ) -> Dict[str, dict]:
        """
        Fetch and prepend warmup candles strictly before ANCHOR for each requested TF.
        `fetcher` must return chronological candles (oldest->newest) with open_time < ANCHOR.
        """
        fetched_history: Dict[str, list[Candle]] = {}
        summary: Dict[str, dict] = {}

        for tf, requested in warmup_by_tf.items():
            want = int(requested)
            if want <= 0:
                continue
            if tf not in self.rivers_by_tf:
                summary[tf] = {"requested": want, "fetched": 0, "seeded": 0, "skipped": "missing_river"}
                continue

            candles = fetcher(tf, anchor, want)
            fetched_history[tf] = candles
            summary[tf] = {"requested": want, "fetched": len(candles), "seeded": len(candles)}

        self.seed_history_before_anchor(anchor, fetched_history)
        return summary
