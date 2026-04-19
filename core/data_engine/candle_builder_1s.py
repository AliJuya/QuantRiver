from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .tick_river import TickRiver, Tick
from .candle_river import CandleRiver, Candle


def floor_to_second(dt: datetime) -> datetime:
    # expects aware or naive; keeps tzinfo
    return dt.replace(microsecond=0)


@dataclass
class CandleBuilder1s:
    """
    Consumes ticks from TickRiver and emits CLOSED 1s candles into CandleRiver("1s").
    Closed-candle driven: candle boundaries are derived from tick timestamps.
    """
    tick_river: TickRiver
    out_1s: CandleRiver

    def __post_init__(self):
        if self.out_1s.tf != "1s":
            raise ValueError("CandleBuilder1s requires out_1s.tf == '1s'")

        self._cur: Candle | None = None
        self._cur_start: datetime | None = None

    def on_tick(self, t: Tick) -> None:
        sec = floor_to_second(t.ts)
        if self._cur is None:
            self._start_new(sec, t)
            return

        # If tick is still within current second
        if sec == self._cur_start:
            self._update(self._cur, t)
            return

        # If tick jumped forward: close current candle and emit,
        # then fill gap seconds with flat candles at last close (optional decision).
        # We will:
        #   - CLOSE current at cur_start+1s
        #   - For missing seconds, emit flat candles (volume=0)
        #   - Then start on sec with new tick
        if sec < self._cur_start:
            # backwards time is invalid in live; drop
            return

        last_close = self._cur.close
        last_close_time = self._cur_start + timedelta(seconds=1)

        # close current
        self._cur.close_time = last_close_time
        self.out_1s.push_closed(self._cur)

        # fill gaps (sec - last_close_time) seconds
        gap = int((sec - last_close_time).total_seconds())
        for i in range(gap):
            ot = last_close_time + timedelta(seconds=i)
            ct = ot + timedelta(seconds=1)
            flat = Candle(
                tf="1s",
                open_time=ot,
                close_time=ct,
                open=last_close,
                high=last_close,
                low=last_close,
                close=last_close,
                volume=0.0,
            )
            self.out_1s.push_closed(flat)

        # start new
        self._start_new(sec, t)

    def _start_new(self, sec: datetime, t: Tick) -> None:
        self._cur_start = sec
        self._cur = Candle(
            tf="1s",
            open_time=sec,
            close_time=sec + timedelta(seconds=1),
            open=t.price,
            high=t.price,
            low=t.price,
            close=t.price,
            volume=float(t.qty),
        )

    @staticmethod
    def _update(c: Candle, t: Tick) -> None:
        if t.price > c.high:
            c.high = t.price
        if t.price < c.low:
            c.low = t.price
        c.close = t.price
        c.volume += float(t.qty)

    def run_forever(self, stop_flag) -> None:
        """
        Builder loop. stop_flag: threading.Event-like with is_set()
        """
        while not stop_flag.is_set():
            tick = self.tick_river.pop(timeout=0.25)
            if tick is None:
                continue
            self.on_tick(tick)