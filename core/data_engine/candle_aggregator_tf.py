from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import Dict, Optional

from .candle_river import CandleRiver, Candle


def tf_to_seconds(tf: str) -> int:
    raw = str(tf).strip().lower()
    if len(raw) < 2:
        raise ValueError(f"Unsupported tf: {tf}")

    unit = raw[-1]
    try:
        value = int(raw[:-1])
    except ValueError as exc:
        raise ValueError(f"Unsupported tf: {tf}") from exc

    if value <= 0:
        raise ValueError(f"Unsupported tf: {tf}")

    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86_400
    raise ValueError(f"Unsupported tf: {tf}")


def floor_time(dt: datetime, tf_sec: int) -> datetime:
    ts = int(dt.timestamp())
    flo = (ts // tf_sec) * tf_sec
    return datetime.fromtimestamp(flo, tz=dt.tzinfo)


@dataclass
class CandleAggregatorTF:
    """
    Event-driven aggregator from one closed base timeframe into higher ones.

    - Does not consume/remove base history
    - Suppresses the first partial bucket for each output timeframe
    - Works for any upward multiple, for example 1s->1m, 5m->1h, 1h->1d
    """

    in_base: CandleRiver
    out_by_tf: Dict[str, CandleRiver]

    def __post_init__(self):
        self.base_tf = str(self.in_base.tf).strip().lower()
        self.base_tf_sec = tf_to_seconds(self.base_tf)

        normalized_out: Dict[str, CandleRiver] = {}
        for raw_tf, river in self.out_by_tf.items():
            tf = str(raw_tf).strip().lower()
            if str(river.tf).strip().lower() != tf:
                raise ValueError(
                    f"out_by_tf key mismatch: key={raw_tf} river.tf={river.tf}"
                )

            tf_sec = tf_to_seconds(tf)
            if tf_sec <= self.base_tf_sec:
                raise ValueError(
                    f"Output tf must be higher than base tf: base={self.base_tf} out={tf}"
                )
            if tf_sec % self.base_tf_sec != 0:
                raise ValueError(
                    f"Output tf must be an integer multiple of base tf: "
                    f"base={self.base_tf} out={tf}"
                )
            normalized_out[tf] = river

        self.out_by_tf = normalized_out
        self._cur: Dict[str, Optional[Candle]] = {tf: None for tf in self.out_by_tf}
        self._bucket: Dict[str, Optional[datetime]] = {tf: None for tf in self.out_by_tf}
        self._can_emit: Dict[str, bool] = {tf: False for tf in self.out_by_tf}
        self._subscribed = False
        self._in_q: SimpleQueue[Optional[Candle]] = SimpleQueue()
        self._worker_stop = Event()
        self._worker: Optional[Thread] = None

    def attach(self) -> None:
        if not self._subscribed:
            self.in_base.subscribe_on_close(self._on_source_close_cb)
            self._subscribed = True
        self._ensure_worker()

    def stop(self) -> None:
        self._worker_stop.set()
        self._in_q.put(None)

        worker = self._worker
        if worker and worker.is_alive():
            worker.join(timeout=1.0)
        self._worker = None

    def _ensure_worker(self) -> None:
        worker = self._worker
        if worker and worker.is_alive():
            return

        self._in_q = SimpleQueue()
        self._worker_stop.clear()
        self._worker = Thread(
            target=self._worker_loop,
            name="CandleAggregatorTF",
            daemon=True,
        )
        self._worker.start()

    def _worker_loop(self) -> None:
        while True:
            try:
                candle = self._in_q.get(timeout=0.25)
            except Empty:
                if self._worker_stop.is_set():
                    return
                continue

            if candle is None:
                if self._worker_stop.is_set():
                    return
                continue

            self.on_source_close(candle)

    def _on_source_close_cb(self, tf: str, candle: Candle) -> None:
        if str(tf).strip().lower() != self.base_tf:
            return
        self._in_q.put(candle)

    def on_source_close(self, candle: Candle) -> None:
        candle_tf = str(candle.tf).strip().lower()
        if candle_tf != self.base_tf:
            raise ValueError(
                f"Candle tf mismatch: expected base tf {self.base_tf}, got {candle.tf}"
            )

        for tf, out_river in self.out_by_tf.items():
            tf_sec = tf_to_seconds(tf)
            bucket = floor_time(candle.open_time, tf_sec)

            cur = self._cur[tf]
            cur_bucket = self._bucket[tf]

            if cur is None:
                self._start(tf, bucket, tf_sec, candle)
                self._can_emit[tf] = False
                continue

            if bucket == cur_bucket:
                self._update(cur, candle)
                continue

            if cur_bucket is not None and bucket < cur_bucket:
                continue

            if self._can_emit[tf]:
                cur.close_time = bucket
                out_river.push_closed(cur)
            else:
                self._can_emit[tf] = True

            self._start(tf, bucket, tf_sec, candle)

    # Backward-compatible alias used by the older 1s runner path.
    def on_1s_close(self, candle: Candle) -> None:
        self.on_source_close(candle)

    def _start(self, tf: str, bucket: datetime, tf_sec: int, candle: Candle) -> None:
        self._bucket[tf] = bucket
        self._cur[tf] = Candle(
            tf=tf,
            open_time=bucket,
            close_time=bucket + timedelta(seconds=tf_sec),
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
        )

    @staticmethod
    def _update(cur: Candle, candle: Candle) -> None:
        if candle.high > cur.high:
            cur.high = candle.high
        if candle.low < cur.low:
            cur.low = candle.low
        cur.close = candle.close
        cur.volume += candle.volume
