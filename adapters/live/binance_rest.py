# adapters/live/binance_rest.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import requests

from core.data_engine.candle_river import Candle


_INTERVAL_MAP = {
    "1s": "1s",   # (Binance futures doesn't support 1s klines; keep for interface completeness)
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
}


@dataclass
class BinanceRestConfig:
    base_url: str = "https://fapi.binance.com"
    timeout_sec: int = 10


class BinanceRest:
    """
    Minimal REST client for klines warmup seeding.
    Futures endpoint: GET /fapi/v1/klines
    """
    def __init__(self, cfg: BinanceRestConfig | None = None):
        self.cfg = cfg or BinanceRestConfig()
        self._sess = requests.Session()

    def fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int = 1000,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
    ) -> list[list]:
        if interval not in _INTERVAL_MAP:
            raise ValueError(f"unsupported interval: {interval}")

        url = f"{self.cfg.base_url}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": _INTERVAL_MAP[interval], "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)

        r = self._sess.get(url, params=params, timeout=self.cfg.timeout_sec)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _kline_to_candle(tf: str, row: list) -> Candle:
        # [ openTime, open, high, low, close, volume, closeTime, ... ]
        ot_ms = int(row[0])
        ct_ms = int(row[6])
        ot = datetime.fromtimestamp(ot_ms / 1000, tz=timezone.utc)
        ct = datetime.fromtimestamp(ct_ms / 1000, tz=timezone.utc)
        return Candle(
            tf=tf,
            open_time=ot,
            close_time=ct,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
        )

    def fetch_candles_before_anchor(
        self,
        *,
        symbol: str,
        tf: str,
        anchor_open_time: datetime,
        count: int,
    ) -> List[Candle]:
        """
        Returns exactly `count` CLOSED candles with open_time < anchor_open_time (if available).
        Pages backward using endTime.
        """
        if tf == "1s":
            raise ValueError("Binance futures REST klines do not support 1s. Seed 1s from ticks only.")

        anchor_ms = int(anchor_open_time.timestamp() * 1000)
        want = int(count)
        out: list[Candle] = []

        # We want candles strictly BEFORE anchor open_time:
        # so set endTime = anchor_ms - 1
        end_ms = anchor_ms - 1

        while len(out) < want:
            batch = self.fetch_klines(symbol=symbol, interval=tf, limit=1000, end_time_ms=end_ms)
            if not batch:
                break

            candles = [self._kline_to_candle(tf, row) for row in batch]

            # Filter strictly open_time < anchor
            candles = [c for c in candles if c.open_time < anchor_open_time]
            if not candles:
                break

            # Binance returns ascending by time; take from the end backwards
            out = candles + out  # prepend older batch

            # Move end_ms backward to before the earliest candle we just got
            earliest_ot_ms = int(candles[0].open_time.timestamp() * 1000)
            end_ms = earliest_ot_ms - 1

            # Safety break if API gives us no progress
            if end_ms <= 0:
                break

        # Keep last `want` (closest to anchor), still strictly < anchor
        out = out[-want:]
        return out