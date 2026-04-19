from __future__ import annotations

from typing import Any

from .base import StrategyBase


def _to_epoch_ms(value: Any) -> int:
    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        return int(float(timestamp()) * 1000)
    return int(value)


class EMACross5mStrategy(StrategyBase):
    """
    Simple 5m EMA cross strategy kept as a public example.

    - enter long when fast EMA crosses above slow EMA
    - enter short when fast EMA crosses below slow EMA
    - supports ATR-based or cash-distance SL/TP planning
    - allow the position handler to reverse on the opposite cross
    """

    name = "EMA_CROSS_5m"
    tfs_needed = ("5m",)
    warmup_req = {"5m": 80}

    def __init__(
        self,
        *,
        name: str | None = None,
        fast_len: int = 12,
        slow_len: int = 48,
        stop_mode: str = "atr",
        stop_value: float = 1.5,
        target_mode: str = "atr",
        target_value: float = 3.0,
        min_body_atr: float = 0.05,
    ) -> None:
        self.name = str(name or self.name)
        self.fast_len = int(fast_len)
        self.slow_len = int(slow_len)
        self.stop_mode = str(stop_mode).strip().lower()
        self.stop_value = float(stop_value)
        self.target_mode = str(target_mode).strip().lower()
        self.target_value = float(target_value)
        self.min_body_atr = float(min_body_atr)
        if self.fast_len < 2 or self.slow_len <= self.fast_len:
            raise ValueError("Require 2 <= fast_len < slow_len")
        if self.stop_mode not in {"atr", "usd"} or self.target_mode not in {"atr", "usd"}:
            raise ValueError("stop_mode and target_mode must be 'atr' or 'usd'")
        if self.stop_value <= 0.0 or self.target_value <= 0.0:
            raise ValueError("stop_value and target_value must be > 0")

        self._last_ts = None
        self._ema_fast: float | None = None
        self._ema_slow: float | None = None
        self._prev_fast: float | None = None
        self._prev_slow: float | None = None

    @staticmethod
    def _ema(prev: float | None, value: float, length: int) -> float:
        alpha = 2.0 / (float(length) + 1.0)
        return float(value) if prev is None else float(prev + alpha * (value - prev))

    @staticmethod
    def _atr(candle) -> float:
        indicators = getattr(candle, "indicators", None) or {}
        raw = indicators.get("ATR14")
        if raw is not None:
            try:
                return max(1e-9, float(raw))
            except Exception:
                pass
        return max(1e-9, float(candle.high) - float(candle.low))

    @staticmethod
    def _distance_from_mode(mode: str, value: float, atr: float, size: float = 1.0) -> float:
        if mode == "atr":
            return float(value) * max(1e-9, float(atr))
        return float(value) / max(1e-9, float(size))

    def _entry_payload(self, *, action: str, candle, atr: float) -> dict[str, Any]:
        entry = float(candle.close)
        position_size = 1.0
        stop_distance = self._distance_from_mode(self.stop_mode, self.stop_value, atr, position_size)
        target_distance = self._distance_from_mode(self.target_mode, self.target_value, atr, position_size)
        if action == "BUY":
            sl = entry - stop_distance
            tp = entry + target_distance
        else:
            sl = entry + stop_distance
            tp = entry - target_distance

        risk = abs(entry - sl)
        reward = abs(tp - entry)
        return {
            "action": action,
            "tf": "5m",
            "strategy_id": self.strategy_id,
            "position_size": float(position_size),
            "entry_price": float(entry),
            "entry": float(entry),
            "sl_mode": str(self.stop_mode),
            "sl_value": float(self.stop_value),
            "tp_mode": str(self.target_mode),
            "tp_value": float(self.target_value),
            "init_sl_price": float(sl),
            "init_tp_price": float(tp),
            "risk_usd": float(risk),
            "reward_usd": float(reward),
            "rr_planned": float(reward / max(risk, 1e-9)),
            "signal_ts_ms": _to_epoch_ms(candle.open_time),
            "entry_ts_ms": _to_epoch_ms(candle.close_time),
            "meta": {
                "mode": "EMA_CROSS",
                "fast_len": int(self.fast_len),
                "slow_len": int(self.slow_len),
                "ema_fast": float(self._ema_fast or 0.0),
                "ema_slow": float(self._ema_slow or 0.0),
                "stop_mode": str(self.stop_mode),
                "stop_value": float(self.stop_value),
                "target_mode": str(self.target_mode),
                "target_value": float(self.target_value),
            },
        }

    def on_tf_close(self, tf: str, candle, state) -> dict[str, Any] | None:
        del state
        if tf != "5m":
            return None

        ts = getattr(candle, "open_time", None)
        if ts is None:
            return None
        if self._last_ts is not None and ts <= self._last_ts:
            return None
        self._last_ts = ts

        close_px = float(candle.close)
        open_px = float(candle.open)
        atr = self._atr(candle)
        body = abs(close_px - open_px)

        self._prev_fast = self._ema_fast
        self._prev_slow = self._ema_slow
        self._ema_fast = self._ema(self._ema_fast, close_px, self.fast_len)
        self._ema_slow = self._ema(self._ema_slow, close_px, self.slow_len)

        if self._prev_fast is None or self._prev_slow is None:
            return None
        if body < self.min_body_atr * atr:
            return None

        crossed_up = self._prev_fast <= self._prev_slow and self._ema_fast > self._ema_slow
        crossed_down = self._prev_fast >= self._prev_slow and self._ema_fast < self._ema_slow

        if crossed_up:
            return self._entry_payload(action="BUY", candle=candle, atr=atr)
        if crossed_down:
            return self._entry_payload(action="SELL", candle=candle, atr=atr)
        return None
