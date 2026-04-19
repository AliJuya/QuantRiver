from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .base import StrategyBase


def _to_epoch_ms(value: Any) -> int:
    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        return int(float(timestamp()) * 1000)
    return int(value)


class OpeningRangeBreakout5m(StrategyBase):
    """
    U.S. regular-hours opening range breakout on 5m bars.

    Rules:
      - Trade only on Monday-Friday in the configured session timezone
      - Build the opening range from the first N bars after the session open
      - Allow at most one entry per session during a limited post-open window
      - Force flat at the session close; never hold overnight
    """

    name = "ORB_5m"
    tfs_needed = ("5m",)
    warmup_req = {"5m": 24}

    TF_SEC = 5 * 60
    MIN_ATR = 1e-9

    def __init__(
        self,
        *,
        name: str | None = None,
        session_tz: str = "America/New_York",
        session_label: str = "US",
        session_open_hour: int = 9,
        session_open_minute: int = 30,
        session_close_hour: int = 16,
        session_close_minute: int = 0,
        opening_bars: int = 6,
        entry_deadline_bars: int = 24,
        break_buffer_atr: float = 0.10,
        stop_buffer_atr: float = 0.05,
        confirm_body_atr: float = 0.10,
        rr_target: float = 2.0,
        min_range_atr: float = 0.50,
        max_range_atr: float = 3.00,
    ) -> None:
        self.name = str(name or self.name)
        self.session_tz_name = str(session_tz)
        self.session_label = str(session_label)
        self.session_open_hour = int(session_open_hour)
        self.session_open_minute = int(session_open_minute)
        self.session_close_hour = int(session_close_hour)
        self.session_close_minute = int(session_close_minute)
        self.opening_bars = int(opening_bars)
        self.entry_deadline_bars = int(entry_deadline_bars)
        self.break_buffer_atr = float(break_buffer_atr)
        self.stop_buffer_atr = float(stop_buffer_atr)
        self.confirm_body_atr = float(confirm_body_atr)
        self.rr_target = float(rr_target)
        self.min_range_atr = float(min_range_atr)
        self.max_range_atr = float(max_range_atr)
        self._session_tz = ZoneInfo(self.session_tz_name)

        if self.opening_bars < 1:
            raise ValueError("opening_bars must be >= 1")
        if self.entry_deadline_bars < 1:
            raise ValueError("entry_deadline_bars must be >= 1")
        if self.rr_target <= 0.0:
            raise ValueError("rr_target must be > 0")
        if self.max_range_atr < self.min_range_atr:
            raise ValueError("max_range_atr must be >= min_range_atr")
        if not 0 <= self.session_open_hour <= 23:
            raise ValueError("session_open_hour must be in [0, 23]")
        if not 0 <= self.session_close_hour <= 23:
            raise ValueError("session_close_hour must be in [0, 23]")
        if self.session_open_minute not in {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}:
            raise ValueError("session_open_minute must align to 5m boundaries")
        if self.session_close_minute not in {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}:
            raise ValueError("session_close_minute must align to 5m boundaries")

        self._last_ts = None
        self._session_day = None
        self._opening_seen = 0
        self._opening_high = 0.0
        self._opening_low = 0.0
        self._range_checked = False
        self._range_size = 0.0
        self._range_atr = 0.0
        self._day_invalid = False
        self._trade_taken = False
        self._session_close_sent = False

    def _reset_session(self, session_day) -> None:
        self._session_day = session_day
        self._opening_seen = 0
        self._opening_high = 0.0
        self._opening_low = 0.0
        self._range_checked = False
        self._range_size = 0.0
        self._range_atr = 0.0
        self._day_invalid = False
        self._trade_taken = False
        self._session_close_sent = False

    def _session_bounds(self, ts) -> tuple[datetime, datetime]:
        local_open = ts.astimezone(self._session_tz)
        session_day = local_open.date()
        start_local = datetime.combine(
            session_day,
            dt_time(self.session_open_hour, self.session_open_minute),
            tzinfo=self._session_tz,
        )
        end_local = datetime.combine(
            session_day,
            dt_time(self.session_close_hour, self.session_close_minute),
            tzinfo=self._session_tz,
        )
        return start_local, end_local

    def _bar_index(self, *, session_start_local: datetime, candle_open_local: datetime) -> int:
        elapsed_s = int((candle_open_local - session_start_local).total_seconds())
        if elapsed_s < 0:
            return -1
        return elapsed_s // self.TF_SEC

    def _is_session_weekday(self, session_start_local: datetime) -> bool:
        return session_start_local.weekday() < 5

    def _is_regular_session_bar(
        self,
        *,
        session_start_local: datetime,
        session_end_local: datetime,
        candle_open_local: datetime,
        candle_close_local: datetime,
    ) -> bool:
        if not self._is_session_weekday(session_start_local):
            return False
        return bool(
            candle_open_local >= session_start_local
            and candle_open_local < session_end_local
            and candle_close_local <= session_end_local
        )

    def _consume_opening_bar(self, *, bar_idx: int, high: float, low: float) -> bool:
        if bar_idx < self.opening_bars:
            if self._opening_seen == 0:
                self._opening_high = float(high)
                self._opening_low = float(low)
            else:
                self._opening_high = max(float(self._opening_high), float(high))
                self._opening_low = min(float(self._opening_low), float(low))
            self._opening_seen += 1
            return True

        if self._opening_seen < self.opening_bars:
            self._day_invalid = True
        return False

    def _validate_range(self, *, atr: float) -> bool:
        self._range_checked = True
        self._range_size = max(0.0, float(self._opening_high) - float(self._opening_low))
        self._range_atr = float(self._range_size / max(self.MIN_ATR, atr))
        if self._range_size <= 0.0:
            self._day_invalid = True
            return False
        if self._range_atr < self.min_range_atr or self._range_atr > self.max_range_atr:
            self._day_invalid = True
            return False
        return True

    def _entry_deadline_index(self) -> int:
        return self.opening_bars + self.entry_deadline_bars - 1

    def _emit_entry(self, *, action: str, candle, atr: float) -> dict[str, Any]:
        entry_price = float(candle.close)
        atr_value = max(self.MIN_ATR, float(atr))

        if action == "BUY":
            sl_price = float(self._opening_low) - self.stop_buffer_atr * atr_value
            risk = max(self.MIN_ATR, entry_price - sl_price)
            tp_price = entry_price + self.rr_target * risk
        else:
            sl_price = float(self._opening_high) + self.stop_buffer_atr * atr_value
            risk = max(self.MIN_ATR, sl_price - entry_price)
            tp_price = entry_price - self.rr_target * risk

        reward = abs(tp_price - entry_price)
        rr_planned = reward / risk if risk > 0.0 else 0.0

        return {
            "action": action,
            "tf": "5m",
            "strategy_id": self.strategy_id,
            "entry_price": float(entry_price),
            "entry": float(entry_price),
            "custom_sl": float(sl_price),
            "custom_tp": float(tp_price),
            "init_sl_price": float(sl_price),
            "init_tp_price": float(tp_price),
            "risk_usd": float(risk),
            "reward_usd": float(reward),
            "rr_planned": float(rr_planned),
            "signal_ts_ms": _to_epoch_ms(candle.open_time),
            "entry_ts_ms": _to_epoch_ms(candle.close_time),
            "meta": {
                "mode": "ORB",
                "session_label": str(self.session_label),
                "session_tz": str(self.session_tz_name),
                "session_day": str(self._session_day),
                "session_open": f"{self.session_open_hour:02d}:{self.session_open_minute:02d}",
                "session_close": f"{self.session_close_hour:02d}:{self.session_close_minute:02d}",
                "opening_bars": int(self.opening_bars),
                "entry_deadline_bars": int(self.entry_deadline_bars),
                "opening_high": float(self._opening_high),
                "opening_low": float(self._opening_low),
                "opening_range": float(self._range_size),
                "range_atr": float(self._range_atr),
                "break_buffer_atr": float(self.break_buffer_atr),
                "stop_buffer_atr": float(self.stop_buffer_atr),
                "confirm_body_atr": float(self.confirm_body_atr),
                "rr_target": float(self.rr_target),
            },
        }

    def _emit_close(self, *, candle, reason: str) -> dict[str, Any]:
        self._session_close_sent = True
        return {
            "action": "CLOSE",
            "tf": "5m",
            "strategy_id": self.strategy_id,
            "reason": reason,
            "signal_ts_ms": _to_epoch_ms(candle.open_time),
            "entry_ts_ms": _to_epoch_ms(candle.close_time),
            "meta": {
                "mode": "ORB",
                "session_label": str(self.session_label),
                "session_tz": str(self.session_tz_name),
                "session_day": str(self._session_day),
                "session_open": f"{self.session_open_hour:02d}:{self.session_open_minute:02d}",
                "session_close": f"{self.session_close_hour:02d}:{self.session_close_minute:02d}",
                "opening_high": float(self._opening_high),
                "opening_low": float(self._opening_low),
                "opening_range": float(self._range_size),
                "range_atr": float(self._range_atr),
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

        local_open = ts.astimezone(self._session_tz)
        local_close = getattr(candle, "close_time", None)
        if local_close is None:
            return None
        local_close = local_close.astimezone(self._session_tz)

        session_start_local, session_end_local = self._session_bounds(ts)
        session_day = session_start_local.date()

        if self._session_day != session_day and self._is_regular_session_bar(
            session_start_local=session_start_local,
            session_end_local=session_end_local,
            candle_open_local=local_open,
            candle_close_local=local_close,
        ):
            self._reset_session(session_day)

        if not self._is_regular_session_bar(
            session_start_local=session_start_local,
            session_end_local=session_end_local,
            candle_open_local=local_open,
            candle_close_local=local_close,
        ):
            return None

        if self._session_day is None:
            self._reset_session(session_day)

        bar_idx = self._bar_index(session_start_local=session_start_local, candle_open_local=local_open)
        high = float(candle.high)
        low = float(candle.low)
        open_price = float(candle.open)
        close = float(candle.close)

        if self._trade_taken and not self._session_close_sent and local_close >= session_end_local:
            return self._emit_close(candle=candle, reason="ORB_SESSION_CLOSE")

        if self._consume_opening_bar(bar_idx=bar_idx, high=high, low=low):
            return None

        if self._day_invalid or self._trade_taken:
            return None

        if bar_idx > self._entry_deadline_index():
            return None

        indicators = getattr(candle, "indicators", None) or {}
        atr = float(indicators.get("ATR14") or max(self.MIN_ATR, abs(high - low)))
        atr = max(self.MIN_ATR, atr)

        if not self._range_checked and not self._validate_range(atr=atr):
            return None

        body = abs(close - open_price)
        if body < self.confirm_body_atr * atr:
            return None

        break_buffer = self.break_buffer_atr * atr
        bullish_break = close > (float(self._opening_high) + break_buffer) and close > open_price
        bearish_break = close < (float(self._opening_low) - break_buffer) and close < open_price

        if bullish_break:
            self._trade_taken = True
            return self._emit_entry(action="BUY", candle=candle, atr=atr)

        if bearish_break:
            self._trade_taken = True
            return self._emit_entry(action="SELL", candle=candle, atr=atr)

        return None
