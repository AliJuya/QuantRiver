from __future__ import annotations

from dataclasses import dataclass, field

from core.indicators.registry import DEFAULT_INDICATOR_REGISTRY, IndicatorRegistry
from core.state.market_state import MarketState


@dataclass
class IndicatorEngine:
    registry: IndicatorRegistry = DEFAULT_INDICATOR_REGISTRY
    atr_period: int = 14
    _atr_prev: dict[str, float] = field(default_factory=dict, init=False)
    ema_periods: tuple[int, ...] = (7, 25, 99)
    _ema_prev: dict[str, dict[int, float]] = field(default_factory=dict, init=False)

    def on_tf_close(self, tf: str, candle, state: MarketState) -> None:
        self._attach_atr14(tf, candle, state)
        self._attach_emas(tf, candle)
        for reg in self.registry.registrations_for(tf):
            reg.handler(tf, candle, state)

    def _attach_atr14(self, tf: str, candle, state: MarketState) -> None:
        candles = state.candles.get(tf)
        if not candles or len(candles) < 2:
            return

        prev = candles[-2]
        tr = max(
            float(candle.high) - float(candle.low),
            abs(float(candle.high) - float(prev.close)),
            abs(float(candle.low) - float(prev.close)),
        )

        prev_atr = self._atr_prev.get(tf)
        if prev_atr is None:
            atr = tr
        else:
            atr = ((prev_atr * (self.atr_period - 1)) + tr) / self.atr_period

        self._atr_prev[tf] = atr
        candle.indicators["ATR14"] = float(atr)

    def _attach_emas(self, tf: str, candle) -> None:
        prev_by_period = self._ema_prev.setdefault(tf, {})
        close_price = float(candle.close)

        for period in self.ema_periods:
            prev = prev_by_period.get(period)
            alpha = 2.0 / (float(period) + 1.0)
            ema = close_price if prev is None else prev + alpha * (close_price - prev)
            prev_by_period[period] = float(ema)
            candle.indicators[f"EMA{period}"] = float(ema)
