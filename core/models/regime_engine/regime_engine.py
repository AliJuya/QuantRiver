from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum


class RegimeState(str, Enum):
    TREND_CONTINUATION = "trend_continuation"
    TREND_PULLBACK = "trend_pullback"
    COMPRESSION_COIL = "compression_coil"
    EXHAUSTION_REVERSAL_WATCH = "exhaustion_reversal_watch"
    NEUTRAL = "neutral"


@dataclass(frozen=True)
class RegimeContext:
    regime_label: str
    change_point_prob: float
    forecast_no_trade_prob_h16: float
    forecast_reversal_prob_h8: float
    transition_risk: float
    state_tf: str = "1h"
    regime_tf: str = "1h"

    def to_dict(self) -> dict[str, object]:
        return {
            "regime_label": self.regime_label,
            "change_point_prob": float(self.change_point_prob),
            "forecast_no_trade_prob_h16": float(self.forecast_no_trade_prob_h16),
            "forecast_reversal_prob_h8": float(self.forecast_reversal_prob_h8),
            "transition_risk": float(self.transition_risk),
            "state_tf": str(self.state_tf),
            "regime_tf": str(self.regime_tf),
        }


class RegimeEngine:
    """
    Public placeholder for the private regime engine.
    """

    def __init__(self) -> None:
        pass

    def update(self, *, ts: int, r_1h: float, log_sigma_slow: float) -> RegimeContext:
        del ts
        abs_ret = abs(float(r_1h))
        sigma = math.exp(float(log_sigma_slow))

        if sigma < 0.002 and abs_ret < 0.0015:
            regime = RegimeState.COMPRESSION_COIL
        elif abs_ret > 0.004 and sigma > 0.003:
            regime = RegimeState.TREND_CONTINUATION
        elif abs_ret < 0.001 and sigma > 0.004:
            regime = RegimeState.EXHAUSTION_REVERSAL_WATCH
        elif abs_ret > 0.0015:
            regime = RegimeState.TREND_PULLBACK
        else:
            regime = RegimeState.NEUTRAL

        change_point_prob = min(1.0, abs_ret / max(1e-12, sigma + 1e-6))
        forecast_reversal_prob_h8 = min(1.0, sigma * 120.0) if abs_ret < 0.0015 else max(0.0, 0.25 - abs_ret * 20.0)
        forecast_no_trade_prob_h16 = 0.75 if regime == RegimeState.COMPRESSION_COIL else (0.55 if regime == RegimeState.NEUTRAL else 0.15)
        transition_risk = min(1.0, 0.5 * change_point_prob + 0.5 * sigma * 100.0)

        return RegimeContext(
            regime_label=str(regime.value),
            change_point_prob=float(change_point_prob),
            forecast_no_trade_prob_h16=float(max(0.0, min(1.0, forecast_no_trade_prob_h16))),
            forecast_reversal_prob_h8=float(max(0.0, min(1.0, forecast_reversal_prob_h8))),
            transition_risk=float(max(0.0, min(1.0, transition_risk))),
            state_tf="1h",
            regime_tf="1h",
        )
