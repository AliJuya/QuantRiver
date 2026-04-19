from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from statistics import pstdev
from typing import Any

from .contracts import DCState, DFAState, HurstBucket, KalmanTrendState, StructureLabel, StructureState


def _candle_close(candle: Any) -> float:
    if isinstance(candle, dict):
        for name in ("close", "c", "C"):
            if name in candle:
                return float(candle[name])
    else:
        for name in ("close", "c", "C"):
            if hasattr(candle, name):
                return float(getattr(candle, name))
    raise KeyError("Missing close price on candle.")


def _candle_range(candle: Any) -> float:
    if isinstance(candle, dict):
        high = float(candle.get("high", candle.get("h", candle.get("H", 0.0))))
        low = float(candle.get("low", candle.get("l", candle.get("L", 0.0))))
    else:
        high = float(getattr(candle, "high", getattr(candle, "h", 0.0)))
        low = float(getattr(candle, "low", getattr(candle, "l", 0.0)))
    return max(0.0, high - low)


@dataclass(frozen=True)
class StructureEngineConfig:
    base_tf: str = "1h"


class StructureEngine:
    """
    Public placeholder for the private structure engine.
    """

    def __init__(self, *, cfg: StructureEngineConfig | None = None, market_accessor=None, vol_accessor=None) -> None:
        self.cfg = cfg or StructureEngineConfig()
        self.market_accessor = market_accessor
        self.vol_accessor = vol_accessor
        self.last_state: StructureState | None = None
        self._close_history: deque[float] = deque(maxlen=256)
        self._range_history: deque[float] = deque(maxlen=256)

    def on_candle_close(self, *, tf: str, kline: Any, vol_ctx: dict[str, Any] | None = None) -> StructureState:
        del tf
        self._close_history.append(_candle_close(kline))
        self._range_history.append(_candle_range(kline))

        closes = list(self._close_history)
        if len(closes) >= 8:
            slope = (closes[-1] - closes[-8]) / 7.0
        elif len(closes) >= 2:
            slope = closes[-1] - closes[-2]
        else:
            slope = 0.0

        if len(closes) >= 20:
            sigma_price = pstdev(closes[-20:])
        elif len(closes) >= 2:
            sigma_price = pstdev(closes)
        else:
            sigma_price = 0.0

        vol_sigma = 0.0
        if isinstance(vol_ctx, dict):
            vol_sigma = float(vol_ctx.get("sigma_effective", vol_ctx.get("sigma_fast", 0.0)) or 0.0)

        trend_strength = 0.0 if sigma_price <= 1e-12 else slope / max(sigma_price, 1e-12)
        trend_strength = max(-3.0, min(3.0, trend_strength))

        recent_ranges = list(self._range_history)[-20:]
        avg_range = sum(recent_ranges) / len(recent_ranges) if recent_ranges else 0.0
        max_range = max(recent_ranges) if recent_ranges else 0.0
        compression_score = 0.0 if max_range <= 1e-12 else max(0.0, 1.0 - (avg_range / max_range))
        expansion_score = 1.0 - compression_score

        if compression_score >= 0.70:
            label = StructureLabel.COMPRESSION_COIL
        elif abs(trend_strength) >= 0.35:
            label = StructureLabel.TREND_CONTINUATION
        else:
            label = StructureLabel.TREND_PULLBACK

        hurst = 0.65 if abs(trend_strength) >= 0.35 else (0.35 if compression_score >= 0.70 else 0.50)
        if hurst >= 0.60:
            bucket = HurstBucket.TREND
        elif hurst <= 0.40:
            bucket = HurstBucket.CHOP
        else:
            bucket = HurstBucket.BALANCED

        state = StructureState(
            label=label,
            dc=DCState(
                compression_score=float(compression_score),
                expansion_score=float(expansion_score),
                dc_rate=float(abs(trend_strength)),
                avg_leg_size=float(avg_range),
                overshoot_ratio=float(expansion_score * 0.5),
                leg_asymmetry=float(max(-1.0, min(1.0, trend_strength))),
                intrinsic_vol=float(vol_sigma),
                confidence=float(min(1.0, abs(trend_strength))),
            ),
            trend=KalmanTrendState(
                level=float(closes[-1]) if closes else 0.0,
                slope=float(slope),
                trend_strength=float(trend_strength),
                slope_variance=float(sigma_price),
                trend_confidence=float(min(1.0, abs(trend_strength))),
                turning_point_score=float(max(0.0, 1.0 - abs(trend_strength))),
            ),
            dfa=DFAState(
                hurst=float(hurst),
                bucket=bucket,
                fit_r2=float(min(1.0, abs(trend_strength))),
                stability=float(max(0.0, 1.0 - compression_score * 0.5)),
                confidence=float(0.5 + min(0.5, abs(trend_strength) * 0.2)),
            ),
            alignment_score=float(abs(trend_strength)),
            struct_energy=float(expansion_score + abs(trend_strength)),
            shift_score=float(expansion_score),
            turning_point_pressure=float(max(0.0, 1.0 - abs(trend_strength))),
            exhaustion_score=float(compression_score * 0.5),
            obs_error_norm=float(vol_sigma),
        )
        self.last_state = state
        return state
