from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from statistics import pstdev
from typing import Any

from .volatility_engine_config import VOLATILITY_ENGINE_CONFIG


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


@dataclass(frozen=True)
class VolatilityState:
    pair_key: str
    fast_tf: str
    slow_tf: str
    sigma_fast: float
    sigma_slow: float
    sigma_latent_fast: float
    sigma_latent_slow: float
    sigma_effective: float
    vol_bucket: str
    transition_risk: float
    shock_score: float
    jump_intensity: float
    uncertainty: float
    term_curvature: float


class VolatilityEngine:
    """
    Public placeholder for the private volatility engine.

    The implementation intentionally uses lightweight realized-vol style statistics so
    the adapter and bundle flow remain demonstrable without shipping proprietary logic.
    """

    def __init__(self) -> None:
        self._close_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=512))
        self._return_history: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=512))
        self._last_pair_state: dict[str, VolatilityState] = {}

    def _update_tf(self, tf: str, close_px: float) -> None:
        history = self._close_history[tf]
        if history:
            prev_close = history[-1]
            ret = 0.0 if prev_close <= 0.0 or close_px <= 0.0 else math.log(close_px / prev_close)
            self._return_history[tf].append(float(ret))
        history.append(float(close_px))

    @staticmethod
    def _sigma(values: deque[float], window: int = 20) -> float:
        sample = list(values)[-int(window) :]
        if len(sample) < 2:
            return 0.0
        return float(pstdev(sample))

    def _bucket(self, sigma_effective: float) -> str:
        if sigma_effective < 0.0025:
            return "LOW"
        if sigma_effective < 0.0060:
            return "MID"
        return "HIGH"

    def _build_state(self, pair_key: str, fast_tf: str, slow_tf: str) -> VolatilityState | None:
        fast_returns = self._return_history.get(fast_tf)
        slow_returns = self._return_history.get(slow_tf)
        if not fast_returns or not slow_returns:
            return self._last_pair_state.get(pair_key)

        sigma_fast = self._sigma(fast_returns)
        sigma_slow = self._sigma(slow_returns)
        sigma_effective = max(sigma_fast, sigma_slow)
        last_fast = float(fast_returns[-1]) if fast_returns else 0.0
        shock_score = 0.0 if sigma_fast <= 1e-12 else abs(last_fast) / sigma_fast
        uncertainty = abs(sigma_fast - sigma_slow)
        transition_risk = min(1.0, uncertainty / max(sigma_effective, 1e-12)) if sigma_effective > 0.0 else 0.0

        state = VolatilityState(
            pair_key=str(pair_key),
            fast_tf=str(fast_tf),
            slow_tf=str(slow_tf),
            sigma_fast=float(sigma_fast),
            sigma_slow=float(sigma_slow),
            sigma_latent_fast=float(sigma_fast),
            sigma_latent_slow=float(sigma_slow),
            sigma_effective=float(sigma_effective),
            vol_bucket=self._bucket(sigma_effective),
            transition_risk=float(transition_risk),
            shock_score=float(shock_score),
            jump_intensity=float(shock_score),
            uncertainty=float(uncertainty),
            term_curvature=float(sigma_fast - sigma_slow),
        )
        self._last_pair_state[pair_key] = state
        return state

    def _compute_pair_state(self, pair_key: str, fast_tf: str, slow_tf: str) -> VolatilityState | None:
        return self._build_state(pair_key, fast_tf, slow_tf)

    def on_candle_close(self, tf: str, kline: Any) -> dict[tuple[str, str], VolatilityState]:
        close_px = _candle_close(kline)
        tf_key = str(tf)
        self._update_tf(tf_key, float(close_px))

        out: dict[tuple[str, str], VolatilityState] = {}
        for spec in VOLATILITY_ENGINE_CONFIG["tf_pairs"]:
            fast_tf = str(spec["fast_tf"])
            slow_tf = str(spec["slow_tf"])
            if tf_key not in {fast_tf, slow_tf}:
                continue
            state = self._build_state(str(spec["key"]), fast_tf, slow_tf)
            if state is not None:
                out[(fast_tf, slow_tf)] = state
        return out


# Backward-compatible aliases for older internal imports.
VolState = VolatilityState
VolatilityEngineV1 = VolatilityEngine
