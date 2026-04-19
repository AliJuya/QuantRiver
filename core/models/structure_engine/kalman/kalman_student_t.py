from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StudentTKalmanTrendState:
    ts: int
    level: float
    slope: float
    trend_strength: float
    confidence: float
    anchor_level: float
    turning_point_score: float


class StudentTKalmanTrendEngine:
    """
    Lightweight public placeholder for the private Student-t local trend filter.
    """

    def __init__(
        self,
        *,
        student_t_df: float = 6.0,
        r_mult: float = 1.0,
        q_level_mult: float = 0.10,
        q_slope_mult: float = 0.02,
    ) -> None:
        self.student_t_df = float(student_t_df)
        self.r_mult = float(r_mult)
        self.q_level_mult = float(q_level_mult)
        self.q_slope_mult = float(q_slope_mult)
        self.last_state: StudentTKalmanTrendState | None = None
        self._prev_log_price: float | None = None
        self._level: float | None = None
        self._slope: float = 0.0

    def reset(self) -> None:
        self.last_state = None
        self._prev_log_price = None
        self._level = None
        self._slope = 0.0

    def update(self, *, ts: int, log_price: float, sigma_latent: float) -> StudentTKalmanTrendState:
        alpha_level = max(0.05, min(0.40, 0.20 * self.r_mult))
        alpha_slope = max(0.01, min(0.25, 0.10 * max(self.q_level_mult, self.q_slope_mult, 0.01)))

        if self._level is None:
            self._level = float(log_price)
            self._prev_log_price = float(log_price)
            self._slope = 0.0
        else:
            raw_slope = float(log_price - (self._prev_log_price if self._prev_log_price is not None else log_price))
            self._slope = (1.0 - alpha_slope) * self._slope + alpha_slope * raw_slope
            self._level = (1.0 - alpha_level) * self._level + alpha_level * float(log_price)
            self._prev_log_price = float(log_price)

        denom = max(1e-9, float(sigma_latent))
        trend_strength = self._slope / denom
        confidence = max(0.0, min(1.0, abs(trend_strength)))
        turning_point_score = max(0.0, min(1.0, 1.0 - abs(trend_strength)))

        state = StudentTKalmanTrendState(
            ts=int(ts),
            level=float(self._level),
            slope=float(self._slope),
            trend_strength=float(trend_strength),
            confidence=float(confidence),
            anchor_level=float(self._level),
            turning_point_score=float(turning_point_score),
        )
        self.last_state = state
        return state
