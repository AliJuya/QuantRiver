from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class StructureLabel(str, Enum):
    UNKNOWN = "unknown"
    TREND_CONTINUATION = "trend_continuation"
    TREND_PULLBACK = "trend_pullback"
    COMPRESSION_COIL = "compression_coil"


class HurstBucket(str, Enum):
    CHOP = "chop"
    BALANCED = "balanced"
    TREND = "trend"


@dataclass(frozen=True)
class DCState:
    compression_score: float = 0.0
    expansion_score: float = 0.0
    dc_rate: float = 0.0
    avg_leg_size: float = 0.0
    overshoot_ratio: float = 0.0
    leg_asymmetry: float = 0.0
    intrinsic_vol: float = 0.0
    confidence: float = 0.0


@dataclass(frozen=True)
class DFAState:
    hurst: float = 0.5
    bucket: HurstBucket = HurstBucket.BALANCED
    fit_r2: float = 0.0
    stability: float = 0.0
    confidence: float = 0.0


@dataclass(frozen=True)
class TrendState:
    level: float = 0.0
    slope: float = 0.0
    trend_strength: float = 0.0
    slope_variance: float = 0.0
    trend_confidence: float = 0.0
    turning_point_score: float = 0.0


@dataclass(frozen=True)
class StructureState:
    label: StructureLabel = StructureLabel.UNKNOWN
    dc: DCState = field(default_factory=DCState)
    trend: TrendState = field(default_factory=TrendState)
    dfa: DFAState = field(default_factory=DFAState)
    alignment_score: float = 0.0
    struct_energy: float = 0.0
    shift_score: float = 0.0
    turning_point_pressure: float = 0.0
    exhaustion_score: float = 0.0
    obs_error_norm: float = 0.0
