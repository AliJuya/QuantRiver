from __future__ import annotations

from typing import Any


DEFAULT_TRAILING_CONFIG: dict[str, Any] = {
    "enabled": True,
    "BE_TRIG": 0.75,
    "BE_ATR_BUFFER": 0.10,
    "TP_EXTEND_ON": 1.00,
    "TP_EXTEND_NEAR_R": 0.15,
    "TP_EXTEND_R": 0.60,
    "TP_EXTEND_DECAY": 0.75,
    "MAX_TP_EXTENSIONS": 3,
    "STALL_ON_R": 1.50,
    "STALL_BARS": 6,
    "STALL_CHAND_ATR": 0.90,
}


STRATEGY_TRAILING_OVERRIDES: dict[str, dict[str, Any]] = {
    "EMA_CROSS_5m": {
        "EXTEND_TP": True,
    },
    "ORB_5m": {
        "EXTEND_TP": False,
    },
}


def get_trailing_config(strategy_id: str | None) -> dict[str, Any]:
    cfg = dict(DEFAULT_TRAILING_CONFIG)
    if strategy_id:
        cfg.update(STRATEGY_TRAILING_OVERRIDES.get(str(strategy_id), {}))
    return cfg
