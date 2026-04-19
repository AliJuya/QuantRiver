from __future__ import annotations

from typing import Any, Mapping


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _candle_flag(candle: Any, name: str) -> Any:
    if isinstance(candle, dict):
        return candle.get(name)
    return getattr(candle, name, None)


def warmup_satisfied(state: Any, warmup_dict: Mapping[str, int]) -> bool:
    candles = _state_get(state, "candles")
    if candles is None:
        return False

    for tf, needed in warmup_dict.items():
        if needed <= 0:
            continue

        series = candles.get(tf) if isinstance(candles, dict) else getattr(candles, tf, None)
        if series is None or len(series) < int(needed):
            return False

        last_candle = series[-1]
        is_closed = _candle_flag(last_candle, "closed")
        if is_closed is None:
            is_closed = _candle_flag(last_candle, "is_closed")
        if is_closed is not None and not bool(is_closed):
            return False

    return True
