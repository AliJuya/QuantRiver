from __future__ import annotations

from typing import TYPE_CHECKING

from core.data_engine.candle_aggregator_tf import tf_to_seconds
from core.state.events import TFClosedEvent
from core.state.market_state import MarketState

if TYPE_CHECKING:
    from core.engine.core_engine import CoreEngine


def prime_existing_history(
    *,
    core_engine: "CoreEngine",
    market_state: MarketState,
    tfs: tuple[str, ...] | list[str],
) -> dict[str, object | None]:
    ordered_tfs = tuple(dict.fromkeys(str(tf) for tf in tfs))
    cutoff_by_tf: dict[str, object | None] = {}
    priming: list[tuple[object, int, str]] = []

    for tf in ordered_tfs:
        last = market_state.last_candle(tf)
        cutoff = last.open_time if last is not None else None
        cutoff_by_tf[tf] = cutoff
        if cutoff is None:
            continue

        for candle in market_state.window(tf):
            if candle.open_time > cutoff:
                break
            priming.append((candle.open_time, tf_to_seconds(tf), tf))

    priming.sort(key=lambda x: (x[0], x[1]))
    for candle_open_time, _, tf in priming:
        core_engine.prime_event(TFClosedEvent(tf=tf, candle_open_time=candle_open_time))

    return cutoff_by_tf
