from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.state.market_state import MarketState
from core.strategies.base import StrategyBase


@dataclass(frozen=True)
class StrategyDecision:
    tf: str
    candle_open_time: object
    candle_close_time: object
    strategy_id: str
    raw_output: Any


class StrategyRouter:
    def __init__(self, *, strategies: list[StrategyBase] | None = None) -> None:
        self.strategies = list(strategies or [])
        self._by_tf: dict[str, list[StrategyBase]] = {}
        for strategy in self.strategies:
            for tf in strategy.trigger_tfs():
                self._by_tf.setdefault(tf, []).append(strategy)

    def strategies_for(self, tf: str) -> tuple[StrategyBase, ...]:
        return tuple(self._by_tf.get(tf, ()))

    def on_tf_close(
        self,
        tf: str,
        candle,
        state: MarketState,
        *,
        strategies: list[StrategyBase] | tuple[StrategyBase, ...] | None = None,
    ) -> list[StrategyDecision]:
        out: list[StrategyDecision] = []
        active = self._by_tf.get(tf, ()) if strategies is None else strategies
        for strategy in active:
            raw = self._call_strategy(strategy, tf, candle, state)
            if raw is None:
                continue

            out.append(
                StrategyDecision(
                    tf=tf,
                    candle_open_time=candle.open_time,
                    candle_close_time=candle.close_time,
                    strategy_id=strategy.strategy_id,
                    raw_output=raw,
                )
            )
        return out

    def prime_on_tf_close(
        self,
        tf: str,
        candle,
        state: MarketState,
        *,
        strategies: list[StrategyBase] | tuple[StrategyBase, ...] | None = None,
    ) -> None:
        active = self._by_tf.get(tf, ()) if strategies is None else strategies
        for strategy in active:
            self._call_strategy(strategy, tf, candle, state)

    @staticmethod
    def _call_strategy(strategy: StrategyBase, tf: str, candle, state: MarketState):
        if strategy.__class__.on_tf_close is not StrategyBase.on_tf_close:
            return strategy.on_tf_close(tf, candle, state)

        if callable(strategy):
            try:
                return strategy(candle)
            except TypeError:
                return strategy(tf, candle, state)

        raise NotImplementedError(
            f"{strategy.__class__.__name__} must implement on_tf_close() or be callable"
        )
