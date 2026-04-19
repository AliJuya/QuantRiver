from __future__ import annotations

from abc import ABC, abstractmethod

from core.state.market_state import MarketState
from core.types import ExecutionIntent


class ExecutionAdapter(ABC):
    def base_tfs(self) -> tuple[str, ...]:
        return ()

    def on_tf_close(self, tf: str, candle, state: MarketState) -> None:
        return None

    def prime_on_tf_close(self, tf: str, candle, state: MarketState) -> None:
        return None

    @abstractmethod
    def execute_intents(
        self,
        intents: list[ExecutionIntent],
        state: MarketState,
    ) -> list[dict]:
        raise NotImplementedError

    def stats(self) -> dict:
        return {}
