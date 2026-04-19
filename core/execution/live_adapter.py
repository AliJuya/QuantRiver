from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from core.execution.execution_adapter import ExecutionAdapter
from core.state.market_state import MarketState
from core.types import ExecutionIntent


@dataclass
class LiveExecutionAdapter(ExecutionAdapter):
    on_intent: Callable[[ExecutionIntent, MarketState], None] | None = None
    on_candle: Callable[[str, object, MarketState], None] | None = None
    clock_tfs: tuple[str, ...] = ("1s",)
    intents_seen: list[ExecutionIntent] = field(default_factory=list)
    clock_events_seen: int = 0

    def base_tfs(self) -> tuple[str, ...]:
        return tuple(str(tf) for tf in self.clock_tfs)

    def on_tf_close(self, tf: str, candle, state: MarketState) -> None:
        self.clock_events_seen += 1
        if self.on_candle is not None:
            self.on_candle(tf, candle, state)

    def execute_intents(
        self,
        intents: list[ExecutionIntent],
        state: MarketState,
    ) -> list[dict]:
        results: list[dict] = []
        for intent in intents:
            self.intents_seen.append(intent)
            if self.on_intent is not None:
                self.on_intent(intent, state)
            results.append({"status": "queued", "action": intent.action})
        return results

    def stats(self) -> dict:
        return {
            "intents_seen": len(self.intents_seen),
            "clock_events_seen": self.clock_events_seen,
            "clock_tfs": list(self.base_tfs()),
        }
