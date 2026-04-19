from __future__ import annotations

from dataclasses import dataclass, field

from core.execution.execution_adapter import ExecutionAdapter
from core.execution.position_handler import PositionHandler
from core.execution.reporter import Reporter
from core.execution.trade_state import ClosedTrade
from core.state.market_state import MarketState
from core.types import ExecutionIntent


@dataclass
class BacktestExecutionAdapter(ExecutionAdapter):
    position_handler: PositionHandler = field(default_factory=PositionHandler)
    reporter: Reporter = field(default_factory=Reporter)

    def base_tfs(self) -> tuple[str, ...]:
        return ("1s",)

    def execute_intents(
        self,
        intents: list[ExecutionIntent],
        state: MarketState,
    ) -> list[dict]:
        results: list[dict] = []
        for intent in intents:
            result = self.position_handler.apply_intent(intent, state)
            self._record_any_closed_trade(result)
            results.append(result)
        return results

    def _record_any_closed_trade(self, result: dict) -> None:
        for key in ("trade", "closed_trade"):
            trade = result.get(key)
            if isinstance(trade, ClosedTrade):
                self.reporter.record_trade(trade)
        for key in ("trades", "closed_trades"):
            trades = result.get(key)
            if not isinstance(trades, list):
                continue
            for trade in trades:
                if isinstance(trade, ClosedTrade):
                    self.reporter.record_trade(trade)

    def on_candle_close(self, tf: str, candle, state: MarketState) -> None:
        for trade in self.position_handler.on_candle(candle):
            self.reporter.record_trade(trade)

    def on_tf_close(self, tf: str, candle, state: MarketState) -> None:
        self.on_candle_close(tf, candle, state)

    def close_open_position(self, state: MarketState, *, tf: str = "1s", reason: str = "EOD") -> list[ClosedTrade]:
        candle = state.last_candle(tf)
        if candle is None:
            return []
        trades = self.position_handler.close_open_position(candle, reason=reason)
        for trade in trades:
            self.reporter.record_trade(trade)
        return trades

    def summary(self, *, starting_balance: float = 0.0) -> dict:
        stats = self.position_handler.stats()
        net_pnl = float(stats["net_pnl"])
        wins = int(stats["wins"])
        losses = int(stats["losses"])
        trades = int(stats["closed_trades"])
        winrate = (wins / trades * 100.0) if trades else 0.0
        return {
            "starting_balance": float(starting_balance),
            "final_balance": float(starting_balance) + net_pnl,
            "net_pnl": net_pnl,
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "winrate": float(winrate),
            "report_path": self.reporter.out_path,
        }

    def stats(self) -> dict:
        out = self.position_handler.stats()
        out["reported_trades"] = self.reporter.stats()["closed_trades"]
        out["report_path"] = self.reporter.out_path
        return out
