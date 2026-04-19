from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from core.execution.trade_state import ClosedTrade


@dataclass
class Reporter:
    out_path: str | None = None
    echo_console: bool = True
    append: bool = True
    rows: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.out_path is None or self.append:
            return
        path = Path(self.out_path)
        if path.exists():
            path.unlink()

    def record_trade(self, trade: ClosedTrade) -> None:
        row = asdict(trade)
        self.rows.append(row)

        if self.echo_console:
            self._print_trade(row)

        if self.out_path is None:
            return

        path = Path(self.out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, default=str) + "\n")

    def stats(self) -> dict:
        return {"closed_trades": len(self.rows), "out_path": self.out_path}

    def _print_trade(self, row: dict) -> None:
        print(
            f"[TRADE] strategy={row.get('strategy_id') or 'NA'} "
            f"side={row.get('side')} tf={row.get('tf')} "
            f"size={float(row.get('size', 1.0)):.4f} "
            f"entry={float(row.get('entry_price', 0.0)):.6f} "
            f"exit={float(row.get('exit_price', 0.0)):.6f} "
            f"pnl={float(row.get('pnl', 0.0)):+.6f} "
            f"reason={row.get('reason') or 'NA'} "
            f"exit_time={row.get('exit_time')}"
        )

        metadata = row.get("metadata")
        if not isinstance(metadata, dict):
            return

        model_bundle = metadata.get("model_bundle")
        if isinstance(model_bundle, dict):
            print(f"[MODELS] {json.dumps(model_bundle, default=str, sort_keys=True)}")
