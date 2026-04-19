from __future__ import annotations

from typing import Any


class StrategyBase:
    """
    Shared strategy contract for live, paper, and backtest.
    """
    name: str = ""
    tfs_needed: tuple[str, ...] = ()
    warmup_req: dict[str, int] = {}

    def __init_subclass__(cls) -> None:
        warmup = getattr(cls, "warmup_req", {})
        if not isinstance(warmup, dict):
            raise ValueError(f"{cls.__name__}.warmup_req must be a dict[str, int]")
        for tf, bars in warmup.items():
            if not isinstance(tf, str):
                raise ValueError(f"{cls.__name__}.warmup_req keys must be str")
            if not isinstance(bars, int) or bars < 0:
                raise ValueError(f"{cls.__name__}.warmup_req[{tf}] must be >= 0")

    @property
    def strategy_id(self) -> str:
        return self.name or self.__class__.__name__

    def warmup_requirements(self) -> dict[str, int]:
        if self.warmup_req:
            return dict(self.warmup_req)

        legacy = getattr(self, "REQUIRED_TIMEFRAMES", None)
        if isinstance(legacy, dict):
            return dict(legacy)
        return {}

    def trigger_tfs(self) -> tuple[str, ...]:
        if self.tfs_needed:
            return tuple(self.tfs_needed)
        req = self.warmup_requirements()
        if req:
            return tuple(req.keys())
        return ("1s",)

    def base_tfs(self) -> tuple[str, ...]:
        return self.trigger_tfs()

    def on_tf_close(self, tf: str, candle, state) -> Any:
        raise NotImplementedError
