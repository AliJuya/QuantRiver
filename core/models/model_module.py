from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .adapters import RegimeAdapter, StructureAdapter, VolAdapter
from .model_engine import ModelEngine
from .time_utils import to_epoch_ms


def _candle_event_ts(candle: Any) -> int:
    if isinstance(candle, dict):
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time"):
            if name in candle:
                return to_epoch_ms(candle[name])
    else:
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time"):
            if hasattr(candle, name):
                return to_epoch_ms(getattr(candle, name))
    raise KeyError("Missing timestamp on candle passed to ModelModule.")


def _fast_tf_from_pair_key(pair_key: str) -> str:
    text = str(pair_key)
    if "|" not in text:
        raise ValueError(f"Invalid pair key: {pair_key!r}. Expected format fast|slow.")
    fast_tf, _ = text.split("|", 1)
    fast_tf = str(fast_tf).strip()
    if not fast_tf:
        raise ValueError(f"Invalid pair key: {pair_key!r}. Missing fast timeframe.")
    return fast_tf


@dataclass
class ModelModule:
    model_engine: ModelEngine = field(default_factory=ModelEngine)

    @classmethod
    def from_recipe(
        cls,
        *,
        vol_pair_key: str = "1h|12h",
        structure_base_tf: str | None = None,
        regime_pair_key: str = "1h|12h",
        regime_base_tf: str | None = None,
        trigger_tf: str | None = None,
        blocking: bool = True,
    ) -> "ModelModule":
        structure_tf = str(structure_base_tf) if structure_base_tf else _fast_tf_from_pair_key(vol_pair_key)
        regime_tf = str(regime_base_tf) if regime_base_tf else _fast_tf_from_pair_key(regime_pair_key)
        selected_trigger_tf = str(trigger_tf) if trigger_tf else structure_tf

        model_engine = ModelEngine(
            adapters={
                "vol": VolAdapter(pair_key=str(vol_pair_key)),
                "structure": StructureAdapter(base_tf=structure_tf),
                "regime": RegimeAdapter(
                    base_tf=regime_tf,
                    vol_pair_key=str(regime_pair_key),
                ),
            },
            trigger_tfs=(selected_trigger_tf,),
            blocking=bool(blocking),
        )
        return cls(model_engine=model_engine)

    def base_tfs(self) -> tuple[str, ...]:
        trigger_tfs = tuple(str(tf) for tf in getattr(self.model_engine, "trigger_tfs", ()) if str(tf))
        return trigger_tfs if trigger_tfs else ("1h",)

    def trigger_tfs(self) -> tuple[str, ...]:
        return self.base_tfs()

    def warmup_requirements(self) -> dict[str, int]:
        req: dict[str, int] = {}
        for name in self.model_engine.required_models:
            adapter = self.model_engine.adapters.get(name)
            warmup = getattr(adapter, "warmup", {}) or {}
            for tf, bars in warmup.items():
                count = int(bars)
                if count <= 0:
                    continue
                tf_key = str(tf)
                req[tf_key] = max(req.get(tf_key, 0), count)
        return req

    def on_tf_close(self, tf: str, candle, state) -> None:
        tf_key = str(tf)
        if tf_key not in self.base_tfs():
            return
        self.model_engine.on_tf_close(
            state,
            {
                "tf": tf_key,
                "close_time": _candle_event_ts(candle),
            },
        )

    def prime_on_tf_close(self, tf: str, candle, state) -> None:
        tf_key = str(tf)
        if tf_key not in self.base_tfs():
            return

        latest = getattr(state, "last_candle", None)
        latest_tf = latest(tf_key) if callable(latest) else None
        if latest_tf is None:
            return
        if getattr(candle, "open_time", None) != getattr(latest_tf, "open_time", None):
            return

        self.on_tf_close(tf, candle, state)

    def shutdown(self) -> None:
        self.model_engine.shutdown()
