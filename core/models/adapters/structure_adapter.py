from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Callable, Mapping

from ..base import StructureSnapshot
from ..time_utils import to_epoch_ms
from ..structure_engine import (
    DefaultAccessor,
    DefaultVolAccessor,
    StructureEngine,
    StructureEngineConfig,
)


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _normalize_payload(result: Any) -> Any:
    if result is None:
        return {}
    if isinstance(result, Enum):
        return result.value
    if is_dataclass(result):
        return _normalize_payload(asdict(result))
    if isinstance(result, Mapping):
        return {str(key): _normalize_payload(value) for key, value in result.items()}
    if isinstance(result, (list, tuple)):
        return [_normalize_payload(item) for item in result]
    return result


def _candle_ts(candle: Any) -> int:
    if isinstance(candle, dict):
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time", "t"):
            if name in candle:
                return to_epoch_ms(candle[name])
    else:
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time", "t"):
            if hasattr(candle, name):
                return to_epoch_ms(getattr(candle, name))
    raise KeyError("Missing timestamp on candle.")


def _candle_bar_ts(candle: Any) -> int:
    if isinstance(candle, dict):
        for name in ("open_time", "ts", "timestamp", "close_time", "close_ts", "time", "t"):
            if name in candle:
                return to_epoch_ms(candle[name])
    else:
        for name in ("open_time", "ts", "timestamp", "close_time", "close_ts", "time", "t"):
            if hasattr(candle, name):
                return to_epoch_ms(getattr(candle, name))
    raise KeyError("Missing bar timestamp on candle.")


class StructureAdapter:
    model_name = "structure"
    deps: tuple[str, ...] = ("vol",)

    def __init__(
        self,
        *,
        base_tf: str | None = None,
        engine: StructureEngine | None = None,
        compute_fn: Callable[[Any, int, Mapping[str, Any]], Any] | None = None,
        warmup: Mapping[str, int] | None = None,
        version: str = "v1",
    ) -> None:
        selected_base_tf = str(base_tf) if base_tf else "1h"
        self.engine = engine or StructureEngine(
            cfg=StructureEngineConfig(base_tf=selected_base_tf),
            market_accessor=DefaultAccessor(
                ts_keys=("close_time", "close_ts", "ts", "t", "time", "timestamp", "open_time"),
            ),
            vol_accessor=DefaultVolAccessor(),
        )
        self.compute_fn = compute_fn
        cfg = getattr(self.engine, "cfg", None)
        if cfg is not None and base_tf is not None and str(getattr(cfg, "base_tf", selected_base_tf)) != selected_base_tf:
            raise ValueError(
                f"StructureAdapter base_tf mismatch: requested={selected_base_tf} "
                f"engine_cfg={getattr(cfg, 'base_tf', None)}"
            )
        self.base_tf = str(getattr(cfg, "base_tf", selected_base_tf))
        default_1h_warmup = 1
        if cfg is not None:
            default_1h_warmup = max(default_1h_warmup, int(getattr(cfg, "dfa_min_bars_required", 1)))

        self.warmup = {self.base_tf: default_1h_warmup}
        if warmup:
            for tf, bars in warmup.items():
                tf_key = str(tf)
                self.warmup[tf_key] = max(self.warmup.get(tf_key, 0), int(bars))
        self.version = version
        self._last_seen_ts: int | None = None
        self._last_vol_ctx: Mapping[str, Any] | None = None

    def _sync_engine(
        self,
        state: Any,
        asof_ts: int,
        vol_ctx: Mapping[str, Any],
        *,
        vol_history: Mapping[int, Mapping[str, Any]] | None = None,
    ) -> Any:
        candles = _state_get(state, "candles")
        if candles is None:
            raise AttributeError("State is missing a candles mapping.")

        rows = candles.get(self.base_tf) if isinstance(candles, dict) else getattr(candles, self.base_tf, None)
        if rows is None or len(rows) == 0:
            raise KeyError(f"State is missing {self.base_tf} candles for structure computation.")

        last_output = None
        for candle in rows:
            ts = _candle_ts(candle)
            if self._last_seen_ts is not None and ts <= self._last_seen_ts:
                continue

            active_vol_ctx: Mapping[str, Any] | None = None
            if vol_history:
                active_vol_ctx = vol_history.get(_candle_bar_ts(candle))

            if active_vol_ctx is None:
                if vol_history:
                    if self._last_seen_ts is None:
                        # Before the first warm vol row there is no valid structure input.
                        continue
                    active_vol_ctx = self._last_vol_ctx or vol_ctx
                else:
                    active_vol_ctx = vol_ctx if ts == int(asof_ts) else (self._last_vol_ctx or vol_ctx)

            last_output = self.engine.on_candle_close(
                tf=self.base_tf,
                kline=candle,
                vol_ctx=active_vol_ctx,
            )
            self._last_seen_ts = ts

        self._last_vol_ctx = dict(vol_ctx)
        if last_output is not None:
            return last_output

        last_state = getattr(self.engine, "last_state", None)
        if last_state is not None:
            return last_state

        raise RuntimeError("Structure adapter could not produce a structure state.")

    def compute(self, state: Any, asof_ts: int, deps_snapshots: Mapping[str, Any] | None = None) -> StructureSnapshot:
        if deps_snapshots is None or "vol" not in deps_snapshots:
            raise ValueError("Structure adapter requires the vol snapshot dependency.")

        vol_snapshot = deps_snapshots["vol"]
        vol_ctx = getattr(vol_snapshot, "payload", vol_snapshot)
        raw_vol_history = deps_snapshots.get("_vol_history")
        vol_history: dict[int, Mapping[str, Any]] | None = None
        if isinstance(raw_vol_history, Mapping):
            vol_history = {int(ts): dict(payload) for ts, payload in raw_vol_history.items()}

        if self.compute_fn is not None:
            try:
                result = self.compute_fn(state, asof_ts, vol_ctx)
            except TypeError:
                result = self.compute_fn(state, asof_ts)
        else:
            result = self._sync_engine(state, asof_ts, vol_ctx, vol_history=vol_history)

        payload = _normalize_payload(result)
        if not isinstance(payload, dict):
            payload = {"value": payload}

        return StructureSnapshot(
            asof_ts=int(asof_ts),
            model_name=self.model_name,
            payload=payload,
            version=self.version,
        )
