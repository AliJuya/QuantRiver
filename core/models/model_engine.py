from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Mapping

from .adapters import RegimeAdapter, StructureAdapter, VolAdapter
from .base import ModelBundle, ModelSnapshot
from .bundling import ensure_model_state, try_commit_bundle
from .time_utils import to_epoch_ms
from .warmup import warmup_satisfied


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _event_get(event: Any, names: tuple[str, ...], default: Any = None) -> Any:
    if event is None:
        return default
    if isinstance(event, dict):
        for name in names:
            if name in event:
                return event[name]
        return default
    for name in names:
        if hasattr(event, name):
            return getattr(event, name)
    return default


def _latest_candle_ts(state: Any, tf: str) -> int | None:
    candles = _state_get(state, "candles")
    if candles is None:
        return None
    rows = candles.get(tf) if isinstance(candles, dict) else getattr(candles, tf, None)
    if rows is None or len(rows) == 0:
        return None
    candle = rows[-1]
    for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time"):
        if isinstance(candle, dict) and name in candle:
            return to_epoch_ms(candle[name])
        if hasattr(candle, name):
            return to_epoch_ms(getattr(candle, name))
    return None


def _normalize_tfs(raw: str | list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (str(raw),)
    return tuple(str(tf) for tf in raw)


class ModelEngine:
    """
    Bundled model runner:
      1) vol first
      2) structure + regime after vol (actual structure engine consumes vol_ctx)
      3) atomic bundle commit when all required snapshots share the same ts

    By default this runs synchronously so `on_tf_close(...)` can return the current
    bundle immediately for drop-in integration. Set `blocking=False` to keep the
    original fire-and-forget behavior.
    """

    def __init__(
        self,
        *,
        adapters: Mapping[str, Any] | None = None,
        required_models: list[str] | None = None,
        trigger_tfs: str | list[str] | tuple[str, ...] | None = None,
        max_workers: int = 3,
        executor: ThreadPoolExecutor | None = None,
        blocking: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        if adapters is None:
            self.adapters = {
                "vol": VolAdapter(),
                "structure": StructureAdapter(),
                "regime": RegimeAdapter(),
            }
        else:
            self.adapters = dict(adapters)
        self.required_models = list(required_models or ["vol", "regime", "structure"])
        configured_trigger_tfs = _normalize_tfs(trigger_tfs)
        if configured_trigger_tfs:
            self.trigger_tfs = configured_trigger_tfs
        else:
            vol_fast_tf = getattr(self.adapters.get("vol"), "fast_tf", None)
            self.trigger_tfs = (str(vol_fast_tf),) if vol_fast_tf else ("1h",)
        self._trigger_tf_set = set(self.trigger_tfs)
        self.executor = executor or ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="model-engine")
        self.blocking = bool(blocking)
        self.logger = logger or logging.getLogger(__name__)
        self._lock = threading.RLock()

        missing = [name for name in self.required_models if name not in self.adapters]
        if missing:
            raise ValueError(f"Missing adapters for required models: {missing}")

    def shutdown(self, wait: bool = True) -> None:
        self.executor.shutdown(wait=wait)

    def on_tf_close(self, state: Any, event: Any) -> ModelBundle | None:
        event_tf = str(_event_get(event, ("tf", "timeframe"), ""))
        if event_tf not in self._trigger_tf_set:
            return _state_get(state, "model_bundle")

        ensure_model_state(state)
        if not all(warmup_satisfied(state, self.adapters[name].warmup) for name in self.required_models):
            return _state_get(state, "model_bundle")

        asof_ts = _event_get(event, ("asof_ts", "close_time", "ts", "close_ts", "event_ts"), None)
        if asof_ts is None:
            asof_ts = _latest_candle_ts(state, event_tf)
        if asof_ts is None:
            raise RuntimeError(f"Could not derive the {event_tf} close timestamp for ModelEngine.")
        asof_ts = to_epoch_ms(asof_ts)

        if self.blocking:
            return self._run_blocking(state, asof_ts)

        vol_future = self.executor.submit(
            self.adapters["vol"].compute,
            state,
            asof_ts,
            {},
        )
        vol_future.add_done_callback(
            lambda future: self._handle_vol_done(state, asof_ts, future)
        )

        return _state_get(state, "model_bundle")

    def _run_blocking(self, state: Any, asof_ts: int) -> ModelBundle | None:
        vol_snapshot = self.adapters["vol"].compute(
            state,
            asof_ts,
            {},
        )
        if not isinstance(vol_snapshot, ModelSnapshot):
            raise TypeError(
                f"Adapter vol returned {type(vol_snapshot).__name__}, expected ModelSnapshot."
            )
        self._publish_snapshot(state, vol_snapshot)

        structure_deps, regime_deps = self._downstream_deps(vol_snapshot)
        structure_future = self.executor.submit(
            self.adapters["structure"].compute,
            state,
            asof_ts,
            structure_deps,
        )
        regime_future = self.executor.submit(
            self.adapters["regime"].compute,
            state,
            asof_ts,
            regime_deps,
        )

        for model_name, future in (
            ("structure", structure_future),
            ("regime", regime_future),
        ):
            snapshot = self._result_or_log(model_name, asof_ts, future)
            if snapshot is None:
                continue
            self._publish_snapshot(state, snapshot)

        return _state_get(state, "model_bundle")

    def _handle_vol_done(self, state: Any, asof_ts: int, future: Future) -> None:
        snapshot = self._result_or_log("vol", asof_ts, future)
        if snapshot is None:
            return

        self._publish_snapshot(state, snapshot)
        structure_deps, regime_deps = self._downstream_deps(snapshot)

        structure_future = self.executor.submit(
            self.adapters["structure"].compute,
            state,
            asof_ts,
            structure_deps,
        )
        structure_future.add_done_callback(
            lambda next_future: self._handle_future_done(state, "structure", next_future)
        )

        regime_future = self.executor.submit(
            self.adapters["regime"].compute,
            state,
            asof_ts,
            regime_deps,
        )
        regime_future.add_done_callback(
            lambda next_future: self._handle_future_done(state, "regime", next_future)
        )

    def _downstream_deps(
        self,
        vol_snapshot: ModelSnapshot,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        structure_deps = {"vol": vol_snapshot}
        regime_deps = {"vol": vol_snapshot}

        vol_adapter = self.adapters.get("vol")
        history_reader = getattr(vol_adapter, "consume_replay_history", None)
        if callable(history_reader):
            history = history_reader()
            if history:
                structure_deps["_vol_history"] = history
                regime_deps["_vol_history"] = history

        return structure_deps, regime_deps

    def _handle_future_done(self, state: Any, model_name: str, future: Future) -> None:
        snapshot = self._result_or_log(model_name, None, future)
        if snapshot is None:
            return
        self._publish_snapshot(state, snapshot)

    def _result_or_log(
        self,
        model_name: str,
        asof_ts: int | None,
        future: Future,
    ) -> ModelSnapshot | None:
        try:
            result = future.result()
        except Exception:
            if self.logger:
                self.logger.exception(
                    "[MODEL] compute failed name=%s asof_ts=%s",
                    model_name,
                    asof_ts if asof_ts is not None else "unknown",
                )
            return None

        if not isinstance(result, ModelSnapshot):
            raise TypeError(f"Adapter {model_name} returned {type(result).__name__}, expected ModelSnapshot.")
        return result

    def _publish_snapshot(self, state: Any, snapshot: ModelSnapshot) -> None:
        with self._lock:
            ensure_model_state(state)
            pending = _state_get(state, "model_pending")
            current_bundle = _state_get(state, "model_bundle")
            current_pending = pending.get(snapshot.model_name)

            latest_bundle_ts = -1
            if current_bundle is not None:
                latest_bundle_ts = int(current_bundle.bundle_asof_ts)

            if snapshot.asof_ts < latest_bundle_ts:
                return
            if current_pending is not None and snapshot.asof_ts < current_pending.asof_ts:
                return

            pending[snapshot.model_name] = snapshot

            self.logger.info(
                "[MODEL] produced name=%s asof_ts=%s keys=%s",
                snapshot.model_name,
                snapshot.asof_ts,
                len(snapshot.payload),
            )

            bundle = try_commit_bundle(state, self.required_models)
            if bundle is not None:
                self.logger.info(
                    "[BUNDLE] commit id=%s asof_ts=%s models=%s",
                    bundle.bundle_id,
                    bundle.bundle_asof_ts,
                    ",".join(self.required_models),
                )
