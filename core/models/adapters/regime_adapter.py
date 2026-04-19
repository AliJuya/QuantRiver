from __future__ import annotations

import math
from typing import Any, Mapping

from ..base import RegimeSnapshot
from ..regime_engine import RegimeEngine
from ..time_utils import to_epoch_ms
from .vol_adapter import VolAdapter


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _candle_close(candle: Any) -> float:
    if isinstance(candle, dict):
        for name in ("close", "c", "C"):
            if name in candle:
                return float(candle[name])
    else:
        for name in ("close", "c", "C"):
            if hasattr(candle, name):
                return float(getattr(candle, name))
    raise KeyError("Missing close price on candle.")


def _candle_event_ts(candle: Any) -> int:
    if isinstance(candle, dict):
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time"):
            if name in candle:
                return to_epoch_ms(candle[name])
    else:
        for name in ("close_time", "close_ts", "ts", "timestamp", "open_time", "time"):
            if hasattr(candle, name):
                return to_epoch_ms(getattr(candle, name))
    raise KeyError("Missing timestamp on candle.")


def _candle_bar_ts(candle: Any) -> int:
    if isinstance(candle, dict):
        for name in ("open_time", "ts", "timestamp", "close_time", "close_ts", "time"):
            if name in candle:
                return to_epoch_ms(candle[name])
    else:
        for name in ("open_time", "ts", "timestamp", "close_time", "close_ts", "time"):
            if hasattr(candle, name):
                return to_epoch_ms(getattr(candle, name))
    raise KeyError("Missing bar timestamp on candle.")


class RegimeAdapter:
    model_name = "regime"
    deps: tuple[str, ...] = ("vol",)

    def __init__(
        self,
        *,
        base_tf: str = "1h",
        vol_pair_key: str | None = None,
        vol_adapter: VolAdapter | None = None,
        engine: RegimeEngine | None = None,
        version: str = "public",
    ) -> None:
        if vol_pair_key is not None and vol_adapter is not None:
            raise ValueError("Pass only one of vol_pair_key or vol_adapter to RegimeAdapter.")
        self._engine = engine or RegimeEngine()
        self.version = str(version)
        self.base_tf = str(base_tf)
        self._vol_adapter = vol_adapter or (VolAdapter(pair_key=str(vol_pair_key)) if vol_pair_key else None)
        self.warmup = {self.base_tf: 2}
        if self._vol_adapter is not None:
            for tf, bars in dict(getattr(self._vol_adapter, "warmup", {}) or {}).items():
                tf_key = str(tf)
                self.warmup[tf_key] = max(self.warmup.get(tf_key, 0), int(bars))
        self._last_seen_ts: int | None = None
        self._last_payload: dict[str, Any] | None = None

    @property
    def engine(self) -> RegimeEngine:
        return self._engine

    def _fallback_return(self, state: Any) -> float:
        candles = _state_get(state, "candles")
        if candles is None:
            raise AttributeError(f"State is missing candles; regime adapter cannot derive {self.base_tf} returns.")

        rows = candles.get(self.base_tf) if isinstance(candles, dict) else getattr(candles, self.base_tf, None)
        if rows is None or len(rows) < 2:
            raise RuntimeError(f"Regime adapter needs at least two {self.base_tf} candles.")

        last_close = _candle_close(rows[-1])
        prev_close = _candle_close(rows[-2])
        if last_close <= 0 or prev_close <= 0:
            return 0.0
        return math.log(last_close / prev_close)

    def _replay_history(self, state: Any, vol_history: Mapping[int, Mapping[str, Any]]) -> dict[str, Any] | None:
        candles = _state_get(state, "candles")
        if candles is None:
            raise AttributeError(f"State is missing candles; regime adapter cannot replay {self.base_tf} history.")

        rows = candles.get(self.base_tf) if isinstance(candles, dict) else getattr(candles, self.base_tf, None)
        if rows is None or len(rows) < 2:
            raise RuntimeError(f"Regime adapter needs at least two {self.base_tf} candles.")

        latest_payload: dict[str, Any] | None = None

        for idx, candle in enumerate(rows):
            ts = _candle_event_ts(candle)
            if self._last_seen_ts is not None and ts <= self._last_seen_ts:
                continue

            vol_ctx = vol_history.get(_candle_bar_ts(candle))
            if vol_ctx is None:
                continue

            prev_close = _candle_close(rows[idx - 1]) if idx > 0 else _candle_close(candle)
            last_close = _candle_close(candle)
            r_1h = 0.0 if prev_close <= 0 or last_close <= 0 else math.log(last_close / prev_close)
            log_sigma_slow = float(
                vol_ctx.get(
                    "log_sigma_slow",
                    math.log(max(1e-12, float(vol_ctx.get("sigma_latent_slow", 0.0)))),
                )
            )

            regime_context = self.engine.update(
                ts=int(ts),
                r_1h=float(r_1h),
                log_sigma_slow=log_sigma_slow,
            )

            latest_payload = regime_context.to_dict()
            latest_payload["input_r_1h"] = float(r_1h)
            latest_payload["input_log_sigma_slow"] = float(log_sigma_slow)
            self._last_seen_ts = int(ts)

        if latest_payload is not None:
            self._last_payload = dict(latest_payload)
            return latest_payload
        if self._last_payload is not None:
            return dict(self._last_payload)
        return None

    def compute(
        self,
        state: Any,
        asof_ts: int,
        deps_snapshots: Mapping[str, Any] | None = None,
    ) -> RegimeSnapshot:
        if self._vol_adapter is not None:
            vol_snapshot = self._vol_adapter.compute(state, asof_ts, None)
            vol_payload = getattr(vol_snapshot, "payload", vol_snapshot)
            raw_vol_history = self._vol_adapter.consume_replay_history()
        else:
            if deps_snapshots is None or "vol" not in deps_snapshots:
                raise ValueError("Regime adapter requires the vol snapshot dependency.")
            vol_snapshot = deps_snapshots["vol"]
            vol_payload = getattr(vol_snapshot, "payload", vol_snapshot)
            raw_vol_history = deps_snapshots.get("_vol_history")

        if isinstance(raw_vol_history, Mapping):
            replay_payload = self._replay_history(
                state,
                {int(ts): dict(payload) for ts, payload in raw_vol_history.items()},
            )
            if replay_payload is not None:
                return RegimeSnapshot(
                    asof_ts=int(asof_ts),
                    model_name=self.model_name,
                    payload=replay_payload,
                    version=self.version,
                )

        r_1h = float(
            vol_payload.get(
                "last_return_1h",
                vol_payload.get("last_return_fast", self._fallback_return(state)),
            )
        )
        log_sigma_slow = float(
            vol_payload.get(
                "log_sigma_slow",
                math.log(max(1e-12, float(vol_payload.get("sigma_latent_slow", 0.0)))),
            )
        )

        regime_context = self.engine.update(
            ts=int(asof_ts),
            r_1h=r_1h,
            log_sigma_slow=log_sigma_slow,
        )

        payload = regime_context.to_dict()
        payload["input_r_1h"] = float(r_1h)
        payload["input_log_sigma_slow"] = float(log_sigma_slow)
        self._last_seen_ts = int(asof_ts)
        self._last_payload = dict(payload)

        return RegimeSnapshot(
            asof_ts=int(asof_ts),
            model_name=self.model_name,
            payload=payload,
            version=self.version,
        )
