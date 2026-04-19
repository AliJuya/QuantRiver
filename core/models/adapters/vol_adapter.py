from __future__ import annotations

import math
from dataclasses import asdict, is_dataclass
from typing import Any, Mapping

from ..base import VolSnapshot
from ..time_utils import to_epoch_ms
from ..vol_engine import VOLATILITY_ENGINE_CONFIG, VolatilityEngine


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _series(candles_map: Any, tf: str) -> Any:
    if isinstance(candles_map, dict):
        return candles_map.get(tf)
    return getattr(candles_map, tf, None)


def _candle_value(candle: Any, names: tuple[str, ...]) -> Any:
    if isinstance(candle, dict):
        for name in names:
            if name in candle:
                return candle[name]
    else:
        for name in names:
            if hasattr(candle, name):
                return getattr(candle, name)
    raise KeyError(f"Missing candle field in {names}")


def _candle_bar_ts(candle: Any) -> int:
    value = _candle_value(candle, ("open_time", "ts", "timestamp", "close_time", "close_ts", "time"))
    return to_epoch_ms(value)


def _candle_close(candle: Any) -> float:
    return float(_candle_value(candle, ("close", "c", "C")))


def _tf_token(tf: str) -> str:
    out = []
    for ch in str(tf):
        if ch.isalnum():
            out.append(ch.lower())
        else:
            out.append("_")
    return "".join(out)


def _pair_spec(pair_key: str) -> Mapping[str, Any]:
    for pair in VOLATILITY_ENGINE_CONFIG["tf_pairs"]:
        if pair["key"] == pair_key:
            return pair
    raise KeyError(f"Unknown volatility pair key: {pair_key}")


class VolAdapter:
    model_name = "vol"
    deps: tuple[str, ...] = ()

    def __init__(
        self,
        *,
        pair_key: str = "1h|12h",
        engine: VolatilityEngine | None = None,
        version: str = "v2",
    ) -> None:
        pair = _pair_spec(pair_key)
        windows = VOLATILITY_ENGINE_CONFIG["rv_bv"]["per_pair_windows"][pair_key]
        pct_cfg = VOLATILITY_ENGINE_CONFIG["percentiles"]
        pct_lookbacks = pct_cfg["lookbacks_per_pair"][pair_key]

        self.pair_key = pair_key
        self.fast_tf = str(pair["fast_tf"])
        self.slow_tf = str(pair["slow_tf"])
        self.base_tf = self.fast_tf
        self.engine = engine or VolatilityEngine()
        self.version = version
        self.warmup = {
            self.fast_tf: max(
                int(pct_lookbacks["lookback_fast"]),
                max(int(windows["n_fast"]), int(windows.get("n_bv_fast", windows["n_fast"]))) + 2,
            ),
            self.slow_tf: max(
                int(pct_lookbacks["lookback_slow"]),
                int(windows["n_slow"]) + 1,
            ),
        }
        self._last_seen_bar_ts: dict[str, int] = {}
        self._recent_replay_history: list[tuple[int, dict[str, Any]]] = []
        self._last_payload: dict[str, Any] | None = None

    def _normalize_payload(self, target: Any) -> dict[str, Any]:
        if is_dataclass(target):
            return asdict(target)
        return dict(target)

    def _unseen_rows(self, state: Any, tf: str) -> list[tuple[int, Any]]:
        candles_map = _state_get(state, "candles")
        if candles_map is None:
            raise AttributeError("State is missing a candles mapping.")

        rows = _series(candles_map, tf)
        if rows is None:
            raise KeyError(f"State is missing candles for timeframe {tf}.")

        unseen: list[tuple[int, Any]] = []
        last_seen = self._last_seen_bar_ts.get(tf)
        for candle in rows:
            ts = _candle_bar_ts(candle)
            if last_seen is not None and ts <= last_seen:
                continue
            unseen.append((ts, candle))

        return unseen

    def _sync_tf(self, state: Any, tf: str) -> list[tuple[int, dict[str, Any]]]:
        emitted: list[tuple[int, dict[str, Any]]] = []
        for ts, candle in self._unseen_rows(state, tf):
            latest_output = self.engine.on_candle_close(tf=tf, kline=candle)
            self._last_seen_bar_ts[tf] = ts
            target = latest_output.get((self.fast_tf, self.slow_tf))
            if target is None:
                continue
            emitted.append((ts, self._normalize_payload(target)))
        return emitted

    def _replay_fast_master(self, state: Any) -> list[tuple[int, dict[str, Any]]]:
        emitted: list[tuple[int, dict[str, Any]]] = []
        slow_rows = self._unseen_rows(state, self.slow_tf) if self.slow_tf != self.fast_tf else []
        slow_idx = 0

        for fast_ts, candle in self._unseen_rows(state, self.fast_tf):
            # Match the legacy runner: the 1h bar is the master clock, then slower bars
            # that are aligned to or behind that master timestamp are applied.
            self.engine.on_candle_close(tf=self.fast_tf, kline=candle)
            self._last_seen_bar_ts[self.fast_tf] = fast_ts

            while slow_idx < len(slow_rows) and slow_rows[slow_idx][0] <= fast_ts:
                slow_ts, slow_candle = slow_rows[slow_idx]
                self.engine.on_candle_close(tf=self.slow_tf, kline=slow_candle)
                self._last_seen_bar_ts[self.slow_tf] = slow_ts
                slow_idx += 1

            target = self.engine._compute_pair_state(self.pair_key, self.fast_tf, self.slow_tf)
            if target is None:
                continue
            emitted.append((fast_ts, self._normalize_payload(target)))

        return emitted

    def consume_replay_history(self) -> dict[int, dict[str, Any]]:
        history = {int(ts): dict(payload) for ts, payload in self._recent_replay_history}
        self._recent_replay_history = []
        return history

    def compute(self, state: Any, asof_ts: int, deps_snapshots: Mapping[str, Any] | None = None) -> VolSnapshot:
        del deps_snapshots

        self._recent_replay_history = []

        if self.fast_tf == self.base_tf and self.fast_tf != self.slow_tf:
            emitted = self._replay_fast_master(state)
        else:
            emitted = []
            ordered_tfs = [self.slow_tf]
            if self.fast_tf != self.slow_tf:
                ordered_tfs.append(self.fast_tf)
            for tf in ordered_tfs:
                emitted.extend(self._sync_tf(state, tf))

        self._recent_replay_history = emitted

        latest_payload: dict[str, Any] | None = None
        if emitted:
            latest_payload = dict(emitted[-1][1])
        elif self._last_payload is not None:
            latest_payload = dict(self._last_payload)

        if latest_payload is None:
            raise RuntimeError(
                f"Vol adapter could not produce pair {self.pair_key}; check warmup and candle history."
            )

        candles_map = _state_get(state, "candles")
        fast_rows = _series(candles_map, self.fast_tf)
        if fast_rows is None or len(fast_rows) < 2:
            raise RuntimeError("Vol adapter needs at least two fast candles to compute the latest return.")

        last_fast = fast_rows[-1]
        prev_fast = fast_rows[-2]
        last_close = _candle_close(last_fast)
        prev_close = _candle_close(prev_fast)
        last_return = 0.0 if prev_close <= 0 or last_close <= 0 else math.log(last_close / prev_close)
        sigma_slow = float(latest_payload.get("sigma_latent_slow", 0.0))

        latest_payload.update(
            {
                "pair_key": self.pair_key,
                "fast_tf": self.fast_tf,
                "slow_tf": self.slow_tf,
                "fast_candle_ts": _candle_bar_ts(last_fast),
                "last_return_fast": float(last_return),
                f"last_return_{_tf_token(self.fast_tf)}": float(last_return),
                "log_sigma_slow": math.log(max(1e-12, sigma_slow)),
            }
        )
        if self.fast_tf == "1h":
            # Backward-compatible key used by current regime adapter call sites.
            latest_payload["last_return_1h"] = float(last_return)

        self._last_payload = dict(latest_payload)

        return VolSnapshot(
            asof_ts=int(asof_ts),
            model_name=self.model_name,
            payload=latest_payload,
            version=self.version,
        )
