from __future__ import annotations

from typing import Any, Iterable

from .base import ModelBundle, ModelSnapshot


def _state_has(state: Any, name: str) -> bool:
    if isinstance(state, dict):
        return name in state
    return hasattr(state, name)


def _state_get(state: Any, name: str, default: Any = None) -> Any:
    if isinstance(state, dict):
        return state.get(name, default)
    return getattr(state, name, default)


def _state_set(state: Any, name: str, value: Any) -> None:
    if isinstance(state, dict):
        state[name] = value
        return
    setattr(state, name, value)


def ensure_model_state(state: Any) -> None:
    if not _state_has(state, "model_pending") or _state_get(state, "model_pending") is None:
        _state_set(state, "model_pending", {})
    if not _state_has(state, "model_bundle"):
        _state_set(state, "model_bundle", None)
    if not _state_has(state, "_model_bundle_id") or _state_get(state, "_model_bundle_id") is None:
        _state_set(state, "_model_bundle_id", 0)


def try_commit_bundle(state: Any, required_models: Iterable[str]) -> ModelBundle | None:
    ensure_model_state(state)

    pending = _state_get(state, "model_pending")
    ordered_names = list(required_models)
    snapshots: dict[str, ModelSnapshot] = {}

    for name in ordered_names:
        snapshot = pending.get(name)
        if snapshot is None:
            return None
        snapshots[name] = snapshot

    ts_values = {snapshot.asof_ts for snapshot in snapshots.values()}
    if len(ts_values) != 1:
        return None

    bundle_id = int(_state_get(state, "_model_bundle_id", 0)) + 1
    bundle_ts = ts_values.pop()
    bundle = ModelBundle(
        bundle_id=bundle_id,
        bundle_asof_ts=bundle_ts,
        snapshots=snapshots,
    )

    _state_set(state, "_model_bundle_id", bundle_id)
    _state_set(state, "model_bundle", bundle)

    for name in ordered_names:
        pending.pop(name, None)

    return bundle

