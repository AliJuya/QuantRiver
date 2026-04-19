from __future__ import annotations

from dataclasses import dataclass, field


def _normalize_tfs(raw) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    return tuple(str(tf) for tf in raw)


def _component_tfs(component) -> tuple[str, ...]:
    if hasattr(component, "base_tfs") and callable(component.base_tfs):
        return _normalize_tfs(component.base_tfs())

    if hasattr(component, "trigger_tfs") and callable(component.trigger_tfs):
        return _normalize_tfs(component.trigger_tfs())

    raw = getattr(component, "tfs_needed", None)
    if raw:
        return _normalize_tfs(raw)

    return ()


def _copy_mapping(mapping: dict[str, list[object]] | None) -> dict[str, list[object]]:
    if not mapping:
        return {}
    return {str(tf): list(items) for tf, items in mapping.items()}


@dataclass
class ModuleScheduler:
    models_by_tf: dict[str, list[object]] = field(default_factory=dict)
    modules_by_tf: dict[str, list[object]] = field(default_factory=dict)
    strategies_by_tf: dict[str, list[object]] = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        *,
        models: list[object] | tuple[object, ...] | None = None,
        modules: list[object] | tuple[object, ...] | None = None,
        strategies: list[object] | tuple[object, ...] | None = None,
        models_by_tf: dict[str, list[object]] | None = None,
        modules_by_tf: dict[str, list[object]] | None = None,
        strategies_by_tf: dict[str, list[object]] | None = None,
    ) -> "ModuleScheduler":
        scheduler = cls(
            models_by_tf=_copy_mapping(models_by_tf),
            modules_by_tf=_copy_mapping(modules_by_tf),
            strategies_by_tf=_copy_mapping(strategies_by_tf),
        )
        scheduler._register_many(scheduler.models_by_tf, models or ())
        scheduler._register_many(scheduler.modules_by_tf, modules or ())
        scheduler._register_many(scheduler.strategies_by_tf, strategies or ())
        return scheduler

    @staticmethod
    def _register_many(target: dict[str, list[object]], components) -> None:
        for component in components:
            for tf in _component_tfs(component):
                target.setdefault(tf, []).append(component)

    def models_for(self, tf: str) -> tuple[object, ...]:
        return tuple(self.models_by_tf.get(tf, ()))

    def modules_for(self, tf: str) -> tuple[object, ...]:
        return tuple(self.modules_by_tf.get(tf, ()))

    def strategies_for(self, tf: str) -> tuple[object, ...]:
        return tuple(self.strategies_by_tf.get(tf, ()))
