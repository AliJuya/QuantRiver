from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


IndicatorHandler = Callable[[str, object, object], None]


@dataclass(frozen=True)
class IndicatorRegistration:
    name: str
    handler: IndicatorHandler


class IndicatorRegistry:
    def __init__(self) -> None:
        self._by_tf: dict[str, list[IndicatorRegistration]] = {}

    def register(self, tf: str, name: str, handler: IndicatorHandler) -> None:
        self._by_tf.setdefault(tf, []).append(
            IndicatorRegistration(name=name, handler=handler)
        )

    def registrations_for(self, tf: str) -> tuple[IndicatorRegistration, ...]:
        return tuple(self._by_tf.get(tf, ()))


DEFAULT_INDICATOR_REGISTRY = IndicatorRegistry()

