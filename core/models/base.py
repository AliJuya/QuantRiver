from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True, slots=True)
class ModelSnapshot:
    asof_ts: int
    model_name: str
    payload: Dict[str, Any] = field(default_factory=dict)
    version: Optional[str] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "asof_ts", int(self.asof_ts))
        object.__setattr__(self, "model_name", str(self.model_name))
        object.__setattr__(self, "payload", dict(self.payload))
        if self.version is not None:
            object.__setattr__(self, "version", str(self.version))

    @classmethod
    def from_payload(
        cls,
        *,
        asof_ts: int,
        model_name: str,
        payload: Optional[Mapping[str, Any]] = None,
        version: Optional[str] = None,
    ) -> "ModelSnapshot":
        return cls(
            asof_ts=asof_ts,
            model_name=model_name,
            payload=dict(payload or {}),
            version=version,
        )


@dataclass(frozen=True, slots=True)
class VolSnapshot(ModelSnapshot):
    pass


@dataclass(frozen=True, slots=True)
class RegimeSnapshot(ModelSnapshot):
    pass


@dataclass(frozen=True, slots=True)
class StructureSnapshot(ModelSnapshot):
    pass


@dataclass(frozen=True, slots=True)
class ModelBundle:
    bundle_id: int
    bundle_asof_ts: int
    snapshots: Dict[str, ModelSnapshot] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "bundle_id", int(self.bundle_id))
        object.__setattr__(self, "bundle_asof_ts", int(self.bundle_asof_ts))
        object.__setattr__(self, "snapshots", dict(self.snapshots))

