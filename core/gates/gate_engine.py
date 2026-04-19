from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple


@dataclass(frozen=True)
class GateDecision:
    allow: bool
    prob: float
    threshold: float
    reason_codes: Tuple[str, ...]
    gate_id: str
    strategy_id: str
    bundle_asof_ts: int
    feature_count: int


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _bundle_asof_ts(bundle: Any) -> int:
    if isinstance(bundle, dict):
        value = bundle.get("bundle_asof_ts")
    else:
        value = getattr(bundle, "bundle_asof_ts", None)
    try:
        return int(value)
    except Exception:
        return -1


def _is_warm(market_state: Any) -> bool:
    if isinstance(market_state, dict):
        return bool(market_state.get("is_warm", market_state.get("warm", False)))
    return bool(
        getattr(
            market_state,
            "is_warm",
            getattr(market_state, "warm", getattr(market_state, "ready", False)),
        )
    )


class GateEngine:
    """
    Public placeholder gate engine.

    The production repository used a more advanced gate stack. This public version
    keeps only the integration seam so another engineer can see where strategy
    filtering happens and replace it with their own logic safely.
    """

    def __init__(
        self,
        manifest_path: str | Path,
        logger: Optional[Any] = None,
    ) -> None:
        self.manifest_path = Path(manifest_path)
        self.logger = logger
        self._strategy_to_gate = self._load_manifest(self.manifest_path)

    @classmethod
    def from_default_artifacts(cls, logger: Optional[Any] = None) -> "GateEngine":
        base_dir = Path(__file__).resolve().parent
        return cls(manifest_path=base_dir / "manifest.json", logger=logger)

    def _log(self, msg: str) -> None:
        if self.logger is not None and hasattr(self.logger, "info"):
            self.logger.info(msg)
            return
        print(msg)

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, str]:
        if not path.exists():
            return {}
        data = _read_json(path)
        mapping = data.get("strategy_to_gate", {})
        if not isinstance(mapping, dict):
            return {}
        return {str(key): str(value) for key, value in mapping.items() if str(key).strip()}

    def _decision(
        self,
        *,
        allow: bool,
        prob: float,
        threshold: float,
        reason_codes: Tuple[str, ...],
        gate_id: str,
        strategy_id: str,
        bundle_asof_ts: int,
    ) -> GateDecision:
        self._log(
            f"[GATE] strategy={strategy_id or 'NA'} gate={gate_id or 'NA'} "
            f"{'ALLOW' if allow else 'BLOCK'} reasons={list(reason_codes)}"
        )
        return GateDecision(
            allow=allow,
            prob=float(prob),
            threshold=float(threshold),
            reason_codes=reason_codes,
            gate_id=str(gate_id),
            strategy_id=str(strategy_id),
            bundle_asof_ts=int(bundle_asof_ts),
            feature_count=0,
        )

    def eval(
        self,
        strategy_id: str,
        signal: Any,
        bundle: Any,
        market_state: Any,
    ) -> GateDecision:
        del signal
        bundle_ts = _bundle_asof_ts(bundle)
        gate_id = self._strategy_to_gate.get(str(strategy_id), "")

        if not gate_id:
            return self._decision(
                allow=True,
                prob=1.0,
                threshold=0.0,
                reason_codes=("NO_GATE_CONFIGURED",),
                gate_id="",
                strategy_id=strategy_id,
                bundle_asof_ts=bundle_ts,
            )

        if not _is_warm(market_state):
            return self._decision(
                allow=False,
                prob=0.0,
                threshold=0.0,
                reason_codes=("PLACEHOLDER_GATE_NOT_WARM",),
                gate_id=gate_id,
                strategy_id=strategy_id,
                bundle_asof_ts=bundle_ts,
            )

        return self._decision(
            allow=True,
            prob=1.0,
            threshold=0.0,
            reason_codes=("PLACEHOLDER_GATE_ALLOW",),
            gate_id=gate_id,
            strategy_id=strategy_id,
            bundle_asof_ts=bundle_ts,
        )
