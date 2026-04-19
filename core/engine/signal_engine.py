from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from core.engine.strategy_router import StrategyDecision
from core.state.market_state import MarketState
from core.types import ExecutionIntent, Signal


SignalGate = Callable[[Signal, MarketState], bool]


@dataclass
class SignalEngine:
    gates: tuple[SignalGate, ...] = ()
    gate_engine: Any | None = None

    def to_intents(
        self,
        decisions: list[StrategyDecision],
        state: MarketState,
    ) -> list[ExecutionIntent]:
        out: list[ExecutionIntent] = []
        bundle = getattr(state, "model_bundle", None)
        for decision in decisions:
            for item in self._iter_outputs(decision.raw_output):
                signal = self._normalize_signal(item, decision)
                if signal is None:
                    continue
                signal = self._with_runtime_context(signal, decision)
                if not self._passes_gates(signal, state):
                    continue
                gate_meta = self._gate_meta(signal, state, bundle)
                if gate_meta is not None and not bool(gate_meta.get("allow", False)):
                    continue

                payload = dict(signal.payload)
                if gate_meta is not None:
                    payload["gate"] = gate_meta
                out.append(
                    ExecutionIntent(
                        action=signal.action,
                        tf=signal.tf,
                        candle_open_time=decision.candle_open_time,
                        strategy_id=signal.strategy_id or decision.strategy_id,
                        reason=signal.reason,
                        payload=payload,
                        close_only=signal.close_only,
                        force_close_all=signal.force_close_all,
                        flip=signal.flip,
                    )
                )
        return out

    @staticmethod
    def _iter_outputs(raw: Any) -> Iterable[Any]:
        if raw is None:
            return ()
        if isinstance(raw, (list, tuple)):
            return raw
        return (raw,)

    def _passes_gates(self, signal: Signal, state: MarketState) -> bool:
        for gate in self.gates:
            if not gate(signal, state):
                return False
        return True

    def _gate_meta(
        self,
        signal: Signal,
        state: MarketState,
        bundle: Any,
    ) -> dict[str, Any] | None:
        gate_engine = self.gate_engine
        if gate_engine is None:
            return None

        evaluator = getattr(gate_engine, "eval", None)
        if not callable(evaluator):
            raise TypeError("SignalEngine.gate_engine must expose eval().")

        decision = evaluator(
            strategy_id=signal.strategy_id,
            signal=signal,
            bundle=bundle,
            market_state=state,
        )
        return {
            "allow": bool(getattr(decision, "allow", False)),
            "prob": float(getattr(decision, "prob", 0.0)),
            "threshold": float(getattr(decision, "threshold", 0.0)),
            "reason_codes": list(getattr(decision, "reason_codes", ()) or ()),
            "gate_id": str(getattr(decision, "gate_id", "")),
            "bundle_asof_ts": int(getattr(decision, "bundle_asof_ts", -1)),
            "feature_count": int(getattr(decision, "feature_count", 0)),
        }

    @staticmethod
    def _with_runtime_context(signal: Signal, decision: StrategyDecision) -> Signal:
        payload = dict(signal.payload)
        changed = False

        signal_ts_ms = SignalEngine._to_epoch_ms(decision.candle_open_time)
        if signal_ts_ms is not None and "signal_ts_ms" not in payload:
            payload["signal_ts_ms"] = signal_ts_ms
            changed = True

        if "entry_ts_ms" not in payload:
            entry_ts_ms = SignalEngine._to_epoch_ms(decision.candle_close_time)
            if entry_ts_ms is not None:
                payload["entry_ts_ms"] = entry_ts_ms
                changed = True

        if not changed:
            return signal

        return Signal(
            action=signal.action,
            tf=signal.tf,
            strategy_id=signal.strategy_id,
            reason=signal.reason,
            payload=payload,
            close_only=signal.close_only,
            force_close_all=signal.force_close_all,
            flip=signal.flip,
        )

    @staticmethod
    def _to_epoch_ms(value: Any) -> int | None:
        if value is None:
            return None

        timestamp = getattr(value, "timestamp", None)
        if callable(timestamp):
            try:
                return int(float(timestamp()) * 1000)
            except Exception:
                pass

        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _normalize_signal(raw: Any, decision: StrategyDecision) -> Signal | None:
        if raw is None:
            return None

        if isinstance(raw, ExecutionIntent):
            return Signal(
                action=raw.action.upper(),
                tf=raw.tf,
                strategy_id=raw.strategy_id or decision.strategy_id,
                reason=raw.reason,
                payload=dict(raw.payload),
                close_only=raw.close_only,
                force_close_all=raw.force_close_all,
                flip=raw.flip,
            )

        if isinstance(raw, Signal):
            return Signal(
                action=raw.action.upper(),
                tf=raw.tf or decision.tf,
                strategy_id=raw.strategy_id or decision.strategy_id,
                reason=raw.reason,
                payload=dict(raw.payload),
                close_only=raw.close_only,
                force_close_all=raw.force_close_all,
                flip=raw.flip,
            )

        if isinstance(raw, dict):
            action = str(raw.get("action", raw.get("signal", raw.get("side", "")))).strip().upper()
            if not action or action == "NONE":
                return None
            return Signal(
                action=action,
                tf=str(raw.get("tf", decision.tf)),
                strategy_id=str(raw.get("strategy_id", decision.strategy_id)),
                reason=raw.get("reason"),
                payload=dict(raw),
                close_only=bool(raw.get("close_only", False)),
                force_close_all=bool(raw.get("force_close_all", False)),
                flip=bool(raw.get("flip", False)),
            )

        if isinstance(raw, str):
            action = raw.strip().upper()
            if not action or action == "NONE":
                return None
            return Signal(
                action=action,
                tf=decision.tf,
                strategy_id=decision.strategy_id,
            )

        return None
