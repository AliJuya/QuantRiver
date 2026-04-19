from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    import lightgbm as lgb
except Exception as exc:
    lgb = None
    _LIGHTGBM_IMPORT_ERROR = exc
else:
    _LIGHTGBM_IMPORT_ERROR = None


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


@dataclass(frozen=True)
class _GateArtifacts:
    gate_id: str
    strategy_id: str
    threshold: float
    feature_cols: List[str]
    feature_count: int
    combine_mode: str
    rule_clauses: Tuple[Dict[str, Any], ...]
    policies: Dict[str, Any]
    booster: "lgb.Booster"


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        out = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(out):
        return 0.0
    return out


def _get_attr_any(obj: Any, names: List[str], default: Any = None) -> Any:
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _get_dict_any(data: Dict[str, Any], names: List[str], default: Any = None) -> Any:
    for name in names:
        if name in data:
            return data[name]
    return default


def _payload_dict(obj: Any) -> Optional[Dict[str, Any]]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        payload = obj.get("payload")
        return payload if isinstance(payload, dict) else None
    payload = getattr(obj, "payload", None)
    return payload if isinstance(payload, dict) else None


def _get_value_any(obj: Any, names: List[str], default: Any = None) -> Any:
    if isinstance(obj, dict):
        value = _get_dict_any(obj, names, None)
        if value is not None:
            return value

    value = _get_attr_any(obj, names, None)
    if value is not None:
        return value

    payload = _payload_dict(obj)
    if isinstance(payload, dict):
        value = _get_dict_any(payload, names, None)
        if value is not None:
            return value

    return default


def _to_int_ms(value: Any) -> Optional[int]:
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


def _floor_hour_open_ms(ts_ms: int) -> int:
    return (ts_ms // 3_600_000) * 3_600_000


def _expected_bundle_asof_ts_ms(ts_ms: int) -> int:
    """
    Use the trade decision timestamp (the lower-tf bar close / entry time) as the
    anchor for the latest completed 1h bundle.

    This keeps on-the-hour 5m signals aligned with the 1h bar that just closed at
    that same boundary while still remaining closed-bar-only.
    """
    return _floor_hour_open_ms(ts_ms)


def _extract_signal_ts_ms(signal: Any) -> Optional[int]:
    return _to_int_ms(
        _get_value_any(signal, ["entry_ts_ms", "entry_time_ms", "signal_ts_ms", "ts_ms", "ts"], None)
    )


def _time_feature_values(ts_ms: int | None) -> Dict[str, float]:
    if ts_ms is None:
        return {
            "hour": 0.0,
            "dow": 0.0,
            "month": 0.0,
            "hour_sin": 0.0,
            "hour_cos": 0.0,
            "dow_sin": 0.0,
            "dow_cos": 0.0,
        }

    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    hour = float(dt.hour)
    dow = float(dt.weekday())
    month = float(dt.month)
    return {
        "hour": hour,
        "dow": dow,
        "month": month,
        "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
        "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
        "dow_sin": math.sin(2.0 * math.pi * dow / 7.0),
        "dow_cos": math.cos(2.0 * math.pi * dow / 7.0),
    }


def _bundle_get_payload(bundle: Any, engine_key: str) -> Optional[Dict[str, Any]]:
    if hasattr(bundle, "snapshots"):
        snapshots = getattr(bundle, "snapshots", None)
        if isinstance(snapshots, dict):
            snapshot = snapshots.get(engine_key)
            payload = _payload_dict(snapshot)
            if isinstance(payload, dict):
                return payload
            if isinstance(snapshot, dict):
                return snapshot

    if isinstance(bundle, dict):
        snapshots = bundle.get("snapshots")
        if isinstance(snapshots, dict):
            snapshot = snapshots.get(engine_key)
            payload = _payload_dict(snapshot)
            if isinstance(payload, dict):
                return payload
            if isinstance(snapshot, dict):
                return snapshot

    if hasattr(bundle, "models"):
        models = getattr(bundle, "models", None)
        if isinstance(models, dict):
            row = models.get(engine_key)
            payload = _payload_dict(row)
            if isinstance(payload, dict):
                return payload
            if isinstance(row, dict):
                return row

    if isinstance(bundle, dict):
        models = bundle.get("models")
        if isinstance(models, dict):
            row = models.get(engine_key)
            payload = _payload_dict(row)
            if isinstance(payload, dict):
                return payload
            if isinstance(row, dict):
                return row

    return None


def _bundle_asof_ts(bundle: Any) -> int:
    value = _get_attr_any(bundle, ["bundle_asof_ts"], None)
    if value is None and isinstance(bundle, dict):
        value = bundle.get("bundle_asof_ts")
    as_int = _to_int_ms(value)
    return as_int if as_int is not None else -1


def _is_warm(market_state: Any) -> bool:
    value = _get_attr_any(market_state, ["is_warm", "warm", "ready"], None)
    if value is None and isinstance(market_state, dict):
        value = _get_dict_any(market_state, ["is_warm", "warm", "ready"], None)
    return bool(value)


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _flatten_structure_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    flat: Dict[str, Any] = {}

    for key, value in data.items():
        if key in {"dc", "trend", "dfa"} and isinstance(value, dict):
            continue
        if key == "label":
            flat["structure_label"] = value
            continue
        if not isinstance(value, dict):
            flat[key] = value

    dc = data.get("dc")
    if isinstance(dc, dict):
        dc_map = {
            "compression_score": "compression_score",
            "expansion_score": "expansion_score",
            "dc_rate": "dc_rate",
            "avg_leg_size": "avg_leg_size",
            "overshoot_ratio": "overshoot_ratio",
            "leg_asymmetry": "leg_asymmetry",
            "intrinsic_vol": "intrinsic_vol",
            "confidence": "dc_confidence",
        }
        for src_key, dst_key in dc_map.items():
            if src_key in dc:
                flat[dst_key] = dc[src_key]

    trend = data.get("trend")
    if isinstance(trend, dict):
        trend_map = {
            "level": "trend_level",
            "slope": "trend_slope",
            "trend_strength": "trend_strength",
            "slope_variance": "slope_variance",
            "trend_confidence": "trend_confidence",
            "turning_point_score": "turning_point_score",
        }
        for src_key, dst_key in trend_map.items():
            if src_key in trend:
                flat[dst_key] = trend[src_key]

    dfa = data.get("dfa")
    if isinstance(dfa, dict):
        dfa_map = {
            "hurst": "hurst",
            "bucket": "hurst_bucket",
            "fit_r2": "hurst_fit_r2",
            "stability": "hurst_stability",
            "confidence": "hurst_confidence",
        }
        for src_key, dst_key in dfa_map.items():
            if src_key in dfa:
                flat[dst_key] = dfa[src_key]

    return flat


def _normalize_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper().replace(" ", "_")


def _rule_clause_passes(feature_row: Dict[str, float], clause: Dict[str, Any]) -> bool:
    feature = str(clause.get("feature") or "").strip()
    op = str(clause.get("op") or "").strip()
    threshold = _safe_float(clause.get("threshold"))
    if not feature or op not in {">=", "<="}:
        return False
    value = _safe_float(feature_row.get(feature, 0.0))
    if op == ">=":
        return value >= threshold
    return value <= threshold


def _evaluate_rule_clauses(feature_row: Dict[str, float], clauses: Tuple[Dict[str, Any], ...]) -> bool:
    if not clauses:
        return True
    return all(_rule_clause_passes(feature_row, clause) for clause in clauses)


class GateEngine:
    """
    Evaluate candidate signals against per-strategy LightGBM gate artifacts.
    """

    def __init__(
        self,
        artifacts_root: str | Path,
        manifest_path: str | Path,
        logger: Optional[Any] = None,
    ) -> None:
        if lgb is None:
            raise ImportError(
                "GateEngine requires lightgbm installed. "
                "Install with: pip install lightgbm\n"
                f"Original import error: {_LIGHTGBM_IMPORT_ERROR}"
            )

        self.artifacts_root = Path(artifacts_root)
        self.manifest_path = Path(manifest_path)
        self.logger = logger
        self._strategy_to_gate = self._load_manifest(self.manifest_path)
        self._cache: Dict[str, _GateArtifacts] = {}

    @classmethod
    def from_default_artifacts(cls, logger: Optional[Any] = None) -> "GateEngine":
        base_dir = Path(__file__).resolve().parent
        return cls(
            artifacts_root=base_dir / "artifacts",
            manifest_path=base_dir / "manifest.json",
            logger=logger,
        )

    def _log(self, msg: str) -> None:
        if self.logger is not None and hasattr(self.logger, "info"):
            self.logger.info(msg)
            return
        print(msg)

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
        feature_count: int,
    ) -> GateDecision:
        reasons = reason_codes or ("OK",)
        self._log(
            f"[GATE] strategy={strategy_id} gate={gate_id or 'NA'} "
            f"p={prob:.4f} thr={threshold:.2f} "
            f"{'ALLOW' if allow else 'BLOCK'} "
            f"bundle_ts={bundle_asof_ts} features={feature_count} "
            f"reasons={list(reasons)}"
        )
        return GateDecision(
            allow=allow,
            prob=prob,
            threshold=threshold,
            reason_codes=reasons,
            gate_id=gate_id,
            strategy_id=strategy_id,
            bundle_asof_ts=bundle_asof_ts,
            feature_count=feature_count,
        )

    @staticmethod
    def _load_manifest(path: Path) -> Dict[str, str]:
        data = _read_json(path)
        mapping = data.get("strategy_to_gate", {})
        if not isinstance(mapping, dict) or not mapping:
            raise ValueError(f"manifest.json missing strategy_to_gate mapping: {path}")
        return {str(key): str(value) for key, value in mapping.items()}

    def _load_gate(self, gate_id: str) -> _GateArtifacts:
        cached = self._cache.get(gate_id)
        if cached is not None:
            return cached

        gate_dir = self.artifacts_root / gate_id
        if not gate_dir.exists():
            raise FileNotFoundError(f"Gate artifacts folder not found: {gate_dir}")

        cfg = _read_json(gate_dir / "gate_config.json")
        meta = _read_json(gate_dir / "train_matrix_meta.json")

        feature_cols = meta.get("feature_cols")
        if not isinstance(feature_cols, list) or not feature_cols:
            raise ValueError(f"{gate_dir / 'train_matrix_meta.json'} missing feature_cols list")

        artifacts = _GateArtifacts(
            gate_id=str(cfg.get("gate_id", gate_id)),
            strategy_id=str(cfg.get("strategy_id", "")),
            threshold=float(cfg.get("threshold", 0.5)),
            feature_cols=[str(col) for col in feature_cols],
            feature_count=int(cfg.get("feature_count", len(feature_cols))),
            combine_mode=str(cfg.get("combine_mode", "model_only") or "model_only"),
            rule_clauses=tuple(
                clause
                for clause in cfg.get("rules", {}).get("clauses", [])
                if isinstance(clause, dict)
            ),
            policies=dict(cfg.get("policies", {})),
            booster=lgb.Booster(model_file=str(gate_dir / "model.txt")),
        )

        if artifacts.feature_count != len(artifacts.feature_cols):
            artifacts = _GateArtifacts(
                gate_id=artifacts.gate_id,
                strategy_id=artifacts.strategy_id,
                threshold=artifacts.threshold,
                feature_cols=artifacts.feature_cols,
                feature_count=len(artifacts.feature_cols),
                combine_mode=artifacts.combine_mode,
                rule_clauses=artifacts.rule_clauses,
                policies=artifacts.policies,
                booster=artifacts.booster,
            )

        self._cache[gate_id] = artifacts
        return artifacts

    def _build_feature_row(
        self,
        artifacts: _GateArtifacts,
        signal: Any,
        bundle: Any,
    ) -> Tuple[np.ndarray, Dict[str, float], Tuple[str, ...]]:
        reasons: List[str] = []

        vol = _bundle_get_payload(bundle, "vol")
        reg = _bundle_get_payload(bundle, "regime")
        structure = _bundle_get_payload(bundle, "structure")

        if vol is None:
            reasons.append("MISSING_VOL")
            vol = {}
        if reg is None:
            reasons.append("MISSING_REGIME")
            reg = {}
        if structure is None:
            reasons.append("MISSING_STRUCTURE")
            structure = {}
        structure_flat = _flatten_structure_payload(structure)

        entry = _safe_float(_get_value_any(signal, ["entry_price", "entry"], None))
        init_sl = _safe_float(_get_value_any(signal, ["init_sl_price", "sl", "sl_price", "custom_sl"], None))
        init_tp = _safe_float(_get_value_any(signal, ["init_tp_price", "tp", "tp_price", "custom_tp"], None))
        if entry <= 0.0 or init_sl <= 0.0 or init_tp <= 0.0:
            reasons.append("MISSING_TRADE_PLAN")

        sl_dist_pct = 0.0 if entry == 0.0 else abs(entry - init_sl) / entry
        tp_dist_pct = 0.0 if entry == 0.0 else abs(init_tp - entry) / entry
        sl_tp_ratio = 0.0 if sl_dist_pct == 0.0 else tp_dist_pct / sl_dist_pct
        sig_ts = _extract_signal_ts_ms(signal)

        feat: Dict[str, float] = {
            "entry_price": entry,
            "init_sl_price": init_sl,
            "init_tp_price": init_tp,
            "sl_dist_pct": _safe_float(sl_dist_pct),
            "tp_dist_pct": _safe_float(tp_dist_pct),
            "sl_tp_ratio": _safe_float(sl_tp_ratio),
            "risk_usd": _safe_float(_get_value_any(signal, ["risk_usd"], None)),
            "reward_usd": _safe_float(_get_value_any(signal, ["reward_usd"], None)),
            "rr_planned": _safe_float(_get_value_any(signal, ["rr_planned", "rr"], None)),
        }
        feat.update(_time_feature_values(sig_ts))

        for key, value in vol.items():
            feat[f"vol_{key}"] = _safe_float(value)
        for key, value in reg.items():
            feat[f"reg_{key}"] = _safe_float(value)
        for key, value in structure_flat.items():
            feat[f"str_{key}"] = _safe_float(value)

        vol_bucket = _get_dict_any(vol, ["vol_bucket", "bucket", "volatility_bucket"], None)
        vol_bucket_key = _normalize_token(vol_bucket)
        if vol_bucket_key:
            feat[f"vol_vol_bucket_{vol_bucket_key}"] = 1.0

        hurst_bucket = _get_dict_any(structure_flat, ["hurst_bucket", "str_hurst_bucket"], None)
        hurst_bucket_key = _normalize_token(hurst_bucket)
        if hurst_bucket_key:
            feat[f"str_hurst_bucket_{hurst_bucket_key}"] = 1.0

        structure_label = _get_dict_any(structure_flat, ["structure_label", "label"], None)
        structure_label_key = _normalize_token(structure_label)
        if structure_label_key:
            feat[f"str_structure_label_{structure_label_key}"] = 1.0

        side_key = _normalize_token(_get_value_any(signal, ["side"], None))
        if side_key:
            feat[f"side_{side_key}"] = 1.0

        cols = artifacts.feature_cols
        x = np.zeros((1, len(cols)), dtype=np.float32)
        for idx, col in enumerate(cols):
            x[0, idx] = np.float32(_safe_float(feat.get(col, 0.0)))

        return x, feat, tuple(reasons)

    def eval(
        self,
        strategy_id: str,
        signal: Any,
        bundle: Any,
        market_state: Any,
    ) -> GateDecision:
        gate_id = self._strategy_to_gate.get(strategy_id)
        bundle_ts = _bundle_asof_ts(bundle)

        if gate_id is None:
            return self._decision(
                allow=True,
                prob=1.0,
                threshold=0.0,
                reason_codes=("NO_GATE_CONFIGURED",),
                gate_id="",
                strategy_id=strategy_id,
                bundle_asof_ts=bundle_ts,
                feature_count=0,
            )

        artifacts = self._load_gate(gate_id)
        policies = artifacts.policies or {}
        reasons: List[str] = []

        if policies.get("block_if_not_warm", True) and not _is_warm(market_state):
            reasons.append("NOT_WARM")
            return self._decision(
                allow=False,
                prob=0.0,
                threshold=artifacts.threshold,
                reason_codes=tuple(reasons),
                gate_id=gate_id,
                strategy_id=strategy_id,
                bundle_asof_ts=bundle_ts,
                feature_count=artifacts.feature_count,
            )

        if policies.get("block_if_stale_bundle", True):
            sig_ts = _extract_signal_ts_ms(signal)
            if sig_ts is None:
                reasons.append("MISSING_SIGNAL_TS")
                return self._decision(
                    allow=False,
                    prob=0.0,
                    threshold=artifacts.threshold,
                    reason_codes=tuple(reasons),
                    gate_id=gate_id,
                    strategy_id=strategy_id,
                    bundle_asof_ts=bundle_ts,
                    feature_count=artifacts.feature_count,
                )
            if bundle_ts <= 0:
                reasons.append("MISSING_BUNDLE_TS")
                return self._decision(
                    allow=False,
                    prob=0.0,
                    threshold=artifacts.threshold,
                    reason_codes=tuple(reasons),
                    gate_id=gate_id,
                    strategy_id=strategy_id,
                    bundle_asof_ts=bundle_ts,
                    feature_count=artifacts.feature_count,
                )

            expected_bundle_ts = _expected_bundle_asof_ts_ms(sig_ts)
            if bundle_ts != expected_bundle_ts:
                reasons.append(f"STALE_BUNDLE(expected={expected_bundle_ts},got={bundle_ts})")
                return self._decision(
                    allow=False,
                    prob=0.0,
                    threshold=artifacts.threshold,
                    reason_codes=tuple(reasons),
                    gate_id=gate_id,
                    strategy_id=strategy_id,
                    bundle_asof_ts=bundle_ts,
                    feature_count=artifacts.feature_count,
                )

        x, feature_row, missing_reasons = self._build_feature_row(artifacts, signal, bundle)
        reasons.extend(list(missing_reasons))

        if "MISSING_TRADE_PLAN" in reasons:
            return self._decision(
                allow=False,
                prob=0.0,
                threshold=artifacts.threshold,
                reason_codes=tuple(reasons),
                gate_id=gate_id,
                strategy_id=strategy_id,
                bundle_asof_ts=bundle_ts,
                feature_count=artifacts.feature_count,
            )

        if policies.get("block_if_missing_engine", True):
            if any(code in reasons for code in ("MISSING_VOL", "MISSING_REGIME", "MISSING_STRUCTURE")):
                return self._decision(
                    allow=False,
                    prob=0.0,
                    threshold=artifacts.threshold,
                    reason_codes=tuple(reasons),
                    gate_id=gate_id,
                    strategy_id=strategy_id,
                    bundle_asof_ts=bundle_ts,
                    feature_count=artifacts.feature_count,
                )

        if policies.get("block_if_schema_mismatch", True):
            if x.shape[1] != artifacts.feature_count:
                reasons.append(f"SCHEMA_MISMATCH(n={x.shape[1]},expected={artifacts.feature_count})")
                return self._decision(
                    allow=False,
                    prob=0.0,
                    threshold=artifacts.threshold,
                    reason_codes=tuple(reasons),
                    gate_id=gate_id,
                    strategy_id=strategy_id,
                    bundle_asof_ts=bundle_ts,
                    feature_count=x.shape[1],
                )

        prob = float(artifacts.booster.predict(x)[0])
        model_pass = prob >= artifacts.threshold
        rule_pass = _evaluate_rule_clauses(feature_row, artifacts.rule_clauses)

        combine_mode = artifacts.combine_mode
        if combine_mode == "model_only":
            allow = model_pass
        elif combine_mode == "rule_only":
            allow = rule_pass
        elif combine_mode == "model_or_rule":
            allow = model_pass or rule_pass
        elif combine_mode == "model_and_rule":
            allow = model_pass and rule_pass
        else:
            reasons.append(f"INVALID_COMBINE_MODE({combine_mode})")
            allow = model_pass

        if model_pass:
            reasons.append("MODEL_PASS")
        else:
            reasons.append("MODEL_BLOCK")

        if artifacts.rule_clauses:
            reasons.append("RULE_PASS" if rule_pass else "RULE_BLOCK")

        return self._decision(
            allow=allow,
            prob=prob,
            threshold=artifacts.threshold,
            reason_codes=tuple(reasons or ["OK"]),
            gate_id=gate_id,
            strategy_id=strategy_id,
            bundle_asof_ts=bundle_ts,
            feature_count=x.shape[1],
        )
