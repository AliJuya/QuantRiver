from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

from core.execution.trade_state import ClosedTrade, OpenPosition
from core.execution.trailing_engine_v3 import TrailingDefaults, attach_trailing, maybe_trail
from core.state.market_state import MarketState
from core.types import ExecutionIntent


def _normalize_side(action: str) -> str | None:
    up = action.strip().upper()
    if up in {"BUY", "LONG"}:
        return "LONG"
    if up in {"SELL", "SHORT"}:
        return "SHORT"
    if up == "CLOSE":
        return "CLOSE"
    return None


def _clone_payload(value):
    if isinstance(value, dict):
        return {str(key): _clone_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clone_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_clone_payload(item) for item in value]
    return value


def _safe_float(value) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _normalize_mode(value: str | None) -> str | None:
    if value is None:
        return None
    mode = str(value).strip().lower()
    if not mode:
        return None
    aliases = {
        "cash": "usd",
        "dollar": "usd",
        "price_abs": "price",
    }
    return aliases.get(mode, mode)


def _infer_plan_mode(payload: dict, kind: str) -> tuple[str | None, float | None]:
    prefixes = (kind,)
    if kind == "sl":
        prefixes = ("sl", "stop")
    elif kind == "tp":
        prefixes = ("tp", "target")

    for prefix in prefixes:
        mode = _normalize_mode(payload.get(f"{prefix}_mode"))
        value = _safe_float(payload.get(f"{prefix}_value"))
        if mode is not None and value is not None and value > 0.0:
            return mode, float(value)

    infer_keys = {
        "atr": [f"{prefix}_atr" for prefix in prefixes],
        "usd": [f"{prefix}_usd" for prefix in prefixes] + [f"{prefix}_dollar" for prefix in prefixes],
        "price": [f"{prefix}_price_abs" for prefix in prefixes],
    }
    for mode, keys in infer_keys.items():
        for key in keys:
            value = _safe_float(payload.get(key))
            if value is not None and value > 0.0:
                return mode, float(value)

    return None, None


def _plan_price_from_mode(
    *,
    side: str,
    entry_price: float,
    size: float,
    atr: float,
    kind: str,
    mode: str,
    value: float,
) -> float | None:
    normalized = _normalize_mode(mode)
    if normalized is None:
        return None

    if normalized == "price":
        return float(value)

    if normalized == "atr":
        distance = float(value) * max(1e-9, float(atr))
    elif normalized == "usd":
        distance = float(value) / max(1e-9, float(size))
    else:
        return None

    if kind == "sl":
        if side == "LONG":
            return float(entry_price - distance)
        return float(entry_price + distance)

    if side == "LONG":
        return float(entry_price + distance)
    return float(entry_price - distance)


def _resolve_plan_prices(
    side: str,
    entry_price: float,
    size: float,
    atr: float,
    payload: dict,
    default_sl: float,
    default_tp: float,
) -> tuple[float, float]:
    sl_mode, sl_value = _infer_plan_mode(payload, "sl")
    tp_mode, tp_value = _infer_plan_mode(payload, "tp")
    if sl_mode is not None and sl_value is not None and tp_mode is not None and tp_value is not None:
        sl_price = _plan_price_from_mode(
            side=side,
            entry_price=entry_price,
            size=size,
            atr=atr,
            kind="sl",
            mode=sl_mode,
            value=sl_value,
        )
        tp_price = _plan_price_from_mode(
            side=side,
            entry_price=entry_price,
            size=size,
            atr=atr,
            kind="tp",
            mode=tp_mode,
            value=tp_value,
        )
        if sl_price is not None and tp_price is not None:
            if side == "LONG" and sl_price < entry_price < tp_price:
                return float(sl_price), float(tp_price)
            if side == "SHORT" and tp_price < entry_price < sl_price:
                return float(sl_price), float(tp_price)

    custom_sl = None
    for key in ("custom_sl", "init_sl_price", "sl_price", "sl"):
        custom_sl = _safe_float(payload.get(key))
        if custom_sl is not None:
            break

    custom_tp = None
    for key in ("custom_tp", "init_tp_price", "tp_price", "tp"):
        custom_tp = _safe_float(payload.get(key))
        if custom_tp is not None:
            break

    if custom_sl is None or custom_tp is None:
        return float(default_sl), float(default_tp)

    if side == "LONG" and custom_sl < entry_price < custom_tp:
        return float(custom_sl), float(custom_tp)
    if side == "SHORT" and custom_tp < entry_price < custom_sl:
        return float(custom_sl), float(custom_tp)
    return float(default_sl), float(default_tp)


def _apply_slippage(price: float, side: str, slippage_rate: float, *, is_entry: bool) -> float:
    rate = max(0.0, float(slippage_rate))
    if rate <= 0.0:
        return float(price)

    if side == "LONG":
        return float(price * (1.0 + rate)) if is_entry else float(price * (1.0 - rate))
    return float(price * (1.0 - rate)) if is_entry else float(price * (1.0 + rate))


def _fee_amount(price: float, size: float, fee_rate: float) -> float:
    rate = max(0.0, float(fee_rate))
    if rate <= 0.0:
        return 0.0
    return float(abs(price * size) * rate)


def _serialize_model_bundle(state: MarketState) -> dict[str, object] | None:
    bundle = getattr(state, "model_bundle", None)
    if bundle is None:
        return None

    snapshots = getattr(bundle, "snapshots", None)
    if not isinstance(snapshots, dict) or not snapshots:
        return None

    models: dict[str, object] = {}
    for name, snapshot in snapshots.items():
        payload = getattr(snapshot, "payload", snapshot)
        model_row = _clone_payload(payload)
        version = getattr(snapshot, "version", None)
        if version is not None:
            model_row = {
                "version": str(version),
                "payload": model_row,
            }
        models[str(name)] = model_row

    return {
        "bundle_id": int(getattr(bundle, "bundle_id", 0)),
        "bundle_asof_ts": int(getattr(bundle, "bundle_asof_ts", 0)),
        "models": models,
    }


def _resolve_position_size(payload: dict) -> float:
    for key in ("position_size", "size", "quantity", "qty", "units"):
        value = _safe_float(payload.get(key))
        if value is None:
            continue
        if value > 0.0:
            return float(value)
        break
    return 1.0


@dataclass
class PositionHandler:
    BE_TRIG: float = 0.75
    BE_ATR_BUFFER: float = 0.10
    TP_EXTEND_ON: float = 1.00
    TP_EXTEND_NEAR_R: float = 0.15
    TP_EXTEND_R: float = 0.60
    TP_EXTEND_DECAY: float = 0.75
    MAX_TP_EXTENSIONS: int = 3
    STALL_ON_R: float = 1.50
    STALL_BARS: int = 6
    STALL_CHAND_ATR: float = 0.90

    sl_atr_mult: float = 1.2
    tp_atr_mult: float = 2.4
    fee_rate: float = 0.0
    slippage_rate: float = 0.0
    trailing_enabled: bool = False
    open_positions: dict[tuple[str, str], OpenPosition] = field(default_factory=dict)
    closed_trades: list[ClosedTrade] = field(default_factory=list)
    applied: int = 0
    _trail_logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__), init=False, repr=False)

    def __post_init__(self) -> None:
        self.fee_rate = max(0.0, float(self.fee_rate))
        self.slippage_rate = max(0.0, float(self.slippage_rate))

    def apply_intent(self, intent: ExecutionIntent, state: MarketState) -> dict:
        self.applied += 1
        candle = state.get_candle(intent.tf, intent.candle_open_time) or state.last_candle(intent.tf)
        if candle is None:
            return {"status": "ignored", "reason": "missing_candle"}

        side = _normalize_side(intent.action)
        if side is None:
            return {"status": "ignored", "reason": "unsupported_action", "action": intent.action}

        key = self._position_key(intent.strategy_id, intent.tf)
        closed_trades: list[ClosedTrade] = []

        if intent.force_close_all:
            closed_trades.extend(
                self._close_positions(
                    tuple(self.open_positions.keys()),
                    exit_time=candle.close_time,
                    exit_price=float(candle.close),
                    reason=intent.reason or "FORCE_CLOSE_ALL",
                )
            )

        if side == "CLOSE":
            target_keys = tuple(self.open_positions.keys()) if intent.force_close_all or not intent.strategy_id else (key,)
            closed_trades.extend(
                self._close_positions(
                    target_keys,
                    exit_time=candle.close_time,
                    exit_price=float(candle.close),
                    reason=intent.reason or "CLOSE",
                )
            )
            if not closed_trades:
                return {"status": "ignored", "reason": "no_open_position"}
            return self._result_with_closed(
                status="closed",
                closed_trades=closed_trades,
            )

        existing = self.open_positions.get(key)
        if existing and existing.side == side:
            return self._result_with_closed(
                status="held",
                closed_trades=closed_trades,
                side=side,
            )

        if existing and existing.side != side:
            closed_trades.extend(
                self._close_positions(
                    (key,),
                    exit_time=candle.close_time,
                    exit_price=float(candle.close),
                    reason=intent.reason or "FLIP",
                )
            )

        raw_entry_price = float(candle.close)
        entry_price = _apply_slippage(raw_entry_price, side, self.slippage_rate, is_entry=True)
        atr = float(candle.indicators.get("ATR14") or max(abs(float(candle.high) - float(candle.low)), 1e-9))
        if side == "LONG":
            sl_price = entry_price - self.sl_atr_mult * atr
            tp_price = entry_price + self.tp_atr_mult * atr
        else:
            sl_price = entry_price + self.sl_atr_mult * atr
            tp_price = entry_price - self.tp_atr_mult * atr

        metadata = dict(intent.payload)
        position_size = _resolve_position_size(metadata)
        sl_price, tp_price = _resolve_plan_prices(
            side,
            entry_price,
            position_size,
            atr,
            metadata,
            sl_price,
            tp_price,
        )
        unit_risk = abs(entry_price - sl_price)
        unit_reward = abs(tp_price - entry_price)
        risk = unit_risk * position_size
        reward = unit_reward * position_size
        rr_planned = (unit_reward / unit_risk) if unit_risk > 0 else 0.0
        entry_fee = _fee_amount(entry_price, position_size, self.fee_rate)
        entry_slippage_cash = abs(entry_price - raw_entry_price) * position_size

        metadata["position_size"] = float(position_size)
        metadata["entry_price"] = float(entry_price)
        metadata["raw_entry_price"] = float(raw_entry_price)
        metadata["init_sl_price"] = float(sl_price)
        metadata["init_tp_price"] = float(tp_price)
        metadata["risk_usd"] = float(risk)
        metadata["reward_usd"] = float(reward)
        metadata["rr_planned"] = float(rr_planned)
        metadata["fee_rate"] = float(self.fee_rate)
        metadata["slippage_rate"] = float(self.slippage_rate)
        metadata["entry_fee"] = float(entry_fee)
        metadata["entry_slippage_cash"] = float(entry_slippage_cash)

        model_bundle = _serialize_model_bundle(state)
        if model_bundle is not None and "model_bundle" not in metadata:
            metadata["model_bundle"] = model_bundle

        self.open_positions[key] = OpenPosition(
            side=side,
            entry_time=candle.close_time,
            entry_price=entry_price,
            size=float(position_size),
            strategy_id=intent.strategy_id,
            tf=intent.tf,
            sl_price=sl_price,
            tp_price=tp_price,
            entry_atr=atr,
            metadata=metadata,
        )
        if self.trailing_enabled:
            init_risk_dist = max(1e-9, abs(entry_price - sl_price))
            attach_trailing(
                pos=self.open_positions[key],
                strategy_id=intent.strategy_id,
                entry_price=entry_price,
                init_dist=init_risk_dist,
            )
            self.open_positions[key].metadata["trailing_enabled"] = True
            self.open_positions[key].metadata["trail_init_risk_dist"] = float(init_risk_dist)
        return self._result_with_closed(
            status="opened",
            closed_trades=closed_trades,
            side=side,
        )

    def on_candle(self, candle) -> list[ClosedTrade]:
        closed: list[ClosedTrade] = []
        for key, pos in list(self.open_positions.items()):
            if self.trailing_enabled:
                maybe_trail(
                    pos=pos,
                    price=float(candle.close),
                    logger=self._trail_logger,
                    defaults=TrailingDefaults(
                        BE_TRIG=self.BE_TRIG,
                        BE_ATR_BUFFER=self.BE_ATR_BUFFER,
                        TP_EXTEND_ON=self.TP_EXTEND_ON,
                        TP_EXTEND_NEAR_R=self.TP_EXTEND_NEAR_R,
                        TP_EXTEND_R=self.TP_EXTEND_R,
                        TP_EXTEND_DECAY=self.TP_EXTEND_DECAY,
                        MAX_TP_EXTENSIONS=self.MAX_TP_EXTENSIONS,
                        STALL_ON_R=self.STALL_ON_R,
                        STALL_BARS=self.STALL_BARS,
                        STALL_CHAND_ATR=self.STALL_CHAND_ATR,
                    ),
                )
            if pos.side == "LONG":
                if float(candle.low) <= pos.sl_price:
                    closed.extend(
                        self._close_positions(
                            (key,),
                            exit_time=candle.close_time,
                            exit_price=float(pos.sl_price),
                            reason="SL",
                        )
                    )
                    continue
                if float(candle.high) >= pos.tp_price:
                    closed.extend(
                        self._close_positions(
                            (key,),
                            exit_time=candle.close_time,
                            exit_price=float(pos.tp_price),
                            reason="TP",
                        )
                    )
                    continue
            else:
                if float(candle.high) >= pos.sl_price:
                    closed.extend(
                        self._close_positions(
                            (key,),
                            exit_time=candle.close_time,
                            exit_price=float(pos.sl_price),
                            reason="SL",
                        )
                    )
                    continue
                if float(candle.low) <= pos.tp_price:
                    closed.extend(
                        self._close_positions(
                            (key,),
                            exit_time=candle.close_time,
                            exit_price=float(pos.tp_price),
                            reason="TP",
                        )
                    )
                    continue
        return closed

    def close_open_position(self, candle, reason: str = "EOD") -> list[ClosedTrade]:
        return self._close_positions(
            tuple(self.open_positions.keys()),
            exit_time=candle.close_time,
            exit_price=float(candle.close),
            reason=reason,
        )

    @staticmethod
    def _position_key(strategy_id: str, tf: str) -> tuple[str, str]:
        sid = str(strategy_id or "__default__")
        return sid, str(tf or "1s")

    def _close_positions(
        self,
        keys: tuple[tuple[str, str], ...],
        exit_time,
        exit_price: float,
        reason: str,
    ) -> list[ClosedTrade]:
        out: list[ClosedTrade] = []
        for key in keys:
            pos = self.open_positions.pop(key, None)
            if pos is None:
                continue

            raw_exit_price = float(exit_price)
            actual_exit_price = _apply_slippage(raw_exit_price, pos.side, self.slippage_rate, is_entry=False)
            gross_pnl = (actual_exit_price - pos.entry_price) * float(pos.size)
            if pos.side == "SHORT":
                gross_pnl = -gross_pnl

            entry_fee = _safe_float(pos.metadata.get("entry_fee")) or _fee_amount(pos.entry_price, pos.size, self.fee_rate)
            exit_fee = _fee_amount(actual_exit_price, pos.size, self.fee_rate)
            total_fees = float(entry_fee + exit_fee)
            exit_slippage_cash = abs(actual_exit_price - raw_exit_price) * float(pos.size)
            net_pnl = float(gross_pnl - total_fees)
            metadata = dict(pos.metadata)
            metadata["raw_exit_price"] = float(raw_exit_price)
            metadata["exit_fee"] = float(exit_fee)
            metadata["total_fees"] = float(total_fees)
            metadata["exit_slippage_cash"] = float(exit_slippage_cash)
            metadata["gross_pnl"] = float(gross_pnl)
            metadata["net_pnl"] = float(net_pnl)

            trade = ClosedTrade(
                side=pos.side,
                entry_time=pos.entry_time,
                exit_time=exit_time,
                entry_price=pos.entry_price,
                exit_price=actual_exit_price,
                size=float(pos.size),
                strategy_id=pos.strategy_id,
                tf=pos.tf,
                reason=reason,
                pnl=float(net_pnl),
                metadata=metadata,
            )
            self.closed_trades.append(trade)
            out.append(trade)
        return out

    @staticmethod
    def _result_with_closed(
        *,
        status: str,
        closed_trades: list[ClosedTrade],
        side: str | None = None,
    ) -> dict:
        result = {"status": status}
        if side is not None:
            result["side"] = side
        if not closed_trades:
            return result
        if len(closed_trades) == 1:
            result["closed_trade"] = closed_trades[0]
        else:
            result["closed_trades"] = list(closed_trades)
        return result

    def stats(self) -> dict:
        wins = sum(1 for t in self.closed_trades if t.pnl > 0)
        losses = sum(1 for t in self.closed_trades if t.pnl < 0)
        net_pnl = sum(t.pnl for t in self.closed_trades)
        return {
            "applied": self.applied,
            "open_positions": len(self.open_positions),
            "open_position_keys": [f"{sid}:{tf}:{pos.side}" for (sid, tf), pos in self.open_positions.items()],
            "closed_trades": len(self.closed_trades),
            "wins": wins,
            "losses": losses,
            "net_pnl": float(net_pnl),
        }
