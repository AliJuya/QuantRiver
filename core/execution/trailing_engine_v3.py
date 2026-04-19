from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from core.execution.trailing_configs import get_trailing_config
except Exception:  # pragma: no cover
    get_trailing_config = None


@dataclass(frozen=True)
class TrailingDefaults:
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


class _NullLogger:
    def info(self, message: str) -> None:
        del message


def attach_trailing(*, pos, strategy_id: str | None = None, entry_price: float, init_dist: float) -> None:
    try:
        pos._trail_strategy_id = strategy_id
    except Exception:
        pass

    if get_trailing_config is not None:
        try:
            pos._trail_cfg = get_trailing_config(strategy_id)
        except Exception:
            pos._trail_cfg = None
    else:
        pos._trail_cfg = None

    pos._trail_init_dist = float(max(1e-9, float(init_dist)))
    pos._trail_best = float(entry_price)
    pos._trail_best_r = 0.0
    pos._trail_stall = 0
    pos._trail_tp_ext_count = 0


def _get_extend_tp(cfg: dict[str, Any], pos) -> bool:
    strategy_id = None
    try:
        strategy_id = getattr(pos, "_trail_strategy_id", None) or getattr(pos, "strategy_id", None)
    except Exception:
        strategy_id = None

    if "EXTEND_TP" in cfg:
        return bool(cfg.get("EXTEND_TP", True))
    if isinstance(strategy_id, str) and strategy_id.strip() == "TrendBreakPullbackContinue5m":
        return False
    return True


def _profit_lock_r(mfe_r: float) -> float | None:
    if mfe_r < 0.75:
        return None
    if mfe_r < 1.00:
        return 0.00
    if mfe_r < 1.50:
        return 0.15
    if mfe_r < 2.00:
        return 0.55
    if mfe_r < 3.00:
        return 1.00
    base = 1.00 + 0.80 * (mfe_r - 2.00)
    return max(1.00, base)


def _chandelier_atr_mult(mfe_r: float) -> float | None:
    if mfe_r < 1.50:
        return None
    if mfe_r < 2.00:
        return 1.80
    if mfe_r < 3.00:
        return 1.40
    return 1.10


def _tp_extension_size_r(*, defaults: TrailingDefaults, cfg: dict[str, Any], ext_count: int) -> float:
    base_r = float(cfg.get("TP_EXTEND_R", defaults.TP_EXTEND_R))
    decay = float(cfg.get("TP_EXTEND_DECAY", defaults.TP_EXTEND_DECAY))
    ext_size = base_r * (decay ** max(0, ext_count))
    return max(0.10, ext_size)


def maybe_trail(*, pos, price: float, logger=None, defaults: TrailingDefaults | None = None) -> None:
    cfg = getattr(pos, "_trail_cfg", None) or {}
    if not bool(cfg.get("enabled", True)):
        return

    logger = logger or _NullLogger()
    defaults = defaults or TrailingDefaults()

    atr = float(getattr(pos, "entry_atr", 0.0) or 0.0)
    if atr <= 0.0:
        return

    entry = float(pos.entry_price)
    price = float(price)
    init_dist = float(getattr(pos, "_trail_init_dist", 0.0) or 0.0)
    if init_dist <= 0.0:
        init_dist = max(1e-9, abs(entry - float(pos.sl_price)))
        pos._trail_init_dist = init_dist

    best = float(getattr(pos, "_trail_best", entry))
    if pos.side == "LONG":
        best = max(best, price)
        mfe = best - entry
        current_tp_gap = float(pos.tp_price) - best
    else:
        best = min(best, price)
        mfe = entry - best
        current_tp_gap = best - float(pos.tp_price)
    pos._trail_best = best

    mfe_r = mfe / init_dist

    best_r = float(getattr(pos, "_trail_best_r", 0.0) or 0.0)
    stall = int(getattr(pos, "_trail_stall", 0) or 0)
    if mfe_r > best_r + 1e-9:
        best_r = mfe_r
        stall = 0
    else:
        stall += 1
    pos._trail_best_r = best_r
    pos._trail_stall = stall

    be_trig = float(cfg.get("BE_TRIG", defaults.BE_TRIG))
    be_atr_buffer = float(cfg.get("BE_ATR_BUFFER", defaults.BE_ATR_BUFFER))

    target_sl = float(pos.sl_price)
    if mfe_r >= be_trig:
        if pos.side == "LONG":
            target_sl = max(target_sl, entry + be_atr_buffer * atr)
        else:
            target_sl = min(target_sl, entry - be_atr_buffer * atr)

    lock_r = _profit_lock_r(mfe_r)
    if lock_r is not None:
        lock_price = entry + lock_r * init_dist if pos.side == "LONG" else entry - lock_r * init_dist
        if pos.side == "LONG":
            target_sl = max(target_sl, float(lock_price))
        else:
            target_sl = min(target_sl, float(lock_price))

    chand_mult = _chandelier_atr_mult(mfe_r)
    if chand_mult is not None:
        chand_price = best - chand_mult * atr if pos.side == "LONG" else best + chand_mult * atr
        if pos.side == "LONG":
            target_sl = max(target_sl, float(chand_price))
        else:
            target_sl = min(target_sl, float(chand_price))

    if target_sl != float(pos.sl_price):
        pos.sl_price = float(target_sl)
        logger.info(f"[TRAIL][LOCK] {pos.side} SL->{float(pos.sl_price):.2f} mfe_r={mfe_r:.2f}")

    extend_tp = _get_extend_tp(cfg, pos)
    tp_extend_on = float(cfg.get("TP_EXTEND_ON", defaults.TP_EXTEND_ON))
    tp_extend_near_r = float(cfg.get("TP_EXTEND_NEAR_R", defaults.TP_EXTEND_NEAR_R))
    max_ext = int(cfg.get("MAX_TP_EXTENSIONS", defaults.MAX_TP_EXTENSIONS))
    ext_count = int(getattr(pos, "_trail_tp_ext_count", 0) or 0)

    if extend_tp and mfe_r >= tp_extend_on and ext_count < max_ext and current_tp_gap <= tp_extend_near_r * init_dist:
        ext_r = _tp_extension_size_r(defaults=defaults, cfg=cfg, ext_count=ext_count)
        ext_px = ext_r * init_dist
        if pos.side == "LONG":
            pos.tp_price = float(pos.tp_price) + ext_px
        else:
            pos.tp_price = float(pos.tp_price) - ext_px
        pos._trail_tp_ext_count = ext_count + 1
        logger.info(
            f"[TRAIL][TPX] {pos.side} TP->{float(pos.tp_price):.2f} mfe_r={mfe_r:.2f} ext_count={pos._trail_tp_ext_count}"
        )

    stall_on_r = float(cfg.get("STALL_ON_R", defaults.STALL_ON_R))
    stall_bars = int(cfg.get("STALL_BARS", defaults.STALL_BARS))
    stall_chand_atr = float(cfg.get("STALL_CHAND_ATR", defaults.STALL_CHAND_ATR))

    if best_r >= stall_on_r and stall >= stall_bars:
        stall_target = best - stall_chand_atr * atr if pos.side == "LONG" else best + stall_chand_atr * atr
        if pos.side == "LONG":
            if stall_target > float(pos.sl_price):
                pos.sl_price = float(stall_target)
                logger.info(
                    f"[TRAIL][STALL] {pos.side} SL->{float(pos.sl_price):.2f} best_r={best_r:.2f} stall={stall}"
                )
        else:
            if stall_target < float(pos.sl_price):
                pos.sl_price = float(stall_target)
                logger.info(
                    f"[TRAIL][STALL] {pos.side} SL->{float(pos.sl_price):.2f} best_r={best_r:.2f} stall={stall}"
                )
        pos._trail_stall = 0
