from __future__ import annotations

import calendar
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from multiprocessing import freeze_support
from pathlib import Path

import pandas as pd

from adapters.backtest import ParquetBacktestDataSource
from core.data_engine.boot_seeder import BootSeeder
from core.data_engine.candle_aggregator_tf import tf_to_seconds
from core.data_engine.candle_river import Candle, CandleRiver
from core.data_engine.data_engine import DataEngine, DataEngineConfig
from core.data_engine.warmup_registry import WarmupReq, compute_global_warmup
from core.engine import CoreEngine, SignalEngine, StrategyRouter, prime_existing_history
from core.execution import BacktestExecutionAdapter, Reporter
from core.execution.position_handler import PositionHandler
from core.indicators import IndicatorEngine
from core.models import ModelModule
from core.state.events import TFClosedEvent, create_engine_event_queue
from core.state.market_state import MarketState
from core.strategies import EMACross5mStrategy, OpeningRangeBreakout5m
from runners.runtime_config import env_bool, env_float, env_int, env_int_list, env_str

KLINES_BASE_PATH = env_str("QR_KLINES_BASE_PATH", "data/klines")
MODEL_KLINES_BASE_PATH = env_str("QR_MODEL_KLINES_BASE_PATH", "data/model_klines")
REPORTS_BASE_DIR = env_str("QR_REPORTS_BASE_DIR", "backtest_reports")
SYMBOL = env_str("QR_SYMBOL", "ETHUSDT")
BACKTEST_SOURCE_TF = "1s"
# years can be:
#   [2022, 2025] -> inclusive range 2022..2025
#   [2022, 2023, 2025] -> explicit list
YEARS = env_int_list("QR_YEARS", (2022, 2025))
DAY_FROM = env_int("QR_DAY_FROM", 1)
DAY_TO = env_int("QR_DAY_TO", 31)
START_BALANCE = env_float("QR_START_BALANCE", 1000.0)

# months can be:
#   [3, 9]  -> inclusive range 3..9
#   [1, 2, 5, 12] -> explicit list
MONTHS = env_int_list("QR_MONTHS", (1, 12))
AUTO_DAY_TO = env_bool("QR_AUTO_DAY_TO", True)

AGG_TFS = ("1m", "5m")
MAX_WORKERS = env_int("QR_MAX_WORKERS", min(8, max(1, (os.cpu_count() or 1) - 1)))
VERBOSE_MONTH_LOGS = env_bool("QR_VERBOSE_MONTH_LOGS", False)
WRITE_AGGREGATE_ARTIFACTS = env_bool("QR_WRITE_AGGREGATE_ARTIFACTS", True)
RUN_NAME_OVERRIDE = env_str("QR_RUN_NAME_OVERRIDE", "")
TRAILING_ENABLED = env_bool("QR_TRAILING_ENABLED", False)
FEE_RATE = env_float("QR_FEE_RATE", 0.0004)
SLIPPAGE_RATE = env_float("QR_SLIPPAGE_RATE", 0.0002)

ENABLE_EMA_CROSS_5M = env_bool("QR_ENABLE_EMA_CROSS_5M", True)
ENABLE_ORB_5M = env_bool("QR_ENABLE_ORB_5M", False)

EMA_FAST_LEN = env_int("QR_EMA_FAST_LEN", 12)
EMA_SLOW_LEN = env_int("QR_EMA_SLOW_LEN", 48)
EMA_STOP_MODE = env_str("QR_EMA_STOP_MODE", "atr")
EMA_STOP_VALUE = env_float("QR_EMA_STOP_VALUE", 1.5)
EMA_TARGET_MODE = env_str("QR_EMA_TARGET_MODE", "atr")
EMA_TARGET_VALUE = env_float("QR_EMA_TARGET_VALUE", 3.0)

# Pure strategy mode: hard-disable model engine.
USE_MODELS = env_bool("QR_BACKTEST_USE_MODELS", False)
MODEL_RECIPE = {
    "vol_pair_key": "15m|2h",
    "structure_base_tf": "15m",
    "regime_pair_key": "1h|12h",
    "regime_base_tf": "1h",
    "trigger_tf": "15m",
}
MODEL_NATIVE_TFS: tuple[str, ...] = ("1h", "2h", "12h") if USE_MODELS else ()
BUILD_TFS = MODEL_NATIVE_TFS + AGG_TFS
PROGRESS_EVERY_1S = 200_000


class _ManualCandle1sSource:
    def start(self, *, on_candle_1s) -> None:  # pragma: no cover - no-op adapter
        self._on_candle_1s = on_candle_1s

    def stop(self) -> None:  # pragma: no cover - no-op adapter
        pass


@dataclass
class MonthBacktestResult:
    year: int
    month: int
    day_from: int
    day_to: int
    strategy: str
    starting_balance: float
    final_balance: float
    net_pnl: float
    trades: int
    wins: int
    losses: int
    winrate: float
    max_dd_abs: float
    max_dd_pct: float
    first_ts: datetime | None
    last_ts: datetime | None
    elapsed_s: float
    report_path: str | None


@dataclass(frozen=True)
class TradePnlEvent:
    year: int
    month: int
    exit_time: datetime
    pnl: float


@dataclass(frozen=True)
class MonthJob:
    symbol: str
    run_name: str
    year: int
    month: int
    day_from: int
    day_to: int
    starting_balance: float


@dataclass
class MonthRunPayload:
    result: MonthBacktestResult
    trade_events: list[TradePnlEvent]


@dataclass(frozen=True)
class AggregateStats:
    starting_balance: float
    final_balance: float
    net_pnl: float
    trades: int
    wins: int
    losses: int
    winrate: float
    max_dd_abs: float
    max_dd_pct: float


def _month_days(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _available_days_for_month(symbol: str, year: int, month: int) -> list[int]:
    month_dir = Path(KLINES_BASE_PATH) / symbol / BACKTEST_SOURCE_TF / f"{year:04d}" / f"{month:02d}"
    if not month_dir.is_dir():
        return []

    days: list[int] = []
    for path in month_dir.glob("*.parquet"):
        try:
            day = int(path.stem)
        except ValueError:
            continue
        if 1 <= day <= 31:
            days.append(day)
    return sorted(set(days))


def _expand_months(months) -> list[int]:
    if not months:
        return []
    if isinstance(months, (tuple, list)) and len(months) == 2:
        start, end = months
        return list(range(int(start), int(end) + 1))
    return [int(m) for m in months]


def _expand_years(years) -> list[int]:
    if not years:
        return []
    if isinstance(years, (tuple, list)) and len(years) == 2:
        start, end = years
        return list(range(int(start), int(end) + 1))
    return [int(y) for y in years]


def _month_log(message: str) -> None:
    if VERBOSE_MONTH_LOGS:
        print(message)


def _scalar_for_json(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _sanitize_name(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "run"

    out: list[str] = []
    prev_sep = False
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
            prev_sep = False
            continue
        if ch in {"-", "_"}:
            out.append(ch)
            prev_sep = False
            continue
        if not prev_sep:
            out.append("_")
            prev_sep = True

    cleaned = "".join(out).strip("_-")
    return cleaned or "run"


def _format_selection_label(values: list[int], *, prefix: str, width: int = 0) -> str:
    if not values:
        return f"{prefix}none"

    ordered = sorted(set(int(v) for v in values))
    if len(ordered) >= 2 and ordered == list(range(ordered[0], ordered[-1] + 1)):
        start = f"{ordered[0]:0{width}d}" if width else str(ordered[0])
        end = f"{ordered[-1]:0{width}d}" if width else str(ordered[-1])
        return f"{prefix}{start}-{end}"

    items = [f"{value:0{width}d}" if width else str(value) for value in ordered]
    return f"{prefix}{'_'.join(items)}"


def _strategy_label(strategies: list) -> str:
    labels = [
        str(getattr(strategy, "strategy_id", strategy.__class__.__name__))
        for strategy in strategies
    ]
    return "__".join(labels) if labels else "NO_STRATEGY"


def _serialize_strategies(strategies: list) -> list[dict]:
    rows: list[dict] = []
    for strategy in strategies:
        params: dict[str, object] = {}
        for key, value in vars(strategy).items():
            if key.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                params[key] = value
            elif isinstance(value, tuple):
                params[key] = [_scalar_for_json(item) for item in value]
            elif isinstance(value, list):
                params[key] = [_scalar_for_json(item) for item in value]

        rows.append(
            {
                "class_name": strategy.__class__.__name__,
                "strategy_id": str(
                    getattr(strategy, "strategy_id", strategy.__class__.__name__)
                ),
                "params": params,
            }
        )
    return rows


def _build_run_name(
    *,
    symbol: str,
    years: list[int],
    months: list[int],
    day_from: int,
    day_to: int,
    strategy_label: str,
) -> str:
    base_prefix = RUN_NAME_OVERRIDE.strip()
    if not base_prefix:
        year_label = _format_selection_label(years, prefix="y")
        month_label = _format_selection_label(months, prefix="m", width=2)
        day_suffix = (
            f"d{int(day_from):02d}-eom"
            if AUTO_DAY_TO
            else f"d{int(day_from):02d}-{int(day_to):02d}"
        )
        strategy_part = _sanitize_name(strategy_label)
        base_prefix = f"{year_label}__{month_label}__{day_suffix}__{strategy_part}"

    base_prefix = _sanitize_name(base_prefix)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    trailing_label = "trail_on" if TRAILING_ENABLED else "trail_off"
    root_dir = Path(REPORTS_BASE_DIR) / symbol / "aggregate"
    candidate = f"{base_prefix}__{timestamp}__{trailing_label}"
    run_dir = root_dir / candidate
    suffix = 2
    while run_dir.exists():
        candidate = f"{base_prefix}__{timestamp}__r{suffix:02d}__{trailing_label}"
        run_dir = root_dir / candidate
        suffix += 1
    return candidate


def _build_run_config(
    *,
    run_name: str,
    symbol: str,
    years: list[int],
    months: list[int],
    day_from: int,
    day_to: int,
    strategy_label: str,
    strategies: list,
) -> dict[str, object]:
    return {
        "run_name": run_name,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "strategy_label": strategy_label,
        "reports_base_dir": REPORTS_BASE_DIR,
        "data_paths": {
            "stream_1s": KLINES_BASE_PATH,
            "model_native": MODEL_KLINES_BASE_PATH,
        },
        "backtest_source_tf": BACKTEST_SOURCE_TF,
        "years_input": [int(year) for year in years],
        "months_input": [int(month) for month in months],
        "day_from": int(day_from),
        "day_to": int(day_to),
        "auto_day_to": bool(AUTO_DAY_TO),
        "starting_balance": float(START_BALANCE),
        "max_workers": int(MAX_WORKERS),
        "verbose_month_logs": bool(VERBOSE_MONTH_LOGS),
        "pure_strategy_mode": {
            "use_models": bool(USE_MODELS),
            "use_signal_gates": False,
        },
        "progress_every_1s": int(PROGRESS_EVERY_1S),
        "agg_tfs": list(AGG_TFS),
        "build_tfs": list(BUILD_TFS),
        "trailing_enabled": bool(TRAILING_ENABLED),
        "strategies": _serialize_strategies(strategies),
    }


def build_strategies() -> list:
    strategies: list = []
    if ENABLE_EMA_CROSS_5M:
        strategies.append(
            EMACross5mStrategy(
                fast_len=EMA_FAST_LEN,
                slow_len=EMA_SLOW_LEN,
                stop_mode=EMA_STOP_MODE,
                stop_value=EMA_STOP_VALUE,
                target_mode=EMA_TARGET_MODE,
                target_value=EMA_TARGET_VALUE,
            )
        )
    if ENABLE_ORB_5M:
        strategies.append(OpeningRangeBreakout5m())
    if not strategies:
        raise ValueError("At least one strategy must be enabled.")
    return strategies


def _compute_warmup_by_tf(
    strategies: list, extra_reqs: list[WarmupReq] | None = None
) -> dict[str, int]:
    reqs = [
        WarmupReq(
            name=str(getattr(s, "strategy_id", s.__class__.__name__)),
            req=dict(getattr(s, "warmup_requirements", lambda: {})()),
        )
        for s in strategies
    ]
    if extra_reqs:
        reqs.extend(extra_reqs)

    out = compute_global_warmup(reqs)
    defaults = {"1m": 100, "5m": 200, "15m": 300}
    for tf, bars in defaults.items():
        out[tf] = max(out.get(tf, 0), bars)
    return out or defaults


def _estimate_lookback_days(warmup_by_tf: dict[str, int]) -> int:
    if not warmup_by_tf:
        return 1
    max_seconds = 0
    for tf, bars in warmup_by_tf.items():
        if bars <= 0:
            continue
        max_seconds = max(max_seconds, tf_to_seconds(tf) * int(bars))
    # Add one extra day so leap-year boundaries and the first historical bucket
    # do not leave model warmup one bar short.
    return max(1, math.ceil(max_seconds / 86_400.0) + 1)


def _next_month_start(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def _ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _row_to_native_candle(tf: str, row) -> Candle:
    open_time = _ms_to_utc(int(row.open_time))
    # Native parquet close_time is inclusive ms. Convert to the same exclusive
    # boundary convention used by CandleRiver/runtime candles.
    close_time = _ms_to_utc(int(row.close_time) + 1)
    return Candle(
        tf=tf,
        open_time=open_time,
        close_time=close_time,
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
        volume=float(row.volume),
    )


def _load_native_tf_candles(
    *,
    symbol: str,
    tf: str,
    start_date: date,
    end_date: date,
    lookback_days: int,
) -> list[Candle]:
    load_start = start_date - timedelta(days=max(0, int(lookback_days)))
    load_end = end_date + timedelta(days=1)

    month = date(load_start.year, load_start.month, 1)
    frames: list[pd.DataFrame] = []
    base_dir = Path(MODEL_KLINES_BASE_PATH) / symbol / tf

    while month < load_end:
        path = base_dir / f"{month.year:04d}" / f"{month.month:02d}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
        month = _next_month_start(month)

    if not frames:
        return []

    frame = pd.concat(frames, ignore_index=True)
    frame = frame.sort_values("open_time").reset_index(drop=True)

    start_ms = int(_dt_start(load_start).timestamp() * 1000)
    end_ms = int(_dt_start(load_end).timestamp() * 1000)
    frame = frame.loc[
        (frame["open_time"] >= start_ms) & (frame["open_time"] < end_ms)
    ].reset_index(drop=True)

    return [_row_to_native_candle(tf, row) for row in frame.itertuples(index=False)]


def _native_fetch_before_anchor(
    candles: list[Candle],
    *,
    anchor: datetime,
    count: int,
) -> list[Candle]:
    want = int(count)
    if want <= 0 or not candles:
        return []

    out = [c for c in candles if c.close_time <= anchor]
    if not out:
        return []
    return out[-want:]


def _next_native_index(candles: list[Candle], last_open_time) -> int:
    if not candles:
        return 0
    if last_open_time is None:
        return 0

    idx = 0
    size = len(candles)
    while idx < size and candles[idx].open_time <= last_open_time:
        idx += 1
    return idx


def _month_report_path(symbol: str, run_name: str, year: int, month: int) -> str:
    path = (
        Path(REPORTS_BASE_DIR)
        / symbol
        / "aggregate"
        / run_name
        / "months"
        / f"trades_{year}_{month:02d}.jsonl"
    )
    return str(path)


def _aggregate_report_dir(symbol: str, run_name: str) -> Path:
    return Path(REPORTS_BASE_DIR) / symbol / "aggregate" / run_name


def _dt_start(d: date) -> datetime:
    return datetime.combine(d, dt_time.min, tzinfo=timezone.utc)


def _emit_event(
    core_engine: CoreEngine,
    market_state: MarketState,
    *,
    tf: str,
    candle_open_time,
) -> None:
    if not market_state.is_warm:
        return
    core_engine.process_event(TFClosedEvent(tf=tf, candle_open_time=candle_open_time))


def _prime_pre_ready_history(
    core_engine: CoreEngine,
    market_state: MarketState,
) -> dict[str, object | None]:
    return prime_existing_history(
        core_engine=core_engine,
        market_state=market_state,
        tfs=("1s",) + BUILD_TFS,
    )


def _trade_events_from_rows(
    rows: list[dict], *, year: int, month: int
) -> list[TradePnlEvent]:
    events: list[TradePnlEvent] = []
    for row in rows:
        exit_time = row.get("exit_time")
        if not isinstance(exit_time, datetime):
            continue
        events.append(
            TradePnlEvent(
                year=year,
                month=month,
                exit_time=exit_time,
                pnl=float(row.get("pnl", 0.0)),
            )
        )
    return sorted(events, key=lambda item: item.exit_time)


def _compute_drawdown(
    trade_events: list[TradePnlEvent],
    *,
    starting_balance: float,
) -> tuple[float, float, float]:
    # Realized drawdown from closed-trade equity only.
    equity = float(starting_balance)
    peak = float(starting_balance)
    max_dd_abs = 0.0
    max_dd_pct = 0.0

    for event in sorted(trade_events, key=lambda item: item.exit_time):
        equity += float(event.pnl)
        if equity > peak:
            peak = equity
        dd_abs = peak - equity
        if dd_abs > max_dd_abs:
            max_dd_abs = dd_abs
        if peak > 0.0:
            dd_pct = (dd_abs / peak) * 100.0
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

    return equity, max_dd_abs, max_dd_pct


def _aggregate_stats(
    rows: list[MonthBacktestResult],
    trade_events: list[TradePnlEvent],
    *,
    starting_balance: float,
) -> AggregateStats:
    trades = sum(row.trades for row in rows)
    wins = sum(row.wins for row in rows)
    losses = sum(row.losses for row in rows)
    net_pnl = sum(row.net_pnl for row in rows)
    winrate = (wins / trades * 100.0) if trades else 0.0
    _, max_dd_abs, max_dd_pct = _compute_drawdown(
        trade_events, starting_balance=starting_balance
    )
    return AggregateStats(
        starting_balance=float(starting_balance),
        final_balance=float(starting_balance) + float(net_pnl),
        net_pnl=float(net_pnl),
        trades=int(trades),
        wins=int(wins),
        losses=int(losses),
        winrate=float(winrate),
        max_dd_abs=float(max_dd_abs),
        max_dd_pct=float(max_dd_pct),
    )


def _build_month_jobs(
    *,
    symbol: str,
    run_name: str,
    years: list[int],
    months: list[int],
    day_from: int,
    day_to: int,
    starting_balance: float,
) -> list[MonthJob]:
    jobs: list[MonthJob] = []
    for year in years:
        for month in months:
            requested_day_to = _month_days(year, month) if AUTO_DAY_TO else day_to
            available_days = _available_days_for_month(symbol, year, month)
            eligible_days = [
                day for day in available_days if day_from <= day <= requested_day_to
            ]
            if not eligible_days:
                print(
                    f"[SKIP] {year}-{month:02d} -> "
                    f"No daily parquet files found for {symbol} in requested range [{day_from},{requested_day_to}]"
                )
                continue

            jobs.append(
                MonthJob(
                    symbol=symbol,
                    run_name=run_name,
                    year=year,
                    month=month,
                    day_from=day_from,
                    day_to=max(eligible_days),
                    starting_balance=starting_balance,
                )
            )
    return jobs


def _run_jobs(jobs: list[MonthJob]) -> list[MonthRunPayload]:
    if not jobs:
        return []

    if MAX_WORKERS <= 1 or len(jobs) == 1:
        payloads: list[MonthRunPayload] = []
        total = len(jobs)
        for idx, job in enumerate(jobs, start=1):
            payload = _run_month(
                symbol=job.symbol,
                run_name=job.run_name,
                year=job.year,
                month=job.month,
                day_from=job.day_from,
                day_to=job.day_to,
                starting_balance=job.starting_balance,
            )
            payloads.append(payload)
            print(f"[MONTH_DONE] {job.year}-{job.month:02d} finished ({idx}/{total})")
        return payloads

    payloads: list[MonthRunPayload] = []
    worker_count = min(MAX_WORKERS, len(jobs))
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _run_month,
                symbol=job.symbol,
                run_name=job.run_name,
                year=job.year,
                month=job.month,
                day_from=job.day_from,
                day_to=job.day_to,
                starting_balance=job.starting_balance,
            ): job
            for job in jobs
        }
        completed = 0
        total = len(jobs)
        for future in as_completed(future_map):
            payload = future.result()
            payloads.append(payload)
            completed += 1
            result = payload.result
            print(
                f"[MONTH_DONE] {result.year}-{result.month:02d} finished ({completed}/{total})"
            )
    return payloads


def _print_aggregate_line(label: str, stats: AggregateStats) -> None:
    print(
        f"{label} -> equity:{stats.final_balance:.6f} , "
        f"pnl:{stats.net_pnl:+.6f} , "
        f"trades:{stats.trades} , "
        f"win/lose:{stats.wins}/{stats.losses} , "
        f"winrate:{stats.winrate:.2f}% , "
        f"max_dd:{stats.max_dd_abs:.6f} ({stats.max_dd_pct:.2f}%)"
    )


def _print_month_table(year: int, rows: list[MonthBacktestResult]) -> None:
    by_month = {row.month: row for row in rows}
    print(f"\n=== MONTHS ({year}) ===")
    print(
        "month | days  | pnl | trades | w/l | winrate | max_dd | end_balance | elapsed"
    )
    for month in range(1, 13):
        row = by_month.get(month)
        if row is None:
            print(
                f"{year}-{month:02d} | --    | -- | --     | --  | --      | --     | --          | --"
            )
            continue

        print(
            f"{row.year}-{row.month:02d} | "
            f"{row.day_from:02d}-{row.day_to:02d} | "
            f"{row.net_pnl:+.6f} | "
            f"{row.trades:6d} | "
            f"{row.wins}/{row.losses} | "
            f"{row.winrate:6.2f}% | "
            f"{row.max_dd_abs:.6f} ({row.max_dd_pct:.2f}%) | "
            f"{row.final_balance:.6f} | "
            f"{row.elapsed_s:.2f}s"
        )


def _load_trade_rows_from_reports(rows: list[MonthBacktestResult]) -> list[dict]:
    trade_rows: list[dict] = []
    for row in rows:
        if not row.report_path:
            continue

        path = Path(row.report_path)
        if not path.exists():
            continue

        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                raw = json.loads(text)
                exit_time = pd.to_datetime(
                    raw.get("exit_time"), utc=True, errors="coerce"
                )
                entry_time = pd.to_datetime(
                    raw.get("entry_time"), utc=True, errors="coerce"
                )
                if pd.isna(exit_time):
                    continue

                raw["entry_time"] = (
                    entry_time.to_pydatetime() if not pd.isna(entry_time) else None
                )
                raw["exit_time"] = exit_time.to_pydatetime()
                raw["pnl"] = float(raw.get("pnl", 0.0))
                raw["size"] = float(raw.get("size", 1.0))
                raw["year"] = int(getattr(raw["exit_time"], "year", row.year))
                raw["month"] = int(getattr(raw["exit_time"], "month", row.month))
                trade_rows.append(raw)

    trade_rows.sort(key=lambda item: item["exit_time"])
    return trade_rows


def _build_equity_curve_rows(
    trade_rows: list[dict],
    *,
    starting_balance: float,
) -> list[dict]:
    equity = float(starting_balance)
    peak_equity = float(starting_balance)
    out: list[dict] = []

    for idx, row in enumerate(trade_rows, start=1):
        pnl = float(row.get("pnl", 0.0))
        equity += pnl
        if equity > peak_equity:
            peak_equity = equity
        dd_abs = peak_equity - equity
        dd_pct = (dd_abs / peak_equity * 100.0) if peak_equity > 0.0 else 0.0
        exit_time = row.get("exit_time")
        year = getattr(exit_time, "year", row.get("year"))
        month = getattr(exit_time, "month", row.get("month"))

        out.append(
            {
                "trade_index": idx,
                "exit_time": exit_time,
                "year": int(year) if year is not None else None,
                "month": int(month) if month is not None else None,
                "strategy_id": str(row.get("strategy_id", "")),
                "tf": str(row.get("tf", "")),
                "reason": row.get("reason"),
                "pnl": pnl,
                "equity": float(equity),
                "peak_equity": float(peak_equity),
                "drawdown_abs": float(dd_abs),
                "drawdown_pct": float(dd_pct),
            }
        )

    return out


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str) + "\n")


def _save_equity_curve_plot(
    curve_rows: list[dict],
    *,
    symbol: str,
    strategy_name: str,
    interval_label: str,
    out_path: Path,
) -> str | None:
    if not curve_rows:
        return None

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        print(f"[WARN] could not save equity curve plot: {exc}")
        return None

    frame = pd.DataFrame(curve_rows)
    fig, (ax_eq, ax_dd) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        dpi=120,
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
        constrained_layout=True,
    )

    ax_eq.plot(
        frame["exit_time"],
        frame["equity"],
        color="#0f172a",
        linewidth=1.6,
        label="Equity",
    )
    ax_eq.plot(
        frame["exit_time"],
        frame["peak_equity"],
        color="#94a3b8",
        linewidth=1.0,
        alpha=0.50,
        label="Peak",
    )
    ax_eq.set_title(f"{symbol} | {strategy_name} | Equity Curve ({interval_label})")
    ax_eq.set_ylabel("Equity")
    ax_eq.grid(alpha=0.25)
    ax_eq.legend(loc="best")

    ax_dd.fill_between(
        frame["exit_time"],
        frame["drawdown_abs"],
        color="#dc2626",
        alpha=0.35,
        label="Drawdown",
    )
    ax_dd.set_ylabel("DD")
    ax_dd.set_xlabel("Exit Time")
    ax_dd.grid(alpha=0.25)
    ax_dd.legend(loc="best")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return str(out_path)


def _write_aggregate_artifacts(
    *,
    symbol: str,
    run_name: str,
    strategy_name: str,
    interval_label: str,
    run_config: dict[str, object],
    rows: list[MonthBacktestResult],
    year_stats_by_year: dict[int, AggregateStats],
    overall_stats: AggregateStats,
    trade_rows: list[dict],
    curve_rows: list[dict],
) -> dict[str, str | None]:
    out_dir = _aggregate_report_dir(symbol, run_name)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades_path = out_dir / "trades.jsonl"
    equity_csv_path = out_dir / "equity_curve.csv"
    months_csv_path = out_dir / "months.csv"
    years_csv_path = out_dir / "years.csv"
    summary_json_path = out_dir / "summary.json"
    run_config_json_path = out_dir / "run_config.json"
    equity_png_path = out_dir / "equity_curve.png"

    _write_jsonl(trades_path, trade_rows)
    pd.DataFrame(curve_rows).to_csv(equity_csv_path, index=False)
    pd.DataFrame([asdict(row) for row in rows]).to_csv(months_csv_path, index=False)
    pd.DataFrame(
        [
            {
                "year": year,
                "starting_balance": stats.starting_balance,
                "final_balance": stats.final_balance,
                "net_pnl": stats.net_pnl,
                "trades": stats.trades,
                "wins": stats.wins,
                "losses": stats.losses,
                "winrate": stats.winrate,
                "max_dd_abs": stats.max_dd_abs,
                "max_dd_pct": stats.max_dd_pct,
            }
            for year, stats in sorted(year_stats_by_year.items())
        ]
    ).to_csv(years_csv_path, index=False)

    summary_payload = {
        "symbol": symbol,
        "run_name": run_name,
        "strategy": strategy_name,
        "interval": interval_label,
        "overall": asdict(overall_stats),
        "years": {
            str(year): asdict(stats)
            for year, stats in sorted(year_stats_by_year.items())
        },
        "artifacts": {
            "trades_jsonl": str(trades_path),
            "equity_curve_csv": str(equity_csv_path),
            "equity_curve_png": str(equity_png_path),
            "months_csv": str(months_csv_path),
            "years_csv": str(years_csv_path),
            "run_config_json": str(run_config_json_path),
        },
    }
    summary_json_path.write_text(
        json.dumps(summary_payload, indent=2, default=str), encoding="utf-8"
    )
    run_config_json_path.write_text(
        json.dumps(run_config, indent=2, default=str), encoding="utf-8"
    )
    png_path = _save_equity_curve_plot(
        curve_rows,
        symbol=symbol,
        strategy_name=strategy_name,
        interval_label=interval_label,
        out_path=equity_png_path,
    )

    return {
        "dir": str(out_dir),
        "trades_jsonl": str(trades_path),
        "equity_curve_csv": str(equity_csv_path),
        "equity_curve_png": png_path,
        "months_csv": str(months_csv_path),
        "years_csv": str(years_csv_path),
        "summary_json": str(summary_json_path),
        "run_config_json": str(run_config_json_path),
    }


def _run_month(
    *,
    symbol: str,
    run_name: str,
    year: int,
    month: int,
    day_from: int,
    day_to: int,
    starting_balance: float,
) -> MonthRunPayload:
    available_days = _available_days_for_month(symbol, year, month)
    eligible_days = [day for day in available_days if day_from <= day <= day_to]
    if not eligible_days:
        raise FileNotFoundError(
            f"No daily parquet files found for {symbol} {year}-{month:02d} "
            f"in requested range [{day_from},{day_to}]"
        )

    effective_day_to = max(eligible_days)
    day_to = effective_day_to

    strategies = build_strategies()
    strategy_label = "__".join(strategy.strategy_id for strategy in strategies)
    report_path = _month_report_path(symbol, run_name, year, month)
    model_module = ModelModule.from_recipe(**MODEL_RECIPE) if USE_MODELS else None
    extra_reqs = (
        [WarmupReq(name="model_engine", req=model_module.warmup_requirements())]
        if model_module
        else None
    )
    warmup_by_tf = _compute_warmup_by_tf(
        strategies,
        extra_reqs=extra_reqs,
    )
    stream_warmup_by_tf = {
        tf: bars for tf, bars in warmup_by_tf.items() if tf not in MODEL_NATIVE_TFS
    }
    model_warmup_by_tf = {
        tf: bars for tf, bars in warmup_by_tf.items() if tf in MODEL_NATIVE_TFS
    }
    lookback_days = _estimate_lookback_days(stream_warmup_by_tf)
    model_lookback_days = _estimate_lookback_days(model_warmup_by_tf)

    start_date = date(year, month, day_from)
    end_date = date(year, month, day_to)
    stream_from = _dt_start(start_date)

    source = ParquetBacktestDataSource.from_daily_1s_range(
        base_path=KLINES_BASE_PATH,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
        allow_missing_required=True,
        replay_hz=None,
        start_paused=True,
    )
    native_model_candles = {}
    if USE_MODELS and MODEL_NATIVE_TFS:
        native_model_candles = {
            tf: _load_native_tf_candles(
                symbol=symbol,
                tf=tf,
                start_date=start_date,
                end_date=end_date,
                lookback_days=model_lookback_days,
            )
            for tf in MODEL_NATIVE_TFS
        }

    de = DataEngine(
        config=DataEngineConfig(
            input_mode="1s",
            tick_river_maxlen=50_000,
            candle_river_maxlen=50_000,
            tfs=AGG_TFS,
        ),
        candle1s_source=_ManualCandle1sSource(),
    )
    for tf in MODEL_NATIVE_TFS:
        de.rivers_by_tf[tf] = CandleRiver(tf, maxlen=de.cfg.candle_river_maxlen)

    seeder = BootSeeder(rivers_by_tf=de.rivers_by_tf, tick_river=de.tick_river)
    market_state = MarketState(rivers_by_tf=de.rivers_by_tf, tick_river=de.tick_river)
    execution_adapter = BacktestExecutionAdapter(
        position_handler=PositionHandler(
            trailing_enabled=TRAILING_ENABLED,
            fee_rate=FEE_RATE,
            slippage_rate=SLIPPAGE_RATE,
        ),
        reporter=Reporter(out_path=report_path, append=False, echo_console=False),
    )
    core_engine = CoreEngine(
        event_q=create_engine_event_queue(),
        market_state=market_state,
        indicator_engine=IndicatorEngine(),
        strategy_router=StrategyRouter(strategies=strategies),
        signal_engine=SignalEngine(gates=(), gate_engine=None),
        execution_adapter=execution_adapter,
        models=[model_module] if model_module is not None else [],
        modules=[execution_adapter],
    )

    first_ts: datetime | None = None
    last_ts: datetime | None = None
    fed_1s = 0
    anchor: datetime | None = None
    warmup_summary: dict | None = None
    last_seen_by_tf = {tf: None for tf in ("1s",) + BUILD_TFS}
    native_next_idx = {tf: 0 for tf in MODEL_NATIVE_TFS}

    _month_log(
        f"[BACKTEST] {year}-{month:02d} {symbol} strategy={strategy_label} "
        f"days=[{day_from},{day_to}] warmup={warmup_by_tf}"
    )
    t0 = time.time()

    try:
        for c1s in source.iter_candles_1s(stream_from=stream_from):
            fed_1s += 1
            if first_ts is None:
                first_ts = c1s.open_time
            last_ts = c1s.open_time

            de.river_1s.push_closed(c1s)
            de.agg_tf.on_1s_close(c1s)

            if anchor is None:
                c1m = de.rivers_by_tf["1m"].last()
                if c1m is None:
                    continue

                anchor = c1m.open_time
                removed = seeder.cleanup_pre_anchor(anchor)
                fetched_history: dict[str, list[Candle]] = {}
                warmup_summary = {}
                for tf, requested in warmup_by_tf.items():
                    want = int(requested)
                    if want <= 0:
                        continue
                    if tf not in de.rivers_by_tf:
                        warmup_summary[tf] = {
                            "requested": want,
                            "fetched": 0,
                            "seeded": 0,
                            "skipped": "missing_river",
                        }
                        continue
                    if tf in MODEL_NATIVE_TFS:
                        candles = _native_fetch_before_anchor(
                            native_model_candles.get(tf, []),
                            anchor=anchor,
                            count=want,
                        )
                    else:
                        candles = source.fetch_candles_before_anchor(tf, anchor, want)
                    fetched_history[tf] = candles
                    warmup_summary[tf] = {
                        "requested": want,
                        "fetched": len(candles),
                        "seeded": len(candles),
                    }
                seeder.seed_history_before_anchor(anchor, fetched_history)
                pre_ready_last = _prime_pre_ready_history(core_engine, market_state)
                market_state.set_warm(True)

                for tf, last_open_time in pre_ready_last.items():
                    last_seen_by_tf[tf] = last_open_time
                for tf in MODEL_NATIVE_TFS:
                    native_next_idx[tf] = _next_native_index(
                        native_model_candles.get(tf, []),
                        last_seen_by_tf.get(tf),
                    )

                _month_log(f"[BACKTEST] ANCHOR={anchor.isoformat()} cleanup={removed}")
                _month_log(f"[BACKTEST] warmup seed -> {warmup_summary}")
                for tf, info in (warmup_summary or {}).items():
                    requested = int(info.get("requested", 0))
                    fetched = int(info.get("fetched", 0))
                    if fetched < requested:
                        _month_log(
                            f"[WARMUP_WARN] tf={tf} requested={requested} fetched={fetched} "
                            f"short={requested - fetched}"
                        )
                continue

            for tf in MODEL_NATIVE_TFS:
                candles = native_model_candles.get(tf, [])
                idx = native_next_idx[tf]
                while idx < len(candles) and candles[idx].close_time <= c1s.open_time:
                    de.rivers_by_tf[tf].push_closed(candles[idx])
                    idx += 1
                native_next_idx[tf] = idx

            _emit_event(
                core_engine, market_state, tf="1s", candle_open_time=c1s.open_time
            )
            last_seen_by_tf["1s"] = c1s.open_time

            for tf in BUILD_TFS:
                last = de.rivers_by_tf[tf].last()
                if last is None:
                    continue
                if last.open_time == last_seen_by_tf[tf]:
                    continue
                last_seen_by_tf[tf] = last.open_time
                _emit_event(
                    core_engine, market_state, tf=tf, candle_open_time=last.open_time
                )

            if fed_1s % PROGRESS_EVERY_1S == 0:
                _month_log(
                    f"[PROGRESS] {year}-{month:02d} fed_1s={fed_1s} "
                    f"last_ts={last_ts.isoformat() if last_ts else 'NA'} "
                    f"core={core_engine.stats()} exec={execution_adapter.stats()}"
                )

        if anchor is None:
            raise RuntimeError("No anchor was formed in the requested period.")

        execution_adapter.close_open_position(market_state, tf="1s", reason="EOD")
        trade_events = _trade_events_from_rows(
            execution_adapter.reporter.rows, year=year, month=month
        )
        summary = execution_adapter.summary(starting_balance=starting_balance)
        _, max_dd_abs, max_dd_pct = _compute_drawdown(
            trade_events, starting_balance=starting_balance
        )
        elapsed_s = time.time() - t0

        _month_log(
            f"[RESULT] {year}-{month:02d} -> balance:{summary['final_balance']:.6f} "
            f"pnl:{summary['net_pnl']:+.6f} trades:{summary['trades']} "
            f"win/lose:{summary['wins']}/{summary['losses']} "
            f"winrate:{summary['winrate']:.2f}% elapsed:{elapsed_s:.2f}s"
        )

        return MonthRunPayload(
            result=MonthBacktestResult(
                year=year,
                month=month,
                day_from=day_from,
                day_to=day_to,
                strategy=strategy_label,
                starting_balance=starting_balance,
                final_balance=summary["final_balance"],
                net_pnl=summary["net_pnl"],
                trades=summary["trades"],
                wins=summary["wins"],
                losses=summary["losses"],
                winrate=summary["winrate"],
                max_dd_abs=max_dd_abs,
                max_dd_pct=max_dd_pct,
                first_ts=first_ts,
                last_ts=last_ts,
                elapsed_s=elapsed_s,
                report_path=summary["report_path"],
            ),
            trade_events=trade_events,
        )
    finally:
        core_engine.stop()
        de.stop()


def main() -> None:
    if not Path(KLINES_BASE_PATH).exists():
        print(f"Klines base path not found: {KLINES_BASE_PATH}")
        return
    if USE_MODELS and not Path(MODEL_KLINES_BASE_PATH).exists():
        print(f"Model klines base path not found: {MODEL_KLINES_BASE_PATH}")
        return

    years = _expand_years(YEARS)
    if not years:
        print("No years selected.")
        return

    months = _expand_months(MONTHS)
    if not months:
        print("No months selected.")
        return

    configured_strategies = build_strategies()
    strategy_label = _strategy_label(configured_strategies)
    interval_label = f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])
    run_name = _build_run_name(
        symbol=SYMBOL,
        years=years,
        months=months,
        day_from=DAY_FROM,
        day_to=DAY_TO,
        strategy_label=strategy_label,
    )
    run_config = _build_run_config(
        run_name=run_name,
        symbol=SYMBOL,
        years=years,
        months=months,
        day_from=DAY_FROM,
        day_to=DAY_TO,
        strategy_label=strategy_label,
        strategies=configured_strategies,
    )

    jobs = _build_month_jobs(
        symbol=SYMBOL,
        run_name=run_name,
        years=years,
        months=months,
        day_from=DAY_FROM,
        day_to=DAY_TO,
        starting_balance=START_BALANCE,
    )
    if not jobs:
        print("No month jobs found.")
        return

    print(
        f"[RUN] {run_name} | symbol={SYMBOL} | strategy={strategy_label} | "
        f"jobs={len(jobs)} | workers={min(MAX_WORKERS, len(jobs))}"
    )

    t0 = time.time()
    payloads = _run_jobs(jobs)
    elapsed_s = time.time() - t0

    rows = sorted(
        (payload.result for payload in payloads),
        key=lambda item: (item.year, item.month),
    )
    trade_events = sorted(
        (event for payload in payloads for event in payload.trade_events),
        key=lambda item: item.exit_time,
    )
    rows_by_year: dict[int, list[MonthBacktestResult]] = {year: [] for year in years}
    trade_events_by_year: dict[int, list[TradePnlEvent]] = {year: [] for year in years}

    for row in rows:
        rows_by_year.setdefault(row.year, []).append(row)
    for event in trade_events:
        trade_events_by_year.setdefault(event.year, []).append(event)

    strategy_label = rows[0].strategy if rows else strategy_label
    worker_count = min(MAX_WORKERS, len(jobs))
    print(
        f"\n=== BACKTEST SUMMARY ===\n"
        f"symbol={SYMBOL} strategy={strategy_label} jobs={len(jobs)} workers={worker_count} elapsed={elapsed_s:.2f}s"
    )

    print("\n=== YEARLY SUMMARY ===")
    year_stats_by_year: dict[int, AggregateStats] = {}
    for year in years:
        year_rows = sorted(rows_by_year.get(year, []), key=lambda item: item.month)
        if not year_rows:
            continue
        year_stats = _aggregate_stats(
            year_rows,
            trade_events_by_year.get(year, []),
            starting_balance=START_BALANCE,
        )
        year_stats_by_year[year] = year_stats
        _print_aggregate_line(f"YEAR {year}", year_stats)

    for year in years:
        year_rows = sorted(rows_by_year.get(year, []), key=lambda item: item.month)
        if not year_rows:
            continue
        _print_month_table(year, year_rows)
        year_stats = _aggregate_stats(
            year_rows,
            trade_events_by_year.get(year, []),
            starting_balance=START_BALANCE,
        )
        print("-----")
        _print_aggregate_line(f"TOTAL ({year})", year_stats)

    overall_stats = _aggregate_stats(rows, trade_events, starting_balance=START_BALANCE)
    print("\n=== ALL-TIME SUMMARY ===")
    _print_aggregate_line(f"TOTAL ({interval_label})", overall_stats)

    artifact_paths: dict[str, str | None] = {}
    if WRITE_AGGREGATE_ARTIFACTS:
        trade_rows = _load_trade_rows_from_reports(rows)
        curve_rows = _build_equity_curve_rows(
            trade_rows, starting_balance=START_BALANCE
        )
        artifact_paths = _write_aggregate_artifacts(
            symbol=SYMBOL,
            run_name=run_name,
            strategy_name=strategy_label,
            interval_label=interval_label,
            run_config=run_config,
            rows=rows,
            year_stats_by_year=year_stats_by_year,
            overall_stats=overall_stats,
            trade_rows=trade_rows,
            curve_rows=curve_rows,
        )

    if artifact_paths:
        print("\n=== ARTIFACTS ===")
        print(f"dir={artifact_paths.get('dir')}")
        print(f"trades={artifact_paths.get('trades_jsonl')}")
        print(f"equity_csv={artifact_paths.get('equity_curve_csv')}")
        print(f"equity_png={artifact_paths.get('equity_curve_png')}")
        print(f"months_csv={artifact_paths.get('months_csv')}")
        print(f"years_csv={artifact_paths.get('years_csv')}")
        print(f"summary_json={artifact_paths.get('summary_json')}")
        print(f"run_config_json={artifact_paths.get('run_config_json')}")
    print()


if __name__ == "__main__":
    freeze_support()
    main()
