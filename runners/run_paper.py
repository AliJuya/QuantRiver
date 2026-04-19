from __future__ import annotations

import time

from adapters.live.binance_rest import BinanceRest, BinanceRestConfig
from adapters.live.binance_ws_aggtrade import BinanceAggTradeWSTickSource
from core.data_engine.boot_seeder import BootSeeder
from core.data_engine.data_engine import DataEngine, DataEngineConfig
from core.data_engine.warmup_registry import WarmupReq, compute_global_warmup
from core.engine import (
    CoreEngine,
    EventBridge,
    SignalEngine,
    StrategyRouter,
    prime_existing_history,
)
from core.execution import PaperExecutionAdapter
from core.execution.position_handler import PositionHandler
from core.gates.gate_engine import GateEngine
from core.indicators import IndicatorEngine
from core.models import ModelModule
from core.state.events import create_engine_event_queue
from core.state.market_state import MarketState
from core.strategies import EMACross5mStrategy, OpeningRangeBreakout5m
from runners.runtime_config import env_bool, env_float, env_int, env_str


SYMBOL = env_str("QR_SYMBOL", "ETHUSDT")
IS_USD_M_FUTURES = env_bool("QR_IS_USD_M_FUTURES", True)
BINANCE_REST_BASE_URL = env_str("QR_BINANCE_REST_BASE_URL", "https://fapi.binance.com")
BINANCE_REST_TIMEOUT_SEC = env_int("QR_BINANCE_REST_TIMEOUT_SEC", 10)
WS_TIMEOUT_SEC = env_int("QR_WS_TIMEOUT_SEC", 20)
LOG_INTERVAL_SEC = env_float("QR_LOG_INTERVAL_SEC", 1.0)

USE_MODELS = env_bool("QR_USE_MODELS", False)
USE_GATES = env_bool("QR_USE_GATES", False)

ENABLE_EMA_CROSS_5M = env_bool("QR_ENABLE_EMA_CROSS_5M", True)
ENABLE_ORB_5M = env_bool("QR_ENABLE_ORB_5M", False)

EMA_FAST_LEN = env_int("QR_EMA_FAST_LEN", 12)
EMA_SLOW_LEN = env_int("QR_EMA_SLOW_LEN", 48)
EMA_STOP_MODE = env_str("QR_EMA_STOP_MODE", "atr")
EMA_STOP_VALUE = env_float("QR_EMA_STOP_VALUE", 1.5)
EMA_TARGET_MODE = env_str("QR_EMA_TARGET_MODE", "atr")
EMA_TARGET_VALUE = env_float("QR_EMA_TARGET_VALUE", 3.0)

FEE_RATE = env_float("QR_FEE_RATE", 0.0004)
SLIPPAGE_RATE = env_float("QR_SLIPPAGE_RATE", 0.0002)

MODEL_RECIPE = {
    "vol_pair_key": "15m|2h",
    "structure_base_tf": "15m",
    "regime_pair_key": "1h|12h",
    "regime_base_tf": "1h",
    "trigger_tf": "15m",
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


def main() -> None:
    if USE_GATES and not USE_MODELS:
        raise ValueError("QR_USE_GATES=true requires QR_USE_MODELS=true.")

    strategies = build_strategies()
    model_module = ModelModule.from_recipe(**MODEL_RECIPE) if USE_MODELS else None
    gate_engine = GateEngine.from_default_artifacts() if USE_GATES else None

    tick_source = BinanceAggTradeWSTickSource(
        symbol=SYMBOL,
        is_usd_m_futures=IS_USD_M_FUTURES,
        timeout_sec=WS_TIMEOUT_SEC,
    )
    rest = BinanceRest(
        BinanceRestConfig(
            base_url=BINANCE_REST_BASE_URL,
            timeout_sec=BINANCE_REST_TIMEOUT_SEC,
        )
    )

    agg_tfs = ("1m", "5m", "15m")
    model_tfs = ("1h", "2h", "12h") if USE_MODELS else ()
    event_tfs = ("1s",) + agg_tfs + model_tfs

    warmup_reqs = [
        WarmupReq(
            name=str(getattr(strategy, "strategy_id", strategy.__class__.__name__)),
            req=dict(getattr(strategy, "warmup_requirements", lambda: {})()),
        )
        for strategy in strategies
    ]
    if model_module is not None:
        warmup_reqs.append(
            WarmupReq(name="model_engine", req=model_module.warmup_requirements())
        )
    warmup_by_tf = compute_global_warmup(warmup_reqs) or {"1m": 100, "5m": 200}

    de = DataEngine(
        config=DataEngineConfig(
            input_mode="ticks",
            tick_river_maxlen=50_000,
            candle_river_maxlen=20_000,
            tfs=agg_tfs + model_tfs,
        ),
        tick_source=tick_source,
    )

    print(
        f"[PAPER] symbol={SYMBOL} strategies={len(strategies)} "
        f"use_models={USE_MODELS} use_gates={USE_GATES}"
    )
    de.start()

    seeder = BootSeeder(rivers_by_tf=de.rivers_by_tf, tick_river=de.tick_river)
    market_state = MarketState(rivers_by_tf=de.rivers_by_tf, tick_river=de.tick_river)
    event_q = create_engine_event_queue()
    bridge = EventBridge(rivers_by_tf=de.rivers_by_tf, out_q=event_q, tfs=event_tfs)
    execution_adapter = PaperExecutionAdapter(
        position_handler=PositionHandler(
            fee_rate=FEE_RATE,
            slippage_rate=SLIPPAGE_RATE,
        )
    )
    core_engine = CoreEngine(
        event_q=event_q,
        market_state=market_state,
        indicator_engine=IndicatorEngine(),
        strategy_router=StrategyRouter(strategies=strategies),
        signal_engine=SignalEngine(gate_engine=gate_engine),
        execution_adapter=execution_adapter,
        models=[model_module] if model_module is not None else [],
        modules=[execution_adapter],
    )

    try:
        anchor = seeder.wait_anchor_1m(timeout_sec=180.0)
        print(f"[PAPER] anchor={anchor.isoformat()}")
        print(f"[PAPER] cleanup={seeder.cleanup_pre_anchor(anchor)}")

        warmup = seeder.fetch_and_seed_history_before_anchor(
            anchor=anchor,
            warmup_by_tf=warmup_by_tf,
            fetcher=lambda tf, anchor_open_time, count: rest.fetch_candles_before_anchor(
                symbol=SYMBOL,
                tf=tf,
                anchor_open_time=anchor_open_time,
                count=count,
            ),
        )
        print(f"[PAPER] warmup={warmup}")

        prime_existing_history(core_engine=core_engine, market_state=market_state, tfs=event_tfs)
        bridge.attach()
        market_state.set_warm(True)
        core_engine.start()

        t0 = time.time()
        while True:
            st = de.stats()
            print(
                f"[t+{time.time() - t0:6.1f}s] "
                f"ticks={st['tick_river']['size']} "
                f"1s={st['candle_rivers']['1s']['size']} "
                f"1m={st['candle_rivers'].get('1m', {}).get('size', 0)} "
                f"5m={st['candle_rivers'].get('5m', {}).get('size', 0)} "
                f"core={core_engine.stats()} "
                f"exec={execution_adapter.stats()}"
            )
            time.sleep(LOG_INTERVAL_SEC)
    finally:
        core_engine.stop()
        de.stop()


if __name__ == "__main__":
    main()
