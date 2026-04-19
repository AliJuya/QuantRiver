# QuantRiver Public

QuantRiver Public is a stripped-down, publish-safe version of the QuantRiver architecture:

- event-driven market data engine
- closed-candle state management
- backtest runner
- paper runner
- live market-data runner
- example strategies
- optional model/gate integration points

This repository is meant to demonstrate the architecture clearly enough that another engineer can:

1. run it locally,
2. understand the flow end to end,
3. swap in their own data, strategies, or models,
4. extend it without reverse-engineering private project details.

## What This Public Repo Includes

- `core/data_engine/`
  Data ingestion, 1s builder from ticks, candle rivers, and higher-timeframe aggregation.
- `core/engine/`
  The event loop, strategy routing, signal normalization, and execution flow.
- `core/execution/`
  Backtest, paper, and live execution adapters plus the position handler and reporting.
- `core/strategies/`
  Two public example strategies:
  - `EMACross5mStrategy`
  - `OpeningRangeBreakout5m`
- `core/models/`
  Public-safe placeholder model orchestration layer. The wiring remains, but proprietary research logic is intentionally not shipped.
- `adapters/backtest/`
  Parquet-backed backtest data sources.
- `adapters/live/`
  Binance REST and WebSocket market-data adapters.
- `runners/`
  Small entry points for backtest, paper, and live usage.

## What This Public Repo Does Not Include

- proprietary trained artifacts
- private research scripts
- private feature engineering stacks
- exchange order-routing credentials or brokerage automation

The `live` runner in this public repository means:

- live market data
- live signal generation
- live engine/event processing

It does **not** place exchange orders by itself.

## Architecture Overview

At a high level the runtime flow is:

1. A source produces ticks or closed candles.
2. `DataEngine` stores them in bounded rivers.
3. `CandleAggregatorTF` builds higher timeframes from the configured base timeframe.
4. `IndicatorEngine` updates indicator values on each closed bar.
5. `StrategyRouter` asks each strategy for decisions on the relevant timeframe.
6. `SignalEngine` normalizes those decisions into `ExecutionIntent`s.
7. An execution adapter handles those intents:
   - backtest: simulated fills + report files
   - paper: simulated fills on live data
   - live: queues intents for an external executor

Optional model and gate layers can sit between state and signal execution, but they are off by default in the public runners.

## Dynamic Data Engine

The public data engine supports:

- `ticks`
- closed candles in `1s`, `1m`, `5m`, `15m`, `1h`, `2h`, `1d`
- any valid `Ns`, `Nm`, `Nh`, `Nd` source timeframe

Higher timeframes are built upward from that source timeframe.

Examples:

- `1s -> 1m -> 5m -> 15m`
- `5m -> 15m -> 1h -> 4h`
- `1h -> 4h -> 1d`

It does **not** downsample into lower timeframes.

Important:

- the engine/adapters are dynamic
- the public example backtest runner still uses a `1s` candle input path by default for higher-fidelity stop/TP handling

If you want to build a custom runner from `5m` or `1h` directly, the engine and parquet source layer now support that.

## Repository Layout

```text
QuantRiver-public/
├── adapters/
│   ├── backtest/
│   └── live/
├── core/
│   ├── data_engine/
│   ├── engine/
│   ├── execution/
│   ├── gates/
│   ├── indicators/
│   ├── models/
│   ├── state/
│   ├── strategies/
│   └── types/
├── runners/
│   ├── run_backtest.py
│   ├── run_paper.py
│   ├── run_live.py
│   └── runtime_config.py
├── .env.example
├── pyproject.toml
└── README.md
```

## Installation

### 1. Create a virtual environment

PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Bash:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install the package

```bash
pip install -e .
```

If you want to experiment with the optional gate layer:

```bash
pip install -e ".[gates]"
```

### 3. Create a local `.env`

The runners automatically read a repo-root `.env` file through `runners/runtime_config.py`.

Copy the example:

PowerShell:

```powershell
Copy-Item .env.example .env
```

Bash:

```bash
cp .env.example .env
```

Then edit the values you want.

## Runner Summary

### `runners/run_backtest.py`

Purpose:

- loads local parquet candle data
- seeds warmup history
- runs strategies through the event engine
- simulates fills through `BacktestExecutionAdapter`
- writes monthly and aggregate reports

Default public assumption:

- input source is closed `1s` candles from local parquet files

### `runners/run_paper.py`

Purpose:

- consumes live Binance aggTrade ticks
- builds candles in real time
- runs strategies and paper-fills locally
- never sends exchange orders

### `runners/run_live.py`

Purpose:

- consumes live Binance aggTrade ticks
- runs the engine in real time
- produces live execution intents

This public runner is architecture/demo focused. It does not route orders to Binance.

## Configuration Model

All public runners use environment variables, either from your shell or from the repo-root `.env` file.

### Shared Variables

| Variable | Meaning | Default |
|---|---|---|
| `QR_SYMBOL` | Trading symbol | `ETHUSDT` |
| `QR_ENABLE_EMA_CROSS_5M` | Enable the EMA cross strategy | `true` |
| `QR_ENABLE_ORB_5M` | Enable the ORB strategy | `false` |

### EMA Strategy Variables

| Variable | Meaning | Default |
|---|---|---|
| `QR_EMA_FAST_LEN` | Fast EMA length | `12` |
| `QR_EMA_SLOW_LEN` | Slow EMA length | `48` |
| `QR_EMA_STOP_MODE` | Stop mode: `atr` or `usd` | `atr` |
| `QR_EMA_STOP_VALUE` | Stop size in ATR or USD | `1.5` |
| `QR_EMA_TARGET_MODE` | Target mode: `atr` or `usd` | `atr` |
| `QR_EMA_TARGET_VALUE` | Target size in ATR or USD | `3.0` |

### Backtest Variables

| Variable | Meaning | Default |
|---|---|---|
| `QR_KLINES_BASE_PATH` | Root folder for backtest parquet data | `data/klines` |
| `QR_MODEL_KLINES_BASE_PATH` | Root folder for model-native parquet data | `data/model_klines` |
| `QR_REPORTS_BASE_DIR` | Output folder for reports | `backtest_reports` |
| `QR_YEARS` | Years list or inclusive 2-point range | `2022,2025` |
| `QR_MONTHS` | Months list or inclusive 2-point range | `1,12` |
| `QR_DAY_FROM` | Starting day of month | `1` |
| `QR_DAY_TO` | Ending day of month | `31` |
| `QR_AUTO_DAY_TO` | If `true`, auto-expand to the last available day in the month | `true` |
| `QR_START_BALANCE` | Starting equity | `1000` |
| `QR_MAX_WORKERS` | Parallel month workers | auto |
| `QR_WRITE_AGGREGATE_ARTIFACTS` | Write combined reports and equity plots | `true` |
| `QR_VERBOSE_MONTH_LOGS` | Print more progress | `false` |
| `QR_RUN_NAME_OVERRIDE` | Optional custom run-name prefix | empty |
| `QR_TRAILING_ENABLED` | Enable trailing logic in the position handler | `false` |
| `QR_FEE_RATE` | Per-side fee rate | `0.0004` |
| `QR_SLIPPAGE_RATE` | Per-side slippage rate | `0.0002` |
| `QR_BACKTEST_USE_MODELS` | Turn on the model layer for backtests | `false` |

### Paper / Live Variables

| Variable | Meaning | Default |
|---|---|---|
| `QR_IS_USD_M_FUTURES` | `true` for USD-M futures, `false` for coin-M | `true` |
| `QR_BINANCE_REST_BASE_URL` | REST base URL | `https://fapi.binance.com` |
| `QR_BINANCE_REST_TIMEOUT_SEC` | REST timeout | `10` |
| `QR_WS_TIMEOUT_SEC` | WebSocket timeout | `20` |
| `QR_LOG_INTERVAL_SEC` | Console status interval | `1` |
| `QR_USE_MODELS` | Turn on model modules | `false` |
| `QR_USE_GATES` | Turn on gate artifacts | `false` |

Important:

- `QR_USE_GATES=true` requires `QR_USE_MODELS=true`
- gate usage also requires the optional LightGBM dependency and actual gate artifacts

## Backtest Data Layout

The public example backtest runner expects this directory shape:

```text
data/
└── klines/
    └── ETHUSDT/
        └── 1s/
            └── 2022/
                └── 01/
                    ├── 01.parquet
                    ├── 02.parquet
                    └── ...
```

Each parquet file should contain closed candles with these columns:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

Timestamps should be UTC or convertible into UTC by pandas.

## How To Run Each Part

### Backtest

1. Put your local parquet data under the folder configured by `QR_KLINES_BASE_PATH`.
2. Copy `.env.example` to `.env`.
3. Set at minimum:
   - `QR_KLINES_BASE_PATH`
   - `QR_SYMBOL`
4. Run:

```powershell
py -3 runners\run_backtest.py
```

Outputs go under the folder configured by `QR_REPORTS_BASE_DIR`.

### Paper

1. Copy `.env.example` to `.env`.
2. Set:
   - `QR_SYMBOL`
   - strategy toggles/parameters
3. Run:

```powershell
py -3 runners\run_paper.py
```

What happens:

- live market data is pulled from Binance
- the engine builds candles in real time
- strategies fire intents
- `PaperExecutionAdapter` simulates fills locally

### Live

1. Copy `.env.example` to `.env`.
2. Set:
   - `QR_SYMBOL`
   - strategy toggles/parameters
3. Run:

```powershell
py -3 runners\run_live.py
```

What happens:

- live market data is pulled from Binance
- the engine builds candles in real time
- strategies fire intents
- `LiveExecutionAdapter` queues those intents

What does **not** happen:

- no exchange order placement
- no brokerage credential management
- no private execution bridge

## Example `.env` Patterns

### Simple EMA backtest

```dotenv
QR_SYMBOL=ETHUSDT
QR_ENABLE_EMA_CROSS_5M=true
QR_ENABLE_ORB_5M=false
QR_KLINES_BASE_PATH=D:/market_data/klines
QR_YEARS=2024,2025
QR_MONTHS=1,12
QR_FEE_RATE=0.0004
QR_SLIPPAGE_RATE=0.0002
```

### Paper trading with both strategies

```dotenv
QR_SYMBOL=ETHUSDT
QR_ENABLE_EMA_CROSS_5M=true
QR_ENABLE_ORB_5M=true
QR_USE_MODELS=false
QR_USE_GATES=false
```

### Model-enabled run

```dotenv
QR_SYMBOL=ETHUSDT
QR_USE_MODELS=true
QR_USE_GATES=false
QR_MODEL_KLINES_BASE_PATH=D:/market_data/model_klines
```

## Strategy Customization

There are three easy levels of customization.

### 1. Toggle strategies on or off

Use:

- `QR_ENABLE_EMA_CROSS_5M`
- `QR_ENABLE_ORB_5M`

### 2. Change strategy parameters from `.env`

For EMA Cross, public parameters are already exposed:

- `QR_EMA_FAST_LEN`
- `QR_EMA_SLOW_LEN`
- `QR_EMA_STOP_MODE`
- `QR_EMA_STOP_VALUE`
- `QR_EMA_TARGET_MODE`
- `QR_EMA_TARGET_VALUE`

### 3. Edit or add strategy classes

Relevant files:

- `core/strategies/strategy_ema_cross_5m.py`
- `core/strategies/strategy_opening_range_breakout_5m.py`
- `core/strategies/__init__.py`

If you add a new strategy:

1. create the new class in `core/strategies/`
2. export it from `core/strategies/__init__.py`
3. instantiate it inside the runner `build_strategies()` function you want to use

## Models and Gates

The public repo keeps the architecture for models and gates, but not the private research stack.

### Models

Model orchestration still exists through:

- `core/models/model_module.py`
- `core/models/model_engine.py`
- `core/models/adapters/`

But the shipped engines are public-safe placeholders intended to preserve the integration shape.

### Gates

Gate support is optional and off by default.

To use it you need:

1. `pip install -e ".[gates]"`
2. actual gate artifacts compatible with `GateEngine.from_default_artifacts()`
3. `QR_USE_MODELS=true`
4. `QR_USE_GATES=true`

If you do not have those artifacts, leave gates off.

## Notes on Fees, Slippage, and Stops

The public position handler supports:

- ATR-based stop/target planning
- USD-distance stop/target planning
- fee simulation
- slippage simulation

Backtest and paper runners expose:

- `QR_FEE_RATE`
- `QR_SLIPPAGE_RATE`

The EMA example strategy exposes:

- `QR_EMA_STOP_MODE`
- `QR_EMA_STOP_VALUE`
- `QR_EMA_TARGET_MODE`
- `QR_EMA_TARGET_VALUE`

## Troubleshooting

### `Klines base path not found`

Set `QR_KLINES_BASE_PATH` to the correct local dataset root.

### `At least one strategy must be enabled`

Set at least one of:

- `QR_ENABLE_EMA_CROSS_5M=true`
- `QR_ENABLE_ORB_5M=true`

### `QR_USE_GATES=true requires QR_USE_MODELS=true`

This is expected. Gates depend on model context.

### `Missing dependency: websocket-client`

Install dependencies again:

```bash
pip install -e .
```

### Windows timezone / `ZoneInfo` issues

The project includes `tzdata` in `pyproject.toml`, which covers common Windows setups.

## Public Extension Points

If you want to extend this repository cleanly:

- add new strategies under `core/strategies/`
- add new data sources under `adapters/backtest/` or `adapters/live/`
- build custom runners under `runners/`
- plug your own model logic behind `core/models/adapters/`

## License

This public repository is currently published under the MIT license.
