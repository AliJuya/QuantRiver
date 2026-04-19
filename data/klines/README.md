# Sample Data

This public repository includes a small synthetic sample backtest dataset so the backtest runner can be exercised quickly after cloning.

Current sample:

- `ETHUSDT`
- timeframe: `1s`
- window: `2024-01-01` through `2024-01-14`
- path: `data/klines/ETHUSDT/1s/2024/01/*.parquet`

The sample is intentionally limited and is meant for smoke-testing the engine, not for strategy research.

To replace it with your own data, keep the same folder layout used by the runner:

`data/klines/<SYMBOL>/<TF>/<YYYY>/<MM>/<DD>.parquet`

Expected columns:

- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`
