# Anti-Leakage Notes

Most retail backtests leak information without meaning to.

The common failure modes are simple:

- using the current bar before it is actually closed
- joining model features with a later timestamp than the trade decision
- warming indicators with history that would not have been available yet
- evaluating fills with a cleaner price than the strategy could really have seen

QuantRiver tries to keep the runtime shape honest in a few ways:

1. strategies are called on closed bars rather than on partially formed candles
2. warmup is seeded from history strictly before the first tradable anchor
3. higher timeframes are built forward from the lower-timeframe stream
4. execution adapters apply fees and slippage to simulated PnL

If you extend the repo with your own models or gates, keep the same rule:

Only use information that was fully known at the decision timestamp.

Practical checklist:

- treat every feature bundle as "as of" a specific time
- never merge a trade with a future bar or future model output
- if a higher-timeframe candle is not closed yet, do not let the strategy read it
- when in doubt, prefer being slightly conservative over accidentally optimistic

That discipline matters more than almost any indicator choice. A weak but honest backtest is much more useful than a strong result produced by timestamp leakage.
