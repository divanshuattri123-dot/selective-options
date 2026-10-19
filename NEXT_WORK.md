# Next independent build steps after 0.3.55

The saved-native CoinDesk resume proof and Stochastic RSI research proof are complete in their documented bounds. Do not repeat them. The complete status of milestone 2 is in `INGESTION_STATUS.md`; all remaining milestones remain gated, and no paper days or trades are credited.

## Next model increment: prior-only ATR percentile

Implement one offline feature over verified, complete, closed bars: rank the current ATR against a strictly prior ATR window, excluding the current bar from the reference distribution. Preserve exact source/scope and both-clock visibility from the existing point-in-time snapshot path, use an explicit minimum history/window size, and return WARMUP or UNAVAILABLE when contiguous history, quality, or freshness requirements fail. The percentile is a descriptive feature only; add no threshold, signal, conviction, position, or execution behavior.

Add an API and CLI research report only if it follows the established saved-database feature/report pattern. Cover constant ranges, ties, current-outlier exclusion, window boundaries, insufficient history, gaps, stale/future clocks, row budgets, and source immutability. Verify one saved-native short-history case stays WARMUP plus a separately labelled independent exact/synthetic fixture. Keep proof artifacts exclusive and preserve existing evidence. Full test, static, config, wheel, and hosted checks must pass before claiming the increment.

## M2 work continues independently

Choose the next ingestion task from live status and access evidence, not from this model feature. Continue public finite capture or recovery work only when it closes a specific row in the requirements table. Preserve denied/unavailable providers as such; do not substitute fixtures, start a scheduler, or claim a full feed from a finite poll. M2 must be complete and sustained before any later signal or backtester acceptance.

No production execution is enabled. Testnet orders, paper-day credit, and paper-trade credit remain zero unless a later authorized and separately validated milestone actually begins them.
