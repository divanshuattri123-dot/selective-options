# Next independent build steps after 0.3.56

The saved-native CoinDesk resume proof, Stochastic RSI, and prior-only ATR percentile research are complete within their documented bounds. Do not repeat them. The complete status of milestone 2 is in `INGESTION_STATUS.md`; all remaining milestones remain gated, and no paper days or trades are credited.

## Next model increment: causally confirmed swing structure

Implement one offline description over verified, complete, closed bars: report confirmed swing highs/lows only after the configured right-side confirmation bars have themselves closed. A pivot may not be visible before that confirmation close; ties, equal plateaus, missing bars, the open right edge, stale input and future clocks need explicit deterministic handling. Preserve source/scope, both clocks, row budgets and exact event IDs in the report.

Do not turn pivot structure into signal thresholds, direction labels, entries, or positions. Add an API/CLI report only through the existing immutable saved-database path and use a separate exact analytical fixture from the saved-native evidence. Unit, full local/hosted tests, static, config and wheel checks must pass before recording completion.

## M2 work continues independently

Choose the next ingestion task from live status and access evidence, not from this model feature. Continue public finite capture or recovery work only when it closes a specific row in the requirements table. Preserve denied/unavailable providers as such; do not substitute fixtures, start a scheduler, or claim a full feed from a finite poll. M2 must be complete and sustained before any later signal or backtester acceptance.

No production execution is enabled. Testnet orders, paper-day credit, and paper-trade credit remain zero unless a later authorized and separately validated milestone actually begins them.
