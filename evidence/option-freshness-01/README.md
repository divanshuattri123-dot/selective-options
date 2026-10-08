# Finite saved option freshness diagnostic

Read-only audit of preserved `reused-validation-public-01` (BTC) and `multiasset-options-public-01` (ETH, SOL, PAXG), at each original report's final cutoff and unchanged 30,000 ms leg-age bound. All original files hash identically before and after the audits; all four canonical coverage dictionaries exactly match their saved reports. No HTTP, WebSocket, private or production requests, orders or synthetic market observations occurred.

Each selected ticker/book includes the canonical leg state, latest source/receipt pair and every tied candidate identity, independent ages, signed receipt-minus-source difference, native timestamp key/value and payload digest. Selected-session SQL statistics record stored observation count, distinct native source timestamps, independent first/last source and receipt times, signed difference ranges/counts and recent-source/recent-receipt counts. SQL aggregates historical rows instead of materializing them in Python. Tied, missing, corrupt, unavailable session and universe evidence remain distinct blockers.

| Asset | Selected tickers | Selected books | Stale books | Diagnosis at original cutoff |
|---|---:|---:|---:|---|
| BTC | 1,010 | 1,010 | 74 | No recent stored native source updates or receipts for these books |
| ETH | 804 | 804 | 158 | No recent stored native source updates or receipts for these books |
| SOL | 142 | 142 | 142 | No recent stored native source updates or receipts for these books |
| PAXG | 0 | 0 | n/a | `OPTION_SELECTED_UNIVERSE_EMPTY` |

Every stale book exceeds both source and receipt age bounds, and has zero recent-source and zero recent-receipt observations in the selected session. No stored recent receipt carrying an old native source timestamp occurs for the selected ticker/book populations. All SOL books have exactly one stored observation each. The 158 stale ETH books comprise 157 with one stored observation and one with eight distinct source timestamps. The 74 stale BTC books comprise 44 with one stored observation, six with two, eleven with three, five with four, five with five, two with six and one with eight. Counts describe stored events, not wire frames, upstream heartbeat policy, unseen updates or order-book continuity.

Selected receipt-minus-source ticker differences range -1,280..-398 ms for BTC, -1,531..-318 ms for ETH and -1,039..-668 ms for SOL. Every selected ticker has a negative difference. Books have mixed signs. These are signed clock differences: no wire-latency or clock-cause inference is made, and both clocks still independently constrain point-in-time availability.

`independent-proof.json` and its saved verifier independently reproduce all 3,912 native latest identities, both clock ages, timestamp provenance and grouped statistics with different SQL selection/aggregation. `final-validation.json` seals exact final script/test snapshots and proves the final code rerun matches the independently verified reports. Original public captures remain untouched. Work reruns live separately under `work/option-freshness-audit`.

Finite bounds: at most 100,000 Python projection rows plus the canonical single overflow sentinel, 200,000 eligible selected-session history rows per asset, two statistics groups per selected symbol, 1 GiB input database, 8 MiB per parsed report/config JSON, 32 source files and 2 GiB total source bytes. Each asset has a 30-second query/diagnostic deadline and the capture audit has a 120-second deadline. SQL scan cost and internal engine memory are not certified by the Python row bound. Budgets are diagnostic refusal limits; they do not alter canonical thresholds or freshness decisions.

Validation: 54 new audit cases, 174 combined audit/coverage/projection/metadata cases pass. Cases include real DuckDB deadline interruption, source-byte preservation, strict bool/overflow/duplicate-key/nonfinite input refusal, native corrupt records, ties, independent PIT/session boundaries, signed negative differences, stale receipts, recent receipts with old source timestamps, missing session observations and budget overflow. Ruff check/format and strict mypy over `src` plus the audit script pass.

This diagnostic adds descriptive evidence only. It does not compute M3 features, approve trades, establish all-feed freshness, certify sustained capacity, establish options-trade history continuity or complete M2.

Parent release integration: the worker final-validation.json/audit_used.py/tests_used.py are preserved historical handoff snapshots with 54 cases. Parent repaired seconds-to-milliseconds overflow near 10**307 to raise the same policy ValueError as other invalid input and added one regression. parent-validation.json seals the actual current source/test snapshots and four exact reruns; all 55 audit cases passed in the 81-case parent target and final 1,453-case suite. Temporary worker/final/parent reruns are now under workspace work/option-freshness-audit (../../work from project), after verified path-contained moves. No original native reports or hashes changed.
