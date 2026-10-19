# Selective High-Conviction Options Trading System

Version 0.3.57 adds offline, causally confirmed swing highs/lows over audited closed bars. A pivot is emitted only after its configured right-side bars close; equal local extrema select the rightmost bar. The short native BTC sample remains WARMUP, while separate fixtures test confirmation timing, plateau ties, gaps, freshness and clock refusal. This adds no direction, signal, threshold, entry, backtest policy, or order behavior. The complete local suite passes 3,921 application tests in 798.72 seconds at 94.59% coverage; its 65-case migration companion and the strict combined JUnit gate pass 3,986 cases with zero failures, errors, or skips. M2 remains PARTIAL and trading remains disabled; hosted 0.3.57 validation follows the committed source.

Version 0.3.56 adds a descriptive, offline prior-only Wilder ATR percentile over strictly audited closed bars. The current ATR is ranked against a bounded history that excludes the current observation; ties use an explicit midrank rule, and incomplete, gappy, stale or short history remains unavailable or in warmup. An independent exact-rational fixture checks the calculation, while the short native BTC sample remains WARMUP. This adds no signal, regime, conviction, or order behavior. Its first full run exposed a stale periodic-bars version pin, which was corrected before a clean 3,909-test local retry and successful hosted Windows/cloud validation. M2 remains PARTIAL and trading remains disabled.

Version 0.3.55 adds opt-in CoinDesk checkpoint resume: a closed SQL/Parquet/archive/outbox source is bounded and verified, exact first receipts and a minimal binding-health set are copied pending, then the closed destination is revalidated before the news collector is constructed. Replays do not republish unchanged headlines; revisions append. The 0.3.54 closed-bar Stochastic RSI research remains signal-free and fixture arithmetic remains separate from short native warmup. M2 remains PARTIAL and trading remains disabled. See [RUNBOOK.md](RUNBOOK.md), [RESEARCH.md](RESEARCH.md), and [VALIDATION_REPORT.md](VALIDATION_REPORT.md) for the exact scope and remaining gates.

Version 0.3.53 adds explicit Solana network checkpoint resume and offline Keltner-channel research. The SOL handoff audits every eligible historical poll binding, copies the latest exact native three-row seed, and revalidates the closed destination before producers start. Keltner channels use an explicitly documented close-EMA/Wilder-ATR convention with complete-history and both-clock checks. Saved-native evidence, synthetic analytical fixtures and current source availability stay separate. M2 remains PARTIAL and trading remains disabled. See [RUNBOOK.md](RUNBOOK.md), [RESEARCH.md](RESEARCH.md), and [VALIDATION_REPORT.md](VALIDATION_REPORT.md) for commands, scope and validation.

**Phone and cloud development:** Use the private repository's
[Actions](https://github.com/divanshuattri123-dot/selective-options/actions) to run
hosted builds and inspect results. [CLOUD_MIGRATION.md](CLOUD_MIGRATION.md) contains
the exact mobile controls, portable setup and Codex Cloud instructions. Development
currently continues in this local chat. Hosted cloud-build and Windows validation
of 0.3.55 and 0.3.56 passed. Hosted 0.3.57 validation will run from the next
committed source. The recorded 0.3.53 hosted full Windows, portable Linux and
container checks pass; continuous collector hosting remains unprovisioned.

The system exists to place **very few trades with a large positive expected value**, not many trades with a good-looking win rate. It stays flat by default. It only fires when technical, volatility/flow, and fundamental/news evidence independently point the same direction at the same time, and when the options market is pricing that direction cheaply enough to pay for being right.

The success metric is **expectancy per trade after all costs**, defined as:

```
E = (P_win Ã— Avg_win) âˆ’ (P_loss Ã— Avg_loss) âˆ’ fees âˆ’ slippage âˆ’ financing
```

A 40% hit rate at 4R average winner beats an 85% hit rate at 0.3R. Optimise `E`, never hit rate alone. Any component that improves hit rate while lowering `E` must be rejected.

**Selectivity is a feature, not a bug.** A month with zero trades is a valid, correct output. Hard-code this: the system must never be tuned to "find something to trade."

---

**Build status: milestone 1 readiness passed; milestone 2 ingestion pilot implemented and tested, but incomplete.** Authentication, maximum key permissions, account/connection cancel-on-disconnect, native refresh and authenticated discovery passed. An 11-minute capture stored 61,061 events, including 4,597 underlying trades and eight complete five-minute bars. A separate 10,707-event archive was restored and verified without changing its observation timestamps. No orders were placed. `arm` remains disabled in every profile.

Version 0.3.52 adds opt-in periodic unfinished-bar generations during finite capture and offline Parkinson/Garman–Klass range-volatility research. Consistent collector boundaries and FIFO commits protect each explicitly selected snapshot; short native bar history retains warmup and partial-history refusals. See [RUNBOOK.md](RUNBOOK.md) and [RESEARCH.md](RESEARCH.md). All 3,657 application tests and 65 migration cases pass; local coverage is 95.24%. Hosted full Windows, scoped Linux and container checks pass on the same source. Milestone 2 remains PARTIAL; validation and restore evidence are recorded in [VALIDATION_REPORT.md](VALIDATION_REPORT.md).

Version 0.3.51 adds cooperative finite-capture stop and offline native put/call research. Four saved-data capture pipelines preserve 55 native trades and resume stopped bar state; a separate audit checks all 1,010 saved option members over 13 expiries. One zero-call-volume expiry remains unavailable, so the research report stays PARTIAL. All 3,426 application tests and 65 migration cases pass; local coverage is 95.19%. Full local and hosted verification results are recorded in [VALIDATION_REPORT.md](VALIDATION_REPORT.md). Development continues in this existing local chat after the Codex Cloud environment error. See [RUNBOOK.md](RUNBOOK.md) and [RESEARCH.md](RESEARCH.md) for the explicit stop and research commands.

Version 0.3.50 adds an offline discrete-delta risk-reversal view using native call/put mark-IV values and signed deltas. The saved testnet replay produced 12 pairs from 13 expiries; one invalid put was withheld and a +119.63-point mark-IV difference remains an explicitly unvalidated outlier. All 3,287 tests pass with 95.03% coverage; Ruff, strict mypy and the installed 80-module wheel check pass. Three official BLS paths returned HTTP 403, so that feed remains unavailable. This does not complete ingestion, feature/backtest acceptance or live readiness. See [RESEARCH.md](RESEARCH.md), [INGESTION_STATUS.md](INGESTION_STATUS.md), and [VALIDATION_REPORT.md](VALIDATION_REPORT.md).

Version 0.3.49 adds an opt-in read-only Federal Reserve FOMC calendar collector. It parses whole official snapshots into receipt-time revisions, applies point-in-time health checks, and labels date-only regular meetings separately from notation votes without inventing announcement times. One public response contains 57 listed schedule entries across 2021–2027; offline reclassification identifies 56 regular meetings and one notation vote, while the retained DuckDB/Parquet data and original response checksum pass audit. The collector is disabled by default; CPI/PCE/NFP release values, exact announcement times and other central-bank calendars remain missing. See [INGESTION_STATUS.md](INGESTION_STATUS.md) and [RUNBOOK.md](RUNBOOK.md).

Version 0.3.48 adds an explicit opt-in Coin Metrics checkpoint seed and a read-only saved-option quote/backtest bridge alongside ingestion. The bridge records source-ahead timestamps within the explicit skew bound but the simulator refuses them with `QUOTE_CLOCK_ORDER` before any fill; orders tied with discovery, unsupported quantity assumptions and off-tick prices also refuse. Native minimum size is a floor, and tick-step thresholds use the base tick at equality. A hash-guarded saved testnet archive validates quote conversion only; synthetic cases exercise simulation. This API is a narrow long-premium research harness, not strategy validation or execution. Full test results and exact limitations are in [VALIDATION_REPORT.md](VALIDATION_REPORT.md), [RESEARCH.md](RESEARCH.md), and [INGESTION_STATUS.md](INGESTION_STATUS.md). M2 remains partial; live trading is disabled.

Version 0.3.46 adds committed stopped-capture bar save/resume and independently tested ADX/directional and prior-only volatility percentiles. All 2,888 tests pass. Four actual supervised captures with mocked transport preserve 55 saved native trades and four conservative partial bars, with independent archive, seed, state and source checks. `capture --save-bars-on-stop` is explicit and disabled by default; `--resume-bars-from` verifies the closed seed before producers. Offline `research-features --include-trend` exposes the new features. M2 and the complete feature/backtester milestones remain incomplete; no live trading is enabled. See [RESEARCH.md](RESEARCH.md) and [INGESTION_STATUS.md](INGESTION_STATUS.md).

Version 0.3.45 adds the first closed-bar indicators, conservative quote simulation and counted trial journal alongside ingestion, as requested. Strict open-bar state restoration keeps interrupted coverage partial. All 2,716 tests pass; the independent proof preserves 55 original native trades, checks 240 labelled synthetic bars and verifies a deliberately losing -44 fee-and-spread fixture. These are partial research foundations; M2 and later milestones remain incomplete. The offline `research-features` and `research-backtest` commands are documented in [RESEARCH.md](RESEARCH.md). No keys or account action are needed for these commands.

Version 0.3.44 validates each complete Kraken trade batch and eligible bar flush before any publication. Invalid later records leave the entire batch unpublished and the collector state unchanged. An uncertain sink delivery halts capture, preserves any already delivered valid prefix and refuses collector reuse. All 2,413 tests pass; 55 saved native trade records replay exactly in reconstructed message containers with zero new network requests. This is validation isolation, not an atomic delivery transaction, bar-state restart or sustained validation. M2 remains incomplete and trading disabled.

Version 0.3.43 connects opt-in bounded periodic archive checkpoints to supervised capture. The source remains open, producers can append after the exact snapshot closes, and both clocks gate committed cohorts. Offline actual-capture proof retains 28 native records, creates two exact cumulative snapshots (28 and 31 rows), and drains 32 final source rows under mocked collectors/synthetic ticks. All 2,311 tests pass. Scheduling defaults to disabled; retention purge, sustained public collection and seven-day validation remain pending. Milestone 2 stays incomplete and trading disabled.

Version 0.3.42 adds explicit offline archive rotation and all-part staged restore. Both source and receipt clocks filter the finite cohort before partition budgeting; source data and delivery acknowledgements stay unchanged. Saved-public verification preserves 28 native rows and restores 35 eligible records across two immutable parts, with labelled boundary/failure fixtures and zero new public calls. All 2,187 tests pass. Active-capture rotation, retention purge and seven-day validation remain pending; milestone 2 stays incomplete and trading disabled.

Version 0.3.41 resolves the fixed news endpoint through native publisher redirect and documentation evidence. One separate actual poll verified 25 native metadata items and exact receipt/archive/restart checks. Explicit stablecoin capture resume copies and revalidates a healthy bound pair, refusing failed or stale history before collection; saved-public offline capture checks pass. All 2,030 tests passed. Full ingestion and sustained validation remain incomplete; trading stays disabled.

Version 0.3.40 adds a bounded crypto-news metadata collector and explicit read-only stablecoin checkpoints. All 1,967 tests passed. The one public news request returned HTTP 308; redirects were refused and no headlines were accepted. Saved stablecoin production records pass offline failure/restart/recovery checks with synthetic later outcomes clearly labelled. Independent archive/receipt/checkpoint review passed. Full ingestion and sustained validation remain incomplete; trading stays disabled.

Version 0.3.39 completes three parallel feed increments and bounded store/capture integration. All 1,830 tests passed. One real Ethereum USDC/USDT supply poll verified both native contracts; one Binance liquidation subscription passed with no native snapshots observed. The FRED real-yield request timed out and remains explicitly unavailable. Saved SOL bounded-query, exact receipt/restart/archive and independent parent checks passed. Full ingestion and sustained validation remain incomplete; trading stays disabled.

Version 0.3.38 passed all 1,554 tests. Two actual dRPC gas polls across a closed checkpoint handoff verify native first receipts and current bound health. Offline production RSS replay verifies retained metadata, persisted failure and recovery boundaries. Future or tied latest evidence cannot pass generic freshness diagnostics. Full ingestion and sustained validation remain incomplete; trading stays disabled.

Version 0.3.37 completes another three parallel ingestion tasks. All 1,453 tests passed. Four read-only dRPC calls verify one actual finalized ETH gas sample; one real Federal Reserve poll verifies exact bound news health and 15 metadata records. Saved option diagnostics independently check 3,912 latest identities and explain 374 stale books without changing thresholds. Explicit provider-scoped restarts and bounded RSS capture assessments preserve point-in-time integrity. Full feeds, sustained validation and trading remain incomplete.

Version 0.3.36 integrates three parallel ingestion tasks. All 1,302 tests passed. Exact current-manifest metadata projection retains 110,000 labelled obsolete generations without exhausting current BTC materialization. Parameterized scalar JSON lookups preserve exact conflicts and recovery evidence. All inventoried fields are valid for 1,956 preserved native BTC/ETH/SOL tickers, while stale books and empty gold remain blocked. These finite proofs do not complete ingestion, sustained validation or trading.

Version 0.3.35 bounds Python materialization for option assessment while retaining full history and all tied latest quotes. All 1,229 tests passed. Four saved asset views match 109,845 original native rows exactly. With 110,000 explicitly synthetic older quotes added, BTC assessment fetches 3,033 rows while all 172,089 rows remain retained; stale books and empty gold remain blocked. This is not a database scan/size bound or sustained-capacity proof. Full ingestion and trading remain incomplete.

Version 0.3.34 extends finite verification across requested assets. All 1,181 tests passed. A finite testnet capture acknowledged all 2,840 channels for 804 ETH and 142 SOL options without disconnect; PAXG discovery returned no options. At the saved check 158 ETH books and every SOL book remain stale under the unchanged 30-second bounds. Exact metadata, point-in-time, archive and duplicate replay checks passed. Finite BTC evidence is preserved separately; full system ingestion and trading remain incomplete.

Version 0.3.33 reuses compiled validators while preserving every input check and fresh decoded payload. All 1,170 tests passed; every one of 23,519 saved native events matches the previous release exactly. The changed finite BTC test acknowledged all 3,031 channels for 1,010 options without a disconnect. At the final check 74 books remain stale under the unchanged age limit. Full system ingestion and trading remain incomplete.

Version 0.3.32 measures exclusive receive/normalization/publication time without changing clocks or deadlines. All 1,159 tests passed. An uncapped BTC sample again reached 1,900/3,031 acknowledged channels before finite stop. Normalization consumed 52.9 seconds in the recorded prefix versus 0.66 seconds publishing; this guides the next performance increment. Full coverage remains blocked.

Version 0.3.31 exposes exact subscription-batch progress and the operation pending at disconnect. All 1,149 tests passed. A finite uncapped BTC attempt acknowledged 1,900 of 3,031 channels before batch 20 timed out while awaiting the socket. Full coverage remains blocked; the underlying cause is not yet established.

Version 0.3.30 binds option coverage to exact discovery metadata and acknowledged subscriptions, with independent manifest/quote freshness and fail-closed integrity checks. All 1,145 tests passed. A finite uncapped BTC attempt selected 1,010 options but timed out before complete acknowledgement; it remains blocked. Saved data/archive/replay checks passed, while full ingestion and trading remain incomplete.

Version 0.3.29 adds optional scheduled option-universe rediscovery and withholds cached quotes during disconnect/refresh/resynchronization. A finite capped BTC run verified one refresh across two acknowledged sessions, and all 1,089 tests passed. The refresh records an unverified history gap; it does not establish continuous trades or full uncapped coverage.

Version 0.3.28 adds opt-in public SOL performance and returned vote-account stake observations with canonical as-of quality. One finite four-request check and independent saved-response replay passed, alongside 1,065 tests. This does not provide DEX volume, fee-payer or complete staking-flow coverage. [COMPLETION_CHECKLIST.md](COMPLETION_CHECKLIST.md) records every remaining feed group and validation requirement; milestone 2 remains partial.

The data layer preserves source and receipt times, checks freshness, and separates Deribit testnet from public reference prices. It includes sampled option streams, perpetual/funding snapshots, underlying trades and closed bars, public policy RSS, Redis outbox replay, and verified Parquet restore. Version 0.3.9 adds opt-in automatic Kraken gap repair with bounded socket routing and preserved message receipts. A live public BTC test recovered an intentionally omitted trade through REST and verified the next live successor. A source-clock edge case found in the first run now waits within the existing deadline, preserving the look-ahead guard. This is data testing only; no orders or paper trading have started. See [INGESTION_STATUS.md](INGESTION_STATUS.md).

Version 0.3.10 adds an opt-in public Bybit USDT perpetual collector. A finite live check verified BTC, ETH, SOL and PAXG funding/OI snapshots, their native funding intervals, receipt-time visibility and exact archive round-trip. The 0.3.10 checkpoint passed 389 tests with 96.70% combined coverage. This adds a source within M2; cross-venue history, other required feeds and later milestones remain unfinished.

Version 0.3.11 adds bounded settled-funding archives, verified with six public BTC settlements and receipt-time visibility checks. The 0.3.11 checkpoint passed 416 tests with 96.78% combined coverage. OKX public probes timed out from this host; that source remains unavailable here.

Version 0.3.12 adds bounded historical open interest, verified with 12 public BTC samples across three pages. The 0.3.12 checkpoint passed 446 tests with 96.86% combined coverage. Source units, original observation times and unavailable feeds remain explicit; M2 is still incomplete.

Version 0.3.13 added opt-in public Binance mark/index, native funding and OI snapshots, verified for BTC/ETH/SOL/PAXG. That checkpoint passed 473 tests with 96.98% combined coverage.

Version 0.3.14 adds explicit current Binance funding intervals, caps and floors with metadata receipt times. A finite public probe returned 480-minute BTC/ETH/SOL and 240-minute PAXG intervals; omitted symbols remain unknown. That checkpoint passed 489 tests with 97.00% combined coverage. OI unit/side normalization remains unresolved.

Version 0.3.15 adds opt-in official BEA calendar snapshots. Public verification stored 136 dated entries across 30 series, with receipt-time availability and complete-snapshot revision handling. That checkpoint passed 526 tests with 97.11% combined coverage. BLS calendar access returned HTTP 403; other calendars, actual release values and consensus remain pending.

Version 0.3.16 adds as-of calendar quality assessment. Recent health and schedule observations plus future dates for the named BEA series are required; failed polls, exhausted dates and malformed or ambiguous evidence produce explicit blockers. Saved-content replay verifies that future failures never alter earlier assessments. That checkpoint passed 549 tests with 97.17% combined coverage.

Version 0.3.17 adds a read-only ETH finalized-block gas adapter with exact fee/gas units and timestamp/continuity checks. Its offline tests pass, but public access failed certificate validation; no live gas data is claimed and the source remains disabled. That checkpoint passed 597 tests with 97.27% combined coverage. The Federal Reserve calendar page also returned HTTP 403.

Version 0.3.18 adds explicit ETH restart recovery from a validated committed anchor, preserving original receipt times and rejecting block regressions after restart. Synthetic recovery and interrupted-startup tests pass. TLS diagnosis identifies a hostname mismatch in both Python and Windows; verification stays enabled and the feed remains unavailable. That checkpoint passed 621 tests with 97.32% combined coverage. M2 remains incomplete and trading stays disabled.

Version 0.3.19 fixes repeated-cancellation races in database workers and releases connections when startup handoff is interrupted. Offline commit/reopen and receipt-time checks pass; that checkpoint passed 630 tests with 97.33% combined coverage. No new public-feed or trading-readiness claim is made.

Version 0.3.20 adds opt-in Binance taker buy/sell aggregate sampling. A finite public run verified 12 observations across BTC/ETH/SOL/PAXG, with exact reported values and receipt-time visibility. That checkpoint passed 674 tests with 97.40% combined coverage. Native volume normalization and continuous history remain pending.

Version 0.3.21 adds opt-in public Binance global account-ratio samples, verified with 12 observations across four assets. Named fields and period-end/receipt timestamps remain exact; documentation inconsistencies are flagged without inferred corrections. That checkpoint passed 706 tests with 97.44% combined coverage.

Version 0.3.22 preserves each flow poll receipt so A → B → A revisions survive deduplication, and adds scoped point-in-time selection of one observed revision per period. Saved public compatibility and synthetic archive/restore checks passed. That checkpoint passed 732 tests with 97.47% combined coverage. Older archives cannot reconstruct discarded receipts.

Version 0.3.23 adds scoped flow quality checks for consecutive recent periods, independent source/receipt/health ages, and global discovery outages. Saved public windows and explicitly synthetic outage/recovery checks passed. That checkpoint passed 777 tests with 97.55% combined coverage.

Version 0.3.24 adds bounded Binance settled-funding archives, publicly verified with six BTC observations over 48 hours. Exact rates, mark prices, reported types and source/receipt clocks survive repeat/PIT/archive checks. That checkpoint passed 841 tests with 97.60% combined coverage.

Version 0.3.25 adds canonical point-in-time funding revision selection, choosing one observed revision per charge timestamp in a scoped window. Saved public replay and clearly synthetic revisions survive archive restore without counting repeated downloads as extra charges. That checkpoint passed 873 tests with 97.63% combined coverage. Historical intervals and complete schedules remain unverified; M2 remains incomplete and trading stays disabled.

Version 0.3.26 adds explicit finite funding-archive restart, preserving earlier observation receipts and verifying the closed combined database. A public BTC restart retained five scoped prior rows and six new observations, with independent archive/PIT checks. That checkpoint passed 891 tests with 97.65% combined coverage. Recurring history repair and other required feeds remain unfinished; trading stays disabled.

Version 0.3.27 adds optional recurring recent funding-history collection to supervised capture, with staged bounded downloads, explicit poll health and retained receipt times. One finite public BTC cycle and labelled offline later-poll/outage replay passed. The current suite passes 928 tests with 97.68% combined coverage. Older gaps, long-running deployment and other required feeds remain unfinished; trading stays disabled.

## Quickstart

From this repository directory, with Python 3.11 or newer:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.lock
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\selective-options check-config
.venv\Scripts\selective-options probe --output state/probe.json
.venv\Scripts\selective-options capture --duration 60 --output state/capture-001
```

Linux/macOS use `.venv/bin/` instead of `.venv\Scripts\`. No exchange credentials are needed for public probing. Do not supply production keys. The `.env.example` file names optional testnet variables; dotenv is not automatically loaded. See [RUNBOOK.md](RUNBOOK.md) for authenticated readiness and Docker instructions.

```text
ruff check .
ruff format --check .
mypy
pytest --cov --cov-report=term-missing
```

Verified here: 357 tests passed; 96.51% combined statement/branch coverage of implemented code; strict mypy and Ruff passed. Docker runtime and Python 3.11 execution were not available locally. CI covers Python 3.11/3.12. No risk-manager, execution-engine or strategy coverage is claimed before those modules exist.

## Current venue evidence

The actual public testnet probe captured 1,050 BTC, 854 ETH and 294 SOL options. SOL had zero positive two-sided summaries in this snapshot; PAXG returned no options and is `GOLD_UNTRADEABLE`. All assets have `tradable=false`: discovery is not liquidity or strategy approval. See [API_FINDINGS.md](API_FINDINGS.md) and [captured probe](evidence/public-testnet-probe.json) for timestamps, units and limitations.

The prompt's gold-perpetual fixed-loss assumption, neutral-volatility versus directional-gate conflict, missing risk units, and runtime COD permission conflict are recorded explicitly in [ARCHITECTURE.md](ARCHITECTURE.md). Current India reporting references also need review; see [compliance_notes.md](compliance_notes.md). Tax rates are scenario inputs, not classifications of these products.

## Repository map

- [ARCHITECTURE.md](ARCHITECTURE.md): module flow, implemented interfaces and clearly labelled future contracts.
- `src/selective_options/`: testnet foundation and M2 ingestion pilot; no trading modules.
- `config/`: generated annotated defaults plus testnet/production profiles; both profiles are prevented from trading.
- `tests/`: configuration/security, protocol failure, rate-limiter, discovery, clock, auth and CLI tests.
- `evidence/`: real public venue snapshots, automated test results and timestamped readiness and ingestion evidence.
- [VALIDATION_REPORT.md](VALIDATION_REPORT.md): milestone/acceptance status, observed failures/fixes and missing evidence.
- [RUNBOOK.md](RUNBOOK.md): installation, operation and fault response.
- [SPECIFICATION.md](SPECIFICATION.md): original user-supplied requirements, retained verbatim.

## Progression

Milestone 2 remains open: historical data, underlying bars, historical perpetual/funding coverage, cross-venue volatility/flow, on-chain, news, macro and XAU/USD coverage, durable gap recovery, and a deployed Redis bus still need implementation and verification. No provider account was created or paid subscription purchased. Milestones 3â€“14 have not started. The finite captures have stopped. A user-authorized development heartbeat continues this task every 30 minutes while the local computer and app are running; it does not trade. Seven-day soak and 60-day paper results require actual elapsed observations and remain absent.

## Configuration

Precedence is typed defaults, `config/default.yaml`, selected profile, then `SELECTIVE_SECTION__FIELD` environment overrides. Unknown YAML/nested keys, nonfinite numbers, incompatible profiles and malformed risk/expiry bounds fail validation. Profile-changing environment overrides are rejected. For example, `SELECTIVE_GATE__MIN_COMPOSITE_CONVICTION=80` raises the reserved conviction threshold. It never enables trading.

`system`, `venue`, `rate_limits`, `ingestion` and data-quality settings are operational in the M2 pilot. Other sections are validated policy values reserved for their future milestones. `null` gamma/tail-loss/FX settings indicate unresolved inputs, not unlimited risk or zero conversion cost. Parameters not specified in the original prompt, such as the 15-point IV-crush scenario, are provisional and need validation before use.

The following guide is generated from schema descriptions. Change typed defaults and run `python scripts/render_config.py` to regenerate commented YAML and the guide. For operator experiments, edit a profile or set environment overrides without silently changing the canonical defaults.

<!-- PARAMETERS_START -->
| Parameter | Default | Effect |
| --- | --- | --- |
| `system.mode` | `"testnet"` | Select environment; all modes remain unable to trade in this build. |
| `system.assets` | `["BTC", "ETH", "SOL", "PAXG"]` | Assets to discover; adding assets increases public API work. |
| `system.base_currency` | `"USD"` | Reporting denomination; retain native quote and settlement units. |
| `system.loop_interval_s` | `15` | Planned decision interval; higher means fewer evaluations. |
| `system.heartbeat_timeout_s` | `60` | Stream receive deadline; lower detects stalled sessions sooner. |
| `system.log_level` | `"INFO"` | Application log verbosity; protocol payload debug logging is disabled. |
| `venue.key_profile` | `"testnet"` | Credential namespace; must match the exact endpoint and mode. |
| `venue.ws_url` | `"wss://test.deribit.com/ws/api/v2"` | Exact allowlisted endpoint; overrides cannot redirect credentials. |
| `venue.request_timeout_s` | `15` | Whole RPC deadline after limiter wait; higher tolerates slower replies. |
| `venue.connect_timeout_s` | `15` | WebSocket handshake deadline; higher waits longer on network failures. |
| `venue.max_message_bytes` | `16777216` | Inbound message bound; higher supports larger dynamically discovered chains. |
| `venue.retry_attempts` | `3` | Public probe attempts; increases bounded reconnect attempts, never order retries. |
| `venue.retry_base_s` | `1` | Base exponential retry delay for public transport failures. |
| `venue.retry_max_s` | `10` | Maximum retry delay; longer reduces reconnect pressure. |
| `venue.auth_refresh_margin_s` | `60` | Refresh this far before expiry, capped at half the token lifetime. |
| `venue.ticker_samples_per_asset` | `2` | Detailed ticker samples per discovered chain; summaries still cover the full chain. |
| `rate_limits.discovery.capacity` | `500000.0` | Maximum credits; increasing allows larger bursts. |
| `rate_limits.discovery.refill_per_s` | `10000.0` | Credits restored each second; increasing raises sustained traffic. |
| `rate_limits.discovery.cost` | `10000.0` | Credits per call; increasing lowers effective call frequency. |
| `rate_limits.subscriptions.capacity` | `30000.0` | Maximum credits; increasing allows larger bursts. |
| `rate_limits.subscriptions.refill_per_s` | `10000.0` | Credits restored each second; increasing raises sustained traffic. |
| `rate_limits.subscriptions.cost` | `3000.0` | Credits per call; increasing lowers effective call frequency. |
| `rate_limits.ordinary.capacity` | `5000.0` | Maximum credits; increasing allows larger bursts. |
| `rate_limits.ordinary.refill_per_s` | `2500.0` | Credits restored each second; increasing raises sustained traffic. |
| `rate_limits.ordinary.cost` | `500.0` | Credits per call; increasing lowers effective call frequency. |
| `data.max_staleness_s` | `30` | Maximum accepted feed age; reducing tightens freshness. |
| `data.max_price_divergence_bps` | `25` | Independent-reference divergence diagnostic threshold; reducing tightens agreement. |
| `data.min_sources_per_asset` | `2` | Required independent spot sources; increasing raises data requirements. |
| `data.max_clock_skew_s` | `5` | Maximum local/server clock offset plus half round-trip uncertainty. |
| `ingestion.coinmetrics_enabled` | `false` | Opt in to public daily BTC/ETH market-cap and MVRV observations; no realized-cap/SOPR, exchange attribution or publication-time inference. |
| `ingestion.coinmetrics_poll_interval_s` | `3600` | Pause after every bounded Community API poll, including failures; no immediate retry or provider fallback. |
| `ingestion.coinmetrics_lookback_days` | `7` | Explicit inclusive provider daily-label request window ending on the prior UTC day; missing dates never imply complete history. |
| `ingestion.coinmetrics_max_data_age_s` | `259200` | Maximum age of latest provider daily period label; separate from response receipt and not independent finality. |
| `ingestion.coinmetrics_max_pages` | `4` | Whole-poll page budget; a continuation beyond this cap refuses success instead of truncating history. |
| `ingestion.coinmetrics_max_rows` | `128` | Maximum daily rows staged across the configured BTC/ETH poll; overflow refuses before data publication. |
| `ingestion.coinmetrics_max_response_bytes` | `2000000` | Decoded HTTP entity budget per page; fixed HTTPS origin and endpoint, no automatic redirects. |
| `ingestion.coinmetrics_timeout_s` | `30` | Bounded whole-poll timeout in seconds; transport failure retains explicit failed health. |
| `ingestion.stablecoin_supply_enabled` | `false` | Opt in to read-only Ethereum mainnet USDC/USDT supply at one provider-asserted finalized block; no all-chain supply or exchange balances. |
| `ingestion.stablecoin_supply_poll_interval_s` | `600` | Pause between paired stablecoin supply polls; native observations and poll receipts remain distinct. |
| `ingestion.stablecoin_supply_max_block_age_s` | `1800` | Maximum native finalized header age for paired ERC-20 supply; provider assertion is not independently verified finality. |
| `ingestion.tips_yield_enabled` | `false` | Opt in to official FRED DFII10 daily 10-year TIPS real-yield CSV snapshots; dates never substitute for unknown publication times. |
| `ingestion.tips_yield_poll_interval_s` | `3600` | Pause between bounded official daily real-yield window snapshots; revisions and missing values remain retained. |
| `ingestion.tips_yield_history_days` | `90` | Calendar-day window requested per DFII10 snapshot, not a claim of complete historical publication knowledge. |
| `ingestion.liquidations_enabled` | `false` | Opt in to public Binance USDT perpetual forceOrder snapshots for configured assets; the 1000ms stream is lossy and is not a complete liquidation ledger. |
| `ingestion.kraken_auto_recovery_enabled` | `false` | Opt in to bounded public REST repair of forward Kraken trade gaps; failure halts capture and affected bars stay partial. |
| `ingestion.kraken_backfill_max_pages` | `5` | Maximum public REST trade-history pages per explicit bounded backfill; exhaustion fails coverage. |
| `ingestion.kraken_backfill_page_size` | `1000` | Trades requested per public REST history page; recovered transport timestamps remain separate from WebSocket events. |
| `ingestion.kraken_backfill_timeout_s` | `60` | Total deadline for a bounded public trade-history fetch. |
| `ingestion.kraken_backfill_interval_s` | `1` | Minimum pause between REST history pages; no automatic retry on rate limiting. |
| `ingestion.policy_news_sources` | `["federal_reserve", "ecb"]` | Public official policy RSS metadata; no model classification or economic-calendar coverage is implied. |
| `ingestion.crypto_news_enabled` | `false` | Opt in to fixed public CoinDesk RSS metadata and bound poll health; no classification, first-publication verification or all-news coverage. |
| `ingestion.crypto_news_poll_interval_s` | `300` | Pause after a bounded crypto RSS poll, including failures; minimum five minutes and no immediate retries. |
| `ingestion.news_poll_interval_s` | `300` | Seconds between public policy RSS polls; publication time and polling health are tracked separately. |
| `ingestion.policy_news_max_publication_age_s` | `2592000` | Descriptive recent-publication window for exact policy RSS metadata; separate from poll receipt age and never trading approval or independently verified first publication. |
| `ingestion.spot_trades_enabled` | `true` | Discover public Kraken USD spot trade streams and build local closed bars. |
| `ingestion.bar_timeframes_s` | `[300, 900, 3600, 14400, 86400]` | UTC bar intervals built from underlying trades; forming and empty intervals are not synthesized. |
| `ingestion.bar_lateness_s` | `5` | Wait after a candle closes before emitting it; later unexpected trades halt capture instead of revising history silently. |
| `ingestion.bar_checkpoint_on_stop` | `false` | Opt in to a committed native-input bar checkpoint after producers stop and the writer drains; this is not periodic crash recovery. |
| `ingestion.bar_checkpoint_max_events` | `10000` | Maximum exact union of open-bar native inputs and sequence anchors for optional bar checkpoints/resume; overflow refuses before publication. |
| `ingestion.bar_checkpoint_interval_s` | `null` | Opt in to gated, FIFO-committed immutable open-bar generations during finite capture; null disables. This does not certify whole-source continuity or power-loss recovery. |
| `ingestion.bar_checkpoint_max_generations` | `32` | Maximum published periodic bar generations per finite capture; exhaustion halts instead of overwriting or silently dropping recovery evidence. |
| `ingestion.eth_gas_enabled` | `false` | Opt in to public Ethereum mainnet finalized-block gas samples; read-only. |
| `ingestion.eth_gas_provider` | `"publicnode"` | Explicit public no-key Ethereum RPC provider; source identities and restart checkpoints remain provider-scoped, with no automatic fallback. |
| `ingestion.solana_network_enabled` | `false` | Opt in to read-only public Solana performance and visible vote-account activated stake; no transactions. |
| `ingestion.solana_network_poll_interval_s` | `300` | Pause between Solana network samples; failures wait ten minutes. |
| `ingestion.solana_network_timeout_s` | `90` | Whole Solana genesis/performance/time/stake collection deadline. |
| `ingestion.solana_network_max_sample_age_s` | `180` | Maximum estimated sample-end slot age at performance receipt; stake snapshots use receipt time only. |
| `ingestion.eth_gas_poll_interval_s` | `120` | Ethereum gas polling interval; failures back off 600 seconds. |
| `ingestion.eth_gas_max_block_age_s` | `1800` | Maximum finalized-block age; finality is delayed, not a live gas quote. |
| `ingestion.bea_calendar_enabled` | `false` | Opt in to official BEA dated release schedule snapshots; no release values or consensus. |
| `ingestion.bea_calendar_poll_interval_s` | `3600` | Seconds between BEA calendar polls, including failures; minimum fifteen minutes. |
| `ingestion.fomc_calendar_enabled` | `false` | Opt in to official Federal Reserve date-only FOMC meeting snapshots; no exact announcement time or unscheduled meeting coverage. |
| `ingestion.fomc_calendar_poll_interval_s` | `3600` | Seconds between official FOMC schedule polls, including failures; minimum fifteen minutes. |
| `ingestion.binance_enabled` | `false` | Opt in to public Binance USDT perpetual/funding/native OI snapshots; no orders. |
| `ingestion.binance_funding_history_enabled` | `false` | Opt in to recurring bounded recent Binance settled-funding downloads; retains poll receipts, no orders or full-history claim. |
| `ingestion.binance_funding_history_hours` | `48` | Rolling funding-history window per poll; older gaps are outside automatic repair coverage. |
| `ingestion.binance_funding_history_page_size` | `200` | Funding observations requested per history page; native values remain unchanged. |
| `ingestion.binance_funding_history_max_pages` | `10` | Maximum history pages per asset/poll; exhausted coverage fails before market publication. |
| `ingestion.binance_funding_history_timeout_s` | `90` | Whole discovery/history deadline per asset; transport and page budgets also apply. |
| `ingestion.binance_funding_history_poll_interval_s` | `900` | Pause after a funding-history cycle, including failures; minimum ten minutes and no immediate retries. |
| `ingestion.binance_taker_enabled` | `false` | Opt in to public Binance five-minute taker buy/sell aggregates; native volumes only, no orders. |
| `ingestion.binance_account_ratio_enabled` | `false` | Opt in to public Binance global account-ratio samples; reported fields preserved, no orders. |
| `ingestion.binance_account_ratio_poll_interval_s` | `300` | Global account-ratio poll interval; at least five minutes, failures wait at least ten minutes. |
| `ingestion.binance_taker_poll_interval_s` | `300` | Pause between taker-flow polls; minimum five minutes, failed cycles wait at least ten minutes. |
| `ingestion.binance_poll_interval_s` | `30` | Binance poll interval; failed cycles back off 600 seconds. |
| `ingestion.bybit_enabled` | `false` | Opt in to public Bybit USDT perpetual funding/OI snapshots; no account or orders. |
| `ingestion.bybit_poll_interval_s` | `30` | Public Bybit polling interval; transport, data and access failures back off 600 seconds. |
| `ingestion.derivatives_enabled` | `true` | Collect public testnet perpetual ticker/funding and available BTC/ETH volatility-index candles. |
| `ingestion.derivatives_poll_interval_s` | `20` | Pause between derivative snapshot cycles; actual cycle also includes network request time. |
| `ingestion.volatility_lookback_minutes` | `5` | Recent closed one-minute volatility-index candle window; not a historical archive. |
| `ingestion.duration_s` | `60` | Supervised capture duration; process closes sockets and drains writes at the end. |
| `ingestion.max_stream_instruments_per_asset` | `6` | Per-asset streaming cap for local pilot; null requests all instruments. A cap marks coverage partial. |
| `ingestion.subscription_batch_size` | `50` | Channels per subscribe call; larger batches reduce credit cost. |
| `ingestion.stream_universe_refresh_interval_s` | `null` | Optional seconds after complete stream acknowledgement before closing, rediscovering and resubscribing; null refreshes only on reconnect. Rotation records a gap, not historical continuity. |
| `ingestion.option_universe_max_age_s` | `3600` | Maximum age of the exact discovery manifest bound to current option subscriptions; independent of quote freshness, with no listing-continuity guarantee. |
| `ingestion.queue_capacity` | `20000` | Maximum queued events; saturation raises rather than silently dropping data. |
| `ingestion.queue_timeout_s` | `10` | Backpressure deadline; exceeding it fails capture loudly. |
| `ingestion.storage_batch_size` | `500` | Maximum events per atomic DuckDB insert transaction. |
| `ingestion.archive_rotation_interval_s` | `null` | Opt in to periodic cumulative committed-store checkpoints during finite capture; null creates no periodic task. Each checkpoint follows a commit barrier, without claiming complete history or a complete source frontier. |
| `ingestion.archive_rotation_partition_ms` | `86400000` | Availability bucket width for periodic checkpoints, using the later of source and receipt clocks; both clocks must be at or before the selected cutoff. |
| `ingestion.archive_rotation_max_partitions` | `366` | Maximum nonempty availability partitions per checkpoint; exceeding the bound fails capture without publishing a completed checkpoint. |
| `ingestion.archive_rotation_max_snapshots` | `10` | Total periodic checkpoint budget per finite capture; the next due checkpoint after this many successes fails capture instead of silently stopping archives or creating unbounded files. |
| `ingestion.flush_interval_s` | `1` | Maximum wait before persisting a partially filled batch. |
| `ingestion.spot_poll_interval_s` | `10` | Coinbase/Kraken public reference poll period; lower increases request load. |
| `ingestion.chain_poll_interval_s` | `30` | Full option summary-chain refresh period; lower increases request load. |
| `ingestion.reconnect_delay_s` | `1` | Initial stream reconnect delay; subsequent failures back off to venue.retry_max_s. |
| `ingestion.reference_sources` | `["coinbase", "kraken"]` | Independent public spot-reference sources; no private accounts are used. |
| `ingestion.hot_buffer_events` | `2000` | In-memory recent-event ring capacity per source/asset/kind. |
| `ingestion.redis_url` | `null` | Optional Redis stream mirror; null uses the bounded in-process bus. Set credential-bearing URLs only through SELECTIVE_INGESTION__REDIS_URL. |
| `signals.weights.technical` | `0.35` | Technical pillar contribution; weights must sum to one. |
| `signals.weights.volatility_flow` | `0.35` | Options/positioning pillar contribution; weights must sum to one. |
| `signals.weights.fundamental` | `0.3` | News/macro pillar contribution; weights must sum to one. |
| `signals.lookback_days_iv_rank` | `252` | IV rank/percentile history; longer windows reflect slower regimes. |
| `signals.news_decay_halflife_hours` | `8` | News weight half-life; higher retains old news longer. |
| `signals.news_dedup_similarity` | `0.88` | Embedding similarity duplicate threshold; lower merges more stories. |
| `gate.min_composite_conviction` | `72` | Minimum absolute conviction; higher selects fewer setups. |
| `gate.min_pillar_agreement` | `3` | All three directional pillars required; lowering is not supported. |
| `gate.min_pillar_score_each` | `40` | Minimum absolute score per pillar; higher rejects weak evidence. |
| `gate.min_confidence_each` | `0.55` | Minimum confidence per pillar; higher is more selective. |
| `gate.min_expectancy_R` | `0.35` | Required after-cost expectancy in risk units; strictly exceed this. |
| `gate.min_reward_risk` | `2` | Minimum structural reward/risk at targets; higher selects larger payoffs. |
| `gate.max_spread_cost_pct_of_debit` | `6` | Maximum spread burden; lower rejects more expensive entries. |
| `gate.max_trades_per_month` | `10` | 30-day selectivity alert threshold; never a target or minimum. |
| `gate.cooldown_after_n_losses` | `2` | Consecutive losses before benching asset; lower triggers earlier. |
| `gate.cooldown_hours` | `48` | Asset loss-cooldown duration; higher waits longer. |
| `gate.min_samples` | `30` | Historical analogues required for empirical prior; higher needs more evidence. |
| `gate.p_haircut` | `0.85` | Fallback probability multiplier; lower discounts low-evidence priors more. |
| `vol.buy_premium_below_iv_pct` | `30` | Upper IV percentile for cheap premium classification. |
| `vol.sell_premium_above_iv_pct` | `60` | Lower IV percentile for rich premium classification. |
| `vol.min_move_edge_pct` | `20` | Required model move excess over implied move; higher is stricter. |
| `vol.require_panic_floor_for_short_premium` | `true` | Short premium always requires a floor during panic. |
| `structure.min_dte` | `21` | Minimum entry DTE; higher avoids near-expiry gamma. |
| `structure.max_dte` | `60` | Maximum entry DTE; higher admits longer expiries. |
| `structure.min_dte_exit` | `14` | Long-premium expiry exit threshold; higher exits earlier. |
| `structure.target_long_delta` | `[0.25, 0.4]` | Allowed absolute long-leg delta band; higher selects closer-to-money options. |
| `structure.short_strike_delta` | `[0.15, 0.25]` | Allowed absolute short-leg delta band; higher brings shorts closer to money. |
| `structure.catalyst_buffer_days` | `7` | Minimum option life after catalyst; higher leaves more time. |
| `liquidity.max_spread_pct_of_mid` | `8` | Per-leg spread ceiling relative to mid; lower is stricter. |
| `liquidity.min_open_interest` | `50` | Minimum per-leg OI; higher demands more participation. |
| `liquidity.min_24h_volume` | `20` | Minimum per-leg daily volume; higher rejects quiet strikes. |
| `liquidity.book_depth_multiple` | `3` | Required top size / intended quantity; higher demands more depth. |
| `risk.risk_per_trade_pct` | `1` | Planned fixed-fractional max-loss budget as percent of equity. |
| `risk.max_risk_per_trade_pct` | `2` | Absolute per-trade risk ceiling; lower constrains sizing. |
| `risk.kelly_fraction` | `0.25` | Fraction of calibrated Kelly sizing; higher increases size. |
| `risk.kelly_cap_pct` | `2` | Kelly size ceiling as percent of equity. |
| `risk.max_concurrent_positions` | `4` | Total position count limit; lower reduces concurrency. |
| `risk.max_positions_per_asset` | `1` | Per-asset position cap; never permits adding to losers. |
| `risk.max_crypto_beta_positions` | `3` | Correlated crypto position cap; cannot allow a fourth. |
| `risk.allow_naked_short` | `false` | Undefined-risk shorts disabled; no override implemented in this build. |
| `risk.blackout_before_min` | `120` | Tier-1 pre-event blackout; higher stops entries earlier. |
| `risk.blackout_after_min` | `30` | Tier-1 post-event blackout; higher waits longer. |
| `risk.max_tail_loss_pct` | `null` | Unspecified tail-loss ceiling; null must block future arming. |
| `risk.tail_paths` | `10000` | Monte Carlo paths; more improve tail resolution at higher compute cost. |
| `risk.gap_shock_pct` | `30` | Absolute underlying gap stress; higher stresses larger gaps. |
| `risk.iv_shock_points` | `50` | Absolute IV shock in volatility points, not relative percent. |
| `greeks.max_net_delta_pct_equity` | `25` | Delta-dollar equity limit; lower constrains direction exposure. |
| `greeks.max_net_vega_pct_equity` | `8` | Vega equity limit per one IV percentage point; lower tightens vol exposure. |
| `greeks.max_net_theta_daily_pct` | `0.6` | Daily theta equity limit; lower tightens time-decay exposure. |
| `greeks.max_net_gamma_notional` | `null` | Unspecified gamma-dollar limit; null blocks future arming; units must be resolved. |
| `drawdown.daily_stop_pct` | `3` | Daily loss halt threshold; lower halts sooner for 24 hours. |
| `drawdown.weekly_stop_pct` | `6` | Weekly loss threshold; halt 72 hours and require review. |
| `drawdown.monthly_stop_pct` | `10` | Monthly loss threshold; review and fresh validation required. |
| `drawdown.peak_to_trough_pct` | `15` | Peak equity drawdown threshold; return to paper mode. |
| `execution.passive_first` | `true` | Passive limit execution required; no market-option order path. |
| `execution.max_slippage_bps` | `40` | Maximum entry escalation; lower stands down sooner. |
| `execution.slice_count` | `3` | Target number of entry slices; higher reduces quantity per slice. |
| `execution.leg_timeout_s` | `20` | Time allowed for second leg before long-leg unwind. |
| `execution.cancel_on_disconnect` | `true` | Mandatory exchange connection cancellation protection. |
| `execution.reconciliation_interval_s` | `60` | Maximum interval between portfolio reconciliations. |
| `exits.long_profit_pct` | `100` | Long debit profit target; higher waits for larger profits. |
| `exits.long_scale_out_fraction` | `0.5` | Fraction closed at first long-premium target. |
| `exits.long_stop_loss_pct` | `50` | Debit-loss stop threshold; lower exits losses earlier. |
| `exits.iv_crush_exit_pts` | `15` | Provisional IV-point crush stop; requires validation before use. |
| `exits.short_take_profit_pct` | `50` | Credit profit captured before exit. |
| `exits.short_loss_credit_multiple` | `2` | Loss / original credit defensive exit threshold. |
| `exits.short_exit_dte` | `21` | Short-premium close/roll threshold; higher avoids gamma earlier. |
| `exits.reevaluate_atr_multiple` | `2` | Daily underlying move triggering thesis re-evaluation. |
| `gold.mode` | `"auto"` | Requested route; none is tradable until verified with appropriate risk controls. |
| `gold.max_basis_bps` | `150` | Wrapper/spot basis veto; lower is more conservative. |
| `gold.session_filter` | `"LBMA_COMEX"` | Requires venue holiday and DST calendars, not crypto 24/7 hours. |
| `gold.external_adapter` | `null` | Operator-selected gold broker; null means no external route. |
| `promotion.min_paper_days` | `60` | Minimum observed testnet days before promotion. |
| `promotion.min_paper_trades` | `25` | Minimum closed paper trades; selectivity may take longer than 60 days. |
| `promotion.min_paper_expectancy_R` | `0.25` | Minimum paper after-cost expectancy in risk units. |
| `promotion.max_paper_vs_backtest_divergence_pct` | `40` | Maximum paper/backtest expectancy divergence. |
| `promotion.required_brier_score` | `0.22` | Maximum Brier score; lower requires better calibration. |
| `promotion.live_ramp` | `[0.25, 0.5, 1]` | Initial, intermediate and validated live size fractions. |
| `promotion.trades_per_ramp_stage` | `20` | Completed trades before each possible ramp step. |
| `promotion.min_live_trades` | `40` | Minimum trades for full size, with expectancy tolerance also required. |
| `promotion.artifact_max_age_days` | `90` | Maximum validation artifact age; lower demands more recent evidence. |
| `promotion.max_pbo` | `0.35` | Strict PBO upper bound for promotion. |
| `promotion.min_dsr_confidence` | `0.95` | Deflated Sharpe confidence required against zero benchmark. |
| `promotion.positive_regimes_required` | `3` | Minimum profitable claimed regimes; needs adequate samples per regime. |
| `tax.jurisdiction` | `"IN"` | Operator reporting jurisdiction; not a legal classification. |
| `tax.asymmetric_regime` | `true` | Planned asymmetric-tax scenario; applicability requires accountant review. |
| `tax.vda_rate_pct` | `31.2` | User-supplied scenario rate including cess, excluding any applicable surcharge. |
| `tax.tds_rate_pct` | `1` | User-supplied withholding scenario; track receivable/cash drag separately from final tax. |
| `tax.specified_person_threshold_inr` | `50000` | User-supplied annual threshold scenario, subject to current guidance. |
| `tax.other_person_threshold_inr` | `10000` | User-supplied alternative annual threshold scenario. |
| `tax.retention_years` | `7` | Minimum audit-record retention; longer increases storage obligations. |
| `tax.fx_source` | `null` | Unselected historical INR FX source; exports must not invent conversion rates. |
<!-- PARAMETERS_END -->
