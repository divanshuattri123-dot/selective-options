# Testnet readiness and ingestion runbook

This build offers a one-shot readiness probe and a finite public-data recorder. It has no positions, execution loop, dashboard, kill command, or live deployment. Stopping it closes its WebSocket. It never places orders and cannot cancel orders opened by other software.

## Install and run

From the repository directory, with Python 3.11+:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.lock
.venv\Scripts\python -m pip install -e ".[dev]"
.venv\Scripts\selective-options check-config
.venv\Scripts\selective-options probe --output state/probe.json
```

On Linux/macOS use `.venv/bin/python` and `.venv/bin/selective-options` instead. No credentials are required for these commands. Each run discovers current instruments and writes a report plus complete per-asset summary-chain JSON to `state/chains/`. Detailed ticker greeks are sampled according to configuration; this is not a full live greeks feed. Quotes include original units and timestamps. Do not interpret the probe's two-sided-book count as a liquidity approval.

Exit codes: `0` means the requested config/probe operation succeeded, `2` means startup halted, and `130` means interrupted. A successful public probe still has `validation_status=NOT_VALIDATED` and `trading_enabled=false`. Failure does not overwrite the previous report, so always check its timestamp and the current process exit code.

## Authenticated readiness (verified for the supplied testnet key)

1. Create a separate Deribit **testnet** account and a dedicated subaccount for future testing.
2. Configure a testnet API key with `trade:read_write`, `account:read` for key/COD inspection and `wallet:none` (wallet read is accepted in maximum metadata, wallet write is rejected). Other non-trading write scopes are rejected. Use an IP restriction where appropriate.
3. Resolve the COD permission conflict in `API_FINDINGS.md`: account administration must enable the account default before connecting with the restricted runtime key. Do not add account-write privileges to the runtime key to bypass the check.
4. Set `DERIBIT_TESTNET_CLIENT_ID` and `DERIBIT_TESTNET_CLIENT_SECRET` in a local process environment or secret-manager injection. `.env.example` is documentation; the app deliberately does not auto-load a dotenv file. Never commit actual secrets.
5. Run `selective-options probe --authenticated --output state/authenticated-probe.json`. It checks the returned token, actual key maximum scope and enabled connection COD. API errors, missing metadata and TFA requirements halt startup. No orders are created.
6. Account readiness also needs an observed token-refresh test over a real session; fixture refresh tests are not sufficient evidence. The one-shot probe calls the freshness check between asset requests, but may finish before a token needs refreshing.

Keys are isolated by environment variable namespace. The endpoint must exactly match the chosen profile. Credential origin is ultimately established by the venue, not the local profile label. M1 rejects production connections even with a valid production config.

## Test and container commands

```text
ruff check .
ruff format --check .
mypy
pytest --cov --cov-report=term-missing
python scripts/render_config.py
pre-commit install
docker compose run --rm probe
```

Docker Compose runs the public probe once as a non-root user, with a read-only root filesystem and a persistent output volume. Docker was not available on the build host, so the image was not built or run here. There are no Redis/Postgres/Grafana services yet; they belong to later milestones. CI intentionally has no claimed smoke backtest before milestone 4.

## Current event responses

| Event | Action |
| --- | --- |
| `OPTIONS_DISCOVERED` | Inspect chain and two-sided counts; this does not permit trading. |
| `NO_OPTION_CHAIN` | Check venue capabilities and currency metadata. Never invent a replacement contract. |
| `GOLD_UNTRADEABLE` | Stay flat in gold. Select an external route or validate synthetic risk in a later milestone. |
| `GOLD_OPTIONS_UNVERIFIED` | Chain exists but liquidity/depth and basis/session gates remain unverified. |
| `STALE_SUMMARIES` | Snapshot flags stale data; do not use it for an entry. Inspect feed timing. |
| `CLOCK_SKEW` / `STALE_TICKER` | Startup fails. Check Windows Time/NTP and network latency; do not simply widen tolerance. |
| `TRANSPORT_RECONNECT` | Public probe reconnects with bounded exponential delay and repeats discovery using the same limiter. |
| `STARTUP_HALTED` | Read safe error type/field locations, correct the underlying issue and rerun. Credential payloads are intentionally omitted. |
| RPC code `13004` | Authentication rejected. Confirm the key was created on test.deribit.com, is enabled, and that both values match exactly. Do not automatically switch to production or retry repeatedly. |
| RPC code `10028` | Rate-limit failure invalidates the socket and fails the run. Allow credit recovery; reduce configured rates if appropriate. |
| `INTERRUPTED` | Current connection is closed. There are no application orders to reconcile in M1. |
| `MILESTONE_INCOMPLETE` | Expected result of `arm`; validation cannot be bypassed with config or a fake artifact. |

## Future operational requirements — not implemented

Before any order-capable deployment, implement and prove journal persistence, startup/60-second reconciliation, CLI/dashboard kill, watchdog cancellation, secret-safe alerts, encrypted backups and restore tests. Weekly/monthly/peak halts must persist across restarts. Monthly clearance requires a written review plus validation completed after the halt; peak-to-trough clearance returns to paper. Never clear a halt by editing config.

The 7-day soak and 60-day paper run start only after the prerequisite milestones are verified. A user-authorized development heartbeat resumes this task every 30 minutes while the computer and app are running. It does not submit trades.

## Supervised ingestion capture

Run `selective-options capture --duration 60 --output state/capture-001` from the repository. The output directory must not exist. No credentials are needed. Collection stops at the duration, sockets close, queued events drain, and DuckDB/Parquet/report artifacts are written. Total elapsed time includes startup and shutdown. Use a new output directory for every run. Review the exit code and `report.json`; `CAPTURE_FINISHED` means the recorder finished, not that every required feed was present.

Optional Redis mirror: set `SELECTIVE_INGESTION__REDIS_URL` in the process environment. Do not place a credential-bearing URL in committed YAML. A configured mirror failing to write fails the capture. No Redis server is provisioned here. The default bounded in-process queue works without Redis; full deployed bus acceptance remains outstanding.

`STREAM_DISCONNECTED` records a gap and triggers fresh discovery/subscriptions; the gap is not backfilled. `SEQUENCE_GAP`, malformed payloads and sustained queue saturation fail capture. Preserve partial artifacts and diagnose before restarting. Production endpoints remain blocked. The supplied finite pilot captures are stopped; the development heartbeat is separate from data capture.

Derivative collection is enabled by default with `ingestion.derivatives_enabled`; it shares the same request limiter as option discovery. `ingestion.derivatives_poll_interval_s` defaults to 20 seconds after a completed cycle, and `ingestion.volatility_lookback_minutes` defaults to five. This is a short rolling testnet window, not a historical archive. `VOLATILITY_INDEX_NOT_CONFIGURED` for SOL/PAXG is explicit; no proxy is substituted. Missing funding and unavailable index data produce status events. Invalid OHLC, truncated windows and crossed/mismatched perpetual quotes halt the capture.

## Trades, candles and policy news

Kraken public trades and local bar building are controlled by `ingestion.spot_trades_enabled`, `bar_timeframes_s` and `bar_lateness_s`. Only closed bars are exported; inspect `complete` before using a bar. A complete five-minute bar has been observed and independently replayed in the long pilot. Higher timeframes have fixture verification; no complete live 1h/4h/1D bar is claimed from the short observation period.

`policy_news_sources` selects public Federal Reserve/ECB RSS feeds; polling health is separate from article age. The current ECB source reports a TLS certificate validation failure on this host. Certificate verification stays enabled. Policy news has no sentiment/classification verdict. Credentials and paid accounts are unnecessary for these public feeds.

## Redis replay and event archive restore

Committed events and pending delivery entries are atomic. With a configured `SELECTIVE_INGESTION__REDIS_URL`, run `selective-options replay --database state/capture-001/events.duckdb`. Replay may repeat an event after a crash between Redis acceptance and local acknowledgement; consumers deduplicate its stable `event_id`. Do not interpret an empty pending queue as strategy validation. No real Redis integration or consumer-group deployment has yet been verified.

New captures write `events.parquet.manifest.json` alongside their Parquet archive. Restore with `selective-options restore --archive state/capture-001/events.parquet --output state/restored-001`. The manifest is required and the output directory must not exist. Restore checks checksum, schema, row count, event uniqueness and typed records before publishing `events.duckdb`. It preserves source and observation timestamps. A failure preserves diagnostic staging files and does not publish a completed database; use a different fresh directory after diagnosing the cause. Restore requeues all events for Redis delivery. Checksums are integrity checks, not signatures or permission to trade.

Storage cancellation waits for an in-flight database worker to finish, even after repeated cancellation requests. An interrupted append may already be committed: reopen and inspect durable events/outbox before retrying with the same stable identities. Database/restore-reader opens release their connection when cancellation prevents handoff. This is not a wall-clock shutdown bound; a stuck native worker is not force-killed. Filesystem publication and abrupt process death remain separate recovery concerns.

Pilot-05's manifest was generated after capture during verified restoration. Older archives without manifests cannot be silently restored unchecked. The code exposes `seal_archive` for trusted existing exports; verify their provenance and row counts before creating a manifest. A known-bad file plus a newly generated checksum is still bad evidence.

## Resume Kraken trade sequences from a stopped capture

Use `selective-options capture --duration 60 --output state/capture-002 --resume-trades-from state/capture-001/events.duckdb`. The prior collector must be stopped and the new output directory must not exist. Loading is read-only and derives the checkpoint from committed events, including committed WAL records after a process crash. Never copy an actively changing database as a checkpoint.

The new database imports up to 50 contiguous Kraken trades per configured book with their original timestamps; these count toward total `rows` and are identified in `trade_checkpoint_seed_rows` and `trade_checkpoint_event_ids`. They enter the new outbox as pending deliveries. Snapshot duplicates are compared, newly recovered consecutive trades use current receipt times, and older unknown snapshot prefixes are skipped. Imported rows do not count as fresh observations in the feed monitor.

`TRADE_SEQUENCE_RECOVERED` records successful per-book reconciliation. `SPOT_TRADE_HALTED` records an integrity failure and its exact reason. A snapshot that omits a necessary new trade, conflicts with a checkpoint, or is older than the checkpoint causes capture to fail. Do not discard the checkpoint to disguise a gap. `trade_recovery_pending_symbols` identifies recovery still awaiting a snapshot when a finite capture ends. `CAPTURE_FINISHED` alone is not proof that recovery completed.

Only Kraken raw trade sequences resume. Other collectors start fresh, old bar accumulators are not restored, and the first rebuilt intervals are partial. An in-process disconnect marks existing bars incomplete and suspends their flush until that book's recovery snapshot arrives. Larger gaps require a future REST/historical backfill implementation.

The read-only public verification command is `python scripts/verify_trade_recovery.py --output state/recovery-check-001`. It stops after two finite snapshot sessions and saves both databases, Parquet archives/manifests and a result report. It creates no accounts and submits no orders.

## Bounded public REST trade-range archive

From a stopped capture, run `selective-options backfill-trades --database state/capture-001/events.duckdb --output state/backfill-001 --symbol BTC/USD --through-trade-id TARGET_ID`, replacing `TARGET_ID` with the last native trade ID required for the gap. The symbol must occur in the configured assets' saved Kraken checkpoints. The requested range starts immediately after its latest committed ID. Use a new output directory.

The default budgets are five pages of up to 1,000 trades, a 60-second total fetch deadline and at least one second between pages. Settings are `ingestion.kraken_backfill_max_pages`, `kraken_backfill_page_size`, `kraken_backfill_timeout_s`, and `kraken_backfill_interval_s`. The page budget also covers the anchor, overlap and preceding records; it is not a guarantee that every requested range within the numerical limit will fit. HTTP/rate-limit errors fail the attempt without automatic retry.

Successful output has `report.json` with `status=RANGE_VERIFIED`, the checkpoint `anchor.json`, raw `page-*.json`, an event database, a Parquet archive and its checksum manifest. Events retain source `kraken_rest`, exact decimal prices/quantities/timestamps and actual HTTP response receipt times. Do not add REST and WebSocket records together as if they were disjoint trades; they can describe the same native IDs.

An incomplete fetch writes a failure report and available raw diagnostics without a successful archive. Preserve it and investigate the exact reason: missing anchor, conflicting overlap, uncovered ID, cursor/page limit, wrong pair, oversized response, clock anomaly or timeout. A failure during final database/export writing may leave partial output; the report must say `RANGE_VERIFIED` before treating the result as complete. A source archive is neither an automatic live-stream repair nor a bar reconstruction. The existing `--resume-trades-from` accepts Kraken WebSocket checkpoint databases, not these REST-only archives.

## Verify cross-transport reconciliation from saved data

The asynchronous reconciliation component is available for development replay; capture does not yet orchestrate an automatic fetch. Capture can load a committed plan using the explicit marker flag described below. Run from the project directory:

```powershell
python scripts/verify_reconciliation.py --checkpoint-database evidence/trade-recovery-01/before/events.duckdb --rest-database evidence/trade-backfill-01/events.duckdb --websocket-database evidence/trade-recovery-01/after/events.duckdb --symbol BTC/USD --output state/reconciliation-check-001
```

This performs no network access. It reads the saved databases without migration and writes a fresh report, raw-event database, Parquet archive and manifest. The report identifies selected event IDs, verification time, selected transport counts, and `capture_integration_enabled=false`. Both transports can observe the same native trade; retain the raw records for audit but count only the selected IDs in a verified plan.

`RecoveryBuffer` accepts a single book/anchor, required end ID, maximum events (default 20,000), serialized bytes (default 16 MiB) and source-clock tolerance. `reconcile_while_receiving` enforces a configurable total deadline (default 60 seconds) and cancels/awaits both workers on every exit. Overflow, conflict, missing sequence or an invalid observation permanently invalidates that buffer; use a new attempt after diagnosis. `RecoveryPlan.ready_rows(as_of_ms)` enforces verification and source/receipt-time availability. Preserved raw timestamps must never be relabelled as the plan's later availability time or used to claim that recovery was known earlier.

Without `--recovery-marker`, restart uses only WebSocket checkpoints. To activate a mixed-source plan, first publish its journal marker and then pass that explicit marker as described below. Bars start fresh and partial; raw REST coverage alone cannot make them complete.

## Durable recovery-plan verification

Add `--durable` to the `scripts/verify_reconciliation.py` command above and use a fresh output directory. This commits the anchor, raw events, selected IDs and verification marker atomically, closes/reopens the database, restores its Parquet archive into the output's `restored` subdirectory, and revalidates the same plan there. The report contains `durable_marker_id`, separate raw/database row counts and `restart_and_archive_restore_verified`. No network access is involved.

The application APIs are `publish_recovery(store, anchor, plan)` and `load_recovery(database, marker_id, as_of_ms)`. Publication revalidates the plan, canonicalizes repeated stable IDs to their first observations and returns only after commit. Repeating an identical publication is idempotent and does not reset acknowledged deliveries. An existing record with the same event ID but different contents or receipt time causes a complete transaction rollback; ordinary ingestion's dedup policy is unchanged.

Loading is read-only and requires an explicit marker ID. It checks marker shape, timing, unique references, anchor, exact evidence hash, full sequence, cross-transport conflicts and the selected records. Missing records, altered receipt times or markers fail. Keep the raw records with the marker; a cursor alone is not recovery evidence. Verification timestamps are knowledge times and are not claims about the exact physical commit-completion instant. Checksums detect inconsistency, not malicious rewrites with recomputed hashes.

The journal supports explicit capture startup through the marker flag below. A marker alone does not activate a running cursor: destination commit/reload and a contiguous exchange snapshot are required. Automatic REST fetch during capture remains pending.

## Explicit committed recovery handoff (0.3.6)

To resume one Kraken book from a validated mixed-source recovery plan already committed in a database:

```powershell
selective-options capture --duration 60 --output state/capture-recovered --resume-trades-from state/recovery/events.duckdb --recovery-marker MARKER_EVENT_ID
```

Replace the source and marker with the desired committed evidence. The output directory must be new. Without `--recovery-marker`, the existing WebSocket-tail resume path is used. A raw REST backfill archive alone is not a committed recovery plan; it must first be reconciled and published through the recovery journal. Marker creation is currently a Python API, not an automatic capture operation.

The marker deliberately selects its own through-ID; newer raw trades in the source database do not silently change that selection. One marker seeds one book, with other configured books starting fresh. Capture copies all referenced evidence with original receipt times and reloads the destination before creating producer tasks. A cancelled startup leaves committed evidence usable by a later explicit resume. The new report identifies the marker, selected-row count and pending snapshot symbols.

The next exchange snapshot must extend or overlap the selected sequence without gaps. An old marker may be too far behind the exchange's recent snapshot and will halt. This command does not fetch a new REST gap automatically. Recovery never relabels REST as WebSocket trades or reconstructs complete bars; initial intervals remain partial. No trading path is enabled.

## Offline gap-coordinator verification (0.3.7)

From the project directory:

```powershell
python scripts/verify_gap_recovery.py --checkpoint-database evidence/trade-recovery-01/before/events.duckdb --rest-database evidence/trade-backfill-01/events.duckdb --websocket-database evidence/trade-recovery-01/after/events.duckdb --pages-directory evidence/trade-backfill-01 --output state/gap-replay-001
```

This replaces HTTP transport with saved public pages and feeds saved WebSocket events with their original receipt timestamps. Parsing, pagination, sequence reconciliation, publication and writer-open reload run through the implementation. The new output contains a database, Parquet archive/manifest, replay report and `attempt` diagnostics. It makes no network calls and never activates a runtime collector.

The developer API `recover_trade_gap` requires a canonical anchor, inclusive REST target native ID, existing open `DuckStore`, a book-specific receive callback and a fresh diagnostics directory. An optional initial event tuple preserves the batch that triggered the gap. Calls are serialized per book by the future collector integration; the caller must suspend its normal processing and bar flush, bound its socket queues and handle other books. Receiving/fetching stops and joins before publication. Only a returned `CommittedGap` has passed both commit and reload; a marker's existence alone is insufficient for activation.

Inspect failed attempt reports by stage. `NOT_ATTEMPTED` means publication was not reached. `UNKNOWN_UNTIL_RETURN` means an interrupted write may have committed; inspect the database rather than assuming rollback. `COMMITTED` plus a failed reload leaves the recorded marker available for diagnosis and explicit restart. No failure should advance a collector cursor, silently replay a partially verified range or turn incomplete bars into complete ones. Automatic capture integration remains disabled.

## Explicit runtime recovery verification (0.3.8)

```powershell
python scripts/verify_runtime_recovery.py --database evidence/gap-coordinator-01/events.duckdb --marker d8ffa42be37082ada157ebce0681a27f9b4457ded41c56e12482bff23b301c1c --output state/runtime-replay-001
```

This offline replay intentionally omits a saved earlier trade to trigger collector gap detection, confirms a durable anchor, installs the saved committed recovery through the normal consumer, and checks overlap deduplication and partial active bars. It makes no network calls. Redis is not configured, so its delivery entries remain pending.

Developer orchestration is `begin_gap_repair(collector, bus, store, symbol)`, the existing fetch/buffer coordinator, then `activate_gap_repair(collector, bus, store, anchor, marker_id)`. These APIs must use the actual collector's bus/writer, with a running consumer and exclusive socket routing for the suspended book. Ordinary updates and bar flushes for that book are blocked during repair; other books can continue. Automatic invocation from the socket loop remains unfinished.

A barrier timeout or cancellation never authorizes cursor advancement. The book remains suspended and the durable outbox retains undelivered records. Diagnose the writer/consumer failure or restart explicitly from validated evidence; do not manually clear suspension to bypass checks. Already delivered recovery rows may be delivered again on retry, so consumer event-ID deduplication remains required. Neither a successful barrier nor recovery activation establishes signal/trading eligibility.

## Opt-in automatic public recovery (0.3.9)

Set `ingestion.kraken_auto_recovery_enabled: true` in the selected YAML profile, or set environment override `SELECTIVE_INGESTION__KRAKEN_AUTO_RECOVERY_ENABLED=true`, then run a finite `capture` as usual. The default remains false. This enables bounded recovery of forward Kraken trade gaps only after subscription acknowledgements; it does not enable execution. Per-attempt anchor, raw HTTP pages and reports are written under the capture's `recovery` directory. Capture reports state whether automatic recovery was enabled.

For a deliberately fault-injected, finite public test:

```powershell
python scripts/verify_live_gap.py --duration 90 --output state/live-gap-test-001
```

This test intentionally omits one BTC trade from collector input, verifies that public REST recovers it and checks a subsequent live trade. Normal capture never injects omissions. The test stops on success, failure or its deadline and writes the intentionally omitted event separately for audit. It uses only public endpoints, cannot place orders and is not a paper run. A no-activity timeout is not a successful recovery test.

The 27 September test passed in `evidence/live-gap-02`; the preceding failed clock-availability run remains in `live-gap-01`. More multi-book/reconnect/soak evidence and remaining M2 feeds are still required. Frame/byte/trade overflow, invalid controls, offline markets and recovery transport failures halt. Do not weaken timestamp or sequence checks to force continuation.


## Explicit ETH gas startup recovery (0.3.18)

```powershell
python -m selective_options.cli capture --output state/recovered-eth-001 --duration 60 --resume-eth-gas-from state/prior-eth/events.duckdb
```

Requires `ingestion.eth_gas_enabled: true` and ETH among the configured assets. The prior database must contain committed ETH gas observations, not merely health failures. The current public-failure probe databases contain no gas anchor and will be rejected. Source history is read-only and bounded to 10,000 gas records; malformed/conflicting/over-budget history fails before the destination is created. A successful handoff copies and reloads the exact last observation before any collector starts. The capture report records `eth_gas_checkpoint_event_id` and `COMMITTED_AND_RELOADED`; otherwise it reports `NOT_REQUESTED`. Failure or cancellation during copy/reload prevents activation.

This does not make an old block fresh or backfill missing history. It prevents a process restart from forgetting its committed finalized-block guard. Fixture evidence is explicitly synthetic (`eth-checkpoint-fixture-01`), with no public-access claim. The network feed remains unavailable: Python rejects the configured hostname with code 62, and Windows Schannel independently reports a wrong principal. See `eth-tls-diagnostic-01.json`. No verification settings or certificates were changed; do not treat this as a missing-CA diagnosis.

## Ethereum gas source (0.3.17; public access unavailable on this host)

```powershell
python scripts/verify_eth_gas.py --output state/eth-gas-001
```

This finite probe has a 45-second overall bound and requests only chain identity and a finalized block. It saves validated RPC results and health, separately counts requests attempted/results accepted, and checks PIT/archive/deduplication only where observations exist. An unavailable source is explicitly reported; a health-only archive does not establish gas ingestion. `evidence/eth-gas-public-02` records a certificate-validation failure. Do not disable TLS verification or repeatedly retry without a reason to expect changed access. No user account or key is needed.

The collector is disabled by default. Once source access is properly resolved, finite capture can opt in with `ingestion.eth_gas_enabled: true`; ETH must also be configured as an asset. Polling defaults to 120 seconds and backs off to 600 after failures. Block freshness allows 1,800 seconds by default; health freshness uses twice the poll interval. Inspect both `ethereum_publicnode/ETH/on_chain` and `feed_health`. These are delayed finalized-block gas samples, not live transaction fee quotes. Use explicit startup recovery above to retain a prior committed guard; skipped-block ancestry is not verified. Trading remains disabled.

## Calendar quality replay (0.3.16)

```powershell
python scripts/verify_calendar_quality.py --source evidence/bea-calendar-public-02 --output state/calendar-quality-001.json
```

This is an offline replay: the schedule is saved public content, while health and failure events are explicitly simulated fixtures. It verifies pre-receipt exclusion, missing health, no future-failure leakage, rejection at the failure timestamp, and expiry of both health and schedule. It makes no network requests. Unit tests additionally cover missing/exhausted series, removal/recovery, conflicting observations, scope isolation and malformed clocks/payloads.

Finite capture now includes `calendar_quality`. `BLOCKED` includes the exact reasons. A fresh response alone is insufficient: both Personal Income and Outlays and GDP must have future dates, and matching-scope health must be recent and successful. `OBSERVED_SCOPE_AVAILABLE` is limited to those named series; it is not a trade gate or complete economic-calendar approval. `NOT_CONFIGURED` means BEA capture was disabled. Trading remains unavailable.

## BEA public calendar (0.3.15)

```powershell
python scripts/verify_bea_calendar.py --output state/bea-calendar-001
```

This finite, unauthenticated probe saves the raw JSON, receipt timestamp, complete calendar snapshot, sealed Parquet archive and verification report. It checks pre-receipt exclusion, availability at receipt, identical-event deduplication and exact archive equality. Use a new output directory for each run. The first preserved public attempt exposed a schema issue; the second passed with 136 unique dates in 30 series.

For normal finite capture, set `ingestion.bea_calendar_enabled: true` or `SELECTIVE_INGESTION__BEA_CALENDAR_ENABLED=true`. Polling defaults to 3,600 seconds with a minimum of 900, including after failures. Default is disabled. Inspect both `bea/GLOBAL/calendar` and `bea/GLOBAL/feed_health` freshness. Use `calendar_at` on point-in-time rows for the latest complete schedule; do not union older snapshots after date changes or removals. A successful fetch establishes observation, not the publisher's original publication time or full calendar coverage. The raw update label has no verified timezone. Actual values and consensus remain unavailable.

BLS ICS returned HTTP 403 (`evidence/bls-calendar-access-01.json`). Do not repeatedly retry or switch hosts to bypass that response. Other calendars remain independent unfinished work. No private API, order or paper run is enabled by calendar capture.

## Federal Reserve FOMC meeting dates (0.3.49)

Set `ingestion.fomc_calendar_enabled: true` or `SELECTIVE_INGESTION__FOMC_CALENDAR_ENABLED=true` only when the bounded public schedule poll is wanted. The fixed endpoint is the [official FOMC calendar](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm). Polling defaults to once per hour, with at least fifteen minutes between requests, no redirects, a 1 MiB response limit and default TLS verification. Each whole-page snapshot and separate health record become visible at their HTTP receipt. `fomc_calendar_quality` reports missing, stale, ambiguous or failed snapshots and checks for a later listed meeting date.

The source provides date-only schedule entries. One entry on the preserved page is explicitly a notation vote rather than a regular meeting; parsing labels it separately. The page does not provide exact policy announcement or press-conference times, unscheduled-meeting coverage, release values, expectations or a verified first-publication time. UTC civil-date comparison is conservative for date-only rows; it is not an event-risk or trade gate. One finite public response has 57 entries spanning 2021–2027. The current parser's offline replay identifies 56 regular meetings plus one notation vote and rechecks the original response hash, receipt visibility, stored database, Parquet and manifest under `evidence/fomc-calendar-public-01/verification.json` without another network request. These ignored binary snapshot files are pinned in the private `v0.3.49` release supplement listed in `GITHUB_ARTIFACTS.json`; the evidence-restore helper rehydrates them by their named archive. Use a new output directory for another public request; the source defaults disabled.

## Binance public snapshots (0.3.13)

```powershell
python scripts/verify_binance.py --output state/binance-public-001
```

The finite 90-second probe discovers BTC/ETH/SOL/PAXG USDT perpetuals, stores public response evidence, verifies both-timestamp availability and seals the archive. No credentials are used. A full success requires accepted records for all configured assets; consult health rows for partial/missing sources. Denials and rate limits never trigger alternate-host retries. The public run in `evidence/binance-public-01` passed with 12 market rows and four health records.

For finite normal capture, opt in with `ingestion.binance_enabled: true` or `SELECTIVE_INGESTION__BINANCE_ENABLED=true`. Polling defaults to 30 seconds (minimum ten); source failures back off ten minutes. Default remains false. Version 0.3.14 reads explicit current intervals/caps/floors from `fundingInfo` each cycle, before market snapshots. Inspect the report's `funding_metadata` field for interval, basis and receipt. A missing symbol is unknown and never inherits an eight-hour default or stale-cycle value. Metadata errors fail the cycle before market publication. The finite `binance-public-02` run verified all four explicit intervals and all market PIT/archive checks. OI unit/side normalization remains unresolved; do not aggregate these native OI values with another venue. Current funding metadata does not establish historical intervals. This feed enables no execution or paper trading.

## Bounded historical open interest (0.3.12)

```powershell
python scripts/verify_oi_history.py --asset BTC --hours 1 --interval 5min --page-size 200 --max-pages 10 --output state/oi-history-001
```

Use a new directory. Supported intervals are `5min`, `15min`, `30min`, `1h`, `4h`, `1d`. Hours: 1..720; page size: 1..200; pages: 1..20. Asset choices are BTC/ETH/SOL/PAXG, subject to discovered active USDT perpetual availability. The overall deadline is 90 seconds. Results include response envelopes, original receipts, exact database/Parquet storage and a report. `NO_OI_SAMPLES_RETURNED` is not availability success; page exhaustion fails without publishing partial events. Do not bypass access denials or assume broader coverage from one probe.

BTC has public historical OI evidence in `bybit-oi-history-01`; other assets/intervals have not been live-tested for history. The script shares archive checks with the funding verifier; both entry paths have offline saved-response regression tests. No orders or paper run are triggered.

## Bounded settled-funding history (0.3.11)

```powershell
python scripts/verify_funding_history.py --asset BTC --hours 48 --page-size 200 --max-pages 10 --output state/funding-history-001
```

Use a new directory. Asset choices: BTC/ETH/SOL/PAXG, subject to discovered availability. Hours: 1..720; page size: 1..200; max pages: 1..20. The script enforces a 90-second overall deadline. It saves public response envelopes, receipts, a database, sealed Parquet and report. Invalid data or page exhaustion fails before partial history is published. API/access failures retain explicit reason/code; transport errors retain their type. `NO_SETTLEMENTS_RETURNED` is not availability success.

BTC history has live public evidence; other assets have snapshot evidence but no live historical probe yet. Pagination completion does not prove an uninterrupted schedule, and downloaded history stays invisible to decisions before receipt. No orders or paper run are triggered. OKX discovery/funding/OI probes timed out on 27 September, with unknown cause; no alternate-host or authentication workaround was attempted.

## Public Bybit data verification (0.3.10)

Run one bounded public collection into a new directory:

```powershell
python scripts/verify_bybit.py --output state/bybit-public-001
```

This verifies configured BTC/ETH/SOL/PAXG USDT perpetual discovery and funding/OI, stores accepted public response envelopes and actual receipts, writes source-health records, checks PIT visibility and seals the exact Parquet archive. A 90-second overall deadline ends the probe. `PUBLIC_SNAPSHOTS_VERIFIED` requires all four assets; consult per-asset health for partial/unavailable results. HTTP denial, API rate limits and TLS failures remain explicit; do not bypass them. The saved envelopes omit unrelated top-level API fields but preserve the response result used for normalization.

For normal finite capture, opt in with `ingestion.bybit_enabled: true` or `SELECTIVE_INGESTION__BYBIT_ENABLED=true`. `ingestion.bybit_poll_interval_s` defaults to 30 (minimum 10); failures impose a 600-second cooldown. Default configuration keeps the source disabled. No credentials are needed. A successful probe neither arms orders nor starts paper trading. Remaining Binance/OKX coverage, historical funding/OI, liquidations and other M2 requirements remain pending.

## Public Binance settled-funding verification

From 0.3.25, inspect `funding_revision_view` in new finite-verifier reports. `REVISION_VIEW_VERIFIED` confirms canonical scoped as-of selection; `MISSING` or `BLOCKED` identifies absent, corrupt, ambiguous or over-budget observations. Use `funding_history_at` with explicit asset/symbol/window/as-of time to select one latest observed revision per native funding timestamp. Requested windows may extend beyond evaluation time; neither source nor receipt future rows are exposed. Exact replays deduplicate. Do not sum repeated raw downloads as extra settlements or infer historical intervals from current metadata. Summary integrity does not prove schedule completeness, finality, first publication, source authenticity or trading readiness. Offline archive/revision evidence and reproduction instructions are in `evidence/funding-view-01/NOTE.md`; its four changed observations are synthetic.

From 0.3.26, explicitly restart a finite archive using a closed source database and a fresh output directory:

```powershell
python scripts/verify_binance_funding_history.py --output state/funding-restart-001 --resume-from state/funding-001/events.duckdb --asset BTC --hours 48
```

Only eligible observations in the new requested window are copied; older/outside-window observations remain in the original archive. Every eligible revision and original receipt is retained. Seeds are read-only, canonically checked and limited to 10,000 observations. Invalid/missing/ambiguous/over-budget seeds fail before output creation or public requests. Fetch/combined-validation failure writes no seed prefix. Check `status`, `failure_phase`, actual `rows`, `seed_rows_copied`, and `seed_committed_reload_verified`; a failed reload retains committed data and reports failure. Cancellation does not imply rollback. This command stops after one bounded download and does not start recurring capture or trading. Public proof and independent saved verification are in `evidence/funding-resume-public-01/NOTE.md`.

From 0.3.27, supervised capture can poll recent funding history when `ingestion.binance_funding_history_enabled: true`. It remains disabled by default. The six `binance_funding_history_*` settings control enablement, hours (48 default, 1..720), page size (200, 1..1000), max pages (10, 1..20), per-asset deadline (90 seconds, 1..300), and pause after every cycle (900 seconds, minimum 600). Canonical publication/view validation also has a 10,000-observation bound. Truncated/invalid fetches emit failure health without publishing a market prefix. Venue/API/rate/transport refusals stop the cycle before the next asset; failures wait the full configured pause. Sink failures halt the producer instead of being relabelled as source outages. Cancellation during sink publication may leave rows without completed health; neither rollback nor atomic whole-poll commit is promised.

Inspect `funding_poll_health` together with `funding_revision_views` and separate health receipt freshness. A verified charge view can coexist with a newer failed poll. Every tied latest health receipt is shown, with explicit payload `ok`, reason/code, window and page termination. A fresh health receipt alone does not mean its poll succeeded. Historical intervals and complete schedules remain unknown; gaps older than the configured window are not repaired. One finite public BTC cycle and labelled saved-response later-poll/outage evidence are in `evidence/funding-poll-public-01/NOTE.md`. No recurring collector is left running by the verification scripts, and no seven-day soak or trading-readiness claim is made.

```powershell
.venv\Scripts\python scripts/verify_binance_funding_history.py --output state/binance-funding-check-001 --asset BTC --hours 48 --page-size 2 --max-pages 10
```

Use a fresh output directory. This finite archive check stages ascending public settlement pages, then writes database/response/receipt/sealed-Parquet evidence and verifies PIT/exact-poll replay/archive equality. `PUBLIC_SETTLEMENT_ARCHIVE_VERIFIED` requires nonempty accepted observations; empty histories and failures remain explicit, and no truncated plan is published. The history API shares a rate budget with funding metadata; use bounded probes without repeated outage retries. No credentials or account changes are needed.

Hours are 1..720 in this verifier; the collector itself bounds windows to 31 days, pages to 1..20 and page size to 1..1000. Default deadline is ninety seconds. Preserve exact millisecond source times and reported Regular/Special types; missing type/interval remains unknown. Distinct downloads retain receipts and must be reduced to observed revisions before aggregation; never sum repeated raw observations as extra charges. This does not start recurring history collection or paper trading. BTC public evidence is in `evidence/binance-funding-public-01`; run its `verify_saved.py` for independent read-only verification with no network calls.

## Public Binance global account-ratio verification

From 0.3.23, inspect capture `flow_quality` alongside revision integrity and generic freshness. `OBSERVED_WINDOW_AVAILABLE` requires three consecutive aligned five-minute periods, recent source/receipt/health clocks and health after the window's latest revision receipt. `BLOCKED` includes explicit gap/staleness/corruption/health/budget reasons. A discovery outage under `GLOBAL` blocks each asset; later BTC health does not recover ETH. Capture age limits are twice each feed's configured poll interval; pure assessments support explicit independent limits and window size. A verified recent window does not establish continuous history, field/unit semantics or trading eligibility. Saved-public and simulated outage proof is documented in `evidence/flow-quality-01/NOTE.md`.

From 0.3.22, new flow polls retain their receipt in identity, including unchanged values and returns to earlier values. Replay of the exact same poll remains deduplicated. For either enabled flow source, inspect capture `flow_revision_views`: `REVISION_VIEW_VERIFIED` confirms canonical, scoped as-of selection, while `MISSING` or `BLOCKED` identifies absent/invalid/ambiguous/over-budget evidence. This status does not replace freshness, source health, unit/semantic checks or continuous-history requirements. Use `flow_at` to select one latest observed revision per period; never sum raw repeated polls. Older archives cannot reconstruct discarded receipts. Offline proof and reproduction instructions are in `evidence/flow-view-01/NOTE.md`; its changed receipts are synthetic and involve no public requests.

```powershell
.venv\Scripts\python scripts/verify_binance_accounts.py --output state/binance-accounts-check-001
```

Use a fresh directory. This finite ninety-second probe requests public discovery and at most three five-minute global account-ratio samples per configured asset, saves responses/receipts and sealed database/Parquet evidence, and checks PIT/repeats/archive equality. `PUBLIC_GLOBAL_ACCOUNT_RATIOS_VERIFIED` requires all assets and verifies captured data integrity without resolving documentation inconsistencies.

Enable `ingestion.binance_account_ratio_enabled` (default false) for finite capture. `binance_account_ratio_poll_interval_s` defaults to 300 seconds with that minimum; failures wait at least 600 seconds. Source and health freshness requirements remain separate. Preserve the named fields and period-end timestamp. Do not silently repair the official example, treat this as a size-weighted position ratio, or substitute a key-required top-trader endpoint.

Successful evidence: `evidence/binance-accounts-public-01`. Run its `verify_saved.py` for independent read-only verification with no network calls. No account changes or credentials are required.

## Public Binance taker-flow verification

```powershell
.venv\Scripts\python scripts/verify_binance_taker.py --output state/binance-taker-check-001
```

Use a new output directory. This finite probe requests public discovery and up to three five-minute taker aggregates for each configured asset, with a ninety-second overall limit. It saves responses/receipts, health, database, sealed Parquet and report. `PUBLIC_TAKER_AGGREGATES_VERIFIED` requires all configured assets; a health-only archive is not usable flow evidence. The first failed diagnostic and the corrected successful four-asset run are preserved in `evidence/binance-taker-public-01` and `02`.

For finite capture, enable `ingestion.binance_taker_enabled` (default false). Its independent `binance_taker_poll_interval_s` defaults to 300 seconds and cannot be lower; failures wait at least 600 seconds. Source/health freshness requirements apply separately. Samples whose period has not ended are withheld; reported native volume units remain unverified. Do not treat these values as normalized volume, continuous history, final observations, or strategy approval. No credentials are required and no trading is enabled.

Replay the independent public evidence check with `python evidence/binance-taker-public-02/verify_saved.py`; it makes no network requests and uses the saved database read-only.

## Public SOL network verification (0.3.28)

Run one finite check in a fresh output directory:

```powershell
.venv\Scripts\python scripts/verify_solana_network.py --output state/solana-network-01
```

The verifier never starts a scheduler. Inspect `report.json`, `results.json`, the database and sealed archive. A successful report verifies one current performance/vote-stake pair, both clock boundaries, replay, pending outbox and closed reload. A failed public fetch records `SOURCE_UNAVAILABLE` with only failure health; persisted health is not public market data. Existing output directories are refused.

Supervised capture polls only when `ingestion.solana_network_enabled: true` and SOL is configured. Enablement defaults false. The other settings are poll interval 300 seconds (minimum 120), complete-poll deadline 90 seconds (1..300), and performance age on fetch 180 seconds (60..3600). Each HTTP body is limited to 4 MB, each request to 20 seconds, and vote records to 10,000 total. A failed cycle pauses 600 seconds without immediate retries. Fixed read-only methods use the official endpoint with normal TLS verification and no redirects.

Inspect committed `solana_network_quality` with generic freshness. `NETWORK_SAMPLE_VERIFIED` requires canonical referenced snapshots and health, all four response receipts within twice the poll interval, and performance source age within fetch age plus twice the poll interval. Receipt freshness alone never overrides newer failed health. Missing or interrupted publication, conflicts, corruption, ties and excess row budgets block quality. Health references sink acceptance, not an atomic whole-poll transaction; storage errors propagate and cancellation may leave a prefix. Historical continuity, restart/rotation and long-running public operation remain unverified.

The public check and separate saved-response/synthetic-outage replay are described in `evidence/solana-network-public-01/README.md`. The saved verifier refuses existing fixture outputs; reproduce in a fresh sibling folder. No accounts or keys are needed. This source observes visible vote-account activated stake and recent node performance, with no DEX-volume, active fee-payer, all-stake-account or stake-flow inference.

## Scheduled streamed-universe rediscovery (0.3.29)

`ingestion.stream_universe_refresh_interval_s` is null by default. Set a finite value from 60 to 86400 seconds to close and rediscover the streamed universe on a monotonic schedule after each session completes its acknowledgements. It works with the existing streaming cap; null `max_stream_instruments_per_asset` requests every discovered option, whose public capacity is still unverified. The separate `chain_poll_interval_s` refreshes summaries without changing subscription membership.

A scheduled rotation records `STREAM_REFRESH_DUE`, closes the old socket, discovers the current universe and subscribes with a fresh registry and existing credit-limited batches. It intentionally creates an unverified gap. Option trade history must still be reconciled before any continuous-history claim. Invalid discovery/acknowledgements halt; ordinary transport failures retain the existing reconnect/backoff policy. Busy updates cannot defer the deadline. Windows timer rounding is handled by rechecking the monotonic deadline.

Inspect `deribit_connections`, `deribit_universe_refreshes`, `stream_universe_refresh_interval_s`, lifecycle status records and `option_instrument_freshness`. `STREAM_INACTIVE` withholds a cached quote during disconnect, refresh or resynchronization. A new session clears previous ticker/book cache entries; pre-ack interleaved observations can satisfy freshness if their receipts belong to the new session. Acknowledged membership replaces retired symbols. These fields describe the last committed lifecycle state during capture; finite capture reports do not imply a currently running socket or liquidity/trading approval.

One bounded public check requires a fresh directory and stops after 110 seconds:

```powershell
.venv\Scripts\python scripts/verify_universe_refresh.py --output state/universe-refresh-01
```

The check explicitly caps BTC at two options. A report with `PUBLIC_CAPPED_REFRESH_VERIFIED` confirms at least two acknowledged sessions and one refresh, not actual listing/expiry changes, full-universe capacity or gap-free trade history. See `evidence/universe-refresh-public-01/README.md`; saved SQL/PIT/archive replay requires no network access.

## Exact option coverage and uncapped diagnostics (0.3.30)

`option_stream_coverage` follows the exact manifest IDs recorded by `STREAM_SUBSCRIBED`, including metadata IDs and selected membership. `ingestion.option_universe_max_age_s` defaults to 3600 seconds (finite 60..86400), independently of `data.max_staleness_s`. An old discovery cannot remain valid merely because some quotes are busy. These are bounded diagnostics at a saved as-of time, not current running-state or trading approval.

`SELECTED_FEEDS_OBSERVED` means each selected ticker/book leg is canonical and fresh within both clock bounds. Inspect `full_discovered_membership` separately: capped sets can have fresh selected legs while lacking whole-universe coverage. Missing/stale legs produce `INCOMPLETE_LEG_OBSERVATIONS`; corruption, ambiguity, inactive/expired universes, missing metadata or overflow produce `BLOCKED` with an explicit blocker. Options-trade continuity, all native option fields, liquidity and sustained capacity are never inferred. Older archives without the new binding remain blocked; do not fabricate bindings.

`scripts/verify_option_coverage.py --output <fresh-directory>` performs one finite uncapped BTC testnet attempt (75 seconds, 100 channels per batch). Existing paths are refused. It reloads the closed database and matches the capture's saved coverage. `UNCAPPED_SUBSCRIPTIONS_OBSERVED` describes whole discovered membership acknowledged within the sample; inspect leg diagnostics independently. `UNCAPPED_COVERAGE_NOT_VERIFIED` is an unresolved result. Cancellation propagates and post-commit capture failure retains its failure type. The probe never places orders.

The preserved attempt in `evidence/option-coverage-public-01` timed out before aggregate acknowledgement: 1,010 options selected, only 633 symbols with any ticker/book observations. Exact timeout cause and batch were not established. Shutdown/archive success does not make that a healthy stream. The saved replay and independent proof perform no network calls and preserve original bytes. Do not repeat the unchanged probe to show activity. Next add bounded per-batch progress and isolate timeout versus ingestion backpressure before another materially changed attempt.

## Subscription batch progress and timeout diagnosis (0.3.31)

`STREAM_SUBSCRIPTION_BATCH_ACK` records each exact validated response: `connection`, `request_id`, `batch_channels`, `acknowledged_batches`, `acknowledged_channels` and `total_channels`. `notification_frames` counts normalized incoming notification envelopes; `published_stream_events` counts successful sink calls. Both counters are cumulative over this collector instance, including prior connections and exact event replays. They are not unique committed-row counts. Batch markers never activate freshness or complete coverage; every required batch must validate before `STREAM_SUBSCRIBED`.

`STREAM_DISCONNECTED` now retains `phase`, `receive_phase`, pending `request_id`/`requested_channels`, completed prefix and total counts. `SOCKET_RECEIVE` indicates the operation pending when the existing deadline/transport exception occurred. `SINK_PUBLISH` indicates a pending sink call. Neither establishes the complete causal history of the failure. Sink saturation still fails loudly under the existing queue policy; no deadline or execution gate is relaxed.

The materially changed finite attempt in `evidence/subscription-progress-public-01` acknowledged nineteen 100-channel batches (1,900/3,031 channels), then request 1901 timed out while awaiting the socket within the response deadline. No aggregate acknowledgement was emitted. The capture retained 21,351 rows and shut down normally; the stopped coverage remains blocked. All original bytes and the prior attempt are preserved. Do not repeat unchanged attempts. Next measure socket response time, normalization and sink/queue waits separately before making a transport change. No cause or venue capacity limit has yet been established.

## Exclusive receive timing (0.3.32)

Batch progress/disconnect records expose `receive_timing_ns` with fixed `SOCKET_RECEIVE`, `NORMALIZE` and `SINK_PUBLISH` counters, cumulative across the collector instance. `response_timing_ns` is the exclusive delta for the current response interval, and `response_elapsed_ns` bounds its total wall time. Values use a monotonic nanosecond clock, including time before timeout/cancellation via `finally`. Non-response phases have null response fields. Limiter wait, send, progress publication and socket close are outside this interval; unmeasured overhead is not attributed to a phase. Socket and sink waits include scheduling delay; these are not pure wire/network or CPU-cycle measurements. Detached status snapshots do not change when counters advance.

The preserved finite sample in `evidence/receive-timing-public-01` ended with nineteen completed batches (1,900/3,031 channels). Its recorded prefix spent 52.8569442 seconds normalizing, 14.3332738 awaiting the socket and 0.6570864 publishing. The last completed response took 10.987262 seconds, of which 9.1504206 seconds were normalization. There is no new timeout marker, no full acknowledgement and no timing snapshot for the interrupted request at finite stop. Do not infer them. All source and receipt times remain unchanged.

Use the saved native frames to benchmark repeated validator construction and verify exact identity/payload/receipt equality before optimizing. Preserve the current integrity checks, fixed bounds and deadlines. Run another public attempt only after a materially changed collector needs actual verification. This sample does not establish earlier timeout cause, sustained capacity, every option field or continuous trade history.

## Compiled validators and finite complete BTC subscriptions (0.3.33)

The collectors and `MarketEvent.payload()` compile their JSON-object schema once per module. Every input still passes that validator. Each payload decode returns fresh nested values; this is not an event/value cache. Exact decimal strings, unknown native fields, identity, source/receipt clocks, book/sequence guards and all existing bounds/deadlines/credits remain unchanged. Concurrent payload reads and invalid-then-valid calls are regression-tested. No configuration change is required.

`evidence/reused-validation-01` compares every one of 23,519 reconstructed saved-native envelopes against both the hash-verified immutable 0.3.32 wheel and current code, with original receipts. Event fields and payload JSON match exactly. Local full replay took 59.09 versus 4.31 seconds; three alternating 250-row medians were 607.62 versus 42.51 ms. These are machine-specific measurements, not original wire order, duplicate reconstruction, a speed test threshold or a whole deployed pipeline guarantee. The saved verifier needs the preserved baseline wheel in `work/dist` and refuses to overwrite its report.

The changed finite 75-second BTC test in `evidence/reused-validation-public-01` acknowledges all 3,031 channels for 1,010 options in 31 batches, retaining 62,089 rows without disconnect. `UNCAPPED_SUBSCRIPTIONS_OBSERVED` confirms the selected discovered membership was acknowledged. It does not imply every leg is fresh. At the saved final as-of time 1,010 ticker and 936 book legs are fresh, while 74 book legs remain stale under independent source/receipt age limits of 30 seconds. No age or timestamp is relaxed; no order is allowed. The finite report describes committed state during the stopped capture, not a currently running socket.

Independent SQL checks verify complete-ack visibility and every latest leg identity/age, plus exact metadata/timing bindings, sealed archive, duplicate replay and closed reload without new network requests. Source bytes and prior attempts remain preserved. Verify other assets and stale-leg timing semantics next, and design bounded longer-running views before a sustained deployment. No current key/account action is required for public data work.

## Finite requested-asset subscription verification (0.3.34)

Run `python scripts/verify_option_coverage.py --output <fresh-directory> --assets ETH SOL PAXG --duration-s 75` for one finite public testnet check. Assets may be BTC/ETH/SOL/PAXG, without duplicates; default BTC preserves existing single-asset fields. Duration is bounded 40..180 seconds and no private/order path exists. Selection remains uncapped, batches are 100 channels and existing deadlines/credits apply. Output must be fresh. Cancellation does not become verification success. Each asset's closed database view must match saved diagnostics in exact order; empty chains remain blocked and aggregate success requires every requested asset to have nonempty acknowledged membership. Even that success does not imply fresh legs, history or all feeds.

`evidence/multiasset-options-public-01` is one stopped 75-second ETH/SOL/PAXG capture with all 2,840 ETH/SOL channels acknowledged, 47,756 rows and no disconnect. PAXG has no selected options; no synthetic route is enabled. Independent proof and age audit verify saved native metadata/acknowledgements, latest identities, both clocks, replay/outbox/archive and unchanged bytes without new requests. It has 158 stale ETH books and 142 stale SOL books; both age bounds fail for each. Source-minus-receipt disagreement is retained, not treated as a network latency measurement. The probe-time `verifier_used.py` preserves the original report flag spelling; the released verifier spells that flag `every_requested_asset_has_nonempty_acknowledged_universe`. Saved reports are unchanged.

Separate BTC and ETH/SOL observations do not establish one simultaneous four-asset run or seven-day operation. Do not raise freshness bounds or rewrite timestamps to pass. Long-running retained views need bounded latest-row projection that preserves all tied conflicts and manifest metadata identities before sustained deployment. Specialist feeds, service deployment and recovery remain tracked in `COMPLETION_CHECKLIST.md`.

## Bounded option assessment of retained history (0.3.35)

`DuckStore.option_coverage(asset, as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows)` is now the capture report path. It checks policy before worker access, holds the serialized store lock through a drained thread, validates metadata/control context with the existing canonical validator, then selects current-symbol latest source/receipt pairs after resync. Both clocks constrain SQL eligibility before ranking; `dense_rank` retains all tied latest rows so conflicting evidence blocks. A bool resync connection cannot match a strict integer session. Each query's fetch limit shares the same application budget, with one overflow sentinel. No historical rows or outbox entries are deleted and stale ages/timestamps remain unchanged.

This bounds rows materialized into Python, not DuckDB query scan/time, engine memory or database size. Every scoped metadata/control row is retained, so that history can still exhaust the budget. The standalone in-memory `coverage_at` and finite verifier's full-row oracle retain their independent bounds. Do not describe this increment as complete long-running or sustained deployment. Metadata-generation selection, control audits and large ID-query costs remain next tasks.

`evidence/option-projection-01` replays 109,845 native rows across two preserved captures and exactly matches BTC/ETH/SOL/PAXG saved views and 20 control/PIT boundaries, including existing staleness and absent gold. A clearly synthetic 110,000-row old-quote addition makes the full-row oracle exceed 100,000 while the new BTC view still fetches 3,033 rows. All 172,089 rows/outbox entries remain retained. Native subset bytes, both baseline archives, fixture-inclusive archive, closed reload and source hashes match. Proofs make zero requests. Complete derived evidence is in `option-projection-replay-02`; the interrupted earlier local bulk-exact harness is preserved in `work/option-projection-attempt-01`, outside release artifacts. No key/account action is needed.

## Current metadata, scalar JSON lookup and native field audits (0.3.36)

The store option projection now audits eligible controls, fetches exact metadata IDs from the fully acknowledged manifest, validates their native bindings, then fetches selected-symbol latest source/receipt quote ties. Both clocks constrain eligibility; missing/corrupt/future/unselected required metadata cannot be dropped. All three queries share the application row budget plus one overflow sentinel. Retired metadata stays stored/outboxed/archived. Every scoped eligible control stays audited; its history can exhaust the budget. This does not bound DuckDB scan/time/memory or database growth.

Exact insert conflict checks and read-events use a single parameterized JSON string through `json_each` and a semi join. Metadata and symbols use scalar JSON set predicates. All native row comparisons, transaction rollback, outbox/delivery semantics and the 20,001-ID recovery read limit remain intact. Do not interpolate IDs or symbols into SQL. Delivery acknowledgement retains its previous query. Finite profiling deadlines/timings are diagnostic observations, not performance pass thresholds or optimizer root-cause conclusions.

`python scripts/audit_option_native_fields.py --capture CAPTURE --assets BTC --output FRESH_DIRECTORY` performs a read-only finite audit of an existing capture. It requires exact saved coverage and unchanged original hashes and refuses existing outputs. Use the separately preserved multiasset capture with `--assets ETH SOL PAXG`. It preserves raw native field values and absence/null/invalid distinctions, never substitutes summaries for tickers, and combines field validity with leg freshness and whole membership. Empty gold and stale books never pass. Native volume units/windows and M3 features are not inferred. Its standalone projection retains all metadata from the finite capture and has a separate explicit application budget.

Saved proofs: `metadata-projection-01/verify_saved.py` uses fresh derived stores and a verified previous wheel; `id-query-profile-01/verify_saved.py` rechecks 12 native scalar JSON ID/symbol sets without timing thresholds. Both refuse existing final reports. All proofs make zero network requests. See each evidence README before replay; prior reports remain immutable. No credentials or account changes are needed for these offline checks.

## Explicit ETH provider and bounded policy RSS assessment (0.3.37)

Use `ingestion.eth_gas_provider` to explicitly choose publicnode, cloudflare or drpc when ETH gas is opted in; default stays publicnode and eth_gas_enabled stays false. Provider mapping is fixed; no URL interpolation or automatic fallback. Capture uses that provider for data, health/freshness requirements and provider-scoped checkpoint preflight/copy/reload. `load_eth_checkpoint(path, now, max_rows, provider=...)` rejects wrong-provider/invalid policy/history before collectors start; source and raw receipts are preserved. `scripts/verify_eth_gas.py --provider drpc --output FRESH_DIRECTORY` performs at most four read-only calls with a 45-second network deadline. Finality is provider asserted; numeric/fee references from the same node do not establish independent consensus. Live collector methods remain chain ID and finalized block reads only. The optional verification hook records raw responses, including actual receipt, before validation.

New RSS health references exact accepted news IDs, response receipt and health receipt. Later repeated polls can reference retained first observations; publication is never backdated or independently verified. `DuckStore.policy_news_quality(source, as_of_ms, max_poll_age_ms, max_publication_age_ms, max_rows)` validates policy first, drains a bounded scoped SQL worker under the store lock and applies the canonical validator. It retains full eligible scoped RSS history plus one overflow sentinel; engine cost/time/size is not bounded. Capture exposes one diagnostic per configured policy source. The publication window is descriptive (`policy_news_max_publication_age_s`, default 30 days), separate from poll freshness and trading readiness. Empty poll, stale publication and failed/unbound poll are distinct. A fresh poll never certifies comprehensive news or M2 completion.

`scripts/audit_option_freshness.py` reads preserved captures with both-clock/session and finite row/group/file/deadline budgets. It exposes native timestamp provenance, source/receipt age and signed differences without rewriting data. The final parent overflow repair/55-case evidence is in option-freshness-01/parent-validation.json; historical 54-case worker snapshots are retained. Replays require fresh output directories/reports. Temporary agent work was preserved under workspace work/, outside release. No key/account changes are needed for these public/offline checks.

## Bound gas quality and finite restart proof — 0.3.38

Capture reports `eth_gas_quality` for enabled ETH gas only. Its exact bound poll state is separate from generic age diagnostics. Failed/missing/ambiguous/corrupt/stale or legacy-unbound evidence stays blocked; choose a fixed provider explicitly and retain original receipts. `POLL_OBSERVED_VERIFIED` certifies one eligible bound gas poll only. It permits no trading and proves no continuous history or independent finality.

The verifier `python -m scripts.verify_eth_runtime_restart --provider drpc --output NEW_DIRECTORY` makes at most four read-only RPC calls across two actual polls with closed anchor copy/reload. Output must be new; no retries, fallback or TLS overrides occur. Existing evidence is immutable. `python -m scripts.verify_news_restart --output NEW_DIRECTORY` is offline and uses saved Fed evidence with clearly labelled synthetic repeat/failure/recovery clocks. It is not a public outage test.

Generic `UNAVAILABLE_AT_AS_OF` means the latest evidence has a source or receipt clock after evaluation; wait for actual time or query the canonical store PIT view. `AMBIGUOUS_LATEST` means distinct observations tie at the latest clocks; inspect the retained evidence and wait for an unambiguous later poll. Neither diagnostic constructs an older fallback or changes timestamps/thresholds. Excessive source clock skew remains `CLOCK_SKEW`.

## Optional supply, real-yield and liquidation inputs (0.3.39)

The default configuration keeps stablecoin_supply_enabled, tips_yield_enabled and liquidations_enabled false. A separately configured finite capture can enable them under ingestion. Ethereum uses fixed public read-only dRPC contract calls at one numeric provider-finalized block; FRED uses bounded DFII10 CSV windows; liquidation snapshots use one public routed Binance market socket. No account/API key is needed for these public inputs. Canonical quality reports appear in capture output only when enabled and never advance readiness.

Retain failed health evidence. The saved FRED attempt timed out before any HTTP entity/status; do not reinterpret that as current yields or repeatedly retry this known failure. The saved liquidation session received a subscription ACK but no native snapshots, so its four assets are UNOBSERVED. Stopping writes disconnection health and blocks final quality. A healthy ACK cannot establish a complete liquidation ledger or native-event availability. The recorded historical active view looks back one millisecond from the actual stop request to exclude tied cancellation clocks, without modifying any event timestamp.

Review evidence/stablecoin-supply-public-01, evidence/tips-yield-public-01, evidence/liquidations-public-01 and evidence/new-feed-parent-validation-0339.json for exact request/receipt/outbox/archive/source checks. Each finite verifier refuses an existing evidence output; supplied verifier snapshots are evidence, not overwrite commands. Ingestion, specialist sources, whole-source recovery/deployment and the real seven-day run remain incomplete. Production execution stays disabled.

## Optional crypto-news metadata and supply checkpoint review (0.3.40)

crypto_news_enabled defaults false; its poll interval is at least 300 seconds. The endpoint and byte/item budgets are fixed. A failed request persists failure health. The saved finite attempt returned HTTP 308 and did not follow the redirect or parse RSS; no native headline was accepted. Review evidence/crypto-news-public-01/report.json and post-probe-provenance.json. The historical report/snapshot is preserved, with a subsequent label-only correction recorded separately. This failure does not establish current publisher unavailability or a redirect target. Broad news, sentiment and classification remain unavailable.

For explicit application integration, await load_stablecoin_checkpoint(database, as_of_ms) and pass the validated result as initial_snapshot to StablecoinSupplyCollector. A current failed/stale/corrupt/ambiguous or over-budget history refuses. Requesting a historical cutoff can return a historical anchor; it is not current health approval. The constructor starts no transport and publishes no seed. This API does not automatically restore the capture command. Review evidence/stablecoin-restart-01 for exact saved-native replay plus clearly synthetic outage/recovery scenarios, and news-supply-parent-validation-0340.json for independent read-only checks. Do not overwrite immutable evidence or reinterpret fixtures as a live provider recovery. Production execution remains disabled and M2 remains PARTIAL.

## News migration and explicit supply capture resume (0.3.41)

The fixed CoinDesk URL is https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml. crypto_news_enabled remains false by default. The saved .41 poll verifies 25 actual metadata items and storage; it does not start continuous collection. Review news-endpoint-discovery-01 and crypto-news-public-02. Earlier HTTP 308 evidence remains historical; a documented fixed URL change resolves that blocker, while transport still refuses redirects. news-provenance-0341 distinguishes native capture code from the final matching-URL binding guard.

Enable stablecoin_supply_enabled in a separately configured finite capture before using capture --output NEW_DIRECTORY --resume-stablecoin-from SOURCE/events.duckdb. Preflight applies configured poll/block freshness at the current clock and refuses unhealthy history before new output. Bounded exact-source eligible history is audited; latest failure cannot hide behind an older success. The exact snapshot/health pair commits together, reopens/revalidates and seeds its collector before collection. Existing output is refused and source reads are read-only. This imports a minimal anchor, not full history or delivery acknowledgements; both copied rows remain pending local delivery and original receipts stay unchanged. No historical cutoff override is exposed through this command.

stablecoin-capture-resume-01 exercises actual capture offline using saved native entities and clearly synthetic later receipts/failure. No actual provider outage/recovery or continuous run is claimed. news-resume-final-validation-0341.json independently checks native metadata, immutable failed news quality, copied pairs, archives/outbox and failed resume refusal. M2 remains PARTIAL and production execution disabled.

## Offline immutable rotation and complete-cohort restore (0.3.42)

Use a closed source database and existing output parent. rotate-archive --database SOURCE/events.duckdb --output NEW_ROTATION --as-of-ms EXPLICIT_CUTOFF exports only records whose source and receipt clocks both satisfy the cutoff. Optional --partition-ms defaults to 86400000 (one UTC day), --max-partitions to 366 (accepted range 1..1000), and --batch-size to configured storage_batch_size (accepted range 1..10000). Cutoff and bucket duration are strict integers; duration is 1..86400000 milliseconds. Paths must be explicit local paths without .., glob, symlink or junction components. No active writer is stopped, source event deleted or ACK changed. Publication is the complete catalog.json written last. Failed prepublication work retains inspectable files/journal and must not be treated as sealed; use a fresh output directory on a new attempt.

restore-rotation --catalog ROTATION/catalog.json --output NEW_RESTORED [--batch-size N] verifies all listed parts and restores the exact finite cohort. The configured source-clock skew policy remains enforced. Output must be new, with an existing parent, and outside the immutable rotation directory. The final events.duckdb and restore.json appear only after validation and closure; failure/cancellation leaves staging and restore-failed.json, with late publication revoked. Do not consume incomplete or failed destinations. Every successful restored row is pending local delivery; event_id consumer deduplication is required. Original ACKs are not reconstructed. Already published export catalogs remain immutable if caller cancellation arrives afterward; inspect catalog validation to determine publication status.

rotation-saved-public-01 is an offline proof, using 28 unchanged native news/supply records and eleven labelled fixtures. It establishes two-part exact restore, both-clock cutoff/partition budgeting, pending outbox and deliberate corruption refusal. It proves no actual provider outage, new native feed availability, retention deletion, active continuous collection, deployed Redis reconciliation or seven-day validation. rotation-parent-validation-0342.json independently audits native bodies, source/cohort/parts/restored identities, clocks, delivery and quality without new network calls. M2 stays PARTIAL and production execution disabled.

## Opt-in periodic archive checkpoints during finite capture (0.3.43)

Set ingestion.archive_rotation_interval_s to numeric seconds from 1 through 86400 in a separately reviewed capture configuration; null is the default and creates no archive task or periodic files. Boolean values are refused. archive_rotation_partition_ms defaults to 86400000 (strict integer 1..86400000), archive_rotation_max_partitions defaults to366 (strict1..1000), and archive_rotation_max_snapshots defaults to10 (strict1..1000). storage_batch_size controls archive application batches as before. Use the ordinary finite capture command; these settings do not enable orders or change venue/profile.

Each archive-rotations/snapshot-000000 directory contains the closed events-only snapshot.duckdb, active-rotation.json, and rotation/catalog.json plus fixed sealed parts. Every checkpoint follows a FIFO commit barrier and records its cutoff/scope. Source events and ACKs remain in the original active database; only the committed both-clock cohort enters the checkpoint. Later commits can have earlier timestamps, so do not interpret an earlier checkpoint as a complete source frontier. Source lock releases before part validation and subsequent source writes continue. A checkpoint failure or the next due tick beyond the snapshot cap fails capture loudly; inspect the final capture report archive_rotation section, last_attempt and artifact failure journals. Snapshot copying/SQL/validation can be slow and can trigger existing queue backpressure; this release does not certify sustained throughput.

Restore a finished checkpoint using restore-rotation --catalog CAPTURE/archive-rotations/snapshot-000000/rotation/catalog.json --output NEW_RESTORED. Require active-rotation.json status ACTIVE_ROTATION_VERIFIED and matching snapshot/catalog hashes in addition to the standard all-part restore checks. Failed/cancelled active attempts retain files but have no successful active report; late active publication is revoked before cancellation propagates. Every restored row is requeued; deduplicate by event_id. Normal capture shutdown cancels/drains in-progress archive work before writer/store closure and can honestly retain a last CANCELLED attempt alongside completed checkpoints. Full final capture events.parquet remains separately sealed.

active-rotation-saved-public-01 executes actual production capture/consumer/archive/restore code with mocked collectors and explicit synthetic scheduling ticks. It preserves28 native records, verifies two cumulative snapshots28/31, commits32 final source records and proves later commits during first partition validation. One source ACK is an offline separation fixture, not Redis delivery. The independent parent native/PIT/quality/source checks use zero new public requests. This is neither a continuous provider test nor seven-day validation. M2 remains PARTIAL and production execution disabled.

## Kraken input validation and sink failure (0.3.44)

An invalid trade later in an input batch or invalid later-symbol bar flush now refuses the entire emission and preserves the preceding collector frontier. Historical snapshot rows below a closed-bar watermark remain raw-only; suspended books do not flush. No new configuration or account action is needed.

SPOT_EMISSION_FAILED identifies an uncertain sink outcome; an earlier valid prefix may already exist in the queue/store. Capture records FAILED, drains and seals retained events, then raises the original failure with its chained cause. A halt-status publication failure preserves the data failure. Do not reuse that collector instance: trade/flush/run calls refuse SPOT_EMISSION_OUTCOME_UNKNOWN, including before reconnect. Inspect the closed FAILED capture and use the existing explicit committed sequence resume workflow in a fresh capture; prior bar accumulators are still not restored and new bars begin partial. Deduplicate durable events by immutable event_id; do not assume a complete batch transaction or automatically retry a possibly delivered batch.

spot-batch-saved-public-01 uses canonical native trade records in explicitly reconstructed message containers, synthetic failure/continuation fixtures and zero requests. spot-sink-failure-fixture-0344-02 injects a synthetic second-trade queue error through actual capture supervision, proves one connection and an archived valid prefix, and is not deployed Redis/service validation. Full coverage and seven-day acceptance remain pending.

## Offline features, quote simulations and interrupted trials (0.3.45)

See RESEARCH.md for both commands, formulas, assumptions and the losing fixture. Research commands require only saved local inputs and a new output directory; do not run them against an active store. Retain a shared explicit trial journal across research runs and count every started, failed and unresolved attempt. Preserve partial journal lines on failure; malformed retained history refuses. A stale lock needs inspection, not automatic deletion. A simulation may have completed in the journal before final report publication fails; absence of a report does not erase that trial.

The bar-state codec is an API primitive, not capture resume. Capture still requires its existing explicit committed sequence workflow and starts interrupted bar coverage partial. Final proof clocks and feature bars are labelled synthetic; original native trade fields/receipts remain exact. Three actual registered integration fixture runs are retained in parallel-foundations-trials-0345; historical strategy/paper counts remain zero. Independent final proof/audit and full source regression evidence are linked in VALIDATION_REPORT.md.

## Explicit stopped-bar checkpoint and recovery (0.3.46)

The disabled default is `ingestion.bar_checkpoint_on_stop: false`. Opt in with `capture --save-bars-on-stop` or override configured enablement with `--no-save-bars-on-stop`. A successful finite capture writes `bar-checkpoint/checkpoint.json` only after producers/rotation and the writer drain. No trades, unresolved snapshots/repair, a poisoned sink outcome, future/uncommitted membership, unsupported native tail or bounded ledger/artifact overflow refuse a successful checkpoint and retain a failed capture/archive for inspection. Do not reinterpret incomplete staging or `checkpoint-incomplete.json` as success.

Start a new capture directory with `capture --resume-bars-from <prior-capture>/bar-checkpoint`. This is exclusive with `--resume-trades-from`/`--recovery-marker`. Config/assets/timeframes/lateness and fixed seals are checked before new output; exact native rows are copied pending, closed/revalidated and installed before producers. All retained buckets are partial across the actual restart, and an actual recovery snapshot is required before updates. First receipts/watermark are preserved. An explicit stopped checkpoint does not recover arbitrary active history or provide periodic crash recovery. The .46 saved-data proof uses reconstructed message containers and synthetic lifecycle clocks; it does not certify a deployed interruption or sustained feed run.

Use `research-features --include-trend` for ADX/directional indices and prior-only width/RV ranks, with `--percentile-window` and `--percentile-min-history` when needed. Explicit annualization is still required for realized volatility. Default research output stays unchanged without the flag. See RESEARCH.md for formulas/quality and VALIDATION_REPORT.md for the final suite/proof/audit.

## Optional daily valuation capture (0.3.47)

Set `ingestion.coinmetrics_enabled: true` in an explicit local profile to add public native daily BTC/ETH market cap/MVRV observations to supervised capture. No key is required. Other configured assets are reported unsupported for this source; an enabled profile with neither BTC nor ETH refuses validation. Defaults: hourly pause after each bounded poll, seven daily labels ending on the prior UTC day, 30-second whole-poll timeout, four pages/128 rows/two-million entity bytes and three-day latest-period age. Strict config bounds match construction: minimum poll 60 seconds; age 1–30 days; response 1,024–4,000,000 bytes. Every repeat is a receipt observation, not first publication. Failed/empty/stale/missing/latest-null outcomes block quality; preserve them and investigate the recorded reason before restarting. Redirects, endpoint substitution and immediate retry are refused. `coinmetrics_quality` in the capture report keeps actual response, poll and provider-period ages separate and records both unsupported assets and metrics. It never certifies complete history or trading. For a new finite native proof only, `python scripts/verify_coinmetrics.py --output <allowed-new-work-path>` makes one bounded poll; `python -m scripts.audit_coinmetrics --proof <proof> --output <new-external-json>` audits saved bytes without transport. Do not overwrite published proofs.

## Cooperative stop of a finite capture (0.3.51)

An explicit voluntary stop now uses the same supervised drain as the finite deadline. The library API works on Windows and POSIX and installs no process signal handlers. Create a fresh `CaptureStopRequest` for each capture and call `request()` from its owning event loop when the operator requests a stop. The first `OPERATOR` or `SIGTERM` reason wins; repeated requests do not interrupt cleanup. Reusing a request, or setting it before capture starts, refuses before output or transport. A request arriving during awaited seed/store setup prevents producer startup and finishes the zero-producer drain/report path; existing setup validation still applies.

This runnable library example voluntarily requests a stop after five seconds, with a separate 300-second finite safety deadline. It uses the configured public collectors if run; it is not an offline fixture. Use a new output directory each time.

```python
import asyncio
from pathlib import Path

from selective_options.capture_lifecycle import CaptureStopRequest
from selective_options.config import load_config
from selective_options.ingestion import capture


async def main():
    config = load_config(Path("config"), "testnet")
    stop = CaptureStopRequest()
    # An operator callback on this same event loop can instead call stop.request().
    scheduled_stop = asyncio.get_running_loop().call_later(5, stop.request)
    try:
        return await capture(
            config,
            Path("state/library-stop-001"),
            duration_s=300,
            stop_request=stop,
        )
    finally:
        scheduled_stop.cancel()


report = asyncio.run(main())
print(report["status"], report["capture_stop"])
```

On a POSIX host, the CLI can opt in to operating-system SIGTERM handling:

```sh
selective-options capture --duration 300 --output state/capture-stop-001 --graceful-stop-signals
```

Send SIGTERM to that process after it starts. The handler schedules a cooperative request on the capture's event loop and is restored to its previous value when the capture exits, including failure. Only this capture coroutine has the handler; it is not installed by imports or ordinary library calls. The flag requires POSIX and the main thread. Windows or non-main-thread use refuses with CLI exit `2` before capture creates output or starts producers. External Windows termination is not made catchable by this feature. The default CLI behavior is unchanged without the flag. SIGINT/Ctrl+C and hard task cancellation still follow the `FAILED` capture path; CLI Ctrl+C returns `130`.

Inspect `report.json` together with the process exit code. `capture_stop.trigger` records why collection ended (`DURATION_ELAPSED`, `STOP_REQUESTED`, `WORKER_FAILURE` or `HARD_CANCEL`). `capture_stop.reason` records the final outcome and becomes `FAILED` if later cleanup, drain or checkpoint work fails. `request_reason` identifies the accepted voluntary reason, while `signal_handling` distinguishes `SCOPED_POSIX_SIGTERM` from `NOT_REQUESTED`. `CAPTURE_FINISHED` remains a finite recorder result and does not certify required feed coverage or permission to trade.

Shutdown cancels and awaits every producer, including its final cleanup publications, then drains the archive worker while the writer remains available for commit barriers. It stops and drains the writer afterward. An accepted producer cleanup tail remains eligible for durable storage and the final sealed archive. A tied worker failure or a non-cancellation exception from producer cleanup overrides a successful stop; ordinary store/mirror close failures also attempt durable `FAILED` report publication before propagating. Filesystem, archive or report-write failures can still prevent a valid report from being published. A native database worker is drained rather than force-killed, so this does not impose a maximum wall-clock shutdown time.

Add `--save-bars-on-stop` only when the existing explicit stopped-bar checkpoint is required. The checkpoint is attempted after producers, archive work and writer drain succeed. Unresolved recovery, missing consumed-input membership and cancellation during an uncertain sink emission still refuse a checkpoint and fail capture; a signaled stop is not automatically resumable. Resume only an accepted `bar-checkpoint/checkpoint.json` through `--resume-bars-from <prior-capture>/bar-checkpoint` in a fresh capture, using the existing configuration, both-clock, exact membership and native-tail checks. Original receipts/watermark remain intact, retained buckets become partial at restart, actual recovery snapshots remain mandatory and delivery acknowledgements are not restored. A stopped checkpoint does not repair the original data gap, recover arbitrary active history or provide periodic crash recovery.

The saved-native replay is retained in `evidence/capture-stop-saved-native-01/verification.json`. Its four actual capture pipelines use mocked transport and explicitly synthetic lifecycle clocks over separate 50/5 original Kraken trade cohorts; original missing trade ID `109230677` remains unrepaired. Historical native proof bytes remain unchanged. That proof establishes cooperative stopped-checkpoint/seed handling, not operating-system signal delivery. `tests/test_capture_sigterm_process.py` and `scripts/verify_capture_sigterm_process.py` separately exercise the actual child-process CLI: Windows verification confirms explicit unsupported refusal. Hosted Linux run 37813119008 on exact source commit `3cca5bb` sends real `os.kill(SIGTERM)` after committed readiness and verifies clean exit, two exact durable rows including the producer-cleanup tail, matching archive hash and restored handlers. Original child/parent reports and the complete artifact checksums are retained in `evidence/full-suite-0.3.51/hosted-linux/` and private release `v0.3.51`. Process evidence is retained under `state/cloud-validation/capture-sigterm-process/<unique-id>/` during cloud validation, or `state/capture-sigterm-process/<unique-id>/` for other runs. M2 stays PARTIAL; no sustained recovery, continuous deployment, seven-day validation or orders are certified.
