# Offline research foundations

Version 0.3.51 adds read-only native option put/call ratios by open interest and rolling 24-hour volume, per expiry and across a complete discovered cohort. The API and CLI retain native base-currency amounts, exact rational arithmetic, both clocks and explicit unavailable results; they do not convert amounts into contract counts or generate signals. The saved BTC testnet result is partial because one expiry has a zero call-volume denominator. Ingestion remains M2 PARTIAL, and later milestone acceptance remains gated.

Version 0.3.50 adds a read-only discrete delta risk-reversal research view while ingestion work continues. It computes call minus put mark implied volatility in native percentage points from signed nearest-delta contracts for each expiry; it rejects tied selections, stale or invalid members, mismatched references, incomplete discovery, and lookahead. It does not interpolate or authorize signals. The finite saved-testnet replay contains an unvalidated 119.63-point observation, preserved without clipping; see the evidence and limitations below. Milestone acceptance still follows the specification: M2 is partial, the full feature and backtester milestones are incomplete, and the package cannot submit orders.

These tools operate on saved local inputs. No provider key is needed. Install the package as described in README.md and run the commands from the project directory. Configuration still validates before CLI commands; every profile keeps trading disabled.

## Features from closed bars

`research-features` opens a closed DuckDB source read-only. Supply the exact asset, environment, source, symbol, timeframe and millisecond cutoff. Both event time and first receipt must be at or before that cutoff. The command hashes the database before and after reading and refuses an active WAL, linked paths, an oversized source or history beyond the explicit row budget. Outputs go into a new directory.

```powershell
selective-options research-features `
  --database CLOSED_CAPTURE/events.duckdb --output NEW_FEATURE_RESULT `
  --asset BTC --environment public_reference --source kraken `
  --symbol BTC/USD --timeframe-s 300 --as-of-ms EXPLICIT_CUTOFF_MS `
  --max-bars 2000 --annualization-periods-per-year 105120
```

Replace the source, symbol and cutoff with values from the actual capture. The annualization factor above is a caller assumption for uninterrupted five-minute crypto bars in a 365-day year; it must be changed for a different interval or session calendar. It is recorded in the output. Omit the factor to make realized volatility explicitly unavailable.

The loader never silently keeps the last N rows and reseeds indicators. An optional `--history-start-ms` defines an inclusive **bar-close** window start; results and their history digest record this choice. The row budget currently counts every selected timeframe for the same source and symbol before the feature transform selects one interval. Other intervals can therefore cause a conservative budget refusal. This bounds materialized rows, not DuckDB scan cost.

Formula version `closed-bars-v1` provides:

| Feature | Definition and warmup |
| --- | --- |
| EMA 8/21/50/200 and slopes | First period-length mean seeds EMA, then alpha = 2/(period+1); slope is price change per bar |
| RSI 14 | Wilder average gains/losses; needs 15 closes; a flat denominator is unavailable |
| ATR 14 | Wilder true range, seeded with 14 changes; needs 15 bars |
| MACD 12/26/9 | EMA12 minus EMA26; signal starts from the first nine valid MACD values, requiring 34 bars |
| Bollinger width 20 | Four times population close standard deviation divided by mean close |
| ROC 5/20 | Latest close divided by close K bars earlier, minus one |
| Realized volatility 20/60 | Sample standard deviation of K log returns times square root of explicit annualization |

Windows are **bar counts**, not days. Short valid history produces per-feature `WARMUP`; missing annualization or undefined arithmetic produces `UNAVAILABLE`, with no numeric placeholder. Incomplete bars, interior gaps, stale final bars, conflicting identities or invalid payloads prevent a usable report. Future clocks in supplied snapshot rows are refused even when a fabricated snapshot bypasses its normal constructor. Every report retains exact selected history identity, formula version, units, warmup and quality reasons. No regime classification or trading signal is produced.

Add `--include-trend` to the same command for the five `trend-bars-v1` features: `plus_di_14`, `minus_di_14`, `adx_14`, `bollinger_width_20_percentile` and `realized_vol_20_percentile`. Default output is unchanged without this flag. `--percentile-window` defaults to 100 prior observations and `--percentile-min-history` to 20; custom percentile settings require the flag.

Directional movement uses strict high/low comparisons, with tied movements contributing neither leg. Unrounded Wilder sums seed the first 13 changes, then apply the 14th change before the first directional indices, requiring 15 bars. ADX seeds the mean of the first 14 valid DX values and then uses alpha 1/14, requiring 28 bars. This follows the unrounded convention in the [primary TA-Lib ADX implementation](https://github.com/TA-Lib/ta-lib/blob/main/src/ta_func/ta_ADX.c). A zero historical true-range or directional denominator makes ADX unavailable for the supplied window; that row is never silently discarded or used to reseed a shorter history.

Width and realized-volatility ranks compare the current value with a bounded window of **prior** contiguous values, excluding the current observation. The exact tie rule is `100 × (less + 0.5 × equal) / N`; a valid constant history ranks 50. Width needs 20 bars plus the minimum prior history; RV needs 21 bars plus that history and explicit annualization. Reports retain candidate/verified reference counts, first/latest reference closes, tie policy and base history identity. Missing or undefined inputs remain unavailable. These primitives do not classify the full market regime or independently infer a full options surface; the discrete expiry subset below is separate.

## A counted quote simulation

Run the supplied deliberately losing fixture into a new directory with a shared journal:

```powershell
selective-options research-backtest `
  --input examples/backtest-known-bad.json `
  --output state/known-bad-run-001 --registry state/research-trials.jsonl
```

Use a fresh output directory on every attempt and retain the same journal for the research family. The fixture declares synthetic prices and fees; it does not represent venue fee terms or a profitable strategy. It buys two contracts, multiplier ten, at ask 101 and sells them at bid 99. Gross loss is 40 currency units; the explicit 0.1% taker fee costs 2.02 + 1.98 = 4.00. Net round-trip P&L is **−44**. This checks that spread and fees survive accounting even when the mid-price never changes.

The strict JSON input must declare data kind, provenance, cutoff, instrument and quantity units, multiplier, every order, every quote, fee policy and simulation assumptions. Duplicate keys, unknown fields, invalid decimals and unsupported instrument requirements refuse. Declaring `historical_research` does not authenticate a dataset; output states that historical provenance was not independently verified.

The simulator is a bounded, single-instrument, long-only model for linear premium accounting. Both quote clocks must follow order submission plus fixed latency. Only fresh, healthy, two-sided, uncrossed quotes with sufficient displayed depth qualify. Buys use the ask and sells the bid, with explicit adverse slippage and taker fees. Passive limits need a strictly later executable crossing; touching a limit is insufficient. This is a quote-based research assumption, not queue-position or confirmed execution evidence.

Repeated observations sharing a native liquidity ID do not replenish depth. Fills cannot exceed that shared depth or the long inventory available for a sale. Equal-time cancellation precedes quotes; remaining ties use deterministic IDs rather than an inferred venue ordering. Orders persist until filled, cancelled or the cutoff. There is no IOC/FOK, short-sale, capital, margin or open-position mark valuation model. Funding, settlement, transfer tax, multiple instruments and multi-leg structures are explicitly unsupported. Results keep these limitations visible and never certify Gate 2.

## Trial accounting and interruption

The journal appends and fsyncs `STARTED` before running a simulation, then appends `COMPLETED` or `FAILED`. All started trials count in N, including failures and unresolved starts. Exact parameters include the full fee/fill policy and an input-file digest binding quotes and orders. Changed input bytes during execution record a failed trial. The final report is created exclusively and cannot overwrite a journal or an operator file that appears during the run.

Canonical JSON, sequence numbers and chained hashes detect malformed or inconsistent retained records. This is a local integrity journal, not a tamper-proof audit service: deleting or truncating an entire tail cannot be detected without an external anchor. Locks serialize cooperating writers only. An abandoned `.lock` requires inspection; it is never automatically removed by a later run.

On a write/fsync error, inspect retained bytes and the lock before deciding how to recover. A partial line causes explicit refusal; do not erase it or discard failed trials to obtain a cleaner N. A completed record visible after failed fsync does not establish durability. Simulation success and final report publication are separate steps: a report-write failure can leave a counted completed simulation without a report. No success is returned in that case. Development unit fixtures and registered integration fixtures are identified separately from historical strategy trials and paper trades.

## Conservative open-bar state

The Python API in `bar_state.py` provides `encode_bar_state`, `decode_bar_state` and `restore_bar_state`. It validates a bounded strict state containing open buckets, exact Decimal totals, original trade prototypes, source/receipt clocks, configuration and closed-bar watermark. Restored retained buckets are always marked incomplete with the restart coverage boundary; no uninterrupted coverage or quiet interval is invented.

The codec alone does not establish committed input provenance. Version 0.3.46 supplies that separate explicit stopped-capture integration: `capture --save-bars-on-stop` binds the prospective consumed-input ledger and native tail to the committed store after producers and the writer drain. It writes a minimal closed database, fixed state files and a manifest published last. The default remains disabled; this is not periodic crash recovery.

`capture --resume-bars-from <prior-capture>/bar-checkpoint` validates policy and hashes before creating a new capture, copies the exact native input/tail union with deliveries pending, closes and revalidates the otherwise empty seed before any producer is constructed, and atomically installs independent state. It is exclusive with trade-tail/recovery-marker resume. Original first receipts and the closed-bar watermark remain unchanged; every retained bucket is partial across the restart and an actual recovery snapshot is still required. Raw-only historical snapshots are excluded from consumed membership. Unsupported, uncommitted, ambiguous, future or over-budget state refuses a successful checkpoint. Declared conservative incomplete flags cannot prove historic gap lineage, uninterrupted coverage, full-source continuity or current feed health.

## Release evidence

The .46 `evidence/bar-checkpoint-resume-01/report.json` records four actual supervised captures with mocked transport and explicitly synthetic lifecycle clocks. Two separate 50/5 cohorts preserve all 55 original native trades and leave the documented missing ID unrepaired. Resumed captures produce four partial bars, preserving OHLC/count/Decimal totals and first receipts. An independent read-only auditor checks the manifests, exact state/input membership, archives/outboxes, both-clock boundaries, preserved closed destination seed bytes and trend/reference values on the unchanged 240-bar labelled fixture. The seed observation is a proof copy of bytes already verified before reopen, not a new production handoff mechanism. Equal records in separately built DuckDB databases need not imply identical file bytes. No new public requests, accounting/strategy trials or orders occur in this proof. Final release validation is recorded in VALIDATION_REPORT.md.

`evidence/parallel-foundations-01/report.json` binds the executed production source and verifier hashes. It preserves 55 original native trade records in two separate cohorts, leaves the known missing trade explicit, produces four conservative partial bars with synthetic restart timing, evaluates 240 labelled synthetic bars and runs the losing fixture once in that proof's journal. The independent audit checks original fields, both-clock SQL visibility, typed Parquet equality, outbox membership, analytical feature values, leakage refusal and the raw trial chain. The source capture and previous release remain unchanged. None of this substitutes for full feed/history coverage, a sustained run, historical strategy validation or elapsed paper trading.

## Explicit discrete options expiry research (0.3.47)

`research-options` reads a closed saved Deribit testnet database under both clocks before a conservative row-overflow sentinel. All retained selected quote/metadata/control history remains supplied; no truncation or silent reseeding. Discovery must bind the exact current metadata IDs. Every active discovered same-type member at each expiry needs a canonical fresh quote; stale, missing or conflicting nearest candidates cannot produce a farther-strike fallback. Native `mark_iv` is in percentage points. ATM ordering uses exact folded-strike/native-underlying fractions corresponding to log-distance; exact ties refuse. Named underlying references and supported contract/currency units must agree. There is no interpolation or fitted surface.

```powershell
selective-options research-options `
  --database evidence/reused-validation-public-01/events.duckdb `
  --output state/new-options-research --asset BTC --as-of-ms 1791050702961 `
  --option-type call --moneyness-method native_underlying_log_distance `
  --tie-policy exact_distance_refuse --max-quote-age-ms 10000 `
  --max-discovery-age-ms 100000 --max-atm-log-moneyness 0.2 `
  --max-underlying-mismatch-fraction 0.1 --flat-threshold-iv-points 1.0
```

These are explicit reproducible research assumptions for the saved cohort, not recommended production tolerances. Repeat with `--option-type put` into a different new output. Default budgets are 100,000 supplied history rows and 128 expiries; they bound materialized records rather than SQL scan cost. The saved native proof contains 1,010 discovered members and 13 expiries per side. All selected native IVs rise; the 1-point flat band classifies some increments FLAT and others rising, producing MIXED. That label does not imply a negative IV slope. Each result retains selected identities, clocks, native quote payloads, policies and input digest. Full-chain bid/ask/book quality, current feed health, a strategy edge and live readiness remain unverified. No counted strategy trial is created by this analytical command.

## Saved option quote and round-trip research (0.3.48)

The Python API `load_saved_option_quotes` reads one discovered option from a closed Deribit testnet DuckDB archive. It validates the discovery manifest against the exact active metadata, both event and receipt cutoffs, source/receipt skew, source-before-receipt order, quote identity, instrument expiry and profile units. No row is truncated to fit a budget. `option_ticker_to_quote` uses native best bid/ask only; marks and implied volatility never determine execution. Displayed depth uses base-currency amount and is divided by the explicit, metadata-matched native contract size to obtain contracts. Currency conversion, inverse option payoff, settlement, funding and transfers are outside this model.

`research_saved_option_roundtrip` is an API-only, explicitly registered, closed-before-expiry single-instrument long-premium simulation. Buy and sell submissions must be strictly later than the discovery observation, so a millisecond tie cannot pass. Source-ahead quote rows within the explicit absolute clock-skew bound are retained for audit, but the shared simulator assigns `QUOTE_CLOCK_ORDER` and cannot fill them; the report preserves that non-fill reason. Tick-price checks use the base tick at an exact `above_price` boundary and switch only strictly above it, following Deribit's [tick-size schedule definition](https://support.deribit.com/hc/en-us/articles/25944779257373-6-July-2023-TS). Native `min_trade_amount` is applied as a minimum order-size floor; it is not assumed to be a quantity increment, consistent with Deribit's [API guidance](https://support.deribit.com/hc/en-us/articles/38507223482781-API-guidance). A separate increment is not modeled unless future validated metadata defines one.

The passive-order model is deliberately narrow: later displayed executable touches can produce taker-assumption fills, and repeated partial fills consume the same quote's displayed liquidity only once. Fee rates and latency are explicit caller assumptions, not venue defaults. Output and trial records state `gate2_complete=false` and `trading_enabled=false`. There is no options surface, mark-to-market/expiry payoff, multi-leg portfolio, funding/settlement/tax accounting, strategy signal, validation of profitable expectancy, production data, or execution route. Synthetic fixtures test accounting and refusal boundaries; the saved native archive is used only to validate quote conversion and source/receipt handling, not to simulate an order. See `evidence/option-backtest-bridge-0349/` and `tests/test_saved_option_backtest.py`.

## Native discrete delta risk reversal (0.3.50)

`research-option-skew` reports call mark IV minus put mark IV for each discovered expiry. It selects the nearest native signed Greek deltas to `+target_absolute_delta` for calls and `-target_absolute_delta` for puts; the caller sets an explicit maximum delta error. For example, ±0.25 with a 0.05 tolerance accepts a selected call delta in [0.20, 0.30] and a put in [−0.30, −0.20]. Equal nearest-distance candidates refuse the feature rather than breaking ties by symbol. Mark IV is retained in native percentage points, so the difference is also percentage points; it is not converted to a fraction.

```powershell
selective-options research-option-skew `
  --database CLOSED_TESTNET_CAPTURE/events.duckdb --output NEW_SKEW_RESULT `
  --asset BTC --as-of-ms EXPLICIT_CUTOFF_MS `
  --target-absolute-delta 0.25 --maximum-delta-error 0.05 `
  --max-quote-age-ms 10000 --max-discovery-age-ms 100000 `
  --max-clock-skew-ms 10000 --max-underlying-mismatch-fraction 0.1 `
  --max-rows 100000 --max-expiries 128
```

The input is read-only and must be a closed DuckDB file. Both source and receipt clocks must be available by the same explicit cutoff. The report binds the selected rows and policy to a history digest. A full same-cutoff discovery manifest is required, and every discovered instrument in an expiry must have one fresh, unambiguous latest quote; otherwise that expiry is withheld so an unseen closer-delta strike cannot be ignored. The chosen call and put must share an underlying index, compatible quote/settlement/type units, and sufficiently close underlying prices. Source/receipt skew has its own bound. `PARTIAL` means some expiry pairs are auditable while one or more are withheld; consumers must inspect every expiry and blocker.

This is mark-IV analytics only. It does not require an executable bid/ask, estimate fills, verify the live full chain/current feed health, interpolate deltas, fit a surface, or produce a signal. A value's sign is descriptive: positive means the selected call's mark IV exceeds the selected put's; it does not establish direction or an edge. `full_chain_verified`, `executable_quotes_verified`, `ready_for_signals` and `trading_enabled` remain false. It is not a counted strategy trial.

`evidence/options-skew-saved-native-01/` replays one saved BTC testnet snapshot with 52,820 bounded history/control rows and 1,010 full-discovery members. Twelve of 13 expiry rows yield nearest-delta pairs; one expiry is withheld because its put has `PUT_DELTA_SIGN_INVALID`. The retained December call mark is 157.26 percentage points and produces a +119.63-point difference. It is preserved as an observed outlier rather than clipped or treated as a validated quote. This is a single historical snapshot, not a time series, current feed-health check or strategy result; zero new network requests and no trial/order are involved.

## Native option put/call amount ratios (0.3.51)

`research-option-flow` reads a closed Deribit testnet DuckDB archive and writes `put-call.json` into a fresh output directory. The pure Python API is `options_flow_features.compute_put_call_ratios`; the read-only database/report wrapper is `option_flow_research.research_option_flow`. Both require an explicit `deribit_option_base_currency` amount policy and `stats.volume` field policy. The venue documents option `open_interest` in underlying base-currency amount and `stats.volume` as base-currency volume over the preceding 24 hours. These are native amount ratios, and `volume_usd` is never substituted. The rolling window is not treated as a calendar day, and contract size never multiplies already denominated amounts. See the [official Deribit ticker field definitions](https://docs.deribit.com/api-reference/market-data/public-ticker).

```powershell
selective-options research-option-flow `
  --database evidence/reused-validation-public-01/events.duckdb `
  --output state/new-option-flow-result --asset BTC --as-of-ms 1791050702961 `
  --amount-unit-policy deribit_option_base_currency --volume-field stats.volume `
  --max-quote-age-ms 10000 --max-discovery-age-ms 100000 `
  --max-clock-skew-ms 10000 --max-rows 100000 --max-expiries 128
```

The displayed tolerances reproduce the saved research cohort; they are not recommended production settings. The three clock bounds are separate: source and receipt quote ages are each limited to 10,000 ms, discovery age to 100,000 ms, and absolute source/receipt skew to 10,000 ms. Both clocks must also be at or before the explicit decision cutoff. Source-ahead quotes are descriptive inputs only after both clocks are visible and their skew passes policy; this does not establish executable quote quality. The row budget applies before deduplication and refuses overflow rather than truncating history; it limits materialized rows, not database scan cost. The loader hashes the closed input before and after reading and refuses an active WAL, linked paths or source changes.

The calculation selects each instrument's latest source-time quote, refusing equal source-time ambiguity instead of resolving it through arrival order. Discovery must be full, canonical and bound to each exact active metadata ID, with unique membership and exact declared counts. Every member must remain unexpired and have a fresh quote observed at or after the discovery manifest. Missing or stale members cannot be dropped to make a smaller aggregate. Quote currency, settlement currency, instrument family and price-index reference must be compatible within each expiry and across the aggregate. Neither implied volatility, delta nor an underlying price converts the native amounts.

For each metric, the ratio is the sum of native put amounts divided by the sum of native call amounts. The aggregate uses sums over all discovered expiries, rather than averaging expiry ratios. Exact `Fraction` arithmetic retains totals and ratios as numerator/denominator strings derived from the canonical numeric values already saved by ingestion; it does not claim precision absent from those saved bytes. A finite float is supplied for convenience. Overflow or nonzero underflow withholds that float, and a zero call denominator returns `UNAVAILABLE` with `CALL_DENOMINATOR_ZERO`, never infinity or a placeholder zero. A zero put total with a positive call total yields a defined zero ratio.

Open interest and volume have separate availability. Missing or null `stats.volume` can withhold volume while independently complete OI remains available; malformed numeric fields refuse. Incomplete member inputs prevent aggregate totals and ratios rather than publishing partial sums. A zero-denominator expiry can still belong to a complete aggregate whose total call amount is positive: its local ratio remains unavailable, the aggregate remains mathematically defined, and the report retains `PARTIAL` plus the expiry blocker. Every member keeps native amount values, metadata and quote identities, both quote clocks, denomination/reference fields, and explicit unavailable reasons. The frozen report also binds the input digest, manifest and caller policies.

`evidence/options-flow-saved-native-01/` uses the unchanged `evidence/reused-validation-public-01/events.duckdb` BTC testnet snapshot at cutoff `1791050702961`, with source SHA-256 `97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc`. Its exact discovery cohort contains 1,010 members and 13 expiries. Aggregate OI put/call is `0.7753088307760276`; aggregate rolling-volume put/call is `0.2829367350165104`. The report is `PARTIAL`: one expiry's call-volume total is zero, so its volume ratio records `CALL_DENOMINATOR_ZERO`, while the aggregate call-volume total remains positive. `scripts/verify_options_flow.py --output FRESH_PROOF_DIRECTORY` independently checks the fixed source hash, exact membership, native Decimal sums, ratios, both clocks and output boundaries; each run requires a new output directory. The retained proof and its source-bound final rerun are recorded with release evidence.

This is one historical testnet snapshot. It does not establish continuous volume history, a simultaneous quote snapshot, current feed health, full-chain executable bid/ask/book quality, a fitted surface, profitable expectancy or strategy acceptance. `full_chain_verified`, `current_feed_health_verified`, `executable_quotes_verified`, `simultaneous_quote_snapshot_verified`, `contract_count_ratio`, `interpolated`, `ready_for_signals` and `trading_enabled` remain false. The analytical command creates no counted strategy trial, paper trade or order, and the saved-native replay makes no new public requests.
