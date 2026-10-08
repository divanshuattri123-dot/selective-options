# Offline research foundations

Version 0.3.49 continues deterministic closed-bar features and options research alongside ingestion. It adds an opt-in official FOMC date-only snapshot source with point-in-time health reporting. The 0.3.48 read-only saved-option quote bridge remains available for offline research. Milestone acceptance still follows the specification: M2 is partial, the full feature and backtester milestones are incomplete, and the package cannot submit orders.

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
