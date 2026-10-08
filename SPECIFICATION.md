# BUILD PROMPT — Selective High-Conviction Options Trading System
### Underlyings: BTC · ETH · SOL · Gold (PAXG/XAUT/XAU)

> **How to use this document:** paste the whole thing into a capable coding agent (Claude Code, Cursor, Codex, etc.) as the system/first message. It is written as instructions *to that agent*. Everything in `CONFIG` blocks is tunable without touching code.

---

## 0. ROLE AND MISSION

You are a senior quantitative developer building a production options trading system. Build it in Python 3.11+, fully asynchronous, modular, testable, containerised.

**Mission statement — read this twice, it defines every downstream decision:**

The system exists to place **very few trades with a large positive expected value**, not many trades with a good-looking win rate. It stays flat by default. It only fires when technical, volatility/flow, and fundamental/news evidence independently point the same direction at the same time, and when the options market is pricing that direction cheaply enough to pay for being right.

The success metric is **expectancy per trade after all costs**, defined as:

```
E = (P_win × Avg_win) − (P_loss × Avg_loss) − fees − slippage − financing
```

A 40% hit rate at 4R average winner beats an 85% hit rate at 0.3R. Optimise `E`, never hit rate alone. Any component that improves hit rate while lowering `E` must be rejected.

**Selectivity is a feature, not a bug.** A month with zero trades is a valid, correct output. Hard-code this: the system must never be tuned to "find something to trade."

---

## 1. SCOPE — INSTRUMENTS AND VENUES

### 1.1 BTC, ETH, SOL options — primary venue: Deribit

- Deribit is the dominant listed crypto options venue and lists options on BTC, ETH and SOL.
- API: **JSON-RPC 2.0 over WebSocket** (preferred) and HTTP. Docs: `https://docs.deribit.com`.
- Testnet: `test.deribit.com` — **separate account and separate API keys** from production. All development and all paper trading happens here first.
- Instrument naming: `BTC-26DEC25-120000-C` → `{CURRENCY}-{DDMMMYY}-{STRIKE}-{C|P}`.
- Discover instruments dynamically at startup via `public/get_instruments` (`currency`, `kind=option`). **Never hard-code an instrument list** — expiries roll.
- Respect rate limits: WebSocket subscribe calls carry a heavy credit cost (3,000 credits per request, 30,000 max, ~3.3 sustained req/s, burst ~10). Implement a token-bucket limiter per endpoint class and batch subscriptions.
- Auth: OAuth2 client credentials with automatic token refresh.

Secondary/optional venues behind the same abstract interface: OKX, Bybit, Binance Options. Build a `VenueAdapter` protocol so a venue is a config line, not a rewrite.

### 1.2 Gold — handle it as its own module, it is NOT a crypto

Gold exposure in a crypto-native stack comes from tokenised gold: **PAXG** (Paxos) and **XAUT** (Tether), both redeemable claims on LBMA-certified vaulted gold. Deribit launched PAXG-denominated spot, perpetual and futures products in partnership with Paxos.

**Required behaviour — do not assume, detect:**

1. At startup, call `public/get_instruments` with `currency=PAXG`, `kind=option`.
2. **If a liquid option chain exists** (passes the liquidity gate in §8.3), trade it exactly like the crypto legs.
3. **If it does not**, the gold module switches to `SYNTHETIC` mode and must:
   - express directional conviction through the PAXG/XAUT **perpetual or futures** with a hard-defined stop and a fixed max loss equal to what the equivalent option premium would have been, **or**
   - route to an external XAU/USD options broker via a pluggable adapter (IBKR / OANDA / any FIX or REST broker the operator configures), **or**
   - if neither is available, log `GOLD_UNTRADEABLE` and skip the asset. Never silently substitute a different risk profile.
4. Gold's drivers are macro, not crypto: real yields, DXY, Fed path, central-bank buying, ETF flows, geopolitical risk. Its signal pillars in §5 use a **different feature set and a different session clock** (LBMA/COMEX hours, not 24/7). Do not reuse the BTC feature vector for gold.
5. Track and log the tokenised-gold basis (PAXG price vs XAU spot) and treat a persistent dislocation > `gold.max_basis_bps` as a `NO-TRADE` veto — it signals redemption or liquidity stress in the wrapper, not a gold view.

---

## 2. SYSTEM ARCHITECTURE

Build these as separate, independently testable modules with typed interfaces:

```
┌──────────────────────────────────────────────────────────────┐
│ 1. INGESTION      WebSocket + REST collectors, one per source│
│                   → normalised events → message bus          │
├──────────────────────────────────────────────────────────────┤
│ 2. STORE          Hot: in-memory ring buffers / Redis        │
│                   Cold: TimescaleDB or DuckDB+Parquet        │
│                   Point-in-time correct. No lookahead. Ever. │
├──────────────────────────────────────────────────────────────┤
│ 3. FEATURES       Deterministic, pure, versioned transforms  │
│                   Same code path in backtest and live.       │
├──────────────────────────────────────────────────────────────┤
│ 4. SIGNALS        3 independent pillars → scores −100..+100  │
│                   Technical | Vol & Flow | Fundamental/News  │
├──────────────────────────────────────────────────────────────┤
│ 5. CONVICTION     Vetoes → alignment test → composite score  │
│    GATE           Outputs: TRADE / NO-TRADE + reason string  │
├──────────────────────────────────────────────────────────────┤
│ 6. STRUCTURE      Direction × IV regime × catalyst timing    │
│    SELECTOR       → concrete options structure + strikes/DTE │
├──────────────────────────────────────────────────────────────┤
│ 7. RISK           Sizing, portfolio greeks, correlation,     │
│                   drawdown kill switches. Has VETO power     │
│                   over everything upstream.                  │
├──────────────────────────────────────────────────────────────┤
│ 8. EXECUTION      Order state machine, passive fills,        │
│                   slippage guards, reconciliation            │
├──────────────────────────────────────────────────────────────┤
│ 9. POSITION MGMT  Exits, rolls, greeks drift, thesis         │
│                   invalidation monitor                       │
├──────────────────────────────────────────────────────────────┤
│ 10. OBSERVABILITY Journal, dashboard, alerts, attribution    │
├──────────────────────────────────────────────────────────────┤
│ 11. RESEARCH      Backtester, walk-forward, overfit gates    │
└──────────────────────────────────────────────────────────────┘
```

**Critical constraint:** modules 3 and 4 must be *pure functions of a point-in-time snapshot*. The backtester and the live engine call the identical code. If a feature cannot be computed from data that existed at time `t`, it does not exist. Enforce with a `PointInTimeSnapshot` object that physically cannot expose future rows.

---

## 3. DATA FEEDS — WHAT TO INGEST

### 3.1 Market / price (all assets)
- Deribit WebSocket channels: `ticker.{instrument}.100ms`, `book.{instrument}.none.20.100ms`, `trades.{instrument}.100ms`, `deribit_price_index.{index}`.
- OHLCV multi-timeframe: 5m, 15m, 1h, 4h, 1D. Build higher timeframes from ticks locally; never trust a vendor's resampling for bar-close logic.
- Spot reference from at least two independent sources; alert on divergence > `data.max_price_divergence_bps`.

### 3.2 Options-native data (this is the real edge — most retail bots ignore it)
- **Full option chain per expiry**: mark price, mark IV, bid/ask IV, open interest, volume, greeks (delta, gamma, vega, theta), strike, moneyness.
- **DVOL** — Deribit's 30-day forward-looking implied volatility index for BTC and ETH, computed via variance-swap methodology across the two expiries nearest 30 days, smoothed with an EMA. The crypto analogue of VIX. Store the full time series; it is the input to IV rank.
- **IV Rank and IV Percentile** per asset, per tenor, computed over a rolling 252-day window:
  - `IV Rank = (IV_now − IV_52w_low) / (IV_52w_high − IV_52w_low) × 100`
  - `IV Percentile = % of days in the last 252 where IV < IV_now`
  - Store both. They disagree, and the disagreement is informative.
- **25-delta risk reversal** (skew): `IV(25Δ call) − IV(25Δ put)`, per tenor (7d, 30d, 60d, 90d, 180d). Positive = calls bid = bullish positioning; negative = puts bid = demand for downside protection. Track the *change* in skew, not only the level — a skew flipping from −5 to 0 is a stronger signal than a static +2.
- **Term structure**: ATM IV by expiry. Flag `CONTANGO` / `FLAT` / `BACKWARDATION`. Front-end inversion = event stress = a distinct regime.
- **Volatility surface** (SVI or SABR fit) for relative-value: find strikes priced rich/cheap versus the fitted smile.
- **Put/Call ratio** by open interest and by volume, per expiry and aggregate.
- **Max pain** strike per expiry, and distance of spot from it.
- **Gamma exposure (GEX)** estimate: dealer gamma positioning by strike. Positive GEX regions pin price; negative GEX regions accelerate moves. This materially changes whether a breakout is worth buying.
- **Volatility Risk Premium (VRP)**: `IV_30d − Realised Vol_30d`. Structurally positive most of the time; when it inverts, long-premium strategies get much better odds.

### 3.3 Perp / futures flow (positioning and fuel)
- Funding rates across Binance / Bybit / OKX / Deribit — level, 8h change, and cross-venue dispersion.
- Open interest (per venue and aggregate), and **OI delta vs price delta** — the four-quadrant read (price↑/OI↑ = new longs; price↑/OI↓ = short covering; etc.).
- Liquidation data — clusters, cascade events, long/short liquidation imbalance.
- Basis: perp vs spot, quarterly futures vs spot (annualised).
- Long/short account ratio, taker buy/sell volume imbalance.

### 3.4 On-chain (BTC/ETH/SOL only)
- Exchange netflows (inflow = distribution pressure, outflow = accumulation).
- Stablecoin supply and exchange stablecoin balances (dry powder).
- Whale transaction counts, large-holder net position change.
- ETH: staking flows, gas, L2 activity. SOL: staked SOL, TPS, DEX volume, active fee payers.
- Realised cap / MVRV / SOPR for regime context, not for timing.

### 3.5 News, sentiment, macro
- **CryptoPanic API** — headline stream with community bullish/bearish/important votes and coin tagging (`BTC,ETH,SOL` filters). Use vote *velocity*, not just count.
- RSS aggregation across tiered-credibility sources; deduplicate near-identical stories via embedding similarity so one story retold by 40 outlets does not register as 40 signals.
- Broad market/financial news API (Benzinga or equivalent) for gold and macro.
- **Economic calendar**: FOMC decisions and minutes, CPI, PCE, NFP, ECB, BoJ. Also crypto-specific scheduled events: large option expiries (monthly/quarterly), token unlocks, ETF flow reports, protocol upgrades.
- Gold-specific: DXY, US 10y real yield (TIPS), central-bank purchase reports, GLD/IAU ETF flows, geopolitical risk indices.
- **LLM classification layer**: for each deduplicated headline, produce a structured JSON verdict — `{asset, direction: bullish|bearish|neutral, magnitude: 1-5, horizon: hours|days|weeks, confidence: 0-1, is_priced_in: bool, rationale: str}`. Prompt the model to return **JSON only**, parse defensively, and treat parse failure as `neutral, confidence 0`. Never let free-text model output reach the trading path.
- Store the *timestamp of first publication*, not the time you ingested it. Latency between the two is itself a feature and a backtest-integrity requirement.

---

## 4. FEATURE ENGINEERING

Compute per asset, per timeframe, all point-in-time:

**Trend / structure:** EMA 8/21/50/200 and their slopes, ADX, market structure (higher-highs/lower-lows detection), swing pivots, Ichimoku cloud position, weekly/daily/session open relationships.

**Momentum:** RSI (with divergence detection), MACD histogram, Stochastic RSI, rate-of-change, momentum percentile rank vs its own history.

**Volatility:** ATR and ATR percentile, Bollinger width and squeeze detection, Keltner, realised vol at 7/14/30/60d, Parkinson and Garman-Klass estimators, volatility-of-volatility.

**Volume / liquidity:** VWAP and anchored VWAP from significant highs/lows, volume profile (POC, value area high/low), CVD (cumulative volume delta), order-book imbalance and depth within ±2%.

**Levels:** prior day/week/month high-low, untested liquidity pools, high-volume nodes, round-number magnets, options max-pain and high-OI strikes (they act as real magnets into expiry).

**Cross-asset:** BTC dominance, ETH/BTC and SOL/BTC ratios, correlation matrix on a 30d rolling window, correlation of each crypto to gold and to DXY, and a `correlation_regime` flag (risk-on / risk-off / decoupled).

**Regime classifier:** an explicit state machine outputting one of `{TRENDING_UP, TRENDING_DOWN, RANGING, VOLATILE_EXPANSION, VOLATILE_CONTRACTION, EVENT_STRESS}`. Derive it from ADX + BB width percentile + realised vol percentile + term structure shape. **Every strategy declares which regimes it is permitted to trade in.** A strategy fired in a disallowed regime is a bug, not a discretionary call.

---

## 5. SIGNAL LAYER — THREE INDEPENDENT PILLARS

Each pillar returns `{score: float ∈ [−100, +100], confidence: float ∈ [0,1], components: dict, reasons: list[str]}`.

They must be computed **independently**. No pillar may read another pillar's output. This independence is the whole point — three correlated signals are one signal wearing three hats, and combining them produces false confidence.

### Pillar A — TECHNICAL (weight `w_tech`, default 0.35)
Trend alignment across 1h/4h/1D, momentum with divergence, position relative to key levels, volume confirmation, volatility regime fit, mean-reversion vs breakout classification. Score positive for bullish.

Require **multi-timeframe agreement**: if 4h and 1D disagree in direction, cap `confidence` at 0.4.

### Pillar B — VOLATILITY & FLOW (weight `w_vol`, default 0.35)
This is the options-native pillar. Inputs: IV rank/percentile, 25Δ risk reversal level *and* 5-day change, term-structure shape, VRP sign and magnitude, put/call ratio extremes, OI build-up at specific strikes, dealer gamma positioning, funding rate extremes, OI/price quadrant, liquidation-cascade aftermath, basis.

Score positive for bullish *positioning and pricing*. Also emit a separate `vol_verdict ∈ {BUY_PREMIUM, SELL_PREMIUM, NEUTRAL}` — this drives structure choice in §7, independently of direction.

Rule of thumb encoded as defaults, tunable: IV percentile < 30 favours **buying** premium; IV percentile > 50 (ideally 60–80) favours **selling** premium. Selling into a panic with no floor identified is forbidden regardless of how rich IV looks — require a `vol.panic_floor_confirmed` flag before any short-premium entry when 1-day realised vol is in its top decile.

### Pillar C — FUNDAMENTAL / NEWS / MACRO (weight `w_fund`, default 0.30)
Aggregated LLM-classified news verdicts weighted by source credibility, recency decay and magnitude; on-chain flow direction; macro-event proximity and expected direction; for gold, real yields and DXY direction.

Include an `is_priced_in` discount: a bullish headline arriving after a 12% rally scores lower than the same headline arriving flat. Compute this from the underlying's move since the story first broke.

### Composite
```
raw = w_tech·S_tech·C_tech + w_vol·S_vol·C_vol + w_fund·S_fund·C_fund
conviction = raw × alignment_multiplier
```
where `alignment_multiplier` rewards agreement and punishes contradiction — e.g. `1.0` if all three share a sign, `0.5` if one dissents, `0.0` if two dissent. Weights must sum to 1 and live in config.

---

## 6. THE CONVICTION GATE — "ONLY WHEN IT IS OBVIOUS"

This is the heart of the system. Evaluate in this order and **short-circuit on the first failure**, logging exactly which gate rejected it.

### 6.1 Hard vetoes (any one = NO TRADE)
1. `killswitch_active` — any drawdown limit breached, manual halt, or reconciliation mismatch.
2. **Data integrity**: any required feed stale beyond `data.max_staleness_s`, price divergence across sources beyond threshold, or missing option chain.
3. **Event blackout**: within `risk.blackout_before_min` of a tier-1 scheduled event (FOMC, CPI, NFP) unless the strategy is explicitly an event strategy with `allow_event=true`.
4. **Liquidity gate** (§8.3) fails on every candidate structure.
5. **Regime mismatch**: current regime not in the strategy's permitted set.
6. **Correlation cap**: adding this position would push net portfolio delta or vega beyond limits, or would stack a fourth correlated crypto-beta position.
7. **Expiry proximity**: DTE outside `[min_dte, max_dte]` for the chosen structure.
8. **Gold-only**: PAXG/XAU basis dislocation beyond `gold.max_basis_bps`, or outside permitted session hours.
9. **Recent-loss cooldown**: `n` consecutive losses on this asset within `cooldown_hours` → asset is benched.

### 6.2 Score gates (all must pass)
```yaml
gate:
  min_composite_conviction: 72        # |conviction| out of 100
  min_pillar_agreement: 3             # all three pillars same sign
  min_pillar_score_each: 40           # no pillar may be a passenger
  min_confidence_each: 0.55
  min_expectancy_R: 0.35              # modelled E in R units, after costs
  min_reward_risk: 2.0                # structural, at defined targets
  max_spread_cost_pct_of_debit: 6.0
```

### 6.3 Expectancy check — mandatory, not advisory
Before any order, compute the modelled expectancy explicitly:
- Estimate `P_win` from the **historical realised** win rate of this exact setup archetype (regime + pillar signature + structure), from the trade database. If fewer than `min_samples` historical analogues exist, use the option's risk-neutral probability (delta as a proxy) **discounted by a haircut factor** `p_haircut` (default 0.85) and mark the trade `LOW_PRIOR`.
- Compute `Avg_win` and `Avg_loss` from the structure's actual payoff at the defined targets and stops, **net of** entry spread, exit spread, taker/maker fees, and for gold-synthetic mode, funding.
- If `E ≤ min_expectancy_R`, reject. Log the arithmetic.

**Never let a high conviction score override a negative expectancy.** Conviction determines *whether* the direction is right; expectancy determines whether being right pays.

### 6.4 Calibration loop
Every trade records its predicted probability. Monthly, compute a **reliability curve** and **Brier score**: when the system said 70%, did it win ~70% of the time? If the model is systematically overconfident, apply a global calibration shrinkage automatically and raise `min_composite_conviction` until calibration is restored. Log every recalibration.

Expected outcome of all of the above: **on the order of 2–8 trades per month across all four underlyings combined.** If the system is producing materially more, the gates are too loose and the mission in §0 is being violated. Emit a `SELECTIVITY_DRIFT` warning when the 30-day trade count exceeds `gate.max_trades_per_month`.

---

## 7. STRUCTURE SELECTION MATRIX

The user's instruction is "take a call or put." Naked long calls/puts are the correct answer only in one of the four cells below. Implement the full matrix — same directional view, better payoff.

| Direction conviction | IV percentile | Catalyst timing | Structure |
|---|---|---|---|
| Strong bullish | < 30 (cheap) | catalyst 3–21d out | **Long call** or **call debit spread** — 25–40Δ long leg, 45–60 DTE, exit before final 14d |
| Strong bearish | < 30 | catalyst 3–21d out | **Long put** or **put debit spread**, same parameters |
| Strong bullish | 30–55 | no near catalyst | **Call debit spread** (buy 40Δ / sell 20Δ) — caps vega bleed |
| Strong bearish | 30–55 | no near catalyst | **Put debit spread** |
| Bullish | > 60 (rich) | — | **Put credit spread** at 20–25Δ short strike, 30–45 DTE, defined risk |
| Bearish | > 60 | — | **Call credit spread**, same |
| Directionally neutral, high conviction on *movement* | < 25 | binary catalyst dated | **Long straddle/strangle** — only when the modelled move exceeds the implied move by `vol.min_move_edge_pct` |
| Neutral, range regime | > 60 | none | **Iron condor**, short strikes at 15–20Δ, wings capped — only if `panic_floor_confirmed` |
| Front-month IV >> back-month | inverted term structure | — | **Calendar / diagonal** — sell rich front, buy cheaper back |
| Skew heavily one-sided | — | — | **Broken-wing butterfly** biased to where skew pays you |

Encode skew awareness: when downside skew is steep, a put credit spread collects richer premium for the same delta — prefer the side skew pays you rather than micro-optimising deltas elsewhere.

**Strike selection algorithm:** given direction, target move (from ATR-projected move and the level map) and DTE, enumerate all candidate structures, price each from live bid/ask (never from mark), compute net debit/credit, max profit, max loss, breakeven, R:R, greeks, and total spread cost. Rank by modelled expectancy. Take the best that passes the liquidity gate. Log the top 3 with their scores so the decision is auditable.

**Expiry selection:** prefer expiries where the catalyst lands with `≥ 7` days of remaining life afterwards. Avoid holding long premium through the final 14 DTE where theta acceleration dominates. Prefer the liquid monthly/quarterly expiries over thin weeklies for anything beyond 7 DTE.

---

## 8. RISK MANAGEMENT — HAS VETO POWER OVER EVERYTHING

### 8.1 Sizing
```yaml
risk:
  risk_per_trade_pct: 1.0            # of equity, at max structural loss
  max_risk_per_trade_pct: 2.0        # absolute ceiling, never exceeded
  kelly_fraction: 0.25               # quarter-Kelly on calibrated edge
  kelly_cap_pct: 2.0
  max_concurrent_positions: 4
  max_positions_per_asset: 1
  max_crypto_beta_positions: 3       # BTC/ETH/SOL are one correlated bet
```
Size = `min(fixed_fractional, fractional_Kelly, greeks_cap_implied_size)`. Kelly uses the **calibrated** probability from §6.4, not the raw score. If calibration data is insufficient, Kelly is disabled and fixed-fractional applies.

For long-premium trades, max loss = net debit, so sizing is exact. For short-premium, max loss = spread width − credit; **never sell undefined risk.** Naked short options are prohibited by config default and require an explicit, logged operator override.

### 8.2 Portfolio greeks limits
```yaml
greeks:
  max_net_delta_pct_equity: 25       # delta-dollar as % of equity
  max_net_vega_pct_equity: 8
  max_net_theta_daily_pct: 0.6
  max_net_gamma_notional: <configured>
```
Recompute continuously. Breach → block new entries and alert; severe breach → auto-hedge with the perp or reduce the largest contributor.

### 8.3 Liquidity gate (per instrument leg)
- Bid-ask spread ≤ `max_spread_pct_of_mid` (default 8% for crypto options, tighter for BTC/ETH).
- Open interest ≥ `min_oi`, 24h volume ≥ `min_volume`.
- Top-of-book size ≥ `k ×` intended contracts (default k = 3).
- Both bid and ask present. **Never trade a one-sided book.**
- Fail on any leg = fail the whole structure.

### 8.4 Drawdown kill switches (hard stops, non-overridable in code)
```yaml
drawdown:
  daily_stop_pct: 3.0     # halt new entries 24h
  weekly_stop_pct: 6.0    # halt 72h, force review
  monthly_stop_pct: 10.0  # full halt, require manual restart + written review
  peak_to_trough_pct: 15.0  # system-wide shutdown, back to paper trading
```
Breaching the monthly stop **requires the operator to re-run the §10 validation suite before live trading resumes.** Automate that requirement — the engine refuses to arm otherwise.

### 8.5 Tail risk
Weekly Monte Carlo across the current book: 10,000 paths, correlated shocks, ±30% gap scenarios, IV +50/−50 vol-point shocks. Report VaR(95), CVaR(95) and worst-path loss. If worst-path exceeds `max_tail_loss_pct`, reduce.

---

## 9. EXECUTION

- **Passive first.** Post limit orders at or inside mid, `post_only` where supported. Ladder in over `n` slices for size.
- Escalation ladder: `mid → mid+1 tick → ... → max_slippage_bps`. If unfilled at the cap, **cancel and stand down.** A missed entry costs nothing; a chased entry costs the edge. Log `MISSED_ENTRY` with the price path so the cost of patience can be measured.
- Multi-leg structures: use the venue's combo/RFQ facility where available to avoid leg risk. If legging manually, fill the **long leg first** (the risk-defining leg), and if the second leg fails within `leg_timeout_s`, immediately unwind leg one.
- Idempotency: every order carries a client-generated ID derived from `(signal_id, leg_index)`. Retries never double-fill.
- **Order state machine**: `PENDING → SUBMITTED → PARTIAL → FILLED | CANCELLED | REJECTED | FAILED`, persisted to disk on every transition. Survives process restart.
- **Reconciliation on every startup and every 60s**: compare local position state to venue truth. Any mismatch → halt trading, alert, require manual clearance. Silent divergence between believed and actual positions is the single most expensive class of bug in this domain.
- Fees, funding and slippage are recorded per trade in the journal, not estimated.

---

## 10. EXIT LOGIC

A trade must know how it dies before it is born. Every position carries an `ExitPlan` at creation.

**Long premium:**
- Profit target: scale out 50% at `+100%` of debit, trail the rest (default, tunable).
- Time stop: exit at `min_dte_exit` (default 14) regardless of P&L.
- **IV crush stop**: if IV drops more than `iv_crush_exit_pts` while the underlying has not moved in your favour, exit — the thesis was volatility-dependent and the volatility left.
- Thesis invalidation: exit if the triggering pillar signature flips sign, even at a profit.
- Hard stop: `−50%` of debit (default), or a level-based stop on the underlying, whichever hits first.

**Short premium:**
- Take profit at 50% of max credit (mechanical, well-supported, and it cuts gamma risk).
- Defensive exit at `2×` credit received in loss.
- Exit or roll at `21 DTE` to avoid the gamma acceleration zone.
- Immediate close if the short strike is breached and the regime classifier flips to `VOLATILE_EXPANSION`.

**Universal:** exit on kill-switch, exit before tier-1 macro events unless the trade *is* the event trade, and re-evaluate on any single-day move > 2× ATR.

---

## 11. VALIDATION BEFORE ANY LIVE CAPITAL — NON-NEGOTIABLE GATES

Optimising four parameters on pure random-walk data can produce an in-sample Sharpe above 1.2. A single good-looking backtest is not evidence of anything. Build the following **as enforced code gates**, not as documentation:

### Gate 1 — Trial registry
Append-only log of **every** backtest run of every variant, with parameters and results. Unregistered results are void. This maintains the honest trial count `N`, which is required for Gate 3.

### Gate 2 — Realistic simulation
The backtester must model: actual historical bid/ask spreads (not mark price), maker/taker fees, partial fills, latency, funding, and unavailability of thin strikes. Optimistic fills are the most common source of fake edge in options backtests. Include a `pessimistic` mode that fills at the far touch and assert the strategy survives it.

### Gate 3 — Anti-overfitting statistics
- **Walk-forward analysis**: rolling in-sample optimisation, out-of-sample test, slide, repeat. Report the **robustness ratio** = OOS performance / IS performance. A ratio near 0.4–0.5 is realistic and healthy. A ratio above 0.8 usually means the OOS window is too small to be informative, not that the strategy is exceptional.
- **Probability of Backtest Overfitting (PBO)** via CSCV. Require `PBO < 0.35`.
- **Deflated Sharpe Ratio** corrected for the trial count `N` from Gate 1 and for non-normal returns. Require `DSR > 0` at 95%.
- **Purged, embargoed K-fold** wherever any ML component is used — naive K-fold leaks on overlapping financial samples.
- Libraries that already implement this: `cpz-quant`, `trialgate`, or equivalent. Use one rather than re-deriving it.

### Gate 4 — Regime breakdown
Report performance separately per regime and per year. A strategy that made all its money in one six-week window is one trade, not a strategy. Require positive expectancy in at least 3 of 4 regimes it claims to trade.

### Gate 5 — Paper trading on testnet
```yaml
promotion:
  min_paper_days: 60
  min_paper_trades: 25
  min_paper_expectancy_R: 0.25
  max_paper_vs_backtest_divergence_pct: 40
  required_brier_score: 0.22
```
The system **cannot be armed for live** until all five gates pass. Encode this literally: `LiveEngine.arm()` reads the validation artefact and raises if any gate is unmet or stale (> 90 days old).

### Gate 6 — Capital ramp
Live sizing starts at 25% of computed size for the first 20 trades, 50% for the next 20, full size only after `min_live_trades` with expectancy within tolerance of paper results. Automatic, not discretionary.

---

## 12. OBSERVABILITY

- **Trade journal** (Postgres): every signal evaluated — including rejected ones — with full pillar scores, every feature value at decision time, the gate that rejected it, the chosen structure with its top-3 alternatives, fills, fees, exits, and a human-readable `reason` string. Rejected signals are as valuable as taken ones; they are how you learn whether the gates are too tight or too loose.
- **Dashboard** (FastAPI + a simple SPA): live positions with greeks, equity curve, per-pillar contribution attribution, regime timeline, IV rank/skew/term-structure panels per asset, calibration reliability plot, gate-rejection histogram.
- **Alerts** (Telegram/Discord webhook): entries, exits, gate breaches, data staleness, reconciliation mismatch, kill-switch activation. Include the reason string in every alert.
- **Monthly attribution report**, generated automatically: expectancy by asset, by structure type, by regime, by pillar signature. Any bucket with negative expectancy over `min_samples` trades gets auto-disabled pending review.
- Structured JSON logging throughout, with a `signal_id` correlation ID threading ingestion → decision → order → fill → exit.

---

## 13. SECURITY AND OPERATIONS

- API keys: **trading scope only, withdrawal permission disabled at the exchange.** Verify programmatically at startup and refuse to run if withdrawal scope is present.
- Secrets from environment or a vault; never in code, config files, logs, or the repo. Pre-commit hook to block key patterns.
- Separate keys and separate config profiles for testnet and production. Make it structurally impossible to point production keys at a testnet config or vice versa — assert on `base_url` ↔ key-profile match at startup.
- **Manual kill switch**: a single command and a dashboard button that cancels all open orders and optionally flattens.
- Heartbeat and watchdog: if the main loop stalls beyond `heartbeat_timeout_s`, cancel all resting orders automatically. Deribit supports a `cancel_on_disconnect` setting — enable it.
- WebSocket resilience: exponential backoff reconnect, full state resync on reconnect, sequence-gap detection.
- NTP-synced clock; reject any decision computed on data with a timestamp skew beyond tolerance.
- Nightly encrypted backup of the trade database.
- Graceful shutdown: never leave orphan orders.

---

## 14. JURISDICTION AND COMPLIANCE MODULE (operator is in India)

Build this as a first-class module, not an afterthought:

- Trading crypto is legal in India, but VDA gains fall under **Section 115BBH: a flat 30% tax plus 4% cess (effective 31.2%)**, regardless of holding period, with **only the cost of acquisition deductible**.
- **Losses cannot be set off** against other income, nor against gains on a different VDA, nor carried forward. This has a direct design consequence: the *pre-tax* expectancy required for a strategy to be worth running is materially higher than it looks, because the tax is asymmetric. Add a config flag `tax.asymmetric_regime: true` that raises `min_expectancy_R` accordingly and reports both pre-tax and post-tax expectancy in every report.
- **1% TDS under Section 194S** on transfers above the annual threshold (₹50,000 for specified persons, ₹10,000 for others). On a high-turnover strategy this is a cash-flow drag long before any P&L is settled — model it as a per-transfer cost in the backtester.
- Reporting: transaction-level records for **Schedule VDA**; holdings on foreign platforms may additionally require **Schedule FA** disclosure. Retain records for at least 7 years.
- Therefore: the journal must export a **tax-ready CSV** with timestamp, instrument, side, quantity, INR-converted consideration (using a documented, consistent FX and price source), cost of acquisition, fees, and TDS where applicable.
- Also emit a `compliance_notes.md` listing what the operator must verify themselves: venue availability and KYC acceptance for Indian residents, FEMA/LRS treatment of remittances for offshore derivative accounts, and current CBDT guidance. **Do not hard-code legal conclusions** — these change, and the system should surface the checklist rather than pretend to answer it. Flag clearly that this module produces records for a chartered accountant, not tax advice.

---

## 15. TECH STACK

```
Language     Python 3.11+, full type hints, mypy strict
Async        asyncio; websockets / aiohttp
Exchange     native Deribit JSON-RPC client (ccxt as a fallback adapter only)
Data         polars or pandas; numpy; TimescaleDB or DuckDB + Parquet
Cache        Redis for hot state and the message bus
Options math py_vollib / QuantLib for greeks and IV; custom SVI surface fit
Validation   pydantic v2 for every external payload and every config
Backtest     custom vectorised engine + cpz-quant / trialgate for the stat gates
API/UI       FastAPI + WebSocket push; lightweight SPA dashboard
Config       YAML with pydantic-settings; env overrides; single source of truth
Testing      pytest; ≥85% coverage on risk, execution and gate modules
Deploy       Docker Compose; systemd or k8s; Prometheus + Grafana
CI           lint, type-check, test, and a smoke backtest on every commit
```

---

## 16. BUILD ORDER

Deliver in these milestones. **Do not proceed to the next until the previous is tested and demonstrably working.**

1. **Skeleton + config + logging + secrets.** Deribit testnet connectivity, instrument discovery, option chain fetch.
2. **Ingestion + storage.** All feeds in §3 flowing, point-in-time correct, with staleness monitoring.
3. **Feature layer + regime classifier.** Unit-tested against fixtures. Prove no lookahead with a deliberate leakage test.
4. **Backtesting engine** with realistic fills, before any signal logic exists. Validate it by backtesting a known-bad strategy and confirming it loses the expected amount.
5. **Three signal pillars**, independently tested and independently backtested.
6. **Conviction gate + expectancy engine + calibration.**
7. **Structure selector + strike/expiry optimiser.**
8. **Risk manager.** Prove every limit and kill switch fires under simulated breach. This module is tested harder than any other.
9. **Execution engine** against testnet, including forced-failure tests (kill the process mid-order, drop the socket mid-fill, reject a leg).
10. **Position manager and exit logic.**
11. **Validation suite (§11)** and the arming interlock.
12. **Dashboard, alerts, journal, attribution.**
13. **Compliance/tax export module.**
14. **60-day testnet paper run.** Only then discuss live capital, at 25% size.

---

## 17. ACCEPTANCE CRITERIA

The build is complete when every one of these is objectively true:

- [ ] Runs unattended for 7 days on testnet with zero unhandled exceptions and zero reconciliation mismatches.
- [ ] Every rejected signal is logged with the exact gate that rejected it and every input value.
- [ ] Backtest and live paths produce **bit-identical** features on the same historical snapshot (assert this in CI).
- [ ] Every risk limit and kill switch demonstrably fires in an integration test.
- [ ] A deliberate leakage test fails loudly — i.e. the framework detects lookahead when it is injected.
- [ ] Walk-forward, PBO and Deflated Sharpe artefacts exist, are current, and gate the `arm()` call.
- [ ] Calibration reliability curve and Brier score generated from paper trades.
- [ ] Trade count over 30 paper days falls within the expected selective range; a `SELECTIVITY_DRIFT` alert fires if not.
- [ ] Tax-ready CSV export validated against a manual reconciliation of 10 trades.
- [ ] `README.md` explains every config parameter and what raising or lowering it does.
- [ ] A one-command kill switch works from both CLI and dashboard.

---

## 18. EXPLICIT PROHIBITIONS

Do not, under any circumstances:

1. **Trade market orders on options.** Crypto options books are thin; a market order on a 15%-wide spread hands the edge away instantly.
2. **Sell undefined-risk options** without an explicit, logged operator override.
3. **Use mark price for fill assumptions** anywhere in the backtester.
4. **Let any single module bypass the risk manager.** Risk has final veto, always.
5. **Auto-tune gates to increase trade frequency.** If the system is quiet, the system is working.
6. **Average down, martingale, or add to a loser.** Ever.
7. **Use future data in any feature**, including "the day's close" for an intraday decision.
8. **Deploy with withdrawal-enabled API keys.**
9. **Hard-code instruments, strikes or expiries.**
10. **Report a win rate without the accompanying expectancy.** They are meaningless apart.
11. **Silently swallow an exception in the trading path.** Fail loud, halt, alert.
12. **Skip a §11 gate because results look good.** Looking good is exactly when the gates matter.

---

## 19. DELIVERABLES

Produce, in order:

1. `ARCHITECTURE.md` — module map, data flow diagram, every interface signature.
2. Complete repository, structured and typed as specified.
3. `config/` — `default.yaml` with every tunable and an inline comment explaining its effect, plus `testnet.yaml` and `production.yaml` profiles.
4. Test suite with the coverage targets above, including the forced-failure and leakage tests.
5. `VALIDATION_REPORT.md` — walk-forward results, PBO, DSR, regime breakdown, robustness ratio, honest trial count.
6. `RUNBOOK.md` — deploy, monitor, respond to each alert type, kill, restart, resume-after-drawdown-halt.
7. `compliance_notes.md` per §14.
8. `README.md` — quickstart, the mission statement from §0 reproduced verbatim at the top, and the parameter guide.

Work in small, reviewable increments. After each milestone, report what was built, what was tested, what failed, and what you are uncertain about. State assumptions explicitly rather than resolving ambiguity silently. If a spec item here conflicts with what the live API actually supports, surface the conflict and propose an alternative — do not silently substitute.

---

## APPENDIX A — DEFAULT CONFIG SKELETON

```yaml
system:
  mode: testnet                 # testnet | paper | live
  assets: [BTC, ETH, SOL, PAXG]
  base_currency: USD
  loop_interval_s: 15
  heartbeat_timeout_s: 60

data:
  max_staleness_s: 30
  max_price_divergence_bps: 25
  min_sources_per_asset: 2

signals:
  weights: {technical: 0.35, volatility_flow: 0.35, fundamental: 0.30}
  lookback_days_iv_rank: 252
  news_decay_halflife_hours: 8
  news_dedup_similarity: 0.88

gate:
  min_composite_conviction: 72
  min_pillar_agreement: 3
  min_pillar_score_each: 40
  min_confidence_each: 0.55
  min_expectancy_R: 0.35
  min_reward_risk: 2.0
  max_spread_cost_pct_of_debit: 6.0
  max_trades_per_month: 10
  cooldown_after_n_losses: 2
  cooldown_hours: 48

vol:
  buy_premium_below_iv_pct: 30
  sell_premium_above_iv_pct: 60
  min_move_edge_pct: 20
  require_panic_floor_for_short_premium: true

structure:
  min_dte: 21
  max_dte: 60
  min_dte_exit: 14
  target_long_delta: [0.25, 0.40]
  short_strike_delta: [0.15, 0.25]

liquidity:
  max_spread_pct_of_mid: 8.0
  min_open_interest: 50
  min_24h_volume: 20
  book_depth_multiple: 3

risk:
  risk_per_trade_pct: 1.0
  max_risk_per_trade_pct: 2.0
  kelly_fraction: 0.25
  max_concurrent_positions: 4
  max_positions_per_asset: 1
  max_crypto_beta_positions: 3
  allow_naked_short: false
  blackout_before_min: 120
  blackout_after_min: 30

greeks:
  max_net_delta_pct_equity: 25
  max_net_vega_pct_equity: 8
  max_net_theta_daily_pct: 0.6

drawdown:
  daily_stop_pct: 3.0
  weekly_stop_pct: 6.0
  monthly_stop_pct: 10.0
  peak_to_trough_pct: 15.0

execution:
  passive_first: true
  max_slippage_bps: 40
  slice_count: 3
  leg_timeout_s: 20
  cancel_on_disconnect: true

gold:
  mode: auto                    # auto | options | synthetic | external | disabled
  max_basis_bps: 150
  session_filter: LBMA_COMEX

promotion:
  min_paper_days: 60
  min_paper_trades: 25
  min_paper_expectancy_R: 0.25
  required_brier_score: 0.22
  live_ramp: [0.25, 0.50, 1.00]

tax:
  jurisdiction: IN
  asymmetric_regime: true
  vda_rate_pct: 31.2
  tds_rate_pct: 1.0
```

---

## APPENDIX B — SELF-CHECK BEFORE YOU START CODING

Confirm you can answer all of these from this document alone. If any answer is missing, ask before writing code:

1. What makes the system stay flat, and what makes it fire?
2. Which single metric is optimised, and which metric must never be optimised alone?
3. Which module can override every other module?
4. What must be true before live capital is permitted?
5. What happens to the gold module if no PAXG option chain exists?
6. Where does the backtester differ from the live engine? (Correct answer: nowhere, in features and signals.)
7. What is logged when a signal is rejected?
8. Why does the tax regime change the minimum acceptable expectancy?
9. What is the expected number of trades per month, and what happens if it is exceeded?
10. Which is worse — a missed entry or a chased entry?