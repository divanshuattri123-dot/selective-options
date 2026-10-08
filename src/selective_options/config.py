"""Single typed source of defaults; configuration descriptions generate operator docs."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Positive = Annotated[float, Field(gt=0, allow_inf_nan=False)]
NonNegative = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Percent = Annotated[float, Field(ge=0, le=100, allow_inf_nan=False)]
Probability = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
Count = Annotated[int, Field(ge=1)]


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class SystemConfig(Section):
    mode: Literal["testnet", "paper", "live"] = Field(
        "testnet", description="Select environment; all modes remain unable to trade in this build."
    )
    assets: tuple[Literal["BTC", "ETH", "SOL", "PAXG"], ...] = Field(
        ("BTC", "ETH", "SOL", "PAXG"),
        min_length=1,
        description="Assets to discover; adding assets increases public API work.",
    )
    base_currency: Literal["USD"] = Field(
        "USD", description="Reporting denomination; retain native quote and settlement units."
    )
    loop_interval_s: Positive = Field(
        15, description="Planned decision interval; higher means fewer evaluations."
    )
    heartbeat_timeout_s: Positive = Field(
        60, description="Stream receive deadline; lower detects stalled sessions sooner."
    )
    log_level: Literal["INFO", "WARNING", "ERROR"] = Field(
        "INFO", description="Application log verbosity; protocol payload debug logging is disabled."
    )


class VenueConfig(Section):
    key_profile: Literal["testnet", "production"] = Field(
        "testnet", description="Credential namespace; must match the exact endpoint and mode."
    )
    ws_url: str = Field(
        "wss://test.deribit.com/ws/api/v2",
        description="Exact allowlisted endpoint; overrides cannot redirect credentials.",
    )
    request_timeout_s: Positive = Field(
        15, description="Whole RPC deadline after limiter wait; higher tolerates slower replies."
    )
    connect_timeout_s: Positive = Field(
        15, description="WebSocket handshake deadline; higher waits longer on network failures."
    )
    max_message_bytes: Count = Field(
        16_777_216,
        description="Inbound message bound; higher supports larger dynamically discovered chains.",
    )
    retry_attempts: Count = Field(
        3,
        le=10,
        description="Public probe attempts; increases bounded reconnect attempts, never order retries.",
    )
    retry_base_s: Positive = Field(
        1, description="Base exponential retry delay for public transport failures."
    )
    retry_max_s: Positive = Field(
        10, le=60, description="Maximum retry delay; longer reduces reconnect pressure."
    )
    auth_refresh_margin_s: Positive = Field(
        60, description="Refresh this far before expiry, capped at half the token lifetime."
    )
    ticker_samples_per_asset: Count = Field(
        2,
        le=20,
        description="Detailed ticker samples per discovered chain; summaries still cover the full chain.",
    )


class BucketConfig(Section):
    capacity: Positive = Field(description="Maximum credits; increasing allows larger bursts.")
    refill_per_s: Positive = Field(
        description="Credits restored each second; increasing raises sustained traffic."
    )
    cost: Positive = Field(
        description="Credits per call; increasing lowers effective call frequency."
    )

    @model_validator(mode="after")
    def affordable(self) -> Self:
        if self.cost > self.capacity:
            raise ValueError("request cost must not exceed bucket capacity")
        return self


class RateConfig(Section):
    discovery: BucketConfig = BucketConfig(capacity=500_000, refill_per_s=10_000, cost=10_000)
    subscriptions: BucketConfig = BucketConfig(capacity=30_000, refill_per_s=10_000, cost=3_000)
    ordinary: BucketConfig = BucketConfig(capacity=5_000, refill_per_s=2_500, cost=500)


class DataConfig(Section):
    max_staleness_s: Positive = Field(
        30, description="Maximum accepted feed age; reducing tightens freshness."
    )
    max_price_divergence_bps: Positive = Field(
        25,
        description="Independent-reference divergence diagnostic threshold; reducing tightens agreement.",
    )
    min_sources_per_asset: int = Field(
        2,
        ge=2,
        description="Required independent spot sources; increasing raises data requirements.",
    )
    max_clock_skew_s: Positive = Field(
        5, description="Maximum local/server clock offset plus half round-trip uncertainty."
    )


class IngestionConfig(Section):
    coinmetrics_enabled: bool = Field(
        False,
        strict=True,
        description="Opt in to public daily BTC/ETH market-cap and MVRV observations; no realized-cap/SOPR, exchange attribution or publication-time inference.",
    )
    coinmetrics_poll_interval_s: int = Field(
        3600,
        strict=True,
        ge=60,
        le=86400,
        description="Pause after every bounded Community API poll, including failures; no immediate retry or provider fallback.",
    )
    coinmetrics_lookback_days: int = Field(
        7,
        strict=True,
        ge=1,
        le=30,
        description="Explicit inclusive provider daily-label request window ending on the prior UTC day; missing dates never imply complete history.",
    )
    coinmetrics_max_data_age_s: int = Field(
        259200,
        strict=True,
        ge=86400,
        le=2592000,
        description="Maximum age of latest provider daily period label; separate from response receipt and not independent finality.",
    )
    coinmetrics_max_pages: int = Field(
        4,
        strict=True,
        ge=1,
        le=8,
        description="Whole-poll page budget; a continuation beyond this cap refuses success instead of truncating history.",
    )
    coinmetrics_max_rows: int = Field(
        128,
        strict=True,
        ge=1,
        le=1024,
        description="Maximum daily rows staged across the configured BTC/ETH poll; overflow refuses before data publication.",
    )
    coinmetrics_max_response_bytes: int = Field(
        2000000,
        strict=True,
        ge=1024,
        le=4000000,
        description="Decoded HTTP entity budget per page; fixed HTTPS origin and endpoint, no automatic redirects.",
    )
    coinmetrics_timeout_s: int = Field(
        30,
        strict=True,
        ge=1,
        le=120,
        description="Bounded whole-poll timeout in seconds; transport failure retains explicit failed health.",
    )
    stablecoin_supply_enabled: bool = Field(
        False,
        description="Opt in to read-only Ethereum mainnet USDC/USDT supply at one provider-asserted finalized block; no all-chain supply or exchange balances.",
    )
    stablecoin_supply_poll_interval_s: float = Field(
        600,
        ge=300,
        le=86400,
        allow_inf_nan=False,
        description="Pause between paired stablecoin supply polls; native observations and poll receipts remain distinct.",
    )
    stablecoin_supply_max_block_age_s: float = Field(
        1800,
        ge=60,
        le=3600,
        allow_inf_nan=False,
        description="Maximum native finalized header age for paired ERC-20 supply; provider assertion is not independently verified finality.",
    )
    tips_yield_enabled: bool = Field(
        False,
        description="Opt in to official FRED DFII10 daily 10-year TIPS real-yield CSV snapshots; dates never substitute for unknown publication times.",
    )
    tips_yield_poll_interval_s: float = Field(
        3600,
        ge=900,
        le=86400,
        allow_inf_nan=False,
        description="Pause between bounded official daily real-yield window snapshots; revisions and missing values remain retained.",
    )
    tips_yield_history_days: int = Field(
        90,
        strict=True,
        ge=1,
        le=366,
        description="Calendar-day window requested per DFII10 snapshot, not a claim of complete historical publication knowledge.",
    )
    liquidations_enabled: bool = Field(
        False,
        description="Opt in to public Binance USDT perpetual forceOrder snapshots for configured assets; the 1000ms stream is lossy and is not a complete liquidation ledger.",
    )
    kraken_auto_recovery_enabled: bool = Field(
        False,
        description="Opt in to bounded public REST repair of forward Kraken trade gaps; failure halts capture and affected bars stay partial.",
    )
    kraken_backfill_max_pages: int = Field(
        5,
        ge=1,
        le=20,
        description="Maximum public REST trade-history pages per explicit bounded backfill; exhaustion fails coverage.",
    )
    kraken_backfill_page_size: int = Field(
        1000,
        ge=1,
        le=1000,
        description="Trades requested per public REST history page; recovered transport timestamps remain separate from WebSocket events.",
    )
    kraken_backfill_timeout_s: float = Field(
        60,
        ge=1,
        le=300,
        allow_inf_nan=False,
        description="Total deadline for a bounded public trade-history fetch.",
    )
    kraken_backfill_interval_s: float = Field(
        1,
        ge=1,
        le=30,
        allow_inf_nan=False,
        description="Minimum pause between REST history pages; no automatic retry on rate limiting.",
    )
    policy_news_sources: tuple[Literal["federal_reserve", "ecb"], ...] = Field(
        ("federal_reserve", "ecb"),
        description="Public official policy RSS metadata; no model classification or economic-calendar coverage is implied.",
    )
    crypto_news_enabled: bool = Field(
        False,
        description="Opt in to fixed public CoinDesk RSS metadata and bound poll health; no classification, first-publication verification or all-news coverage.",
    )
    crypto_news_poll_interval_s: float = Field(
        300,
        ge=300,
        le=86400,
        allow_inf_nan=False,
        description="Pause after a bounded crypto RSS poll, including failures; minimum five minutes and no immediate retries.",
    )
    news_poll_interval_s: float = Field(
        300,
        ge=60,
        allow_inf_nan=False,
        description="Seconds between public policy RSS polls; publication time and polling health are tracked separately.",
    )
    policy_news_max_publication_age_s: float = Field(
        2_592_000,
        ge=1,
        le=31_536_000,
        allow_inf_nan=False,
        description="Descriptive recent-publication window for exact policy RSS metadata; separate from poll receipt age and never trading approval or independently verified first publication.",
    )
    spot_trades_enabled: bool = Field(
        True,
        description="Discover public Kraken USD spot trade streams and build local closed bars.",
    )
    bar_timeframes_s: tuple[Literal[300, 900, 3600, 14400, 86400], ...] = Field(
        (300, 900, 3600, 14400, 86400),
        min_length=1,
        description="UTC bar intervals built from underlying trades; forming and empty intervals are not synthesized.",
    )
    bar_lateness_s: NonNegative = Field(
        5,
        description="Wait after a candle closes before emitting it; later unexpected trades halt capture instead of revising history silently.",
    )
    bar_checkpoint_on_stop: bool = Field(
        False,
        strict=True,
        description="Opt in to a committed native-input bar checkpoint after producers stop and the writer drains; this is not periodic crash recovery.",
    )
    bar_checkpoint_max_events: int = Field(
        10000,
        strict=True,
        ge=1,
        le=100000,
        description="Maximum exact union of open-bar native inputs and sequence anchors for optional bar checkpoints/resume; overflow refuses before publication.",
    )
    eth_gas_enabled: bool = Field(
        False,
        description="Opt in to public Ethereum mainnet finalized-block gas samples; read-only.",
    )
    eth_gas_provider: Literal["publicnode", "cloudflare", "drpc"] = Field(
        "publicnode",
        description="Explicit public no-key Ethereum RPC provider; source identities and restart checkpoints remain provider-scoped, with no automatic fallback.",
    )
    solana_network_enabled: bool = Field(
        False,
        description="Opt in to read-only public Solana performance and visible vote-account activated stake; no transactions.",
    )
    solana_network_poll_interval_s: float = Field(
        300,
        ge=120,
        allow_inf_nan=False,
        description="Pause between Solana network samples; failures wait ten minutes.",
    )
    solana_network_timeout_s: float = Field(
        90,
        ge=1,
        le=300,
        allow_inf_nan=False,
        description="Whole Solana genesis/performance/time/stake collection deadline.",
    )
    solana_network_max_sample_age_s: float = Field(
        180,
        ge=60,
        le=3600,
        allow_inf_nan=False,
        description="Maximum estimated sample-end slot age at performance receipt; stake snapshots use receipt time only.",
    )
    eth_gas_poll_interval_s: float = Field(
        120,
        ge=60,
        allow_inf_nan=False,
        description="Ethereum gas polling interval; failures back off 600 seconds.",
    )
    eth_gas_max_block_age_s: float = Field(
        1800,
        ge=60,
        le=3600,
        allow_inf_nan=False,
        description="Maximum finalized-block age; finality is delayed, not a live gas quote.",
    )
    bea_calendar_enabled: bool = Field(
        False,
        description="Opt in to official BEA dated release schedule snapshots; no release values or consensus.",
    )
    bea_calendar_poll_interval_s: float = Field(
        3600,
        ge=900,
        allow_inf_nan=False,
        description="Seconds between BEA calendar polls, including failures; minimum fifteen minutes.",
    )
    fomc_calendar_enabled: bool = Field(
        False,
        description="Opt in to official Federal Reserve date-only FOMC meeting snapshots; no exact announcement time or unscheduled meeting coverage.",
    )
    fomc_calendar_poll_interval_s: float = Field(
        3600,
        ge=900,
        allow_inf_nan=False,
        description="Seconds between official FOMC schedule polls, including failures; minimum fifteen minutes.",
    )
    binance_enabled: bool = Field(
        default=False,
        description="Opt in to public Binance USDT perpetual/funding/native OI snapshots; no orders.",
    )
    binance_funding_history_enabled: bool = Field(
        False,
        description="Opt in to recurring bounded recent Binance settled-funding downloads; retains poll receipts, no orders or full-history claim.",
    )
    binance_funding_history_hours: int = Field(
        48,
        strict=True,
        ge=1,
        le=720,
        description="Rolling funding-history window per poll; older gaps are outside automatic repair coverage.",
    )
    binance_funding_history_page_size: int = Field(
        200,
        strict=True,
        ge=1,
        le=1000,
        description="Funding observations requested per history page; native values remain unchanged.",
    )
    binance_funding_history_max_pages: int = Field(
        10,
        strict=True,
        ge=1,
        le=20,
        description="Maximum history pages per asset/poll; exhausted coverage fails before market publication.",
    )
    binance_funding_history_timeout_s: float = Field(
        90,
        ge=1,
        le=300,
        allow_inf_nan=False,
        description="Whole discovery/history deadline per asset; transport and page budgets also apply.",
    )
    binance_funding_history_poll_interval_s: float = Field(
        900,
        ge=600,
        allow_inf_nan=False,
        description="Pause after a funding-history cycle, including failures; minimum ten minutes and no immediate retries.",
    )
    binance_taker_enabled: bool = Field(
        False,
        description="Opt in to public Binance five-minute taker buy/sell aggregates; native volumes only, no orders.",
    )
    binance_account_ratio_enabled: bool = Field(
        False,
        description="Opt in to public Binance global account-ratio samples; reported fields preserved, no orders.",
    )
    binance_account_ratio_poll_interval_s: float = Field(
        300,
        ge=300,
        allow_inf_nan=False,
        description="Global account-ratio poll interval; at least five minutes, failures wait at least ten minutes.",
    )
    binance_taker_poll_interval_s: float = Field(
        300,
        ge=300,
        allow_inf_nan=False,
        description="Pause between taker-flow polls; minimum five minutes, failed cycles wait at least ten minutes.",
    )
    binance_poll_interval_s: float = Field(
        default=30, ge=10, description="Binance poll interval; failed cycles back off 600 seconds."
    )
    bybit_enabled: bool = Field(
        default=False,
        description="Opt in to public Bybit USDT perpetual funding/OI snapshots; no account or orders.",
    )
    bybit_poll_interval_s: float = Field(
        default=30,
        ge=10,
        description="Public Bybit polling interval; transport, data and access failures back off 600 seconds.",
    )
    derivatives_enabled: bool = Field(
        True,
        description="Collect public testnet perpetual ticker/funding and available BTC/ETH volatility-index candles.",
    )
    derivatives_poll_interval_s: Positive = Field(
        20,
        description="Pause between derivative snapshot cycles; actual cycle also includes network request time.",
    )
    volatility_lookback_minutes: Count = Field(
        5,
        le=60,
        description="Recent closed one-minute volatility-index candle window; not a historical archive.",
    )
    duration_s: Positive = Field(
        60,
        description="Supervised capture duration; process closes sockets and drains writes at the end.",
    )
    max_stream_instruments_per_asset: Count | None = Field(
        6,
        description="Per-asset streaming cap for local pilot; null requests all instruments. A cap marks coverage partial.",
    )
    subscription_batch_size: Count = Field(
        50, le=100, description="Channels per subscribe call; larger batches reduce credit cost."
    )
    stream_universe_refresh_interval_s: float | None = Field(
        None,
        ge=60,
        le=86400,
        allow_inf_nan=False,
        description="Optional seconds after complete stream acknowledgement before closing, rediscovering and resubscribing; null refreshes only on reconnect. Rotation records a gap, not historical continuity.",
    )
    option_universe_max_age_s: float = Field(
        3600,
        ge=60,
        le=86400,
        allow_inf_nan=False,
        description="Maximum age of the exact discovery manifest bound to current option subscriptions; independent of quote freshness, with no listing-continuity guarantee.",
    )
    queue_capacity: Count = Field(
        20_000,
        description="Maximum queued events; saturation raises rather than silently dropping data.",
    )
    queue_timeout_s: Positive = Field(
        10, description="Backpressure deadline; exceeding it fails capture loudly."
    )
    storage_batch_size: Count = Field(
        500, description="Maximum events per atomic DuckDB insert transaction."
    )
    archive_rotation_interval_s: float | None = Field(
        None,
        strict=True,
        ge=1,
        le=86400,
        allow_inf_nan=False,
        description="Opt in to periodic cumulative committed-store checkpoints during finite capture; null creates no periodic task. Each checkpoint follows a commit barrier, without claiming complete history or a complete source frontier.",
    )
    archive_rotation_partition_ms: int = Field(
        86400000,
        strict=True,
        ge=1,
        le=86400000,
        description="Availability bucket width for periodic checkpoints, using the later of source and receipt clocks; both clocks must be at or before the selected cutoff.",
    )
    archive_rotation_max_partitions: int = Field(
        366,
        strict=True,
        ge=1,
        le=1000,
        description="Maximum nonempty availability partitions per checkpoint; exceeding the bound fails capture without publishing a completed checkpoint.",
    )
    archive_rotation_max_snapshots: int = Field(
        10,
        strict=True,
        ge=1,
        le=1000,
        description="Total periodic checkpoint budget per finite capture; the next due checkpoint after this many successes fails capture instead of silently stopping archives or creating unbounded files.",
    )
    flush_interval_s: Positive = Field(
        1, description="Maximum wait before persisting a partially filled batch."
    )
    spot_poll_interval_s: Positive = Field(
        10,
        description="Coinbase/Kraken public reference poll period; lower increases request load.",
    )
    chain_poll_interval_s: Positive = Field(
        30, description="Full option summary-chain refresh period; lower increases request load."
    )
    reconnect_delay_s: Positive = Field(
        1,
        description="Initial stream reconnect delay; subsequent failures back off to venue.retry_max_s.",
    )
    reference_sources: tuple[Literal["coinbase", "kraken"], ...] = Field(
        ("coinbase", "kraken"),
        description="Independent public spot-reference sources; no private accounts are used.",
    )
    hot_buffer_events: Count = Field(
        2000, description="In-memory recent-event ring capacity per source/asset/kind."
    )
    redis_url: str | None = Field(
        None,
        description="Optional Redis stream mirror; null uses the bounded in-process bus. Set credential-bearing URLs only through SELECTIVE_INGESTION__REDIS_URL.",
    )


class Weights(Section):
    technical: Probability = Field(
        0.35, description="Technical pillar contribution; weights must sum to one."
    )
    volatility_flow: Probability = Field(
        0.35, description="Options/positioning pillar contribution; weights must sum to one."
    )
    fundamental: Probability = Field(
        0.30, description="News/macro pillar contribution; weights must sum to one."
    )

    @model_validator(mode="after")
    def normalized(self) -> Self:
        if not math.isclose(
            sum((self.technical, self.volatility_flow, self.fundamental)), 1, abs_tol=1e-9
        ):
            raise ValueError("pillar weights must sum to one")
        return self


class SignalConfig(Section):
    weights: Weights = Weights()
    lookback_days_iv_rank: Count = Field(
        252, description="IV rank/percentile history; longer windows reflect slower regimes."
    )
    news_decay_halflife_hours: Positive = Field(
        8, description="News weight half-life; higher retains old news longer."
    )
    news_dedup_similarity: Probability = Field(
        0.88, description="Embedding similarity duplicate threshold; lower merges more stories."
    )


class GateConfig(Section):
    min_composite_conviction: Percent = Field(
        72, description="Minimum absolute conviction; higher selects fewer setups."
    )
    min_pillar_agreement: Literal[3] = Field(
        3, description="All three directional pillars required; lowering is not supported."
    )
    min_pillar_score_each: Percent = Field(
        40, description="Minimum absolute score per pillar; higher rejects weak evidence."
    )
    min_confidence_each: Probability = Field(
        0.55, description="Minimum confidence per pillar; higher is more selective."
    )
    min_expectancy_R: NonNegative = Field(
        0.35, description="Required after-cost expectancy in risk units; strictly exceed this."
    )
    min_reward_risk: Positive = Field(
        2, description="Minimum structural reward/risk at targets; higher selects larger payoffs."
    )
    max_spread_cost_pct_of_debit: Percent = Field(
        6, description="Maximum spread burden; lower rejects more expensive entries."
    )
    max_trades_per_month: Count = Field(
        10, description="30-day selectivity alert threshold; never a target or minimum."
    )
    cooldown_after_n_losses: Count = Field(
        2, description="Consecutive losses before benching asset; lower triggers earlier."
    )
    cooldown_hours: Positive = Field(
        48, description="Asset loss-cooldown duration; higher waits longer."
    )
    min_samples: Count = Field(
        30,
        description="Historical analogues required for empirical prior; higher needs more evidence.",
    )
    p_haircut: Probability = Field(
        0.85,
        description="Fallback probability multiplier; lower discounts low-evidence priors more.",
    )


class VolConfig(Section):
    buy_premium_below_iv_pct: Percent = Field(
        30, description="Upper IV percentile for cheap premium classification."
    )
    sell_premium_above_iv_pct: Percent = Field(
        60, description="Lower IV percentile for rich premium classification."
    )
    min_move_edge_pct: NonNegative = Field(
        20, description="Required model move excess over implied move; higher is stricter."
    )
    require_panic_floor_for_short_premium: Literal[True] = Field(
        True, description="Short premium always requires a floor during panic."
    )


class StructureConfig(Section):
    min_dte: Count = Field(21, description="Minimum entry DTE; higher avoids near-expiry gamma.")
    max_dte: Count = Field(60, description="Maximum entry DTE; higher admits longer expiries.")
    min_dte_exit: Count = Field(
        14, description="Long-premium expiry exit threshold; higher exits earlier."
    )
    target_long_delta: tuple[Probability, Probability] = Field(
        (0.25, 0.40),
        description="Allowed absolute long-leg delta band; higher selects closer-to-money options.",
    )
    short_strike_delta: tuple[Probability, Probability] = Field(
        (0.15, 0.25),
        description="Allowed absolute short-leg delta band; higher brings shorts closer to money.",
    )
    catalyst_buffer_days: Count = Field(
        7, description="Minimum option life after catalyst; higher leaves more time."
    )


class LiquidityConfig(Section):
    max_spread_pct_of_mid: Positive = Field(
        8, description="Per-leg spread ceiling relative to mid; lower is stricter."
    )
    min_open_interest: NonNegative = Field(
        50, description="Minimum per-leg OI; higher demands more participation."
    )
    min_24h_volume: NonNegative = Field(
        20, description="Minimum per-leg daily volume; higher rejects quiet strikes."
    )
    book_depth_multiple: Positive = Field(
        3, description="Required top size / intended quantity; higher demands more depth."
    )


class RiskConfig(Section):
    risk_per_trade_pct: Percent = Field(
        1, gt=0, description="Planned fixed-fractional max-loss budget as percent of equity."
    )
    max_risk_per_trade_pct: Percent = Field(
        2, gt=0, description="Absolute per-trade risk ceiling; lower constrains sizing."
    )
    kelly_fraction: Probability = Field(
        0.25, description="Fraction of calibrated Kelly sizing; higher increases size."
    )
    kelly_cap_pct: Percent = Field(2, description="Kelly size ceiling as percent of equity.")
    max_concurrent_positions: Count = Field(
        4, description="Total position count limit; lower reduces concurrency."
    )
    max_positions_per_asset: Count = Field(
        1, description="Per-asset position cap; never permits adding to losers."
    )
    max_crypto_beta_positions: Count = Field(
        3, le=3, description="Correlated crypto position cap; cannot allow a fourth."
    )
    allow_naked_short: Literal[False] = Field(
        False, description="Undefined-risk shorts disabled; no override implemented in this build."
    )
    blackout_before_min: NonNegative = Field(
        120, description="Tier-1 pre-event blackout; higher stops entries earlier."
    )
    blackout_after_min: NonNegative = Field(
        30, description="Tier-1 post-event blackout; higher waits longer."
    )
    max_tail_loss_pct: Percent | None = Field(
        None, description="Unspecified tail-loss ceiling; null must block future arming."
    )
    tail_paths: Count = Field(
        10_000,
        description="Monte Carlo paths; more improve tail resolution at higher compute cost.",
    )
    gap_shock_pct: Positive = Field(
        30, description="Absolute underlying gap stress; higher stresses larger gaps."
    )
    iv_shock_points: Positive = Field(
        50, description="Absolute IV shock in volatility points, not relative percent."
    )


class GreeksConfig(Section):
    max_net_delta_pct_equity: Positive = Field(
        25, description="Delta-dollar equity limit; lower constrains direction exposure."
    )
    max_net_vega_pct_equity: Positive = Field(
        8, description="Vega equity limit per one IV percentage point; lower tightens vol exposure."
    )
    max_net_theta_daily_pct: Positive = Field(
        0.6, description="Daily theta equity limit; lower tightens time-decay exposure."
    )
    max_net_gamma_notional: Positive | None = Field(
        None,
        description="Unspecified gamma-dollar limit; null blocks future arming; units must be resolved.",
    )


class DrawdownConfig(Section):
    daily_stop_pct: Positive = Field(
        3, le=100, description="Daily loss halt threshold; lower halts sooner for 24 hours."
    )
    weekly_stop_pct: Positive = Field(
        6, le=100, description="Weekly loss threshold; halt 72 hours and require review."
    )
    monthly_stop_pct: Positive = Field(
        10, le=100, description="Monthly loss threshold; review and fresh validation required."
    )
    peak_to_trough_pct: Positive = Field(
        15, le=100, description="Peak equity drawdown threshold; return to paper mode."
    )


class ExecutionConfig(Section):
    passive_first: Literal[True] = Field(
        True, description="Passive limit execution required; no market-option order path."
    )
    max_slippage_bps: NonNegative = Field(
        40, description="Maximum entry escalation; lower stands down sooner."
    )
    slice_count: Count = Field(
        3, description="Target number of entry slices; higher reduces quantity per slice."
    )
    leg_timeout_s: Positive = Field(
        20, description="Time allowed for second leg before long-leg unwind."
    )
    cancel_on_disconnect: Literal[True] = Field(
        True, description="Mandatory exchange connection cancellation protection."
    )
    reconciliation_interval_s: Positive = Field(
        60, le=60, description="Maximum interval between portfolio reconciliations."
    )


class ExitConfig(Section):
    long_profit_pct: Positive = Field(
        100, description="Long debit profit target; higher waits for larger profits."
    )
    long_scale_out_fraction: Probability = Field(
        0.5, gt=0, description="Fraction closed at first long-premium target."
    )
    long_stop_loss_pct: Positive = Field(
        50, le=100, description="Debit-loss stop threshold; lower exits losses earlier."
    )
    iv_crush_exit_pts: Positive = Field(
        15, description="Provisional IV-point crush stop; requires validation before use."
    )
    short_take_profit_pct: Positive = Field(
        50, le=100, description="Credit profit captured before exit."
    )
    short_loss_credit_multiple: Positive = Field(
        2, description="Loss / original credit defensive exit threshold."
    )
    short_exit_dte: Count = Field(
        21, description="Short-premium close/roll threshold; higher avoids gamma earlier."
    )
    reevaluate_atr_multiple: Positive = Field(
        2, description="Daily underlying move triggering thesis re-evaluation."
    )


class GoldConfig(Section):
    mode: Literal["auto", "options", "synthetic", "external", "disabled"] = Field(
        "auto",
        description="Requested route; none is tradable until verified with appropriate risk controls.",
    )
    max_basis_bps: Positive = Field(
        150, description="Wrapper/spot basis veto; lower is more conservative."
    )
    session_filter: Literal["LBMA_COMEX"] = Field(
        "LBMA_COMEX", description="Requires venue holiday and DST calendars, not crypto 24/7 hours."
    )
    external_adapter: str | None = Field(
        None, description="Operator-selected gold broker; null means no external route."
    )


class PromotionConfig(Section):
    min_paper_days: int = Field(
        60, ge=60, description="Minimum observed testnet days before promotion."
    )
    min_paper_trades: int = Field(
        25,
        ge=25,
        description="Minimum closed paper trades; selectivity may take longer than 60 days.",
    )
    min_paper_expectancy_R: NonNegative = Field(
        0.25, description="Minimum paper after-cost expectancy in risk units."
    )
    max_paper_vs_backtest_divergence_pct: Percent = Field(
        40, description="Maximum paper/backtest expectancy divergence."
    )
    required_brier_score: Probability = Field(
        0.22, description="Maximum Brier score; lower requires better calibration."
    )
    live_ramp: tuple[Probability, Probability, Probability] = Field(
        (0.25, 0.5, 1), description="Initial, intermediate and validated live size fractions."
    )
    trades_per_ramp_stage: int = Field(
        20, ge=20, description="Completed trades before each possible ramp step."
    )
    min_live_trades: int = Field(
        40,
        ge=40,
        description="Minimum trades for full size, with expectancy tolerance also required.",
    )
    artifact_max_age_days: Count = Field(
        90,
        le=90,
        description="Maximum validation artifact age; lower demands more recent evidence.",
    )
    max_pbo: Probability = Field(0.35, description="Strict PBO upper bound for promotion.")
    min_dsr_confidence: Probability = Field(
        0.95, description="Deflated Sharpe confidence required against zero benchmark."
    )
    positive_regimes_required: Count = Field(
        3, description="Minimum profitable claimed regimes; needs adequate samples per regime."
    )


class TaxConfig(Section):
    jurisdiction: Literal["IN"] = Field(
        "IN", description="Operator reporting jurisdiction; not a legal classification."
    )
    asymmetric_regime: bool = Field(
        True,
        description="Planned asymmetric-tax scenario; applicability requires accountant review.",
    )
    vda_rate_pct: Percent = Field(
        31.2,
        description="User-supplied scenario rate including cess, excluding any applicable surcharge.",
    )
    tds_rate_pct: Percent = Field(
        1,
        description="User-supplied withholding scenario; track receivable/cash drag separately from final tax.",
    )
    specified_person_threshold_inr: NonNegative = Field(
        50_000, description="User-supplied annual threshold scenario, subject to current guidance."
    )
    other_person_threshold_inr: NonNegative = Field(
        10_000, description="User-supplied alternative annual threshold scenario."
    )
    retention_years: int = Field(
        7, ge=7, description="Minimum audit-record retention; longer increases storage obligations."
    )
    fx_source: str | None = Field(
        None,
        description="Unselected historical INR FX source; exports must not invent conversion rates.",
    )


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SELECTIVE_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
        allow_inf_nan=False,
    )
    system: SystemConfig = SystemConfig()
    venue: VenueConfig = VenueConfig()
    rate_limits: RateConfig = RateConfig()
    data: DataConfig = DataConfig()
    ingestion: IngestionConfig = IngestionConfig()
    signals: SignalConfig = SignalConfig()
    gate: GateConfig = GateConfig()
    vol: VolConfig = VolConfig()
    structure: StructureConfig = StructureConfig()
    liquidity: LiquidityConfig = LiquidityConfig()
    risk: RiskConfig = RiskConfig()
    greeks: GreeksConfig = GreeksConfig()
    drawdown: DrawdownConfig = DrawdownConfig()
    execution: ExecutionConfig = ExecutionConfig()
    exits: ExitConfig = ExitConfig()
    gold: GoldConfig = GoldConfig()
    promotion: PromotionConfig = PromotionConfig()
    tax: TaxConfig = TaxConfig()

    @model_validator(mode="after")
    def coherent(self) -> Self:
        expected = "production" if self.system.mode == "live" else "testnet"
        endpoint = {
            "testnet": "wss://test.deribit.com/ws/api/v2",
            "production": "wss://www.deribit.com/ws/api/v2",
        }[expected]
        if self.venue.key_profile != expected or self.venue.ws_url != endpoint:
            raise ValueError("mode, key profile and allowlisted endpoint must agree")
        if len(set(self.system.assets)) != len(self.system.assets):
            raise ValueError("duplicate assets are not permitted")
        if self.ingestion.coinmetrics_enabled and not any(
            asset in ("BTC", "ETH") for asset in self.system.assets
        ):
            raise ValueError(
                "Coin Metrics enabled capture requires BTC or ETH in configured assets"
            )
        if not self.structure.min_dte_exit < self.structure.min_dte <= self.structure.max_dte:
            raise ValueError("require exit DTE < minimum entry DTE <= maximum entry DTE")
        for lo, hi in (self.structure.target_long_delta, self.structure.short_strike_delta):
            if lo > hi:
                raise ValueError("delta bands must be ordered")
        if self.risk.risk_per_trade_pct > self.risk.max_risk_per_trade_pct:
            raise ValueError("fixed risk exceeds absolute risk ceiling")
        if self.risk.kelly_cap_pct > self.risk.max_risk_per_trade_pct:
            raise ValueError("Kelly cap exceeds absolute risk ceiling")
        if self.vol.buy_premium_below_iv_pct >= self.vol.sell_premium_above_iv_pct:
            raise ValueError("premium buy and sell regimes overlap")
        if (
            tuple(sorted(self.promotion.live_ramp)) != self.promotion.live_ramp
            or self.promotion.live_ramp[0] > 0.25
        ):
            raise ValueError("capital ramp must be monotone and start at <= 25%")
        return self


def _read_yaml(path: Path) -> dict[str, JsonValue]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    return TypeAdapter(dict[str, JsonValue]).validate_python(value)


def _merge(base: dict[str, JsonValue], override: dict[str, JsonValue]) -> dict[str, JsonValue]:
    merged = dict(base)
    for key, value in override.items():
        old = merged.get(key)
        merged[key] = (
            _merge(old, value) if isinstance(old, dict) and isinstance(value, dict) else value
        )
    return merged


def load_config(config_dir: Path, profile: str = "testnet") -> AppConfig:
    if profile not in {"testnet", "production"}:
        raise ValueError("unknown profile")
    values = _merge(
        _read_yaml(config_dir / "default.yaml"), _read_yaml(config_dir / f"{profile}.yaml")
    )
    # Settings env data is parsed first; explicit deep merge makes env highest precedence.
    from pydantic_settings import EnvSettingsSource

    env = TypeAdapter(dict[str, JsonValue]).validate_python(EnvSettingsSource(AppConfig)())
    config = AppConfig.model_validate(_merge(values, env))
    if config.venue.key_profile != profile:
        raise ValueError("environment override cannot switch the selected credential profile")
    return config
