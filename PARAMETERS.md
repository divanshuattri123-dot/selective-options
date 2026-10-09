# Configuration guide

System, venue, rate_limits, ingestion and data-quality settings are operational in the M2 pilot. Other sections are validated policy defaults reserved for later milestones. Raising a threshold never guarantees improved expectancy.

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
