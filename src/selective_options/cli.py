import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import TypedDict

from pydantic import ValidationError

from .archive import restore_archive
from .archive_rotation import rotate_archive
from .atr_percentile_research import research_atr_percentile
from .capture_lifecycle import CaptureStopRequest, scoped_sigterm_stop
from .config import load_config
from .ingestion import capture
from .interlock import LiveEngine
from .keltner_research import research_keltner
from .logging import configure_logging
from .option_flow_research import research_option_flow
from .option_skew_research import research_option_skew
from .options_research import research_options
from .range_volatility_research import research_range_volatility
from .recovery import replay_store
from .research import research_backtest, research_features
from .rotation_restore import restore_rotation
from .startup import probe
from .stochastic_rsi_research import research_stochastic_rsi
from .trade_backfill import backfill_trades


class CaptureOptions(TypedDict, total=False):
    resume_bars_from: Path
    save_bars_on_stop: bool
    stop_request: CaptureStopRequest
    resume_solana_from: Path
    resume_crypto_news_from: Path


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Testnet readiness and supervised data capture; cannot trade"
    )
    result.add_argument("--config-dir", type=Path, default=Path("config"))
    result.add_argument("--profile", choices=("testnet", "production"), default="testnet")
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "check-config", help="Validate configuration and print status without secrets"
    )
    probe_parser = commands.add_parser(
        "probe", help="Public testnet discovery and complete summary-chain capture"
    )
    probe_parser.add_argument(
        "--authenticated",
        action="store_true",
        help="Check actual key scope and connection COD; no orders",
    )
    probe_parser.add_argument("--output", type=Path, default=Path("state/probe.json"))
    capture_parser = commands.add_parser(
        "capture", help="Finite public ingestion pilot; cannot trade"
    )
    capture_parser.add_argument("--output", type=Path, required=True)
    capture_parser.add_argument("--duration", type=float, default=None)
    capture_parser.add_argument(
        "--graceful-stop-signals",
        action="store_true",
        help="Opt in to scoped POSIX SIGTERM stop/drain; unsupported on Windows. SIGINT and task cancellation still fail capture",
    )
    capture_parser.add_argument(
        "--resume-bars-from",
        type=Path,
        default=None,
        help="Explicit committed open-bar checkpoint; verifies the closed copied seed before producers",
    )
    capture_parser.add_argument(
        "--save-bars-on-stop",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Opt in to a bar checkpoint after producers stop and the writer drains; not periodic crash recovery",
    )
    capture_parser.add_argument(
        "--resume-eth-gas-from",
        type=Path,
        default=None,
        help="Validate and copy a committed finalized ETH gas anchor before starting capture",
    )
    capture_parser.add_argument(
        "--resume-stablecoin-from",
        type=Path,
        default=None,
        help="Validate and copy a healthy bound paired supply anchor; failures/staleness refuse",
    )
    capture_parser.add_argument(
        "--resume-coinmetrics-from",
        type=Path,
        default=None,
        help="Validate and copy bounded BTC/ETH daily valuation history before capture producers",
    )
    capture_parser.add_argument(
        "--resume-solana-from",
        type=Path,
        default=None,
        help="Validate all eligible SOL network poll bindings and copy the latest exact healthy three-row seed before producers",
    )
    capture_parser.add_argument(
        "--resume-crypto-news-from",
        type=Path,
        default=None,
        help="Validate a closed CoinDesk DuckDB/Parquet/archive/outbox checkpoint and preserve first receipts before crypto-news capture",
    )
    capture_parser.add_argument(
        "--recovery-marker",
        default=None,
        help="Explicit committed mixed-source plan in --resume-trades-from; validates and resumes its sequence",
    )
    capture_parser.add_argument(
        "--resume-trades-from",
        type=Path,
        default=None,
        help="Read committed Kraken trade checkpoints from a prior database; bounded snapshot recovery only",
    )
    replay_parser = commands.add_parser(
        "replay", help="Replay the durable Redis outbox; consumers deduplicate event_id"
    )
    replay_parser.add_argument("--database", type=Path, required=True)
    restore_parser = commands.add_parser(
        "restore", help="Verify an event archive and restore into a new directory"
    )
    restore_parser.add_argument("--archive", type=Path, required=True)
    restore_parser.add_argument("--output", type=Path, required=True)
    rotation_parser = commands.add_parser(
        "rotate-archive",
        help="Write immutable availability partitions from a closed source; no deletion",
    )
    rotation_parser.add_argument("--database", type=Path, required=True)
    rotation_parser.add_argument("--output", type=Path, required=True)
    rotation_parser.add_argument("--as-of-ms", type=int, required=True)
    rotation_parser.add_argument("--partition-ms", type=int, default=86_400_000)
    rotation_parser.add_argument("--max-partitions", type=int, default=366)
    rotation_parser.add_argument("--batch-size", type=int, default=None)
    rotation_restore_parser = commands.add_parser(
        "restore-rotation",
        help="Verify every catalog part and publish one complete restored database",
    )
    rotation_restore_parser.add_argument("--catalog", type=Path, required=True)
    rotation_restore_parser.add_argument("--output", type=Path, required=True)
    rotation_restore_parser.add_argument("--batch-size", type=int, default=None)
    backfill_parser = commands.add_parser(
        "backfill-trades",
        help="Verify an explicit Kraken REST trade range in a new archive; does not repair a live stream",
    )
    backfill_parser.add_argument("--database", type=Path, required=True)
    backfill_parser.add_argument("--output", type=Path, required=True)
    backfill_parser.add_argument("--symbol", required=True)
    backfill_parser.add_argument("--through-trade-id", type=int, required=True)
    features_parser = commands.add_parser(
        "research-features", help="Offline scoped closed-bar features; cannot trade"
    )
    features_parser.add_argument("--database", type=Path, required=True)
    features_parser.add_argument("--output", type=Path, required=True)
    features_parser.add_argument("--asset", required=True)
    features_parser.add_argument("--source", required=True)
    features_parser.add_argument("--symbol", required=True)
    features_parser.add_argument(
        "--environment", choices=("testnet", "public_reference"), required=True
    )
    features_parser.add_argument("--timeframe-s", type=int, required=True)
    features_parser.add_argument("--as-of-ms", type=int, required=True)
    features_parser.add_argument("--max-bars", type=int, default=2000)
    features_parser.add_argument(
        "--history-start-ms",
        type=int,
        default=None,
        help="Explicit inclusive bar-close window start; no automatic tail reseeding",
    )
    features_parser.add_argument("--annualization-periods-per-year", type=float, default=None)
    features_parser.add_argument("--include-trend", action="store_true")
    features_parser.add_argument("--percentile-window", type=int, default=100)
    features_parser.add_argument("--percentile-min-history", type=int, default=20)
    range_parser = commands.add_parser(
        "research-range-volatility",
        help="Offline Parkinson/Garman-Klass OHLC estimators; cannot trade",
    )
    range_parser.add_argument("--database", type=Path, required=True)
    range_parser.add_argument("--output", type=Path, required=True)
    range_parser.add_argument("--asset", required=True)
    range_parser.add_argument(
        "--environment", choices=("testnet", "public_reference"), required=True
    )
    range_parser.add_argument("--source", required=True)
    range_parser.add_argument("--symbol", required=True)
    range_parser.add_argument("--timeframe-s", type=int, required=True)
    range_parser.add_argument("--as-of-ms", type=int, required=True)
    range_parser.add_argument("--windows", type=int, nargs="+", required=True)
    range_parser.add_argument("--max-latest-age-ms", type=int, required=True)
    range_parser.add_argument("--max-bars", type=int, default=2000)
    range_parser.add_argument("--history-start-ms", type=int, default=None)
    range_parser.add_argument("--annualization-periods-per-year", type=float, default=None)
    keltner_parser = commands.add_parser(
        "research-keltner",
        help="Offline EMA/ATR Keltner channels from complete closed bars; cannot trade",
    )
    keltner_parser.add_argument("--database", type=Path, required=True)
    keltner_parser.add_argument("--output", type=Path, required=True)
    keltner_parser.add_argument("--asset", required=True)
    keltner_parser.add_argument(
        "--environment", choices=("testnet", "public_reference"), required=True
    )
    keltner_parser.add_argument("--source", required=True)
    keltner_parser.add_argument("--symbol", required=True)
    keltner_parser.add_argument("--timeframe-s", type=int, required=True)
    keltner_parser.add_argument("--as-of-ms", type=int, required=True)
    keltner_parser.add_argument("--ema-period", type=int, required=True)
    keltner_parser.add_argument("--atr-period", type=int, required=True)
    keltner_parser.add_argument("--atr-multiplier", type=float, required=True)
    keltner_parser.add_argument("--max-latest-age-ms", type=int, required=True)
    keltner_parser.add_argument("--max-bars", type=int, default=2000)
    keltner_parser.add_argument("--history-start-ms", type=int, default=None)
    stoch_parser = commands.add_parser(
        "research-stochastic-rsi",
        help="Offline causal Stochastic RSI from complete closed bars; cannot trade",
    )
    stoch_parser.add_argument("--database", type=Path, required=True)
    stoch_parser.add_argument("--output", type=Path, required=True)
    stoch_parser.add_argument("--asset", required=True)
    stoch_parser.add_argument(
        "--environment", choices=("testnet", "public_reference"), required=True
    )
    stoch_parser.add_argument("--source", required=True)
    stoch_parser.add_argument("--symbol", required=True)
    stoch_parser.add_argument("--timeframe-s", type=int, required=True)
    stoch_parser.add_argument("--as-of-ms", type=int, required=True)
    stoch_parser.add_argument("--max-latest-age-ms", type=int, required=True)
    stoch_parser.add_argument("--rsi-period", type=int, default=14)
    stoch_parser.add_argument("--stochastic-period", type=int, default=14)
    stoch_parser.add_argument("--smooth-k", type=int, default=3)
    stoch_parser.add_argument("--smooth-d", type=int, default=3)
    stoch_parser.add_argument("--max-bars", type=int, default=2000)
    stoch_parser.add_argument("--history-start-ms", type=int, default=None)
    atr_pct_parser = commands.add_parser(
        "research-atr-percentile",
        help="Offline prior-only Wilder ATR percentile from complete closed bars; cannot trade",
    )
    atr_pct_parser.add_argument("--database", type=Path, required=True)
    atr_pct_parser.add_argument("--output", type=Path, required=True)
    atr_pct_parser.add_argument("--asset", required=True)
    atr_pct_parser.add_argument(
        "--environment", choices=("testnet", "public_reference"), required=True
    )
    atr_pct_parser.add_argument("--source", required=True)
    atr_pct_parser.add_argument("--symbol", required=True)
    atr_pct_parser.add_argument("--timeframe-s", type=int, required=True)
    atr_pct_parser.add_argument("--as-of-ms", type=int, required=True)
    atr_pct_parser.add_argument("--max-latest-age-ms", type=int, required=True)
    atr_pct_parser.add_argument("--atr-period", type=int, default=14)
    atr_pct_parser.add_argument("--percentile-window", type=int, default=100)
    atr_pct_parser.add_argument("--percentile-min-history", type=int, default=20)
    atr_pct_parser.add_argument("--max-bars", type=int, default=2000)
    atr_pct_parser.add_argument("--history-start-ms", type=int, default=None)
    options_parser = commands.add_parser(
        "research-options",
        help="Offline native ATM-IV term structure with explicit policies; cannot trade",
    )
    options_parser.add_argument("--database", type=Path, required=True)
    options_parser.add_argument("--output", type=Path, required=True)
    options_parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), required=True)
    options_parser.add_argument("--as-of-ms", type=int, required=True)
    options_parser.add_argument("--option-type", choices=("call", "put"), required=True)
    options_parser.add_argument(
        "--moneyness-method", choices=("native_underlying_log_distance",), required=True
    )
    options_parser.add_argument("--tie-policy", choices=("exact_distance_refuse",), required=True)
    options_parser.add_argument("--max-quote-age-ms", type=int, required=True)
    options_parser.add_argument("--max-discovery-age-ms", type=int, required=True)
    options_parser.add_argument("--max-atm-log-moneyness", type=float, required=True)
    options_parser.add_argument("--max-underlying-mismatch-fraction", type=float, required=True)
    options_parser.add_argument("--flat-threshold-iv-points", type=float, required=True)
    options_parser.add_argument("--max-rows", type=int, default=100000)
    options_parser.add_argument("--max-expiries", type=int, default=128)
    flow_parser = commands.add_parser(
        "research-option-flow",
        help="Offline native OI and rolling-volume put/call ratios; cannot trade",
    )
    flow_parser.add_argument("--database", type=Path, required=True)
    flow_parser.add_argument("--output", type=Path, required=True)
    flow_parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), required=True)
    flow_parser.add_argument("--as-of-ms", type=int, required=True)
    flow_parser.add_argument(
        "--amount-unit-policy", choices=("deribit_option_base_currency",), required=True
    )
    flow_parser.add_argument("--volume-field", choices=("stats.volume",), required=True)
    flow_parser.add_argument("--max-quote-age-ms", type=int, required=True)
    flow_parser.add_argument("--max-discovery-age-ms", type=int, required=True)
    flow_parser.add_argument("--max-clock-skew-ms", type=int, required=True)
    flow_parser.add_argument("--max-rows", type=int, default=100000)
    flow_parser.add_argument("--max-expiries", type=int, default=128)
    skew_parser = commands.add_parser(
        "research-option-skew",
        help="Offline same-expiry native delta risk reversals; cannot trade",
    )
    skew_parser.add_argument("--database", type=Path, required=True)
    skew_parser.add_argument("--output", type=Path, required=True)
    skew_parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), required=True)
    skew_parser.add_argument("--as-of-ms", type=int, required=True)
    skew_parser.add_argument("--target-absolute-delta", type=float, required=True)
    skew_parser.add_argument("--maximum-delta-error", type=float, required=True)
    skew_parser.add_argument("--max-quote-age-ms", type=int, required=True)
    skew_parser.add_argument("--max-discovery-age-ms", type=int, required=True)
    skew_parser.add_argument("--max-clock-skew-ms", type=int, required=True)
    skew_parser.add_argument("--max-underlying-mismatch-fraction", type=float, required=True)
    skew_parser.add_argument("--max-rows", type=int, default=100000)
    skew_parser.add_argument("--max-expiries", type=int, default=128)
    backtest_parser = commands.add_parser(
        "research-backtest", help="Register an offline conservative quote simulation; cannot trade"
    )
    backtest_parser.add_argument("--input", type=Path, required=True)
    backtest_parser.add_argument("--output", type=Path, required=True)
    backtest_parser.add_argument("--registry", type=Path, required=True)
    commands.add_parser("arm", help="Demonstrate the mandatory live-trading refusal")
    return result


def main() -> int:
    args = parser().parse_args()
    configure_logging()
    try:
        config = load_config(args.config_dir, args.profile)
        configure_logging(config.system.log_level)
        if args.command == "check-config":
            print(
                json.dumps(
                    {
                        "config": "VALID",
                        "mode": config.system.mode,
                        "profile": config.venue.key_profile,
                        "trading_enabled": False,
                        "milestone": 2,
                        "milestone_status": "PARTIAL",
                    }
                )
            )
        elif args.command == "arm":
            LiveEngine().arm()
        elif args.command == "capture":
            capture_options: CaptureOptions = {}
            if args.resume_bars_from is not None:
                capture_options["resume_bars_from"] = args.resume_bars_from
            if args.save_bars_on_stop is not None:
                capture_options["save_bars_on_stop"] = args.save_bars_on_stop
            if args.resume_solana_from is not None:
                capture_options["resume_solana_from"] = args.resume_solana_from
            if args.resume_crypto_news_from is not None:
                capture_options["resume_crypto_news_from"] = args.resume_crypto_news_from

            async def run_capture() -> dict[str, object]:
                async def collect() -> dict[str, object]:
                    return await capture(
                        config,
                        args.output,
                        args.duration,
                        args.resume_trades_from,
                        recovery_marker=args.recovery_marker,
                        resume_eth_gas_from=args.resume_eth_gas_from,
                        resume_stablecoin_from=args.resume_stablecoin_from,
                        resume_coinmetrics_from=args.resume_coinmetrics_from,
                        **capture_options,
                    )

                if args.graceful_stop_signals:
                    request = CaptureStopRequest()
                    capture_options["stop_request"] = request
                    with scoped_sigterm_stop(request):
                        return await collect()
                return await collect()

            report_data = asyncio.run(run_capture())
            print(json.dumps(report_data, indent=2))
        elif args.command == "replay":
            print(json.dumps(asyncio.run(replay_store(config, args.database)), indent=2))
        elif args.command == "rotate-archive":
            print(
                json.dumps(
                    asyncio.run(
                        rotate_archive(
                            args.database,
                            args.output,
                            args.as_of_ms,
                            partition_ms=args.partition_ms,
                            batch_size=config.ingestion.storage_batch_size
                            if args.batch_size is None
                            else args.batch_size,
                            max_partitions=args.max_partitions,
                        )
                    ),
                    indent=2,
                )
            )
        elif args.command == "restore-rotation":
            print(
                json.dumps(
                    asyncio.run(
                        restore_rotation(
                            args.catalog,
                            args.output,
                            batch_size=config.ingestion.storage_batch_size
                            if args.batch_size is None
                            else args.batch_size,
                            max_skew_ms=int(config.data.max_clock_skew_s * 1000),
                        )
                    ),
                    indent=2,
                )
            )
        elif args.command == "backfill-trades":
            print(
                json.dumps(
                    asyncio.run(
                        backfill_trades(
                            config, args.database, args.output, args.symbol, args.through_trade_id
                        )
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-features":
            print(
                json.dumps(
                    research_features(
                        args.database,
                        args.output,
                        asset=args.asset,
                        environment=args.environment,
                        source=args.source,
                        symbol=args.symbol,
                        timeframe_s=args.timeframe_s,
                        as_of_ms=args.as_of_ms,
                        max_bars=args.max_bars,
                        annualization_periods_per_year=args.annualization_periods_per_year,
                        history_start_ms=args.history_start_ms,
                        include_trend=args.include_trend,
                        percentile_window=args.percentile_window,
                        percentile_min_history=args.percentile_min_history,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-range-volatility":
            print(
                json.dumps(
                    research_range_volatility(
                        args.database,
                        args.output,
                        asset=args.asset,
                        environment=args.environment,
                        source=args.source,
                        symbol=args.symbol,
                        timeframe_s=args.timeframe_s,
                        as_of_ms=args.as_of_ms,
                        windows=tuple(args.windows),
                        max_latest_age_ms=args.max_latest_age_ms,
                        max_bars=args.max_bars,
                        annualization_periods_per_year=args.annualization_periods_per_year,
                        history_start_ms=args.history_start_ms,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-keltner":
            print(
                json.dumps(
                    research_keltner(
                        args.database,
                        args.output,
                        asset=args.asset,
                        environment=args.environment,
                        source=args.source,
                        symbol=args.symbol,
                        timeframe_s=args.timeframe_s,
                        as_of_ms=args.as_of_ms,
                        ema_period=args.ema_period,
                        atr_period=args.atr_period,
                        atr_multiplier=args.atr_multiplier,
                        max_latest_age_ms=args.max_latest_age_ms,
                        max_bars=args.max_bars,
                        history_start_ms=args.history_start_ms,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-stochastic-rsi":
            print(
                json.dumps(
                    research_stochastic_rsi(
                        args.database,
                        args.output,
                        asset=args.asset,
                        environment=args.environment,
                        source=args.source,
                        symbol=args.symbol,
                        timeframe_s=args.timeframe_s,
                        as_of_ms=args.as_of_ms,
                        max_latest_age_ms=args.max_latest_age_ms,
                        rsi_period=args.rsi_period,
                        stochastic_period=args.stochastic_period,
                        smooth_k=args.smooth_k,
                        smooth_d=args.smooth_d,
                        max_bars=args.max_bars,
                        history_start_ms=args.history_start_ms,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-atr-percentile":
            print(
                json.dumps(
                    research_atr_percentile(
                        args.database,
                        args.output,
                        asset=args.asset,
                        environment=args.environment,
                        source=args.source,
                        symbol=args.symbol,
                        timeframe_s=args.timeframe_s,
                        as_of_ms=args.as_of_ms,
                        max_latest_age_ms=args.max_latest_age_ms,
                        atr_period=args.atr_period,
                        percentile_window=args.percentile_window,
                        percentile_min_history=args.percentile_min_history,
                        max_bars=args.max_bars,
                        history_start_ms=args.history_start_ms,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-options":
            print(
                json.dumps(
                    research_options(
                        args.database,
                        args.output,
                        asset=args.asset,
                        as_of_ms=args.as_of_ms,
                        option_type=args.option_type,
                        moneyness_method=args.moneyness_method,
                        tie_policy=args.tie_policy,
                        max_quote_age_ms=args.max_quote_age_ms,
                        max_discovery_age_ms=args.max_discovery_age_ms,
                        max_atm_log_moneyness=args.max_atm_log_moneyness,
                        max_underlying_mismatch_fraction=args.max_underlying_mismatch_fraction,
                        flat_threshold_iv_points=args.flat_threshold_iv_points,
                        max_rows=args.max_rows,
                        max_expiries=args.max_expiries,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-option-skew":
            print(
                json.dumps(
                    research_option_skew(
                        args.database,
                        args.output,
                        asset=args.asset,
                        as_of_ms=args.as_of_ms,
                        target_absolute_delta=args.target_absolute_delta,
                        maximum_delta_error=args.maximum_delta_error,
                        max_quote_age_ms=args.max_quote_age_ms,
                        max_discovery_age_ms=args.max_discovery_age_ms,
                        max_clock_skew_ms=args.max_clock_skew_ms,
                        max_underlying_mismatch_fraction=args.max_underlying_mismatch_fraction,
                        max_rows=args.max_rows,
                        max_expiries=args.max_expiries,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-option-flow":
            print(
                json.dumps(
                    research_option_flow(
                        args.database,
                        args.output,
                        asset=args.asset,
                        as_of_ms=args.as_of_ms,
                        amount_unit_policy=args.amount_unit_policy,
                        volume_field=args.volume_field,
                        max_quote_age_ms=args.max_quote_age_ms,
                        max_discovery_age_ms=args.max_discovery_age_ms,
                        max_clock_skew_ms=args.max_clock_skew_ms,
                        max_rows=args.max_rows,
                        max_expiries=args.max_expiries,
                    ),
                    indent=2,
                )
            )
        elif args.command == "research-backtest":
            print(json.dumps(research_backtest(args.input, args.output, args.registry), indent=2))
        elif args.command == "restore":
            print(
                json.dumps(
                    asyncio.run(
                        restore_archive(
                            args.archive,
                            args.output,
                            config.ingestion.storage_batch_size,
                            int(config.data.max_clock_skew_s * 1000),
                        )
                    ),
                    indent=2,
                )
            )
        else:
            report = asyncio.run(probe(config, args.authenticated, args.output.parent / "chains"))
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
            print(report.model_dump_json(indent=2))
        return 0
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning("INTERRUPTED connection_closed=true")
        return 130
    except Exception as exc:
        # External validation input and exception messages can contain secret payloads.
        # Log type and safe field locations rather than arbitrary exception text.
        detail = ""
        if isinstance(exc, ValidationError):
            detail = ",".join(
                ".".join(map(str, item["loc"]))
                for item in exc.errors(
                    include_input=False, include_context=False, include_url=False
                )
            )
        elif type(exc).__module__ == "selective_options.errors":
            detail = str(exc)
        logging.getLogger(__name__).error(
            "STARTUP_HALTED type=%s detail=%s trading_enabled=false", type(exc).__name__, detail
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
