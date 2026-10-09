"""Finite, supervised ingestion pilot. No signal or execution path exists."""

import asyncio
import hashlib
import json
import math
from pathlib import Path

from . import __version__
from .archive import seal_archive
from .bar_checkpoint import load_bar_checkpoint, verify_bar_checkpoint_seed
from .bar_runtime import install_bar_resume, save_collector_bar_checkpoint
from .bea_calendar import BEACalendarCollector
from .binance import BinanceCollector
from .binance_accounts import BinanceAccountCollector
from .binance_funding_view import funding_view_summary
from .binance_taker import BinanceTakerCollector
from .bus import CommitBarrier, EventBus, HotBuffer, RedisMirror
from .bybit import BybitCollector
from .calendar_quality import calendar_quality
from .capture_lifecycle import CaptureStopRequest, cancelled_worker_failure
from .checkpoints import load_trade_checkpoints
from .coinmetrics_checkpoint import load_coinmetrics_resume
from .collectors import DeribitCollector, SpotCollector, now_ms, poll_chains
from .config import AppConfig
from .derivatives import DerivativesCollector
from .errors import IntegrityError, TradingUnavailable
from .eth_checkpoint import load_eth_checkpoint
from .eth_gas import PUBLIC_PROVIDERS, EthereumGasCollector
from .events import MarketEvent
from .flow_quality import flow_quality
from .flow_view import FlowKind, flow_view_summary
from .fomc_calendar import FOMCCalendarCollector
from .fomc_calendar import calendar_quality as fomc_calendar_quality
from .funding_poll import BinanceFundingCollector
from .news import PolicyNewsCollector
from .periodic_bar_runtime import PeriodicBarRuntime
from .quality import FeedRequirement, FreshnessMonitor
from .rate_limit import EndpointLimiter
from .recovery_handoff import handoff_recovery
from .recovery_journal import load_recovery_evidence, publish_recovery
from .rotation_runtime import RotationRuntime, drain_shutdown
from .socket_recovery import socket_gap_handler
from .solana_network import SolanaNetworkCollector
from .spot_trades import SpotTradeCollector
from .stablecoin_checkpoint import load_stablecoin_resume
from .store import DuckStore, complete_thread

MISSING_FEEDS = (
    "complete on-chain metrics/history and attributed exchange flows; optional public subsets do not establish complete coverage",
    "crypto news, sentiment and comprehensive financial news",
    "CPI/PCE/NFP calendars and values, plus other central-bank calendars; BEA releases and FOMC meeting dates are limited optional public subsets",
    "XAU/USD gold reference",
    "historical underlying OHLCV",
    "historical underlying trade/bar coverage",
    "historical perpetual/funding archive and full volatility-index coverage",
    "complete liquidation ledger and historical continuity; optional forceOrder snapshots are lossy",
    "complete cross-venue funding/OI coverage: Bybit USDT snapshots are optional; other venues/history pending",
    "cross-venue options flow",
    "full-universe stream coverage",
    "gap recovery outside bounded Kraken REST coverage; periodic crash checkpoints and other stream/full-source recovery",
    "deployed Redis consumer bus",
)


async def consume(
    config: AppConfig,
    bus: EventBus,
    store: DuckStore,
    hot: HotBuffer,
    monitor: FreshnessMonitor,
    stopped: asyncio.Event,
    mirror: RedisMirror | None,
) -> None:
    batch: list[MarketEvent] = []
    barriers: list[CommitBarrier] = []

    def accept(item: MarketEvent | CommitBarrier) -> None:
        if isinstance(item, CommitBarrier):
            barriers.append(item)
        else:
            batch.append(item)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + config.ingestion.flush_interval_s
    while not stopped.is_set() or not bus.queue.empty() or batch or barriers:
        try:
            async with asyncio.timeout(max(0.001, deadline - loop.time())):
                accept(await bus.queue.get())
        except TimeoutError:
            pass
        # Drain already queued rows in batches, including during shutdown.
        while not barriers and len(batch) < config.ingestion.storage_batch_size:
            try:
                accept(bus.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if (batch or barriers) and (
            barriers
            or len(batch) >= config.ingestion.storage_batch_size
            or loop.time() >= deadline
            or stopped.is_set()
        ):
            await store.append(batch)
            if mirror is not None and batch:
                await mirror.publish_batch(batch)
                await store.acknowledge_delivery([event.event_id for event in batch])
            for event in batch:
                hot.append(event)
                monitor.observe(event)
                bus.queue.task_done()
            batch.clear()
            for barrier in barriers:
                if not barrier.completed.done():
                    barrier.completed.set_result(None)
                bus.queue.task_done()
            barriers.clear()
            deadline = loop.time() + config.ingestion.flush_interval_s
        elif not batch:
            deadline = loop.time() + config.ingestion.flush_interval_s


async def capture(
    config: AppConfig,
    output: Path,
    duration_s: float | None = None,
    resume_trades_from: Path | None = None,
    recovery_marker: str | None = None,
    resume_eth_gas_from: Path | None = None,
    resume_stablecoin_from: Path | None = None,
    resume_bars_from: Path | None = None,
    save_bars_on_stop: bool | None = None,
    resume_coinmetrics_from: Path | None = None,
    stop_request: CaptureStopRequest | None = None,
) -> dict[str, object]:
    duration = config.ingestion.duration_s if duration_s is None else duration_s
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("capture duration must be finite and positive")
    if config.venue.ws_url != "wss://test.deribit.com/ws/api/v2":
        raise TradingUnavailable("capture only connects to testnet")
    if stop_request is not None:
        if type(stop_request) is not CaptureStopRequest:
            raise ValueError("stop_request requires a CaptureStopRequest")
        stop_request.bind()
    if save_bars_on_stop is not None and type(save_bars_on_stop) is not bool:
        raise ValueError("save_bars_on_stop requires an explicit boolean")
    save_bars = (
        config.ingestion.bar_checkpoint_on_stop if save_bars_on_stop is None else save_bars_on_stop
    )
    if (resume_bars_from is not None or save_bars) and not config.ingestion.spot_trades_enabled:
        raise ValueError("bar checkpoint/resume requires spot trades enabled")
    if resume_bars_from is not None and (
        resume_trades_from is not None or recovery_marker is not None
    ):
        raise ValueError("bar resume is exclusive with trade-tail/recovery-marker resume")
    bar_resume = (
        await load_bar_checkpoint(
            resume_bars_from,
            now_ms(),
            config.system.assets,
            config.ingestion.bar_timeframes_s,
            int(config.ingestion.bar_lateness_s * 1000),
            max_events=config.ingestion.bar_checkpoint_max_events,
        )
        if resume_bars_from is not None
        else None
    )
    if resume_trades_from is not None and not config.ingestion.spot_trades_enabled:
        raise ValueError("trade checkpoint recovery requires spot trades enabled")
    if recovery_marker is not None and resume_trades_from is None:
        raise ValueError("recovery marker requires a source database")
    if resume_eth_gas_from is not None and (
        not config.ingestion.eth_gas_enabled or "ETH" not in config.system.assets
    ):
        raise ValueError("ETH checkpoint recovery requires enabled ETH gas capture")
    if resume_stablecoin_from is not None and not config.ingestion.stablecoin_supply_enabled:
        raise ValueError("stablecoin checkpoint recovery requires enabled supply capture")
    stablecoin_policy = {
        "max_poll_age_ms": int(config.ingestion.stablecoin_supply_poll_interval_s * 2000),
        "max_block_age_ms": int(config.ingestion.stablecoin_supply_max_block_age_s * 1000),
    }
    stablecoin_resume_at = now_ms()
    stablecoin_pair = (
        await load_stablecoin_resume(
            resume_stablecoin_from, stablecoin_resume_at, **stablecoin_policy
        )
        if resume_stablecoin_from is not None
        else None
    )
    if resume_coinmetrics_from is not None and not config.ingestion.coinmetrics_enabled:
        raise ValueError("Coin Metrics checkpoint recovery requires enabled Coin Metrics capture")
    if resume_coinmetrics_from is not None and not {"BTC", "ETH"}.issubset(
        set(config.system.assets)
    ):
        raise ValueError("Coin Metrics checkpoint recovery requires configured BTC and ETH assets")
    coinmetrics_policy = {
        "max_poll_age_ms": int(config.ingestion.coinmetrics_poll_interval_s * 2000),
        "max_data_age_ms": int(config.ingestion.coinmetrics_max_data_age_s * 1000),
        "max_rows": 10_000,
        "max_bytes": 64_000_000,
    }
    coinmetrics_resume_at: int | None = None
    coinmetrics_seed: tuple[MarketEvent, ...] = ()
    if resume_coinmetrics_from is not None:
        coinmetrics_resume_at = now_ms()
        coinmetrics_seed = await load_coinmetrics_resume(
            resume_coinmetrics_from, coinmetrics_resume_at, **coinmetrics_policy
        )
    eth_seed = (
        await load_eth_checkpoint(
            resume_eth_gas_from, now_ms(), provider=config.ingestion.eth_gas_provider
        )
        if resume_eth_gas_from is not None
        else None
    )
    recovery = (
        await load_recovery_evidence(resume_trades_from, recovery_marker, now_ms())
        if resume_trades_from is not None and recovery_marker is not None
        else None
    )
    if recovery is not None and recovery[0].asset not in config.system.assets:
        raise IntegrityError("RECOVERY_HANDOFF_ASSET_DISABLED")
    checkpoints = (
        await load_trade_checkpoints(resume_trades_from, config.system.assets)
        if resume_trades_from is not None and recovery is None
        else ()
    )
    # A new directory avoids mixing independent sessions or overwriting evidence.
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    public_config = config.model_dump(mode="json", exclude={"ingestion": {"redis_url"}})
    config_json = json.dumps(public_config, sort_keys=True, indent=2)
    await asyncio.to_thread(
        (output / "config.sanitized.json").write_text, config_json + "\n", encoding="utf-8"
    )
    started = now_ms()
    store = await DuckStore.open(output / "events.duckdb", int(config.data.max_clock_skew_s * 1000))
    bar_seed_verification: dict[str, object] | None = None
    bar_resume_at: int | None = None
    try:
        if bar_resume is not None:
            await store.append(list(bar_resume.events), exact=True)
            await store.close()
            bar_seed_verification = await verify_bar_checkpoint_seed(
                output / "events.duckdb",
                bar_resume,
                now_ms(),
                max_events=config.ingestion.bar_checkpoint_max_events,
            )
            store = await DuckStore.open(
                output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
            )
        elif recovery is not None:
            marker = await publish_recovery(store, *recovery)
            if marker.event_id != recovery_marker:
                raise IntegrityError("RECOVERY_MARKER_CHANGED_DURING_COPY")
            await store.close()
        else:
            await store.append(list(checkpoints))
    except BaseException:
        await drain_shutdown(store.close())
        raise
    bus = EventBus(config.ingestion.queue_capacity, config.ingestion.queue_timeout_s)
    hot = HotBuffer(config.ingestion.hot_buffer_events)
    monitor = FreshnessMonitor(int(config.data.max_clock_skew_s * 1000))
    limiter = EndpointLimiter(config.rate_limits)
    spot_trades = None
    if config.ingestion.spot_trades_enabled:
        try:
            spot_trades = (
                SpotTradeCollector(
                    config,
                    bus.publish,
                    checkpoints,
                    track_bar_inputs=True,
                    bar_input_limit=config.ingestion.bar_checkpoint_max_events,
                )
                if (
                    bar_resume is not None
                    or save_bars
                    or config.ingestion.bar_checkpoint_interval_s is not None
                )
                else SpotTradeCollector(config, bus.publish, checkpoints)
            )
            if bar_resume is not None:
                bar_resume_at = now_ms()
                install_bar_resume(spot_trades, bar_resume, bar_resume_at)
        except BaseException:
            await drain_shutdown(store.close())
            raise
    deribit = DeribitCollector(config, bus.publish, limiter)
    if recovery_marker is not None:
        assert spot_trades is not None
        await handoff_recovery(spot_trades, output / "events.duckdb", recovery_marker)
        store = await DuckStore.open(
            output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
        )
    eth_gas = (
        EthereumGasCollector(config, bus.publish, provider=config.ingestion.eth_gas_provider)
        if config.ingestion.eth_gas_enabled and "ETH" in config.system.assets
        else None
    )
    if eth_seed is not None:
        try:
            await store.append([eth_seed], exact=True)
            await store.close()
            copied = await load_eth_checkpoint(
                output / "events.duckdb", now_ms(), provider=config.ingestion.eth_gas_provider
            )
            if copied != eth_seed:
                raise IntegrityError("ETH_CHECKPOINT_CHANGED_DURING_COPY")
            store = await DuckStore.open(
                output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
            )
        except BaseException:
            await store.close()
            raise
        assert eth_gas is not None
        eth_gas.previous = copied
    if stablecoin_pair is not None:
        try:
            await store.append(list(stablecoin_pair), exact=True)
            await store.close()
            copied_pair = await load_stablecoin_resume(
                output / "events.duckdb", now_ms(), **stablecoin_policy
            )
            if copied_pair != stablecoin_pair:
                raise IntegrityError("STABLECOIN_CHECKPOINT_CHANGED_DURING_COPY")
            store = await DuckStore.open(
                output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
            )
        except BaseException:
            await store.close()
            raise
    if coinmetrics_seed:
        try:
            pending_before_coinmetrics = await store.pending_delivery_count()
            await store.append(list(coinmetrics_seed), exact=True)
            await store.close()
            copied_coinmetrics = await load_coinmetrics_resume(
                output / "events.duckdb", now_ms(), **coinmetrics_policy
            )
            if copied_coinmetrics != coinmetrics_seed:
                raise IntegrityError("COINMETRICS_CHECKPOINT_CHANGED_DURING_COPY")
            store = await DuckStore.open(
                output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
            )
            if await store.pending_delivery_count() != pending_before_coinmetrics + len(
                coinmetrics_seed
            ):
                raise IntegrityError("COINMETRICS_CHECKPOINT_OUTBOX_MISMATCH")
        except BaseException:
            await drain_shutdown(store.close())
            raise
    stablecoin = None
    if config.ingestion.stablecoin_supply_enabled:
        from .stablecoin_supply import StablecoinSupplyCollector

        try:
            stablecoin = StablecoinSupplyCollector(
                config,
                bus.publish,
                initial_snapshot=stablecoin_pair[0] if stablecoin_pair else None,
            )
        except BaseException:
            await store.close()
            raise
    coinmetrics = None
    if config.ingestion.coinmetrics_enabled:
        from .coinmetrics import CoinMetricsCollector

        try:
            coinmetrics = CoinMetricsCollector(
                bus.publish,
                assets=tuple(asset for asset in config.system.assets if asset in ("BTC", "ETH")),
                poll_interval_s=config.ingestion.coinmetrics_poll_interval_s,
                lookback_days=config.ingestion.coinmetrics_lookback_days,
                max_age_ms=config.ingestion.coinmetrics_max_data_age_s * 1000,
                max_pages=config.ingestion.coinmetrics_max_pages,
                max_rows=config.ingestion.coinmetrics_max_rows,
                max_response_bytes=config.ingestion.coinmetrics_max_response_bytes,
                timeout_s=config.ingestion.coinmetrics_timeout_s,
            )
        except BaseException:
            await drain_shutdown(store.close())
            raise
    mirror = RedisMirror(config.ingestion.redis_url) if config.ingestion.redis_url else None
    if spot_trades is not None and config.ingestion.kraken_auto_recovery_enabled:
        spot_trades.gap_handler = socket_gap_handler(spot_trades, bus, store, output / "recovery")
    stopped = asyncio.Event()
    writer = asyncio.create_task(consume(config, bus, store, hot, monitor, stopped, mirror))
    producers: list[asyncio.Task[None]] = []
    failure: BaseException | None = None
    rotation_stopped = asyncio.Event()
    rotation_runtime = RotationRuntime(config, bus, store, output, rotation_stopped)
    rotation_task: asyncio.Task[None] | None = None
    periodic_bars_stopped = asyncio.Event()
    periodic_bars_runtime = PeriodicBarRuntime(
        config, bus, store, spot_trades, output, periodic_bars_stopped
    )
    periodic_bars_task: asyncio.Task[None] | None = None
    stop_waiter: asyncio.Task[None] | None = None
    termination_reason = "DURATION_ELAPSED"
    requested_stop_reason: str | None = None
    try:
        # A request arriving during awaited seed/store setup must not start transport.
        if stop_request is None or not stop_request.requested:
            producers.extend(
                (
                    asyncio.create_task(deribit.run()),
                    asyncio.create_task(poll_chains(config, bus.publish, limiter)),
                )
            )
            for source in config.ingestion.reference_sources:
                producers.append(
                    asyncio.create_task(SpotCollector(config, bus.publish, source).run())
                )
            if config.ingestion.derivatives_enabled:
                producers.append(
                    asyncio.create_task(DerivativesCollector(config, bus.publish, limiter).run())
                )
            if config.ingestion.bybit_enabled:
                producers.append(asyncio.create_task(BybitCollector(config, bus.publish).run()))
            if config.ingestion.binance_enabled:
                producers.append(asyncio.create_task(BinanceCollector(config, bus.publish).run()))
            if config.ingestion.binance_funding_history_enabled:
                producers.append(
                    asyncio.create_task(BinanceFundingCollector(config, bus.publish).run())
                )
            if config.ingestion.binance_taker_enabled:
                producers.append(
                    asyncio.create_task(BinanceTakerCollector(config, bus.publish).run())
                )
            if config.ingestion.binance_account_ratio_enabled:
                producers.append(
                    asyncio.create_task(BinanceAccountCollector(config, bus.publish).run())
                )
            if config.ingestion.bea_calendar_enabled:
                producers.append(
                    asyncio.create_task(BEACalendarCollector(config, bus.publish).run())
                )
            if config.ingestion.fomc_calendar_enabled:
                producers.append(
                    asyncio.create_task(FOMCCalendarCollector(config, bus.publish).run())
                )
            if eth_gas is not None:
                producers.append(asyncio.create_task(eth_gas.run()))
            if config.ingestion.solana_network_enabled and "SOL" in config.system.assets:
                producers.append(
                    asyncio.create_task(SolanaNetworkCollector(config, bus.publish).run())
                )
            if stablecoin is not None:
                producers.append(asyncio.create_task(stablecoin.run()))
            if coinmetrics is not None:
                producers.append(asyncio.create_task(coinmetrics.run()))
            if config.ingestion.tips_yield_enabled:
                from .tips_yield import TipsYieldCollector

                producers.append(asyncio.create_task(TipsYieldCollector(config, bus.publish).run()))
            if config.ingestion.liquidations_enabled:
                from .liquidations import LiquidationCollector

                producers.append(
                    asyncio.create_task(LiquidationCollector(config, bus.publish).run())
                )
            if config.ingestion.crypto_news_enabled:
                from .crypto_news import CryptoNewsCollector

                producers.append(
                    asyncio.create_task(CryptoNewsCollector(config, bus.publish).run())
                )
            if spot_trades is not None:
                producers.append(asyncio.create_task(spot_trades.run()))
            for news_source in config.ingestion.policy_news_sources:
                producers.append(
                    asyncio.create_task(PolicyNewsCollector(config, bus.publish, news_source).run())
                )
            if config.ingestion.archive_rotation_interval_s is not None:
                rotation_task = asyncio.create_task(rotation_runtime.run())
            if config.ingestion.bar_checkpoint_interval_s is not None:
                periodic_bars_task = asyncio.create_task(periodic_bars_runtime.run())
        supervised = [writer, *producers]
        if rotation_task is not None:
            supervised.append(rotation_task)
        if periodic_bars_task is not None:
            supervised.append(periodic_bars_task)
        waiting = list(supervised)
        if stop_request is not None:
            stop_waiter = asyncio.create_task(stop_request.wait())
            waiting.append(stop_waiter)
        await asyncio.wait(waiting, timeout=duration, return_when=asyncio.FIRST_COMPLETED)
        # Check every completed worker, including a failure tied with a stop request.
        completed = [task for task in supervised if task.done()]
        for task in completed:
            task.result()
        if completed:
            raise IntegrityError(
                "collector, writer or archive worker exited before capture deadline"
            )
        if stop_request is not None and stop_request.requested:
            termination_reason = "STOP_REQUESTED"
            requested_stop_reason = stop_request.reason
    except BaseException as exc:
        failure = exc
        termination_reason = (
            "HARD_CANCEL" if isinstance(exc, asyncio.CancelledError) else "WORKER_FAILURE"
        )
    finally:
        collection_ended = now_ms()

        async def shutdown() -> None:
            nonlocal failure
            if stop_waiter is not None:
                stop_waiter.cancel()
                await asyncio.gather(stop_waiter, return_exceptions=True)
            for task in producers:
                task.cancel()
            rotation_stopped.set()
            if rotation_task is not None and not rotation_task.done():
                rotation_task.cancel()
            periodic_bars_stopped.set()
            if periodic_bars_task is not None and not periodic_bars_task.done():
                periodic_bars_task.cancel()

            async def producer_outcome(task: asyncio.Task[None]) -> BaseException | None:
                try:
                    await task
                except asyncio.CancelledError as exc:
                    return cancelled_worker_failure(exc)
                except BaseException as exc:
                    return exc
                return None

            producer_results = await asyncio.gather(*(producer_outcome(task) for task in producers))
            for result in producer_results:
                if result is not None:
                    failure = failure or result
            if rotation_task is not None:
                try:
                    await rotation_task
                except asyncio.CancelledError as exc:
                    failure = failure or cancelled_worker_failure(exc)
                except BaseException as exc:
                    failure = failure or exc
            if periodic_bars_task is not None:
                try:
                    await periodic_bars_task
                except asyncio.CancelledError as exc:
                    failure = failure or cancelled_worker_failure(exc)
                except BaseException as exc:
                    failure = failure or exc
            # Keep the consumer available for each checkpoint barrier until both
            # workers stop, then drain the writer before reporting or closing its store.
            stopped.set()
            try:
                await writer
            except BaseException as exc:
                failure = failure or exc

        additional_cancel = await drain_shutdown(shutdown())
        failure = failure or additional_cancel
    try:
        bar_checkpoint: dict[str, object] | None = None
        if save_bars:
            if failure is not None:
                bar_checkpoint = {"status": "NOT_SAVED_CAPTURE_FAILED", "requested": True}
            else:
                assert spot_trades is not None
                try:
                    bar_checkpoint = await save_collector_bar_checkpoint(
                        spot_trades, store, output / "bar-checkpoint", now_ms()
                    )
                except BaseException as exc:
                    failure = exc
                    bar_checkpoint = {
                        "status": "BAR_CHECKPOINT_FAILED",
                        "requested": True,
                        "failure_type": type(exc).__name__,
                    }
        ended = now_ms()
        age = int(config.data.max_staleness_s * 1000)
        requirements = [
            FeedRequirement("deribit", asset, kind, age)
            for asset in config.system.assets
            for kind in ("chain_summary", "ticker", "book", "index")
        ]
        requirements.extend(
            FeedRequirement(source, asset, "spot", age)
            for source in config.ingestion.reference_sources
            for asset in config.system.assets
        )
        if config.ingestion.coinmetrics_enabled:
            for asset in config.system.assets:
                if asset in ("BTC", "ETH"):
                    requirements.extend(
                        (
                            FeedRequirement(
                                "coinmetrics_community",
                                asset,
                                "on_chain",
                                config.ingestion.coinmetrics_max_data_age_s * 1000,
                            ),
                            FeedRequirement(
                                "coinmetrics_community",
                                asset,
                                "feed_health",
                                config.ingestion.coinmetrics_poll_interval_s * 2000,
                            ),
                        )
                    )
        if config.ingestion.stablecoin_supply_enabled:
            requirements.extend(
                FeedRequirement(
                    "ethereum_drpc",
                    "GLOBAL",
                    kind,
                    int(config.ingestion.stablecoin_supply_poll_interval_s * 2000),
                )
                for kind in ("on_chain", "feed_health")
            )
        if config.ingestion.tips_yield_enabled:
            requirements.extend(
                FeedRequirement(
                    "fred_tips",
                    "GLOBAL",
                    kind,
                    int(config.ingestion.tips_yield_poll_interval_s * 2000),
                )
                for kind in ("index", "feed_health")
            )
        if config.ingestion.liquidations_enabled:
            requirements.extend(
                FeedRequirement("binance_liquidations", asset, "feed_health", 300_000)
                for asset in config.system.assets
            )
        if config.ingestion.derivatives_enabled:
            requirements.extend(
                FeedRequirement("deribit", asset, kind, age)
                for asset in config.system.assets
                for kind in ("perpetual", "funding")
            )
        if config.ingestion.eth_gas_enabled and "ETH" in config.system.assets:
            requirements.extend(
                [
                    FeedRequirement(
                        PUBLIC_PROVIDERS[config.ingestion.eth_gas_provider][1],
                        "ETH",
                        "on_chain",
                        int(config.ingestion.eth_gas_max_block_age_s * 1000),
                    ),
                    FeedRequirement(
                        PUBLIC_PROVIDERS[config.ingestion.eth_gas_provider][1],
                        "ETH",
                        "feed_health",
                        int(config.ingestion.eth_gas_poll_interval_s * 2000),
                    ),
                ]
            )
        if config.ingestion.crypto_news_enabled:
            requirements.append(
                FeedRequirement(
                    "coindesk_rss",
                    "GLOBAL",
                    "feed_health",
                    int(config.ingestion.crypto_news_poll_interval_s * 2000),
                )
            )
        if config.ingestion.bea_calendar_enabled:
            requirements.extend(
                FeedRequirement(
                    "bea", "GLOBAL", kind, int(config.ingestion.bea_calendar_poll_interval_s * 2000)
                )
                for kind in ("calendar", "feed_health")
            )
        if config.ingestion.fomc_calendar_enabled:
            requirements.extend(
                FeedRequirement(
                    "federal_reserve_fomc",
                    "GLOBAL",
                    kind,
                    int(config.ingestion.fomc_calendar_poll_interval_s * 2000),
                )
                for kind in ("calendar", "feed_health")
            )
        if config.ingestion.solana_network_enabled and "SOL" in config.system.assets:
            requirements.extend(
                FeedRequirement(
                    "solana_public",
                    "SOL",
                    kind,
                    int(config.ingestion.solana_network_poll_interval_s * 2000),
                )
                for kind in ("on_chain", "feed_health")
            )
        if config.ingestion.binance_enabled:
            requirements.extend(
                FeedRequirement(
                    "binance", asset, kind, int(config.ingestion.binance_poll_interval_s * 2000)
                )
                for asset in config.system.assets
                for kind in ("perpetual", "funding", "open_interest", "feed_health")
            )
        if config.ingestion.binance_funding_history_enabled:
            # A settlement's source timestamp is not a poll freshness clock.
            requirements.extend(
                FeedRequirement(
                    "binance_history",
                    asset,
                    "feed_health",
                    int(config.ingestion.binance_funding_history_poll_interval_s * 2000),
                )
                for asset in config.system.assets
            )
        if config.ingestion.binance_taker_enabled:
            requirements.extend(
                FeedRequirement(
                    "binance_taker",
                    asset,
                    kind,
                    int(config.ingestion.binance_taker_poll_interval_s * 2000),
                )
                for asset in config.system.assets
                for kind in ("taker_flow", "feed_health")
            )
        if config.ingestion.binance_account_ratio_enabled:
            requirements.extend(
                FeedRequirement(
                    "binance_accounts",
                    asset,
                    kind,
                    int(config.ingestion.binance_account_ratio_poll_interval_s * 2000),
                )
                for asset in config.system.assets
                for kind in ("account_ratio", "feed_health")
            )
        if config.ingestion.bybit_enabled:
            requirements.extend(
                FeedRequirement(
                    "bybit", asset, kind, int(config.ingestion.bybit_poll_interval_s * 2000)
                )
                for asset in config.system.assets
                for kind in ("perpetual", "funding", "feed_health")
            )
        if config.ingestion.spot_trades_enabled:
            requirements.extend(
                FeedRequirement("kraken", asset, "trade", age) for asset in config.system.assets
            )
        if config.ingestion.derivatives_enabled:
            requirements.extend(
                FeedRequirement("deribit", asset, "volatility_index", 120_000)
                for asset in config.system.assets
            )
        count = await store.export_parquet(output / "events.parquet")
        archive_manifest = await seal_archive(output / "events.parquet", count)
        requirements.extend(
            FeedRequirement(
                source, "GLOBAL", "feed_health", int(config.ingestion.news_poll_interval_s * 2000)
            )
            for source in config.ingestion.policy_news_sources
        )
        flow_views: list[dict[str, object]] = []
        flow_checks: list[dict[str, object]] = []
        flow_kinds: list[FlowKind] = []
        if config.ingestion.binance_taker_enabled:
            flow_kinds.append("taker_flow")
        if config.ingestion.binance_account_ratio_enabled:
            flow_kinds.append("account_ratio")
        if flow_kinds:
            global_health = (await store.snapshot("GLOBAL", ended, "public_reference")).rows()
            for asset in config.system.assets:
                scoped = (await store.snapshot(asset, ended, "public_reference")).rows()
                flow_views.extend(
                    flow_view_summary(
                        scoped, asset=asset, symbol=asset + "USDT", kind=kind, as_of_ms=ended
                    )
                    for kind in flow_kinds
                )
                for kind in flow_kinds:
                    age_ms = int(
                        (
                            config.ingestion.binance_taker_poll_interval_s
                            if kind == "taker_flow"
                            else config.ingestion.binance_account_ratio_poll_interval_s
                        )
                        * 2000
                    )
                    flow_checks.append(
                        flow_quality(
                            (*scoped, *global_health),
                            asset=asset,
                            symbol=asset + "USDT",
                            kind=kind,
                            as_of_ms=ended,
                            max_period_age_ms=age_ms,
                            max_observation_age_ms=age_ms,
                            max_health_age_ms=age_ms,
                        )
                    )
        funding_views: list[dict[str, object]] = []
        funding_health: list[dict[str, object]] = []
        if config.ingestion.binance_funding_history_enabled:
            for asset in config.system.assets:
                scoped = (await store.snapshot(asset, ended, "public_reference")).rows()
                history_health = [
                    row
                    for row in scoped
                    if row.source == "binance_history"
                    and row.symbol == asset + "USDT"
                    and row.kind == "feed_health"
                ]
                latest_receipt = max((row.observed_at_ms for row in history_health), default=None)
                funding_health.append(
                    {
                        "asset": asset,
                        "latest_receipt_ms": latest_receipt,
                        "latest_observations": [
                            row.model_dump(mode="json")
                            for row in history_health
                            if row.observed_at_ms == latest_receipt
                        ],
                        "scope": "Latest recorded poll outcomes, including every tied receipt; freshness and revision integrity are separate diagnostics",
                    }
                )
                funding_views.append(
                    funding_view_summary(
                        scoped,
                        asset=asset,
                        symbol=asset + "USDT",
                        start_ms=ended - config.ingestion.binance_funding_history_hours * 3_600_000,
                        end_ms=ended,
                        as_of_ms=ended,
                    )
                )
        capture_stop: dict[str, object] = {
            "reason": "FAILED" if failure is not None else termination_reason,
            "trigger": termination_reason,
            "request_reason": requested_stop_reason,
            "request_enabled": stop_request is not None,
            "signal_handling": stop_request.signal_handling
            if stop_request is not None
            else "NOT_REQUESTED",
            "periodic_crash_recovery": False,
        }
        report: dict[str, object] = {
            "archive_rotation": rotation_runtime.summary(),
            "periodic_bar_checkpoints": periodic_bars_runtime.summary(),
            "stablecoin_resume": {
                "requested": True,
                "selection_as_of_ms": stablecoin_resume_at,
                "anchor_event_id": stablecoin_pair[0].event_id,
                "anchor_observed_at_ms": stablecoin_pair[0].observed_at_ms,
                "health_event_id": stablecoin_pair[1].event_id,
                "health_observed_at_ms": stablecoin_pair[1].observed_at_ms,
                "copied_events": 2,
                "original_receipts_preserved": True,
                "complete_history_copied": False,
                "transport_resumed_by_copy": False,
                "delivery_acknowledgements_restored": False,
            }
            if stablecoin_pair is not None
            else None,
            "coinmetrics_resume": {
                "requested": True,
                "selection_as_of_ms": coinmetrics_resume_at,
                "scope": ["BTC", "ETH"],
                "copied_events": len(coinmetrics_seed),
                "max_events_total": coinmetrics_policy["max_rows"],
                "max_event_value_bytes": coinmetrics_policy["max_bytes"],
                "original_native_values_and_receipts_preserved": True,
                "copied_outbox_pending": True,
                "history_complete": False,
                "provider_finality_independently_verified": False,
                "transport_resumed_by_copy": False,
                "ready_for_signals": False,
                "trading_enabled": False,
            }
            if coinmetrics_seed
            else None,
            "coinmetrics_quality": {
                "supported_configured_assets": [
                    asset for asset in config.system.assets if asset in ("BTC", "ETH")
                ],
                "unsupported_configured_assets": [
                    asset for asset in config.system.assets if asset not in ("BTC", "ETH")
                ],
                "assets": [
                    await store.coinmetrics_quality(
                        asset,
                        ended,
                        config.ingestion.coinmetrics_poll_interval_s * 2000,
                        config.ingestion.coinmetrics_max_data_age_s * 1000,
                    )
                    for asset in config.system.assets
                    if asset in ("BTC", "ETH")
                ],
                "history_complete": False,
                "ready_for_signals": False,
                "trading_enabled": False,
            }
            if config.ingestion.coinmetrics_enabled
            else None,
            "crypto_news_quality": await store.crypto_news_quality(
                ended, int(config.ingestion.crypto_news_poll_interval_s * 2000)
            )
            if config.ingestion.crypto_news_enabled
            else None,
            "stablecoin_supply_quality": await store.stablecoin_supply_quality(
                ended,
                int(config.ingestion.stablecoin_supply_poll_interval_s * 2000),
                int(config.ingestion.stablecoin_supply_max_block_age_s * 1000),
            )
            if config.ingestion.stablecoin_supply_enabled
            else None,
            "tips_yield_quality": await store.tips_yield_quality(
                ended,
                int(config.ingestion.tips_yield_poll_interval_s * 2000),
            )
            if config.ingestion.tips_yield_enabled
            else None,
            "liquidations_quality": [
                await store.liquidations_quality(asset, ended) for asset in config.system.assets
            ]
            if config.ingestion.liquidations_enabled
            else [],
            "eth_gas_quality": await store.eth_gas_quality(
                config.ingestion.eth_gas_provider,
                ended,
                int(config.ingestion.eth_gas_poll_interval_s * 2000),
                int(config.ingestion.eth_gas_max_block_age_s * 1000),
            )
            if eth_gas is not None
            else None,
            "funding_revision_views": funding_views,
            "funding_poll_health": funding_health,
            "flow_revision_views": flow_views,
            "flow_quality": flow_checks,
            "policy_news_quality": [
                await store.policy_news_quality(
                    source,
                    ended,
                    int(config.ingestion.news_poll_interval_s * 2000),
                    int(config.ingestion.policy_news_max_publication_age_s * 1000),
                )
                for source in config.ingestion.policy_news_sources
            ],
            "build_version": __version__,
            "configuration_sha256": hashlib.sha256(config_json.encode()).hexdigest(),
            "parquet_sha256": archive_manifest.sha256,
            "status": "FAILED" if failure else "CAPTURE_FINISHED",
            "started_at_ms": started,
            "ended_at_ms": ended,
            "collection_ended_at_ms": collection_ended,
            "shutdown_drain_ms": ended - collection_ended,
            "requested_duration_s": duration,
            "capture_stop": capture_stop,
            "rows": count,
            "trade_checkpoint_seed_rows": len(checkpoints),
            "eth_gas_checkpoint_event_id": eth_seed.event_id if eth_seed else None,
            "eth_gas_provider": config.ingestion.eth_gas_provider,
            "eth_gas_source": PUBLIC_PROVIDERS[config.ingestion.eth_gas_provider][1],
            "eth_gas_checkpoint_handoff": "COMMITTED_AND_RELOADED" if eth_seed else "NOT_REQUESTED",
            "recovery_marker": recovery_marker,
            "automatic_kraken_recovery_enabled": config.ingestion.kraken_auto_recovery_enabled,
            "recovery_handoff": "COMMITTED_AND_RELOADED"
            if recovery is not None
            else "NOT_REQUESTED",
            "recovery_selected_rows": len(recovery[1].selected) if recovery is not None else 0,
            "trade_checkpoint_event_ids": [event.event_id for event in checkpoints],
            "trade_recovery_pending_symbols": sorted(spot_trades.awaiting_snapshot)
            if spot_trades
            else [],
            "trade_recovery_scope": (
                "Kraken only; explicit committed consumed-input bar seed verified before producers. Native first receipts/watermark retained, all open buckets partial after restart, actual recovery snapshot required. Other collectors start fresh unless separately seeded; no whole-source continuity or periodic crash recovery."
                if bar_resume is not None
                else "Kraken only; committed WS tail or explicit recovery marker, with optional bounded automatic REST repair of forward gaps after acknowledgements. Bar state and other collectors start fresh; repaired active bars stay partial."
            ),
            "counts": await store.counts(),
            "redis_delivery_pending": await store.pending_delivery_count(),
            "freshness": monitor.check(requirements, ended),
            "calendar_quality": calendar_quality(
                (await store.snapshot("GLOBAL", ended, "public_reference")).rows(),
                ended,
                int(config.ingestion.bea_calendar_poll_interval_s * 2000),
            )
            if config.ingestion.bea_calendar_enabled
            else {"state": "NOT_CONFIGURED"},
            "fomc_calendar_quality": fomc_calendar_quality(
                (await store.snapshot("GLOBAL", ended, "public_reference")).rows(),
                ended,
                int(config.ingestion.fomc_calendar_poll_interval_s * 2000),
            )
            if config.ingestion.fomc_calendar_enabled
            else {"state": "NOT_CONFIGURED"},
            "solana_network_quality": await store.solana_network_quality(
                ended,
                int(config.ingestion.solana_network_poll_interval_s * 2000),
                int(
                    config.ingestion.solana_network_max_sample_age_s * 1000
                    + config.ingestion.solana_network_poll_interval_s * 2000
                ),
            )
            if config.ingestion.solana_network_enabled and "SOL" in config.system.assets
            else {"state": "NOT_CONFIGURED"},
            "option_instrument_freshness": monitor.option_freshness(ended, age),
            "option_stream_coverage": [
                await store.option_coverage(
                    asset,
                    ended,
                    age,
                    int(config.ingestion.option_universe_max_age_s * 1000),
                )
                for asset in config.system.assets
            ],
            "bar_policy": "Only locally closed bars; complete=false for partial first intervals or connection gaps. Empty intervals are not synthesized.",
            "reference_comparison": [
                monitor.divergence(asset, ended, age, config.data.max_price_divergence_bps)
                for asset in config.system.assets
            ],
            "deribit_connections": deribit.connections,
            "deribit_universe_refreshes": deribit.universe_refreshes,
            "stream_universe_refresh_interval_s": config.ingestion.stream_universe_refresh_interval_s,
            "stream_cap_per_asset": config.ingestion.max_stream_instruments_per_asset,
            "missing_capabilities": list(MISSING_FEEDS),
            "milestone_2_complete": False,
            "ready_for_signals": False,
            "trading_enabled": False,
            "orders_submitted": 0,
            "redis_mirror": "CONFIGURED" if mirror else "NOT_CONFIGURED",
            "failure_type": type(failure).__name__ if failure else None,
            "freshness_scope": "asset/feed aggregates and selected option instruments; not trading approval",
            "reference_caveat": "Coinbase last-trade vs Kraken midpoint; Kraken has receipt time only. Public references are separate from testnet prices.",
        }
        if bar_checkpoint is not None:
            report["bar_checkpoint"] = bar_checkpoint
        if bar_resume is not None:
            report["bar_resume"] = {
                "status": "COMMITTED_SEED_VERIFIED_BEFORE_PRODUCERS",
                "checkpoint_as_of_ms": bar_resume.as_of_ms,
                "restart_ms": bar_resume_at,
                "books": len(bar_resume.books),
                "events": len(bar_resume.events),
                "closed_seed_verification": bar_seed_verification,
                "original_receipts_preserved": True,
                "retained_buckets_partial": True,
                "history_complete": False,
                "current_feed_health_verified": False,
                "periodic_crash_recovery": False,
            }
    finally:

        async def close_capture() -> None:
            try:
                await store.close()
            finally:
                if mirror is not None:
                    await mirror.close()

        try:
            additional_cancel = await drain_shutdown(close_capture())
        except BaseException as exc:
            # A report already built before close must not remain successful when
            # store/mirror cleanup fails. Still attempt durable FAILED publication.
            failure = failure or exc
        else:
            failure = failure or additional_cancel

    async def publish_report() -> None:
        await complete_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )

    if failure is not None:
        report["status"] = "FAILED"
        report["failure_type"] = type(failure).__name__
        capture_stop["reason"] = "FAILED"
    additional_cancel = await drain_shutdown(publish_report())
    failure = failure or additional_cancel
    if additional_cancel is not None and report["status"] != "FAILED":
        report["status"] = "FAILED"
        report["failure_type"] = type(failure).__name__
        capture_stop["reason"] = "FAILED"
        await drain_shutdown(publish_report())
    if failure is not None:
        raise failure
    return report
