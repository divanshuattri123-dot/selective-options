import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from test_gap_recovery import idle
from test_recovery_journal import plan
from test_trade_backfill import body
from test_trade_checkpoints import message, wire
from test_trade_reconciliation import ws

from selective_options.bus import EventBus, HotBuffer
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError, TradeGap
from selective_options.gap_recovery import recover_trade_gap
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.recovery_journal import publish_recovery
from selective_options.recovery_runtime import activate_gap_repair, begin_gap_repair
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore


@asynccontextmanager
async def running(tmp_path, mirror=None, barrier_timeout=2, allow_error=False):
    config = AppConfig.model_validate({"ingestion": {"flush_interval_s": 0.01}})
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    bus, hot, monitor = EventBus(100, barrier_timeout), HotBuffer(100), FreshnessMonitor(5000)
    stopped = asyncio.Event()
    task = asyncio.create_task(consume(config, bus, store, hot, monitor, stopped, mirror))
    collector = SpotTradeCollector(config, bus.publish)
    collector.symbols = {"BTC/USD": "BTC", "ETH/USD": "ETH"}
    try:
        yield collector, bus, store, hot, task
    finally:
        stopped.set()
        result = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 3)
        await store.close()
        if not allow_error:
            assert result == [None]


async def test_barrier_waits_for_actual_commit_and_consumers(tmp_path, monkeypatch):
    async with running(tmp_path) as (_, bus, store, hot, _):
        entered, release = asyncio.Event(), asyncio.Event()
        append = store.append

        async def blocked(rows):
            entered.set()
            await release.wait()
            await append(rows)

        monkeypatch.setattr(store, "append", blocked)
        await bus.publish(ws())
        waiter = asyncio.create_task(bus.flush())
        await entered.wait()
        try:
            assert not waiter.done() and not hot.recent("kraken", "BTC", "trade")
            assert not await store.read_events([ws().event_id])
        finally:
            release.set()
        await waiter
        assert await store.read_events([ws().event_id]) == (ws(),)
        assert hot.recent("kraken", "BTC", "trade") == (ws(),)
        await bus.queue.join()


async def test_empty_concurrent_barriers_and_cancelled_waiter_do_not_break_writer(tmp_path):
    async with running(tmp_path) as (_, bus, store, _, _):
        await asyncio.gather(bus.flush(), bus.flush())
        cancelled = asyncio.create_task(bus.flush())
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await bus.publish(ws())
        await bus.flush()
        await bus.queue.join()
        assert await store.read_events([ws().event_id]) == (ws(),)


@pytest.mark.parametrize("full", [False, True])
async def test_barrier_without_writer_has_bounded_failure(full):
    bus = EventBus(1, 0.01)
    if full:
        await bus.publish(ws())
    with pytest.raises(IntegrityError, match="COMMIT_BARRIER_TIMEOUT"):
        await bus.flush()


async def test_gap_is_structured_and_rejected_batch_does_not_advance_anchor(tmp_path):
    async with running(tmp_path) as (collector, bus, _, _, _):
        await collector._trades(message(3), 500000)
        with pytest.raises(TradeGap) as caught:
            await collector._trades(message(4, 6, kind="update"), 600000)
        assert (caught.value.symbol, caught.value.expected_id, caught.value.received_id) == (
            "BTC/USD",
            5,
            6,
        )
        assert collector.last_events["BTC/USD"] == ws()
        assert collector.sequences["BTC/USD"].last == 3
        await bus.flush()


async def test_begin_suspends_only_affected_book_and_marks_existing_bars(tmp_path):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        builder = collector.bars["BTC/USD"]
        for bar in builder.buckets.values():
            bar.complete = True
        assert await begin_gap_repair(collector, bus, store, "BTC/USD") == ws()
        assert collector.repairing == {"BTC/USD"}
        assert all(not bar.complete for bar in builder.buckets.values())
        watermark = builder.watermark_ms
        await collector._flush(700000)
        assert builder.watermark_ms == watermark
        with pytest.raises(IntegrityError, match="RECOVERY_IN_PROGRESS"):
            await collector._trades(message(4, kind="update"), 600000)
        await collector._trades(
            {"channel": "trade", "type": "snapshot", "data": [wire(10, symbol="ETH/USD")]}, 600000
        )
        assert collector.sequences["ETH/USD"].last == 10
        assert collector.last_events["BTC/USD"] == ws()
        with pytest.raises(IntegrityError, match="RECOVERY_IN_PROGRESS"):
            await begin_gap_repair(collector, bus, store, "BTC/USD")


@pytest.mark.parametrize("mode", ["missing", "receipt", "unknown_book"])
async def test_missing_or_inexact_anchor_cannot_start_repair(tmp_path, mode):
    async with running(tmp_path) as (collector, bus, store, _, _):
        if mode == "receipt":
            await store.append([ws(observed=499999)])
            await collector._trades(message(3), 500000)
        elif mode == "missing":
            collector = SpotTradeCollector(AppConfig(), bus.publish, (ws(),))
            collector.symbols = {"BTC/USD": "BTC"}
        with pytest.raises(IntegrityError, match="ANCHOR"):
            await begin_gap_repair(collector, bus, store, "BTC/USD")
        assert (
            collector.sequences.get("BTC/USD") is None or collector.sequences["BTC/USD"].last == 3
        )


async def test_cancelled_begin_leaves_book_suspended(tmp_path, monkeypatch):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        entered = asyncio.Event()

        async def blocked():
            entered.set()
            await idle()

        monkeypatch.setattr(bus, "flush", blocked)
        task = asyncio.create_task(begin_gap_repair(collector, bus, store, "BTC/USD"))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert collector.repairing == {"BTC/USD"}
        assert collector.last_events["BTC/USD"] == ws()


async def test_repair_fetch_commit_delivery_activation_then_live_successor(tmp_path, monkeypatch):
    mirror = AsyncMock()
    delivered = []
    mirror.publish_batch.side_effect = lambda rows: delivered.extend(rows)
    async with running(tmp_path, mirror) as (collector, bus, store, hot, _):
        await collector._trades(message(3), 500000)
        with pytest.raises(TradeGap):
            await collector._trades(message(5, kind="update"), 550000)
        anchor = await begin_gap_repair(collector, bus, store, "BTC/USD")
        monkeypatch.setattr(
            "selective_options.trade_backfill.fetch_page",
            AsyncMock(return_value=(body([3, 4, 5]), 600000)),
        )
        recovered = await recover_trade_gap(
            collector.config,
            store,
            anchor,
            5,
            idle,
            tmp_path / "gap",
            initial=(ws(5, observed=550000),),
        )
        assert collector.sequences["BTC/USD"].last == 3
        loaded = await activate_gap_repair(collector, bus, store, anchor, recovered.marker.event_id)
        assert loaded == recovered.plan
        assert collector.sequences["BTC/USD"].last == 5
        assert collector.last_events["BTC/USD"] == ws(5, observed=550000)
        assert not collector.repairing
        assert len(hot.recent("kraken_rest", "BTC", "trade")) == 2
        assert await store.pending_delivery_count() == 0
        assert recovered.marker in delivered and all(row in delivered for row in recovered.plan.raw)
        assert all(
            bar.count == 1 and not bar.complete
            for bar in collector.bars["BTC/USD"].buckets.values()
        )
        await collector._trades(message(5, 6, kind="update"), 601000)
        await collector._flush(905000)
        await bus.flush()
        bars = hot.recent("kraken", "BTC", "bar")
        assert bars and all(not row.payload()["complete"] for row in bars)
        assert bars[0].payload()["trade_count"] == 2
        assert collector.last_events["BTC/USD"].payload()["trade_id"] == 6


@pytest.mark.parametrize("mode", ["missing_marker", "wrong_anchor", "delivery_failure", "cancel"])
async def test_activation_failure_preserves_old_cursor_and_suspension(tmp_path, monkeypatch, mode):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        anchor = await begin_gap_repair(collector, bus, store, "BTC/USD")
        marker = await publish_recovery(store, anchor, plan())
        if mode == "wrong_anchor":
            from test_trade_reconciliation import rest

            from selective_options.trade_reconciliation import RecoveryBuffer

            # Wrong anchor receipt cannot coexist under the same event ID; use another native ID.
            buffer = RecoveryBuffer(ws(2), 5)
            for i in (3, 4, 5):
                buffer.add(rest(i))
            marker = await publish_recovery(store, ws(2), buffer.finish(620000))
        if mode in {"delivery_failure", "cancel"}:
            error = (
                IntegrityError("TEST_DELIVERY_FAILURE")
                if mode == "delivery_failure"
                else asyncio.CancelledError()
            )
            monkeypatch.setattr(bus, "flush", AsyncMock(side_effect=error))
        with pytest.raises(asyncio.CancelledError if mode == "cancel" else IntegrityError):
            await activate_gap_repair(
                collector,
                bus,
                store,
                anchor,
                "missing" if mode == "missing_marker" else marker.event_id,
            )
        assert collector.sequences["BTC/USD"].last == 3
        assert collector.last_events["BTC/USD"] == anchor
        assert collector.repairing == {"BTC/USD"}


async def test_duplicate_activation_is_serialized_and_cannot_reapply(tmp_path):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        anchor = await begin_gap_repair(collector, bus, store, "BTC/USD")
        marker = await publish_recovery(store, anchor, plan())
        results = await asyncio.gather(
            activate_gap_repair(collector, bus, store, anchor, marker.event_id),
            activate_gap_repair(collector, bus, store, anchor, marker.event_id),
            return_exceptions=True,
        )
        assert sum(isinstance(result, IntegrityError) for result in results) == 1
        assert collector.sequences["BTC/USD"].last == 5


async def test_failed_writer_never_acknowledges_barrier_or_activates(tmp_path):
    mirror = AsyncMock()
    async with running(tmp_path, mirror, allow_error=True) as (
        collector,
        bus,
        store,
        _,
        task,
    ):
        await collector._trades(message(3), 500000)
        anchor = await begin_gap_repair(collector, bus, store, "BTC/USD")
        marker = await publish_recovery(store, anchor, plan())
        bus.timeout_s = 0.2
        mirror.publish_batch.side_effect = IntegrityError("TEST_REDIS_DOWN")
        with pytest.raises(IntegrityError, match="BARRIER_TIMEOUT"):
            await activate_gap_repair(collector, bus, store, anchor, marker.event_id)
        assert task.done() and isinstance(task.exception(), IntegrityError)
        assert collector.repairing == {"BTC/USD"} and collector.sequences["BTC/USD"].last == 3
        assert await store.pending_delivery_count() == 3
