"""Gated complete batches, actual writer barriers and immutable explicit selections.

Transport and finite clocks are fixtures. These checks do not establish service,
whole-source or crash recovery, and never access a market network or place orders.
"""

import asyncio
import hashlib
import json
import threading
from collections import deque
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import duckdb
import pytest
from test_recovery_journal import plan as recovery_plan
from test_recovery_runtime import running as recovery_running
from test_trade_checkpoints import message as recovery_message

from selective_options import bar_checkpoint, bar_runtime
from selective_options import periodic_bar_runtime as module
from selective_options.bar_state import decode_bar_state
from selective_options.bus import EventBus, HotBuffer
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import consume
from selective_options.periodic_bar_runtime import (
    PeriodicBarRuntime,
    create_bar_generation,
    load_bar_generation,
)
from selective_options.quality import FreshnessMonitor
from selective_options.recovery_journal import publish_recovery
from selective_options.recovery_runtime import activate_gap_repair, begin_gap_repair
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore

CUTOFF = 1004000


def config(**changes):
    return AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "policy_news_sources": [],
            "bar_timeframes_s": [300, 900],
            "bar_lateness_s": 0,
            "flush_interval_s": 0.001,
            "bar_checkpoint_interval_s": 1,
            **changes,
        },
    )


def wire(identifier, timestamp=None):
    timestamp = timestamp if timestamp is not None else 1000000 + identifier * 1000
    return {
        "symbol": "BTC/USD",
        "side": "buy",
        "price": 100.0 + identifier,
        "qty": identifier / 10,
        "trade_id": identifier,
        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(),
    }


def message(*identifiers, kind="update", timestamp=None):
    return {
        "channel": "trade",
        "type": kind,
        "data": [wire(identifier, timestamp) for identifier in identifiers],
    }


@asynccontextmanager
async def running(tmp_path, monkeypatch, *, tracking=True, **changes):
    cfg = config(**changes)
    store = await DuckStore.open(tmp_path / "source.duckdb")
    bus = EventBus(100, 1)
    stopped = asyncio.Event()
    writer = asyncio.create_task(
        consume(cfg, bus, store, HotBuffer(100), FreshnessMonitor(5000), stopped, None)
    )
    collector = SpotTradeCollector(cfg, bus.publish, track_bar_inputs=tracking)
    collector.symbols = {"BTC/USD": "BTC"}
    collector.awaiting_snapshot.add("BTC/USD")
    monkeypatch.setattr(bar_runtime, "now_ms", lambda: CUTOFF)
    try:
        await collector._trades(message(1, kind="snapshot"), 1002000)
        yield collector, bus, store, stopped
    finally:
        stopped.set()
        await asyncio.wait_for(writer, 3)
        await store.close()


async def selected(directory, report, **changes):
    return await load_bar_generation(
        directory,
        CUTOFF,
        ("BTC",),
        (300, 900),
        0,
        expected_generation_sha256=report["generation_sha256"],
        **changes,
    )


def hashes(directory):
    return {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in directory.rglob("*")
        if path.is_file()
    }


async def test_actual_writer_barrier_exact_open_membership_ack_isolation_and_second_generation(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        await collector._trades(message(2), 1003000)
        await bus.flush()
        first = collector.bar_inputs["BTC/USD"][0]
        await store.acknowledge_delivery([first.event_id])
        pending = await store.pending_delivery_count()
        before = deepcopy(vars(collector.bars["BTC/USD"]))
        directory = tmp_path / "generation-000000"
        report = await create_bar_generation(collector, bus, store, directory, 0)
        resume = await selected(directory, report)
        assert resume.books[0].input_events == collector.bar_inputs["BTC/USD"]
        assert resume.books[0].tail_event == collector.last_events["BTC/USD"]
        assert decode_bar_state(resume.books[0].state_blob).as_of_ms == CUTOFF
        assert vars(collector.bars["BTC/USD"]) == before
        assert await store.pending_delivery_count() == pending
        with duckdb.connect(str(directory / "checkpoint/events.duckdb"), read_only=True) as copy:
            assert copy.execute("SELECT delivered FROM delivery_outbox").fetchall() == [
                (False,),
                (False,),
            ]
        for key in module.BarGeneration.model_fields:
            if key.endswith("complete") or key.endswith("verified") or key.endswith("enabled"):
                assert report[key] is False
        original = hashes(directory)
        await collector._trades(message(3), 1004000)
        next_dir = tmp_path / "generation-000001"
        second = await create_bar_generation(collector, bus, store, next_dir, 1)
        newer = await selected(next_dir, second)
        assert len(newer.events) == 3 and len(resume.events) == 2
        assert hashes(directory) == original
        assert await selected(directory, report) == resume
        assert not collector.bar_checkpoint_lock.locked()


async def test_checkpoint_waits_for_entire_published_batch_before_barrier_and_tail(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered, release = asyncio.Event(), asyncio.Event()
        publish = bus.publish

        async def blocked(event):
            await publish(event)
            if event.kind == "trade" and event.payload()["trade_id"] == 2:
                entered.set()
                await release.wait()

        collector.sink = blocked
        producer = asyncio.create_task(collector._trades(message(2, 3), 1004000))
        await entered.wait()
        flush = bus.flush
        barrier = AsyncMock(wraps=flush)
        monkeypatch.setattr(bus, "flush", barrier)
        directory = tmp_path / "generation-000000"
        checkpoint = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        await asyncio.sleep(0.02)
        assert collector.sequences["BTC/USD"].last == 1
        assert not checkpoint.done() and barrier.await_count == 0
        release.set()
        await producer
        report = await checkpoint
        resume = await selected(directory, report)
        assert collector.sequences["BTC/USD"].last == 3
        assert [row.payload()["trade_id"] for row in resume.books[0].input_events] == [1, 2, 3]
        assert resume.books[0].tail_event.payload()["trade_id"] == 3
        assert barrier.await_count == 1


@pytest.mark.parametrize("cancel", [False, True])
async def test_waiting_checkpoint_refuses_actual_ambiguous_producer_prefix(
    tmp_path, monkeypatch, cancel
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered, release = asyncio.Event(), asyncio.Event()

        async def prefix(event):
            if event.kind == "trade" and event.payload()["trade_id"] == 3:
                entered.set()
                await release.wait()
                raise OSError("synthetic ambiguous sink")
            await bus.publish(event)

        collector.sink = prefix
        producer = asyncio.create_task(collector._trades(message(2, 3), 1004000))
        await entered.wait()
        directory = tmp_path / "generation-000000"
        checkpoint = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        await asyncio.sleep(0.01)
        if cancel:
            producer.cancel()
        else:
            release.set()
        with pytest.raises(asyncio.CancelledError if cancel else IntegrityError):
            await producer
        with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
            await checkpoint
        assert collector.sequences["BTC/USD"].last == 1 and collector._emission_failed
        assert not (directory / "checkpoint").exists()
        assert json.loads((directory / "failure.json").read_text())["status"] == "FAILED"
        assert not collector.bar_checkpoint_lock.locked()


async def test_gate_holds_immutable_verification_while_writer_remains_available(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered, release = threading.Event(), threading.Event()
        verify = bar_checkpoint._verify

        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(3)
            return verify(*args, **kwargs)

        monkeypatch.setattr(bar_checkpoint, "_verify", blocked)
        directory = tmp_path / "generation-000000"
        checkpoint = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        assert await asyncio.to_thread(entered.wait, 2)
        producer = asyncio.create_task(collector._trades(message(2), 1003000))
        try:
            await asyncio.sleep(0.01)
            assert not producer.done() and collector.bar_checkpoint_lock.locked()
            assert not store._lock.locked()
            # A separate consumer FIFO flush completes while the producer gate is held.
            await asyncio.wait_for(bus.flush(), 0.5)
        finally:
            release.set()
        report = await checkpoint
        await producer
        await bus.flush()
        assert len((await selected(directory, report)).events) == 1
        assert collector.sequences["BTC/USD"].last == 2


async def test_actual_next_batch_waits_through_source_stage_then_commits_after_generation(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered, release = threading.Event(), threading.Event()
        stage = bar_checkpoint._stage

        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(3)
            return stage(*args, **kwargs)

        monkeypatch.setattr(bar_checkpoint, "_stage", blocked)
        directory = tmp_path / "generation-000000"
        saving = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        assert await asyncio.to_thread(entered.wait, 2)
        next_batch = asyncio.create_task(collector._trades(message(2, 3), CUTOFF))
        try:
            await asyncio.sleep(0.02)
            assert collector.bar_checkpoint_lock.locked() and store._lock.locked()
            assert not next_batch.done() and collector.sequences["BTC/USD"].last == 1
        finally:
            release.set()
        report = await saving
        await next_batch
        await asyncio.wait_for(bus.flush(), 0.5)
        resume = await selected(directory, report)
        assert len(resume.events) == 1
        assert resume.books[0].tail_event.payload()["trade_id"] == 1
        assert collector.sequences["BTC/USD"].last == 3
        assert await store.read_events([collector.last_events["BTC/USD"].event_id]) == (
            collector.last_events["BTC/USD"],
        )


async def test_real_queue_backpressure_after_accepted_prefix_poison_refuses_waiting_checkpoint(
    tmp_path, monkeypatch
):
    emitted = []

    async def seed_sink(event):
        emitted.append(event)

    collector = SpotTradeCollector(config(), seed_sink, track_bar_inputs=True)
    collector.symbols = {"BTC/USD": "BTC"}
    await collector._trades(message(1, kind="snapshot"), 1002000)
    store = await DuckStore.open(tmp_path / "source.duckdb")
    await store.append(emitted)
    bus = EventBus(1, 0.05)
    accepted = asyncio.Event()

    async def publish(event):
        await bus.publish(event)
        accepted.set()

    collector.sink = publish
    monkeypatch.setattr(bar_runtime, "now_ms", lambda: CUTOFF)
    producer = asyncio.create_task(collector._trades(message(2, 3), CUTOFF))
    await asyncio.wait_for(accepted.wait(), 0.5)
    directory = tmp_path / "generation-000000"
    saving = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
    try:
        with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED"):
            await producer
        with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
            await saving
        assert bus.queue.qsize() == 1
        assert bus.queue.get_nowait().payload()["trade_id"] == 2
        assert collector.sequences["BTC/USD"].last == 1 and collector._emission_failed
        assert not collector.bar_checkpoint_lock.locked()
        assert not (directory / "checkpoint").exists()
        assert not (directory / "generation.json").exists()
    finally:
        await store.close()


async def test_discovery_installs_symbols_and_awaiting_snapshot_under_one_gate(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, _, _, _):
        socket = AsyncMock()
        read = AsyncMock(
            side_effect=[
                {
                    "method": "subscribe",
                    "success": True,
                    "req_id": 1,
                    "result": {"channel": "instrument"},
                },
                {
                    "channel": "instrument",
                    "type": "snapshot",
                    "data": {
                        "pairs": [
                            {"symbol": "BTC/USD", "base": "BTC", "quote": "USD", "status": "online"}
                        ]
                    },
                },
            ]
        )
        monkeypatch.setattr(collector, "_read", read)
        await collector.bar_checkpoint_lock.acquire()
        task = asyncio.create_task(collector._discover(socket))
        try:
            await asyncio.sleep(0.01)
            assert read.await_count == 2 and not task.done()
            assert collector.awaiting_snapshot == set()
        finally:
            collector.bar_checkpoint_lock.release()
        await task
        assert collector.symbols == {"BTC/USD": "BTC"}
        assert collector.awaiting_snapshot == {"BTC/USD"}


async def test_committed_recovery_activation_waits_for_gate_before_frontier_install(
    tmp_path, monkeypatch
):
    async with recovery_running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(recovery_message(3), 500000)
        anchor = await begin_gap_repair(collector, bus, store, "BTC/USD")
        marker = await publish_recovery(store, anchor, recovery_plan())
        delivered = asyncio.Event()
        flush = bus.flush

        async def barrier():
            await flush()
            delivered.set()

        monkeypatch.setattr(bus, "flush", barrier)
        await collector.bar_checkpoint_lock.acquire()
        task = asyncio.create_task(
            activate_gap_repair(collector, bus, store, anchor, marker.event_id)
        )
        try:
            await asyncio.wait_for(delivered.wait(), 1)
            assert not task.done() and collector.recovery_lock.locked()
            assert collector.repairing == {"BTC/USD"}
            assert collector.sequences["BTC/USD"].last == 3
            assert collector.last_events["BTC/USD"] == anchor
        finally:
            collector.bar_checkpoint_lock.release()
        result = await task
        assert result.selected == recovery_plan().selected
        assert {row.event_id: row for row in result.raw} == {
            row.event_id: row for row in recovery_plan().raw
        }
        assert collector.sequences["BTC/USD"].last == 5 and not collector.repairing
        assert not collector.recovery_lock.locked()


async def test_wrapper_publication_failure_revokes_all_late_success_markers(tmp_path, monkeypatch):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        seal = module._seal

        def failed(*args):
            seal(*args)
            raise OSError("synthetic wrapper publication failure")

        monkeypatch.setattr(module, "_seal", failed)
        directory = tmp_path / "generation-000000"
        with pytest.raises(OSError, match="synthetic wrapper"):
            await create_bar_generation(collector, bus, store, directory, 0)
        assert not (directory / "generation.json").exists()
        assert not (directory / "checkpoint/checkpoint.json").exists()
        assert (directory / "generation-incomplete.json").is_file()
        assert (directory / "checkpoint/checkpoint-incomplete.json").is_file()
        assert json.loads((directory / "failure.json").read_text())["failure_type"] == "OSError"
        assert not collector.bar_checkpoint_lock.locked()


async def test_cutoff_selected_only_after_fifo_barrier_under_gate(tmp_path, monkeypatch):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered, release = asyncio.Event(), asyncio.Event()
        clock = []
        flush = bus.flush

        async def barrier():
            entered.set()
            await release.wait()
            await flush()

        def now():
            assert release.is_set() and collector.bar_checkpoint_lock.locked()
            clock.append(CUTOFF)
            return CUTOFF

        monkeypatch.setattr(bus, "flush", barrier)
        monkeypatch.setattr(bar_runtime, "now_ms", now)
        directory = tmp_path / "generation-000000"
        task = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        await entered.wait()
        assert clock == [] and not task.done()
        release.set()
        report = await task
        assert clock == [CUTOFF] and report["as_of_ms"] == CUTOFF


@pytest.mark.parametrize(
    "state,reason",
    [
        ("tracking", "TRACKING_DISABLED"),
        ("poison", "SPOT_EMISSION_OUTCOME_UNKNOWN"),
        ("repairing", "UNRESOLVED_RECOVERY"),
        ("awaiting", "UNRESOLVED_RECOVERY"),
        ("pending", "UNRESOLVED_RECOVERY"),
        ("recovery_lock", "UNRESOLVED_RECOVERY"),
        ("missing_inputs", "MISSING_CONSUMPTION_LEDGER"),
        ("missing_tail", "AMBIGUOUS_BOOK_FRONTIER"),
        ("tail_sequence", "SEQUENCE_TAIL_MISMATCH"),
    ],
)
async def test_unresolved_state_rechecked_after_gate_refuses_before_barrier_or_inner_artifact(
    tmp_path, monkeypatch, state, reason
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        await collector.bar_checkpoint_lock.acquire()
        directory = tmp_path / "generation-000000"
        task = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        await asyncio.sleep(0.01)
        if state == "tracking":
            collector.track_bar_inputs = False
        elif state == "poison":
            collector._emission_failed = True
        elif state == "repairing":
            collector.repairing.add("BTC/USD")
        elif state == "awaiting":
            collector.awaiting_snapshot.add("BTC/USD")
        elif state == "pending":
            collector.pending_frames.append((message(2), 1003000))
        elif state == "recovery_lock":
            await collector.recovery_lock.acquire()
        elif state == "missing_inputs":
            collector.bar_inputs.clear()
        elif state == "missing_tail":
            collector.last_events.clear()
        else:
            collector.sequences["BTC/USD"].last = 2
        collector.bar_checkpoint_lock.release()
        flush = AsyncMock(side_effect=AssertionError("unsafe barrier"))
        monkeypatch.setattr(bus, "flush", flush)
        try:
            with pytest.raises(IntegrityError, match=reason):
                await task
            assert not (directory / "checkpoint").exists()
            flush.assert_not_awaited()
        finally:
            if collector.recovery_lock.locked():
                collector.recovery_lock.release()


async def test_unconsumed_newer_native_tail_in_store_refuses_frozen_frontier(tmp_path, monkeypatch):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        # A different collector/store writer can add a later raw event. It must
        # never be silently incorporated as consumed membership or ignored as tail.
        emitted = []

        async def sink(event):
            emitted.append(event)

        other = SpotTradeCollector(config(), sink)
        other.symbols = collector.symbols.copy()
        await other._trades(message(2, kind="snapshot"), 1003000)
        await bus.flush()
        await store.append(emitted)
        directory = tmp_path / "generation-000000"
        with pytest.raises(IntegrityError, match="TAIL_NOT_UNIQUE_COMMITTED_FRONTIER"):
            await create_bar_generation(collector, bus, store, directory, 0)
        assert not (directory / "checkpoint").exists()
        assert not (directory / "generation.json").exists()


async def test_future_skew_accepted_trade_is_never_saved_before_either_clock(tmp_path, monkeypatch):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        await collector._trades(message(2, timestamp=CUTOFF + 100), CUTOFF)
        directory = tmp_path / "generation-000000"
        with pytest.raises((IntegrityError, ValueError)):
            await create_bar_generation(collector, bus, store, directory, 0)
        assert not (directory / "checkpoint").exists()
        assert not (directory / "generation.json").exists()


@pytest.mark.parametrize("operation", ["flush", "disconnect", "defer", "take", "repair"])
async def test_control_mutations_wait_for_checkpoint_gate(tmp_path, monkeypatch, operation):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        await bus.flush()
        before = deepcopy(vars(collector.bars["BTC/USD"]))
        buffered = (message(2), 1003000)
        collector.pending_frames.append(buffered)
        await collector.bar_checkpoint_lock.acquire()
        if operation == "flush":
            coroutine = collector._flush(1200000)
        elif operation == "disconnect":
            coroutine = collector._disconnected(CUTOFF)
        elif operation == "defer":
            coroutine = collector._defer_frames([(message(3), 1004000)])
        elif operation == "take":
            coroutine = collector.take_pending_frame()
        else:
            coroutine = begin_gap_repair(collector, bus, store, "BTC/USD")
        task = asyncio.create_task(coroutine)
        try:
            await asyncio.sleep(0.01)
            assert not task.done() and not collector.repairing
            assert collector.pending_frames == deque([buffered])
            assert vars(collector.bars["BTC/USD"]) == before
        finally:
            collector.bar_checkpoint_lock.release()
        result = await task
        if operation == "take":
            assert result == buffered and not collector.pending_frames
        elif operation == "disconnect":
            assert not collector.pending_frames and collector.awaiting_snapshot == {"BTC/USD"}
        elif operation == "repair":
            assert collector.repairing == {"BTC/USD"}
        elif operation == "defer":
            assert len(collector.pending_frames) == 2
        else:
            assert collector.bars["BTC/USD"].watermark_ms == 1200000


@pytest.mark.parametrize("phase", ["_stage", "_verify", "_publish", "_seal"])
async def test_cancelled_generation_drains_worker_then_revokes_late_inner_and_outer_markers(
    tmp_path, monkeypatch, phase
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        owner = module if phase == "_seal" else bar_checkpoint
        entered, release = threading.Event(), threading.Event()
        original = getattr(owner, phase)

        def blocked(*args, **kwargs):
            result = original(*args, **kwargs)
            entered.set()
            assert release.wait(3)
            return result

        monkeypatch.setattr(owner, phase, blocked)
        directory = tmp_path / "generation-000000"
        task = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            assert not task.done()
            if phase != "_seal":
                assert collector.bar_checkpoint_lock.locked()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not collector.bar_checkpoint_lock.locked() and not store._lock.locked()
        assert not (directory / "generation.json").exists()
        assert not (directory / "checkpoint/checkpoint.json").exists()
        assert (
            json.loads((directory / "failure.json").read_text())["failure_type"] == "CancelledError"
        )
        if phase == "_seal":
            assert (directory / "generation-incomplete.json").exists()
        # The source writer and collector remain usable after drained cancellation.
        await collector._trades(message(2), 1003000)
        await asyncio.wait_for(bus.flush(), 0.5)
        assert await store.read_events([collector.last_events["BTC/USD"].event_id]) == (
            collector.last_events["BTC/USD"],
        )


@pytest.mark.parametrize("phase", ["barrier", "gate"])
async def test_cancel_before_staging_releases_gate_without_success_artifacts(
    tmp_path, monkeypatch, phase
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        entered = asyncio.Event()
        if phase == "gate":
            await collector.bar_checkpoint_lock.acquire()
        else:

            async def blocked():
                entered.set()
                await asyncio.Event().wait()

            monkeypatch.setattr(bus, "flush", blocked)
        directory = tmp_path / "generation-000000"
        task = asyncio.create_task(create_bar_generation(collector, bus, store, directory, 0))
        if phase == "gate":
            await asyncio.sleep(0.02)
        else:
            await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if phase == "gate":
            assert collector.bar_checkpoint_lock.locked()
            collector.bar_checkpoint_lock.release()
        else:
            assert not collector.bar_checkpoint_lock.locked()
        assert not (directory / "checkpoint").exists()
        assert not (directory / "generation.json").exists()


@pytest.mark.parametrize(
    "tamper", ["marker", "inner_marker", "state", "database", "extra", "pending"]
)
async def test_explicit_selected_generation_refuses_corruption_without_older_fallback(
    tmp_path, monkeypatch, tamper
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        older = tmp_path / "generation-000000"
        old_report = await create_bar_generation(collector, bus, store, older, 0)
        directory = tmp_path / "generation-000001"
        report = await create_bar_generation(collector, bus, store, directory, 1)
        if tamper == "marker":
            path = directory / "generation.json"
            path.write_bytes(path.read_bytes() + b" ")
        elif tamper == "inner_marker":
            path = directory / "checkpoint/checkpoint.json"
            path.write_bytes(path.read_bytes() + b" ")
        elif tamper == "state":
            path = directory / "checkpoint/state-000000.json"
            path.write_bytes(path.read_bytes() + b" ")
        elif tamper == "database":
            path = directory / "checkpoint/events.duckdb"
            with path.open("ab") as stream:
                stream.write(b"bad")
        elif tamper == "extra":
            (directory / "unexplained").write_text("extra")
        else:
            (directory / ".generation.pending.json").write_bytes(
                (directory / "generation.json").read_bytes()
            )
        with pytest.raises(IntegrityError):
            await selected(directory, report)
        assert await selected(older, old_report)


async def test_selection_pins_digest_and_directory_index_and_rechecks_after_inner_load(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        directory = tmp_path / "generation-000000"
        report = await create_bar_generation(collector, bus, store, directory, 0)
        with pytest.raises(IntegrityError, match="SELECTION_DIGEST_MISMATCH"):
            await selected(directory, {**report, "generation_sha256": "0" * 64})
        renamed = tmp_path / "generation-000001"
        directory.rename(renamed)
        with pytest.raises(IntegrityError, match="DIRECTORY_INDEX_MISMATCH"):
            await selected(renamed, report)
        renamed.rename(directory)
        load = module.load_bar_checkpoint

        async def changing(*args, **kwargs):
            result = await load(*args, **kwargs)
            path = directory / "generation.json"
            path.write_bytes(path.read_bytes() + b" ")
            return result

        monkeypatch.setattr(module, "load_bar_checkpoint", changing)
        with pytest.raises(IntegrityError, match="SELECTION_DIGEST_MISMATCH"):
            await selected(directory, report)


@pytest.mark.parametrize("digest", [None, 0, True, "", "g" * 64, "A" * 64, "a" * 63])
async def test_invalid_selection_digest_refuses_before_artifact_read(tmp_path, digest):
    with pytest.raises(ValueError, match="explicit lowercase SHA256"):
        await load_bar_generation(
            tmp_path / "missing",
            CUTOFF,
            ("BTC",),
            (300, 900),
            0,
            expected_generation_sha256=digest,
        )


async def test_runtime_actual_two_generations_budget_halts_before_third_barrier(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch, bar_checkpoint_max_generations=2) as (
        collector,
        bus,
        store,
        stopped,
    ):

        async def due(self):
            await asyncio.sleep(0)
            return True

        flush = AsyncMock(wraps=bus.flush)
        monkeypatch.setattr(bus, "flush", flush)
        monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
        runtime = PeriodicBarRuntime(collector.config, bus, store, collector, tmp_path, stopped)
        with pytest.raises(IntegrityError, match="BUDGET_EXHAUSTED"):
            await runtime.run()
        summary = runtime.summary()
        assert summary["status"] == "FAILED"
        assert summary["attempts"] == summary["completed_generations"] == 2
        assert flush.await_count == 2
        assert not (tmp_path / "bar-generations/generation-000002").exists()
        assert summary["last_attempt"]["status"] == "COMMITTED_BAR_GENERATION"
        summary["generations"].clear()
        assert runtime.summary()["completed_generations"] == 2


@pytest.mark.parametrize("mode", ["disabled", "no_collector", "stopped"])
async def test_runtime_disabled_or_requested_stop_never_creates_generation(
    tmp_path, monkeypatch, mode
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, stopped):
        cfg = config(bar_checkpoint_interval_s=None) if mode == "disabled" else config()
        runtime = PeriodicBarRuntime(
            cfg, bus, store, None if mode == "no_collector" else collector, tmp_path, stopped
        )
        core = AsyncMock(side_effect=AssertionError("core called"))
        monkeypatch.setattr(module, "create_bar_generation", core)
        if mode == "stopped":
            task = asyncio.create_task(runtime.run())
            await asyncio.sleep(0)
            stopped.set()
            await asyncio.wait_for(task, 0.5)
        else:
            await runtime.run()
        assert runtime.summary()["status"] == ("STOPPED" if mode == "stopped" else "DISABLED")
        assert runtime.summary()["attempts"] == 0
        assert not (tmp_path / "bar-generations").exists()
        core.assert_not_awaited()


@pytest.mark.parametrize("requested", [False, True])
async def test_runtime_cancellation_preserves_attempt_outcome_and_stop_state(
    tmp_path, monkeypatch, requested
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, stopped):
        entered = asyncio.Event()

        async def due(self):
            return True

        async def blocked(*args):
            entered.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
        monkeypatch.setattr(module, "create_bar_generation", blocked)
        runtime = PeriodicBarRuntime(collector.config, bus, store, collector, tmp_path, stopped)
        task = asyncio.create_task(runtime.run())
        await entered.wait()
        if requested:
            stopped.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        summary = runtime.summary()
        assert summary["status"] == ("STOPPED" if requested else "FAILED")
        assert summary["last_attempt"]["status"] == "CANCELLED"
        assert summary["completed_generations"] == 0


@pytest.mark.parametrize("requested", [False, True])
async def test_cancelled_real_seal_worker_failure_overrides_requested_stop(
    tmp_path, monkeypatch, requested
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, stopped):
        entered, release = threading.Event(), threading.Event()
        seal = module._seal

        async def due(self):
            return True

        def failing_seal(*args):
            seal(*args)
            entered.set()
            assert release.wait(3)
            raise OSError("EXPLICIT_WORKER_FAILURE_AFTER_STOP")

        monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
        monkeypatch.setattr(module, "_seal", failing_seal)
        runtime = PeriodicBarRuntime(collector.config, bus, store, collector, tmp_path, stopped)
        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(entered.wait, 2)
        if requested:
            stopped.set()
        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(
            IntegrityError, match="BAR_GENERATION_CANCELLATION_DRAIN_FAILURE"
        ) as caught:
            await task
        assert isinstance(caught.value.__cause__, OSError)
        assert str(caught.value.__cause__) == "EXPLICIT_WORKER_FAILURE_AFTER_STOP"
        summary = runtime.summary()
        assert summary["status"] == "FAILED" and summary["failure_type"] == "OSError"
        assert summary["last_attempt"]["status"] == "FAILED"
        assert summary["last_attempt"]["failure_type"] == "OSError"
        assert summary["completed_generations"] == 0
        directory = tmp_path / "bar-generations/generation-000000"
        assert not (directory / "generation.json").exists()
        assert not (directory / "checkpoint/checkpoint.json").exists()
        assert json.loads((directory / "failure.json").read_text())["failure_type"] == "OSError"
        assert not collector.bar_checkpoint_lock.locked() and not store._lock.locked()


@pytest.mark.parametrize("wrapped", [False, True])
async def test_cancelled_generation_cleanup_failure_is_not_successful_requested_stop(
    tmp_path, monkeypatch, wrapped
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, stopped):
        entered, release = threading.Event(), threading.Event()
        seal, cleanup = module._seal, module._failed

        async def due(self):
            return True

        def blocked_seal(*args):
            result = seal(*args)
            entered.set()
            assert release.wait(3)
            return result

        def failing_cleanup(*args):
            cleanup(*args)
            failure = OSError("EXPLICIT_CLEANUP_FAILURE_AFTER_STOP")
            if wrapped:
                raise asyncio.CancelledError("cleanup worker cancelled") from failure
            raise failure

        monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
        monkeypatch.setattr(module, "_seal", blocked_seal)
        monkeypatch.setattr(module, "_failed", failing_cleanup)
        runtime = PeriodicBarRuntime(collector.config, bus, store, collector, tmp_path, stopped)
        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(entered.wait, 2)
        stopped.set()
        task.cancel()
        release.set()
        with pytest.raises(IntegrityError if wrapped else OSError) as caught:
            await task
        if wrapped:
            assert isinstance(caught.value.__cause__, OSError)
        else:
            assert str(caught.value) == "EXPLICIT_CLEANUP_FAILURE_AFTER_STOP"
        summary = runtime.summary()
        assert summary["status"] == "FAILED" and summary["failure_type"] == "OSError"
        assert summary["last_attempt"]["status"] == "FAILED"
        assert summary["last_attempt"]["failure_type"] == "OSError"
        directory = tmp_path / "bar-generations/generation-000000"
        assert not (directory / "generation.json").exists()
        assert not (directory / "checkpoint/checkpoint.json").exists()
        assert not collector.bar_checkpoint_lock.locked() and not store._lock.locked()


async def test_explicit_pure_cancellation_chain_retains_requested_stop(tmp_path, monkeypatch):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, stopped):

        async def due(self):
            return True

        async def cancelled(*args):
            stopped.set()
            raise asyncio.CancelledError("outer cancellation") from asyncio.CancelledError(
                "worker cancellation"
            )

        monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
        monkeypatch.setattr(module, "create_bar_generation", cancelled)
        runtime = PeriodicBarRuntime(collector.config, bus, store, collector, tmp_path, stopped)
        with pytest.raises(asyncio.CancelledError):
            await runtime.run()
        assert runtime.summary()["status"] == "STOPPED"
        assert runtime.summary()["failure_type"] is None
        assert runtime.summary()["last_attempt"]["status"] == "CANCELLED"


@pytest.mark.parametrize("index", [True, -1, 256, 1.0])
async def test_generation_index_refusal_precedes_output_mutation(tmp_path, monkeypatch, index):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        with pytest.raises(ValueError, match="explicit integer"):
            await create_bar_generation(
                collector, bus, store, tmp_path / "generation-000000", index
            )
        assert not (tmp_path / "generation-000000").exists()


async def test_generation_existing_directory_and_traversal_are_never_overwritten(
    tmp_path, monkeypatch
):
    async with running(tmp_path, monkeypatch) as (collector, bus, store, _):
        directory = tmp_path / "generation-000000"
        directory.mkdir()
        retained = directory / "old"
        retained.write_bytes(b"retained")
        with pytest.raises(FileExistsError):
            await create_bar_generation(collector, bus, store, directory, 0)
        assert retained.read_bytes() == b"retained"
        with pytest.raises(ValueError, match="traversal"):
            await create_bar_generation(
                collector, bus, store, tmp_path / ".." / "generation-000000", 0
            )
        with pytest.raises(ValueError, match="basename"):
            await create_bar_generation(collector, bus, store, tmp_path / "wrong", 0)
        with pytest.raises(FileNotFoundError):
            await create_bar_generation(
                collector, bus, store, tmp_path / "missing/generation-000000", 0
            )
