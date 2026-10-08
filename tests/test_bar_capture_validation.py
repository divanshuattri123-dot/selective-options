"""Independent ledger and committed capture-boundary regressions; mocked transport only."""

import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options import ingestion
from selective_options.bar_checkpoint import load_bar_checkpoint, verify_bar_checkpoint_seed
from selective_options.bar_runtime import install_bar_resume, save_collector_bar_checkpoint
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore


def config():
    return AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "policy_news_sources": [],
            "bar_timeframes_s": [300, 900],
            "bar_lateness_s": 0,
            "flush_interval_s": 0.001,
        },
    )


def wire(identifier, timestamp, *, price=100.0, qty=1.0):
    return {
        "symbol": "BTC/USD",
        "side": "buy",
        "price": price,
        "qty": qty,
        "trade_id": identifier,
        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(),
    }


def message(rows, kind="update"):
    return {"channel": "trade", "type": kind, "data": rows}


def frontier(collector):
    return {
        "inputs": deepcopy(collector.bar_inputs),
        "bars": {symbol: deepcopy(vars(builder)) for symbol, builder in collector.bars.items()},
        "sequences": {
            symbol: (sequence.last, tuple(sequence.seen.items()))
            for symbol, sequence in collector.sequences.items()
        },
        "last_events": dict(collector.last_events),
        "awaiting": set(collector.awaiting_snapshot),
    }


async def collector(*, tracking=True, limit=10000, first_clock=1001000):
    emitted = []

    async def sink(event):
        emitted.append(event)

    result = SpotTradeCollector(config(), sink, track_bar_inputs=tracking, bar_input_limit=limit)
    result.symbols = {"BTC/USD": "BTC"}
    result.awaiting_snapshot.add("BTC/USD")
    await result._trades(message([wire(1, first_clock)], "snapshot"), first_clock + 1000)
    emitted.clear()
    return result, emitted


async def test_union_window_trim_retains_below_watermark_inputs_then_releases():
    result, emitted = await collector()
    original = result.bar_inputs["BTC/USD"][0]
    await result._flush(1200000)
    assert result.bars["BTC/USD"].watermark_ms == 1200000 > original.event_time_ms
    assert result.bar_inputs["BTC/USD"] == (original,)
    assert {(interval, opening) for interval, opening in result.bars["BTC/USD"].buckets} == {
        (900000, 900000)
    }
    await result._trades(message([wire(2, 1251000, price=102.0, qty=2.0)]), 1252000)
    assert [row.payload()["trade_id"] for row in result.bar_inputs["BTC/USD"]] == [1, 2]
    await result._flush(1800000)
    assert result.bar_inputs["BTC/USD"] == () and not result.bars["BTC/USD"].buckets
    assert result.last_events["BTC/USD"].payload()["trade_id"] == 2
    long_bar = [
        row for row in emitted if row.kind == "bar" and row.payload()["timeframe_s"] == 900
    ][0]
    assert long_bar.payload()["trade_count"] == 2 and long_bar.payload()["volume"] == 3.0


async def test_raw_only_new_snapshot_advances_tail_without_bar_membership():
    result, emitted = await collector(first_clock=601000)
    original = result.bar_inputs["BTC/USD"]
    await result._flush(650000)
    before = deepcopy(vars(result.bars["BTC/USD"]))
    result.awaiting_snapshot.add("BTC/USD")
    await result._trades(message([wire(2, 649000)], "snapshot"), 651000)
    assert result.bar_inputs["BTC/USD"] == original
    assert vars(result.bars["BTC/USD"]) == before
    assert result.last_events["BTC/USD"].payload()["trade_id"] == 2
    assert any(
        row.kind == "status" and row.payload().get("reason") == "HISTORICAL_TRADE_RAW_ONLY"
        for row in emitted
    )


async def test_tracking_disabled_retains_original_behavior_without_ledger():
    result, _ = await collector(tracking=False)
    await result._trades(message([wire(2, 1002000)]), 1003000)
    await result._flush(1200000)
    assert result.bar_inputs == {} and result.bars["BTC/USD"].buckets


async def test_full_batch_ledger_budget_refusal_precedes_sink_and_preserves_frontier():
    result, emitted = await collector(limit=1)
    before = frontier(result)
    with pytest.raises(IntegrityError, match="BAR_CHECKPOINT_INPUT_BUDGET"):
        await result._trades(message([wire(2, 1002000)]), 1003000)
    assert emitted == [] and frontier(result) == before and not result._emission_failed


async def test_later_invalid_bar_member_keeps_valid_prefix_out_of_ledger():
    result, emitted = await collector(first_clock=601000)
    await result._flush(650000)
    emitted.clear()
    before = frontier(result)
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        await result._trades(message([wire(2, 651000), wire(3, 649000)]), 652000)
    assert emitted == [] and frontier(result) == before


@pytest.mark.parametrize("cancel", [False, True])
async def test_ambiguous_sink_prefix_never_installs_new_ledger_and_stays_poisoned(cancel):
    result, _ = await collector()
    before = frontier(result)
    prefix = []

    async def fail(event):
        if prefix:
            if cancel:
                raise asyncio.CancelledError()
            raise OSError("synthetic second sink failure")
        prefix.append(event)

    result.sink = fail
    with pytest.raises(asyncio.CancelledError if cancel else IntegrityError):
        await result._trades(message([wire(2, 1002000), wire(3, 1003000)]), 1004000)
    assert len(prefix) == 1 and frontier(result) == before and result._emission_failed
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
        await result._flush(1200000)


async def test_flush_emission_failure_does_not_trim_ledger_or_close_live_buckets():
    result, _ = await collector()
    before = frontier(result)
    prefix = []

    async def fail(event):
        if prefix:
            raise OSError("synthetic later bar sink failure")
        prefix.append(event)

    result.sink = fail
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED"):
        await result._flush(1800000)
    assert len(prefix) == 1 and frontier(result) == before and result._emission_failed


# The following tests execute the actual committed handoff and consumer. Only
# transport and finite clocks are synthetic; no network or capture-completeness claim.
async def saved_checkpoint(tmp_path):
    result, emitted = await collector()
    original = result.bar_inputs["BTC/USD"][0]
    store = await DuckStore.open(tmp_path / "source.duckdb")
    try:
        await store.append([original], exact=True)
        await store.acknowledge_delivery([original.event_id])
        await result._flush(1500000)
        await store.append(emitted, exact=True)
        pending = await store.pending_delivery_count()
        manifest = await save_collector_bar_checkpoint(
            result, store, tmp_path / "checkpoint", 1500000
        )
        assert await store.pending_delivery_count() == pending
    finally:
        await store.close()
    resume = await load_bar_checkpoint(tmp_path / "checkpoint", 1500000, ("BTC",), (300, 900), 0)
    return original, resume, manifest


async def test_committed_seed_roundtrip_preserves_receipt_and_resumes_partial(tmp_path):
    original, resume, manifest = await saved_checkpoint(tmp_path)
    assert resume.events == (original,) and manifest["delivery_acknowledgements_copied"] is False
    before = hashlib.sha256((tmp_path / "checkpoint/events.duckdb").read_bytes()).hexdigest()
    with duckdb.connect(str(tmp_path / "source.duckdb"), read_only=True) as connection:
        assert connection.execute(
            "SELECT delivered FROM delivery_outbox WHERE event_id=?", [original.event_id]
        ).fetchone() == (True,)
    copied = tmp_path / "seed.duckdb"
    store = await DuckStore.open(copied)
    await store.append(list(resume.events), exact=True)
    await store.close()
    verified = await verify_bar_checkpoint_seed(copied, resume, 1500000)
    assert verified["events"] == 1 and not verified["delivery_acknowledgements_copied"]
    emitted = []

    async def sink(event):
        emitted.append(event)

    fresh = SpotTradeCollector(config(), sink, track_bar_inputs=True)
    install_bar_resume(fresh, resume, 1500000)
    assert fresh.bar_inputs["BTC/USD"] == (original,)
    assert fresh.last_events["BTC/USD"] == original
    assert fresh.awaiting_snapshot == {"BTC/USD"}
    assert all(not row.complete for row in fresh.bars["BTC/USD"].buckets.values())
    fresh.symbols = {"BTC/USD": "BTC"}  # Explicitly mocked dynamic discovery.
    with pytest.raises(IntegrityError, match="BEFORE_RECOVERY_SNAPSHOT"):
        await fresh._trades(message([wire(2, 1501000)]), 1502000)
    await fresh._trades(message([wire(1, 1001000), wire(2, 1501000, qty=2.0)], "snapshot"), 1502000)
    assert fresh.bar_inputs["BTC/USD"][0].observed_at_ms == 1002000
    await fresh._flush(1800000)
    bars = [row for row in emitted if row.kind == "bar"]
    long_bar = next(row for row in bars if row.payload()["timeframe_s"] == 900)
    assert long_bar.payload()["trade_count"] == 2 and long_bar.payload()["volume"] == 3.0
    assert long_bar.payload()["complete"] is False
    assert (
        next(row for row in bars if row.payload()["timeframe_s"] == 300).payload()["complete"]
        is True
    )
    assert (
        hashlib.sha256((tmp_path / "checkpoint/events.duckdb").read_bytes()).hexdigest() == before
    )


@pytest.mark.parametrize("case", ["disabled", "poisoned", "awaiting", "pending", "tail"])
async def test_runtime_save_refuses_unresolved_or_untracked_frontier_before_output(tmp_path, case):
    result, _ = await collector(tracking=case != "disabled")
    if case == "poisoned":
        result._emission_failed = True
    elif case == "awaiting":
        result.awaiting_snapshot.add("BTC/USD")
    elif case == "pending":
        result.pending_frames.append((message([wire(2, 1002000)]), 1003000))
    elif case == "tail":
        result.sequences["BTC/USD"].last = 99
    store = await DuckStore.open(tmp_path / "source.duckdb")
    try:
        with pytest.raises(IntegrityError):
            await save_collector_bar_checkpoint(result, store, tmp_path / "refused", 1500000)
        assert not (tmp_path / "refused").exists()
    finally:
        await store.close()


async def test_install_revalidates_forged_resume_without_partial_state(tmp_path):
    _, resume, _ = await saved_checkpoint(tmp_path)
    fresh = SpotTradeCollector(config(), AsyncMock(), track_bar_inputs=True)
    forged = resume.model_copy(update={"events": ()})
    with pytest.raises(IntegrityError, match="MEMBERSHIP"):
        install_bar_resume(fresh, forged, 1500000)
    assert not fresh.bars and not fresh.bar_inputs and not fresh.sequences
    install_bar_resume(fresh, resume, 1500000)
    installed = frontier(fresh)
    with pytest.raises(IntegrityError, match="MUST_BE_FRESH"):
        install_bar_resume(fresh, resume, 1500000)
    assert frontier(fresh) == installed


async def idle(*args):
    await asyncio.Event().wait()


def quiet_capture(monkeypatch, clock=1500000):
    monkeypatch.setattr(ingestion, "now_ms", lambda: clock)
    monkeypatch.setattr("selective_options.collectors.now_ms", lambda: clock)
    monkeypatch.setattr(ingestion.DeribitCollector, "run", idle)
    monkeypatch.setattr(ingestion, "poll_chains", idle)


async def test_capture_default_does_not_track_or_save_bars(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    checked = []

    async def run(self):
        checked.append(self.track_bar_inputs)
        await idle()

    monkeypatch.setattr(SpotTradeCollector, "run", run)
    report = await ingestion.capture(config(), tmp_path / "capture", 0.04)
    assert checked == [False] and "bar_checkpoint" not in report
    assert not (tmp_path / "capture/bar-checkpoint").exists()


async def test_capture_save_follows_stopped_producer_and_successful_writer_drain(
    tmp_path, monkeypatch
):
    quiet_capture(monkeypatch)
    producer_stopped = asyncio.Event()
    writer_done = asyncio.Event()
    consumed = ingestion.consume
    save = ingestion.save_collector_bar_checkpoint
    saves = []

    async def writer(*args, **kwargs):
        await consumed(*args, **kwargs)
        writer_done.set()

    async def run(self):
        try:
            assert self.track_bar_inputs
            self.symbols = {"BTC/USD": "BTC"}
            self.awaiting_snapshot.add("BTC/USD")
            await self._trades(message([wire(1, 1001000)], "snapshot"), 1002000)
            await self._flush(1500000)
            await idle()
        finally:
            producer_stopped.set()

    async def saving(collector, store, output, as_of_ms):
        assert producer_stopped.is_set() and writer_done.is_set()
        saves.append(output)
        return await save(collector, store, output, as_of_ms)

    monkeypatch.setattr(ingestion, "consume", writer)
    monkeypatch.setattr(SpotTradeCollector, "run", run)
    monkeypatch.setattr(ingestion, "save_collector_bar_checkpoint", saving)
    report = await ingestion.capture(config(), tmp_path / "capture", 0.06, save_bars_on_stop=True)
    assert saves == [tmp_path / "capture/bar-checkpoint"]
    assert report["bar_checkpoint"]["status"] == "COMMITTED_BAR_CHECKPOINT"
    resume = await load_bar_checkpoint(saves[0], 1500000, ("BTC",), (300, 900), 0)
    assert len(resume.events) == 1 and resume.events[0].observed_at_ms == 1002000
    assert not report["ready_for_signals"] and not report["trading_enabled"]


@pytest.mark.parametrize("case", ["tail", "marker", "disabled", "nonbool", "config"])
async def test_capture_bar_preflight_refusal_creates_nothing_or_producer(
    tmp_path, monkeypatch, case
):
    quiet_capture(monkeypatch)
    _, _, _ = await saved_checkpoint(tmp_path)
    run = AsyncMock()
    monkeypatch.setattr(SpotTradeCollector, "run", run)
    selected = config()
    kwargs = {"resume_bars_from": tmp_path / "checkpoint"}
    if case == "tail":
        kwargs["resume_trades_from"] = tmp_path / "not-opened.duckdb"
    elif case == "marker":
        kwargs["recovery_marker"] = "not-opened"
    elif case == "disabled":
        selected = config().model_copy(
            update={
                "ingestion": config().ingestion.model_copy(update={"spot_trades_enabled": False})
            }
        )
    elif case == "nonbool":
        kwargs["save_bars_on_stop"] = 1
    elif case == "config":
        selected = config().model_copy(
            update={"ingestion": config().ingestion.model_copy(update={"bar_timeframes_s": (300,)})}
        )
    with pytest.raises((ValueError, IntegrityError)):
        await ingestion.capture(selected, tmp_path / "capture", 0.05, **kwargs)
    run.assert_not_awaited()
    assert not (tmp_path / "capture").exists()


@pytest.mark.parametrize("cancel", [False, True])
async def test_capture_closed_seed_refusal_starts_no_producer_and_keeps_exact_pending(
    tmp_path, monkeypatch, cancel
):
    quiet_capture(monkeypatch)
    original, resume, _ = await saved_checkpoint(tmp_path)
    real_verify = ingestion.verify_bar_checkpoint_seed
    calls = []

    async def refuse(path, selection, clock, **policy):
        result = await real_verify(path, selection, clock, **policy)
        calls.append(result)
        if cancel:
            raise asyncio.CancelledError()
        raise IntegrityError("SYNTHETIC_CLOSED_SEED_REFUSAL")

    monkeypatch.setattr(ingestion, "verify_bar_checkpoint_seed", refuse)
    run = AsyncMock()
    monkeypatch.setattr(SpotTradeCollector, "run", run)
    with pytest.raises(asyncio.CancelledError if cancel else IntegrityError):
        await ingestion.capture(
            config(), tmp_path / "capture", 0.05, resume_bars_from=tmp_path / "checkpoint"
        )
    run.assert_not_awaited()
    assert calls[0]["status"] == "BAR_CHECKPOINT_SEED_VERIFIED"
    assert (await verify_bar_checkpoint_seed(tmp_path / "capture/events.duckdb", resume, 1500000))[
        "events"
    ] == 1
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT event_id,observed_at_ms FROM events").fetchall() == [
            (original.event_id, original.observed_at_ms)
        ]
        assert connection.execute("SELECT event_id,delivered FROM delivery_outbox").fetchall() == [
            (original.event_id, False)
        ]


async def test_actual_capture_resume_installs_before_transport_and_emits_partial_continuation(
    tmp_path, monkeypatch
):
    original, resume, _ = await saved_checkpoint(tmp_path)
    quiet_capture(monkeypatch, 1800000)
    seen = []

    async def run(self):
        assert self.last_events["BTC/USD"] == original
        assert self.bar_inputs["BTC/USD"] == (original,)
        assert self.awaiting_snapshot == {"BTC/USD"}
        assert all(not bucket.complete for bucket in self.bars["BTC/USD"].buckets.values())
        self.symbols = {"BTC/USD": "BTC"}
        await self._trades(
            message([wire(1, 1001000), wire(2, 1501000, price=102.0, qty=2.0)], "snapshot"), 1502000
        )
        await self._flush(1800000)
        seen.append(True)
        await idle()

    monkeypatch.setattr(SpotTradeCollector, "run", run)
    report = await ingestion.capture(
        config(), tmp_path / "capture", 0.06, resume_bars_from=tmp_path / "checkpoint"
    )
    assert (
        seen == [True]
        and report["bar_resume"]["status"] == "COMMITTED_SEED_VERIFIED_BEFORE_PRODUCERS"
    )
    assert report["bar_resume"]["closed_seed_verification"]["events"] == len(resume.events)
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as connection:
        bars = [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT payload_json FROM events WHERE kind='bar'"
            ).fetchall()
        ]
        native = connection.execute(
            "SELECT observed_at_ms FROM events WHERE event_id=?", [original.event_id]
        ).fetchone()
        assert native == (original.observed_at_ms,)
        assert len(bars) == 2 and all(not row["complete"] for row in bars)
        long_bar = next(row for row in bars if row["timeframe_s"] == 900)
        assert long_bar["trade_count"] == 2 and long_bar["volume"] == 3.0
        assert connection.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered"
        ).fetchone() == (0,)


async def test_failed_producer_skips_checkpoint_even_after_writer_drains(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)

    async def run(self):
        self.symbols = {"BTC/USD": "BTC"}
        self.awaiting_snapshot.add("BTC/USD")
        await self._trades(message([wire(1, 1001000)], "snapshot"), 1002000)
        raise IntegrityError("SYNTHETIC_PRODUCER_FAILURE")

    monkeypatch.setattr(SpotTradeCollector, "run", run)
    with pytest.raises(IntegrityError, match="SYNTHETIC_PRODUCER_FAILURE"):
        await ingestion.capture(config(), tmp_path / "capture", 0.06, save_bars_on_stop=True)
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["bar_checkpoint"]["status"] == "NOT_SAVED_CAPTURE_FAILED"
    assert not (tmp_path / "capture/bar-checkpoint/checkpoint.json").exists()
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events WHERE kind='trade'").fetchone() == (
            1,
        )


async def test_writer_failure_skips_checkpoint_and_stops_producer(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    stopped = asyncio.Event()

    async def run(self):
        try:
            await idle()
        finally:
            stopped.set()

    async def failed_writer(*args, **kwargs):
        await asyncio.sleep(0.005)
        raise IntegrityError("SYNTHETIC_WRITER_FAILURE")

    monkeypatch.setattr(SpotTradeCollector, "run", run)
    monkeypatch.setattr(ingestion, "consume", failed_writer)
    with pytest.raises(IntegrityError, match="SYNTHETIC_WRITER_FAILURE"):
        await ingestion.capture(config(), tmp_path / "capture", 0.1, save_bars_on_stop=True)
    assert stopped.is_set()
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["bar_checkpoint"]["status"] == "NOT_SAVED_CAPTURE_FAILED"
    assert not (tmp_path / "capture/bar-checkpoint/checkpoint.json").exists()


async def test_cancelled_capture_drains_accepted_rows_without_checkpoint(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    ready = asyncio.Event()

    async def run(self):
        self.symbols = {"BTC/USD": "BTC"}
        self.awaiting_snapshot.add("BTC/USD")
        await self._trades(message([wire(1, 1001000)], "snapshot"), 1002000)
        ready.set()
        await idle()

    monkeypatch.setattr(SpotTradeCollector, "run", run)
    task = asyncio.create_task(
        ingestion.capture(config(), tmp_path / "capture", 10, save_bars_on_stop=True)
    )
    await ready.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["bar_checkpoint"]["status"] == "NOT_SAVED_CAPTURE_FAILED"
    assert not (tmp_path / "capture/bar-checkpoint/checkpoint.json").exists()
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events WHERE kind='trade'").fetchone() == (
            1,
        )


async def test_repeated_cancel_during_final_close_drains_connection(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    monkeypatch.setattr(SpotTradeCollector, "run", idle)
    entered = asyncio.Event()
    release = asyncio.Event()
    real_close = DuckStore.close
    opened = []

    async def gated_close(self):
        opened.append(self)
        entered.set()
        await release.wait()
        await real_close(self)

    monkeypatch.setattr(DuckStore, "close", gated_close)
    task = asyncio.create_task(ingestion.capture(config(), tmp_path / "capture", 0.03))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
        assert opened and all(store._closed for store in opened)
    finally:
        for store in opened:
            await real_close(store)


async def test_repeated_cancel_drains_mirror_after_store_close_before_report(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    monkeypatch.setattr(SpotTradeCollector, "run", idle)
    entered, release = asyncio.Event(), asyncio.Event()
    closes = []
    real_close = DuckStore.close

    async def observed_close(self):
        await real_close(self)
        closes.append(self._closed)

    class Mirror:
        def __init__(self, url):
            self.closed = False

        async def publish_batch(self, rows):
            pytest.fail("mocked producers publish no rows")

        async def close(self):
            assert closes == [True]
            entered.set()
            await release.wait()
            self.closed = True

    monkeypatch.setattr(DuckStore, "close", observed_close)
    monkeypatch.setattr(ingestion, "RedisMirror", Mirror)
    selected = config().model_copy(
        update={
            "ingestion": config().ingestion.model_copy(
                update={"redis_url": "redis://example.invalid:6379/0"}
            )
        }
    )
    task = asyncio.create_task(ingestion.capture(selected, tmp_path / "capture", 0.03))
    await entered.wait()
    assert not (tmp_path / "capture/report.json").exists()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["failure_type"] == "CancelledError"


async def test_repeated_cancel_during_report_write_republishes_failed_status(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    monkeypatch.setattr(SpotTradeCollector, "run", idle)
    entered, release = asyncio.Event(), asyncio.Event()
    complete = ingestion.complete_thread
    calls = []

    async def gated(function, *args, **kwargs):
        result = await complete(function, *args, **kwargs)
        bound = getattr(function, "__self__", None)
        if getattr(bound, "name", None) == "report.json":
            calls.append(json.loads(args[0])["status"])
            if len(calls) == 1:
                entered.set()
                await release.wait()
        return result

    monkeypatch.setattr(ingestion, "complete_thread", gated)
    task = asyncio.create_task(ingestion.capture(config(), tmp_path / "capture", 0.03))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert calls == ["CAPTURE_FINISHED", "FAILED"]
    assert report["status"] == "FAILED" and report["failure_type"] == "CancelledError"
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone() == (0,)
