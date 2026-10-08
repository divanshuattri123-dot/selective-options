import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from test_recovery_journal import plan
from test_trade_backfill import body, config
from test_trade_reconciliation import rest, ws

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.gap_recovery import recover_trade_gap
from selective_options.recovery_journal import (
    load_committed_recovery,
    load_recovery,
    publish_recovery,
)
from selective_options.store import DuckStore
from selective_options.trade_backfill import collect_range


async def idle():
    await asyncio.Event().wait()


async def test_fetch_buffers_live_tail_and_reloads_without_closing_writer(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    await store.append([ws()])
    received = asyncio.Event()
    receive_count = 0

    async def receive():
        nonlocal receive_count
        receive_count += 1
        if receive_count == 1:
            received.set()
            return ws(6, observed=560000)
        return await idle()

    async def fetch(*args):
        await received.wait()
        # An unrelated producer can commit while REST is pending.
        await store.append([ws(20, observed=570000, symbol="ETH/USD")])
        return body([3, 4, 5]), 600000

    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    result = await recover_trade_gap(
        config(kraken_backfill_page_size=3),
        store,
        ws(),
        5,
        receive,
        tmp_path / "attempt",
        initial=(ws(5, observed=550000),),
    )
    assert result.plan.through_id == 6
    assert [(row.source, row.payload()["trade_id"]) for row in result.plan.selected] == [
        ("kraken_rest", 4),
        ("kraken", 5),
        ("kraken", 6),
    ]
    assert [row.observed_at_ms for row in result.plan.selected] == [600000, 550000, 560000]
    assert await store.pending_delivery_count() == 7
    assert (
        await load_committed_recovery(store, result.marker.event_id, result.plan.verified_at_ms)
    )[1] == result.plan
    assert not (await store.snapshot("BTC", 549999, "public_reference")).rows(source="kraken_rest")
    report = json.loads((tmp_path / "attempt/report.json").read_text())
    assert report["status"] == "COMMITTED_AND_RELOADED"
    assert report["publication_outcome"] == "COMMITTED"
    assert report["runtime_activated"] is report["bars_rebuilt"] is False
    await store.close()
    assert (
        await load_recovery(
            tmp_path / "capture.duckdb", result.marker.event_id, result.plan.verified_at_ms
        )
        == result.plan
    )


@pytest.mark.parametrize("mode", ["cancel", "timeout", "receiver_failure", "fetch_failure"])
async def test_failure_joins_workers_and_never_publishes(tmp_path, monkeypatch, mode):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    entered = asyncio.Event()
    stopped = set()

    async def fetch(*args):
        try:
            entered.set()
            if mode == "fetch_failure":
                raise IntegrityError("TEST_FETCH_FAILURE")
            await idle()
        finally:
            stopped.add("fetch")

    async def receive():
        try:
            await entered.wait()
            if mode == "receiver_failure":
                raise IntegrityError("TEST_RECEIVER_FAILURE")
            await idle()
        finally:
            stopped.add("receiver")

    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    task = asyncio.create_task(
        recover_trade_gap(
            config(kraken_backfill_timeout_s=1), store, ws(), 5, receive, tmp_path / "attempt"
        )
    )
    await entered.wait()
    if mode == "cancel":
        task.cancel()
    error = (
        asyncio.CancelledError
        if mode == "cancel"
        else TimeoutError
        if mode == "timeout"
        else IntegrityError
    )
    with pytest.raises(error):
        await task
    assert stopped == {"fetch", "receiver"}
    assert await store.pending_delivery_count() == 0
    report = json.loads((tmp_path / "attempt/report.json").read_text())
    assert report["status"] == "FAILED" and report["publication_outcome"] == "NOT_ATTEMPTED"
    await store.close()


@pytest.mark.parametrize("tail,error", [(7, "INCOMPLETE"), (5, "CONFLICTING")])
async def test_buffer_gap_or_conflict_prevents_publication(tmp_path, monkeypatch, tail, error):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    monkeypatch.setattr(
        "selective_options.trade_backfill.fetch_page",
        AsyncMock(return_value=(body([3, 4, 5]), 600000)),
    )
    with pytest.raises(IntegrityError, match=error):
        await recover_trade_gap(
            config(kraken_backfill_page_size=3),
            store,
            ws(),
            5,
            idle,
            tmp_path / "attempt",
            initial=(ws(tail, observed=550000, price=101.0 if tail == 5 else 100.0),),
        )
    assert await store.pending_delivery_count() == 0
    await store.close()


async def test_existing_receipt_conflict_cannot_publish_partial_plan(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    await store.append([rest(4, observed=590000)])
    monkeypatch.setattr(
        "selective_options.trade_backfill.fetch_page",
        AsyncMock(return_value=(body([3, 4, 5]), 600000)),
    )
    with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
        await recover_trade_gap(
            config(kraken_backfill_page_size=3), store, ws(), 5, idle, tmp_path / "attempt"
        )
    assert await store.pending_delivery_count() == 1
    await store.close()


@pytest.mark.parametrize(
    "interruption", [IntegrityError("TEST_RELOAD_FAILURE"), asyncio.CancelledError()]
)
async def test_after_commit_interruption_preserves_marker_for_explicit_restart(
    tmp_path, monkeypatch, interruption
):
    path = tmp_path / "capture.duckdb"
    store = await DuckStore.open(path)
    monkeypatch.setattr(
        "selective_options.trade_backfill.fetch_page",
        AsyncMock(return_value=(body([3, 4, 5]), 600000)),
    )
    monkeypatch.setattr(
        "selective_options.gap_recovery.load_committed_recovery",
        AsyncMock(side_effect=interruption),
    )
    with pytest.raises(type(interruption)):
        await recover_trade_gap(
            config(kraken_backfill_page_size=3), store, ws(), 5, idle, tmp_path / "attempt"
        )
    assert await store.pending_delivery_count() == 4
    report = json.loads((tmp_path / "attempt/report.json").read_text())
    assert report["status"] == "FAILED" and report["stage"] == "RELOAD"
    assert report["publication_outcome"] == "COMMITTED"
    await store.close()
    recovered = await load_recovery(path, report["marker_id"], report["ended_at_ms"])
    assert recovered.through_id == 5


@pytest.mark.parametrize("case", ["range", "disabled", "identity", "book", "exists"])
async def test_preflight_fails_before_network(tmp_path, monkeypatch, case):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    fetch = AsyncMock()
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    output = tmp_path / "attempt"
    if case == "exists":
        output.mkdir()
    with pytest.raises((ValueError, IntegrityError, FileExistsError)):
        await recover_trade_gap(
            AppConfig.model_validate({"system": {"assets": ["ETH"]}})
            if case == "disabled"
            else config(),
            store,
            ws().model_copy(update={"event_id": "forged"}) if case == "identity" else ws(),
            3 if case == "range" else 5,
            idle,
            output,
            initial=(ws(5, symbol="ETH/USD"),) if case == "book" else (),
        )
    fetch.assert_not_awaited()
    assert not output.exists() or case == "exists"
    await store.close()


async def test_open_store_reload_missing_and_temporal_guards(tmp_path):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    with pytest.raises(IntegrityError, match="MARKER_MISSING"):
        await load_committed_recovery(store, "missing", 620000)
    marker = await publish_recovery(store, ws(), plan())
    with pytest.raises(IntegrityError, match="NOT_YET_PUBLISHED"):
        await load_committed_recovery(store, marker.event_id, 619999)
    assert await store.read_events([]) == ()
    with pytest.raises(ValueError, match="budget"):
        await store.read_events(["x"] * 20002)
    await store.close()


@pytest.mark.parametrize("timestamp", ["303.123456789", "303.123456790"])
async def test_rest_anchor_keeps_exact_precision_for_later_gap(tmp_path, monkeypatch, timestamp):
    anchor = rest(3, observed=500000, timestamp="303.123456789")
    raw = body([3, 4]).replace(b"303,", timestamp.encode() + b",")
    fetch = AsyncMock(return_value=(raw, 600000))
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    if timestamp.endswith("790"):
        with pytest.raises(IntegrityError, match="ANCHOR_CONFLICT"):
            await collect_range(config(), anchor, 4, tmp_path)
    else:
        rows, pages = await collect_range(config(), anchor, 4, tmp_path)
        assert pages == 1 and rows[0].payload()["trade_id"] == 4
    assert fetch.await_args.args[2] == "302123456789"


async def test_collect_range_rejects_empty_range_without_network(tmp_path, monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    with pytest.raises(ValueError, match="not positive"):
        await collect_range(config(), ws(), 3, tmp_path)
    fetch.assert_not_awaited()


async def test_second_gap_can_anchor_on_previously_committed_rest_tail(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    fetch = AsyncMock(side_effect=[(body([3, 4, 5]), 600000), (body([5, 6, 7]), 700000)])
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    settings = config(kraken_backfill_page_size=3)
    first = await recover_trade_gap(settings, store, ws(), 5, idle, tmp_path / "first")
    assert first.plan.selected[-1].source == "kraken_rest"
    second = await recover_trade_gap(
        settings, store, first.plan.selected[-1], 7, idle, tmp_path / "second"
    )
    assert second.plan.anchor_id == 5 and second.plan.through_id == 7
    assert await store.pending_delivery_count() == 7
    assert (await load_committed_recovery(store, first.marker.event_id, first.plan.verified_at_ms))[
        1
    ] == first.plan
    await store.close()


async def test_open_store_loader_rejects_missing_evidence(tmp_path):
    store = await DuckStore.open(tmp_path / "capture.duckdb")
    marker = await publish_recovery(store, ws(), plan())
    # Simulate external corruption outside the append-only application API.
    store._connection.execute("DELETE FROM events WHERE event_id=?", [plan().raw[0].event_id])
    with pytest.raises(IntegrityError, match="EVIDENCE_MISSING"):
        await load_committed_recovery(store, marker.event_id, 620000)
    await store.close()
