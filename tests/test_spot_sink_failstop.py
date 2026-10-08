"""Ambiguous downstream delivery must halt the real stream and capture supervisor."""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from selective_options import ingestion
from selective_options.bus import EventBus
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore


def batch():
    return {
        "channel": "trade",
        "type": "snapshot",
        "data": [
            {
                "symbol": "BTC/USD",
                "side": "buy",
                "price": 100 + identifier,
                "qty": 0.1,
                "trade_id": identifier,
                "timestamp": datetime.fromtimestamp(300 + identifier, UTC).isoformat(),
            }
            for identifier in (1, 2)
        ],
    }


def connection_fixture(monkeypatch):
    opened = []

    class Socket:
        def __init__(self):
            self.messages = [
                {
                    "method": "subscribe",
                    "req_id": 2,
                    "success": True,
                    "result": {"channel": "trade", "symbol": "BTC/USD"},
                },
                batch(),
            ]

        async def __aenter__(self):
            opened.append(self)
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            if self.messages:
                return json.dumps(self.messages.pop(0))
            await asyncio.Event().wait()

    async def discover(self, socket):
        self.symbols = {"BTC/USD": "BTC"}

    monkeypatch.setattr("selective_options.spot_trades.connect", lambda *a, **k: Socket())
    monkeypatch.setattr("selective_options.spot_trades.now_ms", lambda: 303000)
    monkeypatch.setattr(SpotTradeCollector, "_discover", discover)
    return opened


@pytest.mark.parametrize("failure", [OSError, TimeoutError, RuntimeError])
@pytest.mark.parametrize("halt_status_fails", [False, True])
async def test_real_run_never_reconnects_after_partial_sink_delivery(
    monkeypatch, failure, halt_status_fails
):
    opened = connection_fixture(monkeypatch)
    delivered = []
    cause = failure("synthetic downstream failure")

    async def sink(row):
        if row.kind == "trade" and row.payload()["trade_id"] == 2:
            raise cause
        if halt_status_fails and row.payload().get("reason") == "SPOT_TRADE_HALTED":
            raise OSError("synthetic unavailable status sink")
        delivered.append(row)

    collector = SpotTradeCollector(AppConfig(), sink)
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED") as caught:
        await asyncio.wait_for(collector.run(), 3)
    assert caught.value.__cause__ is cause
    assert len(opened) == collector.connections == 1
    assert [row.payload()["trade_id"] for row in delivered if row.kind == "trade"] == [1]
    assert collector.sequences == collector.last_events == collector.bars == {}
    assert collector.awaiting_snapshot == {"BTC/USD"}
    assert not any(row.payload().get("reason") == "SPOT_TRADE_DISCONNECTED" for row in delivered)
    for call in (collector._trades(batch(), 304000), collector._flush(605000), collector.run()):
        with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
            await call
    assert len(opened) == 1


async def test_cancelled_partial_delivery_retains_state_and_forbids_reusing_collector(monkeypatch):
    opened = connection_fixture(monkeypatch)
    waiting = asyncio.Event()
    delivered = []

    async def sink(row):
        if row.kind == "trade" and row.payload()["trade_id"] == 2:
            waiting.set()
            await asyncio.Event().wait()
        delivered.append(row)

    collector = SpotTradeCollector(AppConfig(), sink)
    task = asyncio.create_task(collector.run())
    await asyncio.wait_for(waiting.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(opened) == 1
    assert collector.sequences == collector.bars == collector.last_events == {}
    assert [row.payload()["trade_id"] for row in delivered if row.kind == "trade"] == [1]
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
        await collector._trades(batch(), 304000)


@pytest.mark.parametrize("failure", [OSError, TimeoutError])
async def test_capture_marks_failed_and_drains_valid_queued_prefix(tmp_path, monkeypatch, failure):
    opened = connection_fixture(monkeypatch)
    original_publish = EventBus.publish

    async def publish(self, row):
        if row.kind == "trade" and row.payload()["trade_id"] == 2:
            raise failure("synthetic downstream queue failure")
        await original_publish(self, row)

    async def idle(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(EventBus, "publish", publish)
    monkeypatch.setattr(ingestion.DeribitCollector, "run", idle)
    monkeypatch.setattr(ingestion, "poll_chains", idle)
    config = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
        },
    )
    output = tmp_path / "failed-capture"
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED"):
        await asyncio.wait_for(ingestion.capture(config, output, 2), 10)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["failure_type"] == "IntegrityError"
    assert report["trading_enabled"] is report["milestone_2_complete"] is False
    assert len(opened) == 1
    store = await DuckStore.open(output / "events.duckdb")
    try:
        snapshot = await store.snapshot("BTC", 1000000, "public_reference")
        assert [row.payload()["trade_id"] for row in snapshot.rows(kind="trade")] == [1]
        assert not snapshot.rows(kind="bar")
        statuses = (await store.snapshot("SYSTEM", report["ended_at_ms"], "public_reference")).rows(
            kind="status"
        )
        assert any(
            row.payload().get("reason") == "SPOT_TRADE_HALTED"
            and row.payload().get("detail") == "SPOT_EMISSION_FAILED"
            for row in statuses
        )
    finally:
        await store.close()
    assert (output / "events.parquet").is_file()
    assert json.loads((output / "report.json").read_text())["status"] == "FAILED"


async def test_queue_sink_cancellation_before_any_trade_is_still_ambiguous(monkeypatch):
    connection_fixture(monkeypatch)
    sink = AsyncMock(side_effect=asyncio.CancelledError)
    collector = SpotTradeCollector(AppConfig(), sink)
    collector.symbols = {"BTC/USD": "BTC"}
    with pytest.raises(asyncio.CancelledError):
        await collector._trades(batch(), 303000)
    assert collector.sequences == collector.bars == collector.last_events == {}
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
        await collector._flush(605000)
