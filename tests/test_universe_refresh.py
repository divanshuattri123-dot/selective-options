"""Synthetic stream lifecycle tests; no public continuity claim."""

import asyncio
import json
from collections import deque
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.collectors import ChannelRegistry, DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.quality import FreshnessMonitor


def config(interval=0.02):
    # Accelerated fixture timer only. Validated operator configuration requires >=60 seconds.
    value = AppConfig()
    return value.model_copy(
        update={
            "ingestion": value.ingestion.model_copy(
                update={"stream_universe_refresh_interval_s": interval}
            )
        }
    )


def control(reason, at=1000, symbols=("A",)):
    return make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=at,
        observed_at_ms=at,
        timestamp_kind="receipt",
        payload={"reason": reason, "instruments": [{"asset": "BTC", "symbol": s} for s in symbols]},
    )


def quote(symbol="A", source_time=1000, receipt=1000, kind="book"):
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=symbol,
        kind=kind,
        event_time_ms=source_time,
        observed_at_ms=receipt,
        payload={"bids": [[99, 1]], "asks": [[101, 1]]},
    )


@pytest.mark.parametrize("mode", ["idle", "busy"])
async def test_refresh_deadline_wins_for_idle_and_busy_stream(monkeypatch, mode):
    collector = DeribitCollector(config(), AsyncMock())
    calls = []

    async def receive(*args):
        calls.append(True)
        if mode == "idle":
            await asyncio.Event().wait()
        else:
            await asyncio.sleep(0.001)

    monkeypatch.setattr(collector, "_receive", receive)
    await asyncio.wait_for(collector.receive_until_refresh(None, ChannelRegistry()), 1)
    assert calls
    if mode == "busy":
        assert len(calls) > 1


@pytest.mark.parametrize("interval", [None, 1.0])
async def test_disabled_or_long_refresh_still_enforces_heartbeat(monkeypatch, interval):
    value = config(interval)
    value = value.model_copy(
        update={"system": value.system.model_copy(update={"heartbeat_timeout_s": 0.01})}
    )
    collector = DeribitCollector(value, AsyncMock())

    async def receive(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(collector, "_receive", receive)
    with pytest.raises(TimeoutError):
        await collector.receive_until_refresh(None, ChannelRegistry())


@pytest.mark.parametrize(
    "error",
    [TimeoutError("sink"), IntegrityError("invalid notification"), asyncio.CancelledError()],
)
async def test_errors_and_cancellation_are_not_scheduled_rotation(monkeypatch, error):
    collector = DeribitCollector(config(), AsyncMock())
    monkeypatch.setattr(collector, "_receive", AsyncMock(side_effect=error))
    with pytest.raises(type(error)):
        await collector.receive_until_refresh(None, ChannelRegistry())


async def test_rotation_closes_before_rediscovery_and_replaces_subscription_set(monkeypatch):
    registries = []
    for symbol in ("expired-A", "new-B"):
        registry = ChannelRegistry()
        registry.add_instrument("BTC", symbol, "btc_usd")
        registry.book_changes[symbol] = 100
        registries.append(registry)
    sockets, received, attempts = [], [], []
    ready = asyncio.Event()

    async def discover(*args):
        assert not sockets or sockets[-1].closed
        attempts.append(True)
        return registries[len(attempts) - 1]

    monkeypatch.setattr("selective_options.collectors.discover_and_record", discover)

    class Socket:
        def __init__(self):
            self.messages, self.sent, self.closed = deque(), [], False
            sockets.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

        async def send(self, raw):
            request = json.loads(raw)
            self.sent.extend(request["params"]["channels"])
            self.messages.append(
                json.dumps(
                    {"jsonrpc": "2.0", "id": request["id"], "result": request["params"]["channels"]}
                )
            )

        async def recv(self):
            if self.messages:
                return self.messages.popleft()
            await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.collectors.connect", lambda *args, **kwargs: Socket())

    async def sink(row):
        received.append(row)
        if row.payload().get("reason") == "STREAM_SUBSCRIBED" and len(sockets) == 2:
            ready.set()

    collector = DeribitCollector(config(), sink)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(ready.wait(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert collector.connections == 2 and collector.universe_refreshes == 1
    assert all(socket.closed for socket in sockets)
    assert set(sockets[0].sent) == set(registries[0].channels)
    assert set(sockets[1].sent) == set(registries[1].channels)
    reasons = [
        row.payload()["reason"]
        for row in received
        if row.payload()["reason"] != "STREAM_SUBSCRIPTION_BATCH_ACK"
    ]
    assert reasons == [
        "STREAM_RESYNC",
        "STREAM_SUBSCRIBED",
        "STREAM_REFRESH_DUE",
        "STREAM_RESYNC",
        "STREAM_SUBSCRIBED",
    ]
    refresh = next(
        row.payload() for row in received if row.payload()["reason"] == "STREAM_REFRESH_DUE"
    )
    assert refresh["continuity"] == "UNVERIFIED_GAP_REQUIRES_HISTORY_RECONCILIATION"


async def test_bad_ack_halts_before_refresh_or_subscribed(monkeypatch):
    registry = ChannelRegistry()
    registry.add_instrument("BTC", "A", "btc_usd")
    monkeypatch.setattr(
        "selective_options.collectors.discover_and_record", AsyncMock(return_value=registry)
    )

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            return json.dumps({"jsonrpc": "2.0", "id": 1, "result": []})

    monkeypatch.setattr("selective_options.collectors.connect", lambda *args, **kwargs: Socket())
    sink = AsyncMock()
    collector = DeribitCollector(config(), sink)
    with pytest.raises(IntegrityError, match="incomplete subscription"):
        await collector.run()
    assert collector.universe_refreshes == 0
    assert all(call.args[0].payload()["reason"] == "STREAM_RESYNC" for call in sink.await_args_list)


@pytest.mark.parametrize("reason", ["STREAM_DISCONNECTED", "STREAM_REFRESH_DUE", "STREAM_RESYNC"])
def test_cached_fresh_book_is_withheld_on_inactive_stream(reason):
    monitor = FreshnessMonitor(5)
    monitor.observe(control("STREAM_SUBSCRIBED"))
    monitor.observe(quote())
    assert (
        next(row for row in monitor.option_freshness(1001, 5000) if row["kind"] == "book")["state"]
        == "FRESH"
    )
    monitor.observe(control(reason, 1002))
    assert all(row["state"] == "STREAM_INACTIVE" for row in monitor.option_freshness(1003, 5000))


def test_new_session_requires_new_snapshots_but_accepts_data_interleaved_with_ack():
    monitor = FreshnessMonitor(5)
    monitor.observe(control("STREAM_SUBSCRIBED"))
    monitor.observe(quote())
    monitor.observe(control("STREAM_RESYNC", 1100))
    monitor.observe(quote("B", 1101, 1101))
    monitor.observe(control("STREAM_SUBSCRIBED", 1102, ("A", "B")))
    result = {(row["symbol"], row["kind"]): row for row in monitor.option_freshness(1103, 5000)}
    assert result["A", "book"]["state"] == "MISSING"
    assert result["B", "book"]["state"] == "FRESH"
    # A delayed old-session event cannot satisfy the new session.
    monitor.observe(quote("A", 1100, 1099))
    result = {(row["symbol"], row["kind"]): row for row in monitor.option_freshness(1103, 5000)}
    assert result["A", "book"]["state"] == "MISSING"
    monitor.observe(quote("A", 1103, 1103))
    monitor.observe(control("STREAM_SUBSCRIBED", 1050, ("retired",)))
    result = {(row["symbol"], row["kind"]): row for row in monitor.option_freshness(1104, 5000)}
    assert result["A", "book"]["state"] == "FRESH" and ("retired", "book") not in result
    monitor.observe(control("STREAM_SUBSCRIBED", 1105, ("B",)))
    assert {row["symbol"] for row in monitor.option_freshness(1106, 5000)} == {"B"}


@pytest.mark.parametrize("value", [59, 86401, float("inf"), float("nan")])
def test_operator_refresh_bounds(value):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {"stream_universe_refresh_interval_s": value}})


def test_unrelated_or_malformed_status_does_not_activate_stream():
    monitor = FreshnessMonitor(5)
    for value in ("UNIVERSE_DISCOVERED", [], {}):
        row = control(value)
        monitor.observe(row)
    assert monitor.stream_state == "UNKNOWN"
