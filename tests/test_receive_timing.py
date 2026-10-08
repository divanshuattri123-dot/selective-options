"""Deterministic exclusive monotonic timings and real deadline/cancellation accounting."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from selective_options.collectors import ChannelRegistry, DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError


def index_message():
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "deribit_price_index.btc_usd",
                "data": {"timestamp": 1000, "price": 100, "index_name": "btc_usd"},
            },
        }
    )


def registry():
    value = ChannelRegistry()
    value.add_instrument("BTC", "A", "btc_usd")
    return value


async def test_exclusive_clock_partition_preserves_actual_receipt(monkeypatch):
    ticks = iter([0, 5, 5, 12, 12, 23])
    monkeypatch.setattr("selective_options.collectors.perf_counter_ns", lambda: next(ticks))
    monkeypatch.setattr("selective_options.collectors.now_ms", lambda: 1100)
    sink = AsyncMock()
    collector = DeribitCollector(AppConfig(), sink)
    assert (
        await collector._receive(
            AsyncMock(recv=AsyncMock(return_value=index_message())), registry()
        )
        is None
    )
    assert collector.receive_timing_ns == {"SOCKET_RECEIVE": 5, "NORMALIZE": 7, "SINK_PUBLISH": 11}
    assert collector.receive_phase == "IDLE"
    assert collector.notification_frames == collector.published_stream_events == 1
    event = sink.await_args.args[0]
    assert event.event_time_ms == 1000 and event.observed_at_ms == 1100


async def test_rpc_response_has_no_sink_duration_and_returns_same_result(monkeypatch):
    ticks = iter([0, 9, 9, 13])
    monkeypatch.setattr("selective_options.collectors.perf_counter_ns", lambda: next(ticks))
    sink = AsyncMock()
    collector = DeribitCollector(AppConfig(), sink)
    socket = AsyncMock(
        recv=AsyncMock(return_value=json.dumps({"jsonrpc": "2.0", "id": 1, "result": ["channel"]}))
    )
    assert await collector._receive(socket, registry(), 1) == ["channel"]
    assert collector.receive_timing_ns == {"SOCKET_RECEIVE": 9, "NORMALIZE": 4, "SINK_PUBLISH": 0}
    sink.assert_not_awaited()


@pytest.mark.parametrize("phase", ["SOCKET_RECEIVE", "SINK_PUBLISH"])
@pytest.mark.parametrize("error", [TimeoutError, asyncio.CancelledError])
async def test_interrupted_operation_accounts_time_without_success(monkeypatch, phase, error):
    ticks = iter(range(0, 100, 5))
    monkeypatch.setattr("selective_options.collectors.perf_counter_ns", lambda: next(ticks))
    socket = AsyncMock(
        recv=AsyncMock(
            side_effect=error if phase == "SOCKET_RECEIVE" else None, return_value=index_message()
        )
    )
    sink = AsyncMock(side_effect=error if phase == "SINK_PUBLISH" else None)
    collector = DeribitCollector(AppConfig(), sink)
    with pytest.raises(error):
        await collector._receive(socket, registry())
    assert collector.receive_phase == phase
    assert collector.receive_timing_ns[phase] == 5
    assert collector.published_stream_events == 0


async def test_normalization_failure_keeps_duration_and_no_publication(monkeypatch):
    ticks = iter([0, 2, 2, 9])
    monkeypatch.setattr("selective_options.collectors.perf_counter_ns", lambda: next(ticks))
    collector = DeribitCollector(AppConfig(), AsyncMock())
    socket = AsyncMock(
        recv=AsyncMock(return_value=json.dumps({"id": 2, "jsonrpc": "2.0", "result": []}))
    )
    with pytest.raises(IntegrityError, match="unexpected stream"):
        await collector._receive(socket, registry(), 1)
    assert collector.receive_timing_ns == {"SOCKET_RECEIVE": 2, "NORMALIZE": 7, "SINK_PUBLISH": 0}
    assert collector.receive_phase == "NORMALIZE"


def test_timing_snapshot_is_detached_and_pending_nonresponse_is_unknown():
    collector = DeribitCollector(AppConfig(), AsyncMock())
    before = dict(collector.receive_timing_ns)
    collector.receive_timing_ns["SOCKET_RECEIVE"] = 10
    first = collector._timing_fields(before, 20)
    assert first["response_timing_ns"] == {"SOCKET_RECEIVE": 10, "NORMALIZE": 0, "SINK_PUBLISH": 0}
    collector.receive_timing_ns["SOCKET_RECEIVE"] = 30
    assert first["receive_timing_ns"]["SOCKET_RECEIVE"] == 10
    unknown = collector._timing_fields(before, None)
    assert unknown["response_timing_ns"] is unknown["response_elapsed_ns"] is None


@pytest.mark.parametrize("failure", ["SOCKET_RECEIVE", "SINK_PUBLISH"])
async def test_real_whole_response_deadline_measures_inflight_operation(monkeypatch, failure):
    cfg = AppConfig()
    cfg = cfg.model_copy(update={"venue": cfg.venue.model_copy(update={"request_timeout_s": 0.03})})
    monkeypatch.setattr(
        "selective_options.collectors.discover_and_record", AsyncMock(return_value=registry())
    )

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            if failure == "SOCKET_RECEIVE":
                await asyncio.Event().wait()
            return index_message()

    received = []

    async def sink(event):
        if event.kind == "index":
            await asyncio.Event().wait()
        received.append(event)
        if event.payload()["reason"] == "STREAM_DISCONNECTED":
            raise asyncio.CancelledError

    monkeypatch.setattr("selective_options.collectors.connect", lambda *args, **kwargs: Socket())
    collector = DeribitCollector(cfg, sink)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    failure_body = received[-1].payload()
    partition = failure_body["response_timing_ns"]
    assert failure_body["response_elapsed_ns"] >= sum(partition.values()) > 0
    assert partition[failure] > 0
    assert failure_body["receive_phase"] == failure
    assert failure_body["acknowledged_channels"] == 0
    assert not any(event.payload().get("reason") == "STREAM_SUBSCRIBED" for event in received)
