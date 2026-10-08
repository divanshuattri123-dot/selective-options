"""Bounded progress records are observations, never full subscription approval."""

import asyncio
import json
from collections import deque
from unittest.mock import AsyncMock

import pytest

from selective_options.collectors import ChannelRegistry, DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError


async def test_batch_progress_matches_exact_validated_responses(monkeypatch):
    registry = ChannelRegistry()
    for symbol in ("A", "B"):
        registry.add_instrument("BTC", symbol, "btc_usd")
    cfg = AppConfig.model_validate({"ingestion": {"subscription_batch_size": 3}})
    monkeypatch.setattr(
        "selective_options.collectors.discover_and_record", AsyncMock(return_value=registry)
    )
    queued = deque()

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            request = json.loads(raw)
            queued.append(
                json.dumps(
                    {"id": request["id"], "jsonrpc": "2.0", "result": request["params"]["channels"]}
                )
            )

        async def recv(self):
            return queued.popleft()

    monkeypatch.setattr("selective_options.collectors.connect", lambda *args, **kwargs: Socket())
    received = []

    async def sink(row):
        received.append(row)
        if row.payload()["reason"] == "STREAM_SUBSCRIBED":
            raise asyncio.CancelledError

    collector = DeribitCollector(cfg, sink)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    progress = [
        row.payload()
        for row in received
        if row.payload()["reason"] == "STREAM_SUBSCRIPTION_BATCH_ACK"
    ]
    assert [row["request_id"] for row in progress] == [1, 4, 7]
    assert [row["batch_channels"] for row in progress] == [3, 3, 1]
    assert [row["acknowledged_channels"] for row in progress] == [3, 6, 7]
    assert [row["acknowledged_batches"] for row in progress] == [1, 2, 3]
    assert all(row["total_channels"] == 7 and row["connection"] == 1 for row in progress)
    assert all(
        row["notification_frames"] == row["published_stream_events"] == 0 for row in progress
    )
    assert received[-1].payload()["reason"] == "STREAM_SUBSCRIBED"


@pytest.mark.parametrize("failure", ["SOCKET_RECEIVE", "SINK_PUBLISH", "malformed_ack"])
async def test_timeout_retains_pending_stage_and_only_prior_acknowledgement(monkeypatch, failure):
    registry = ChannelRegistry()
    registry.add_instrument("BTC", "A", "btc_usd")
    cfg = AppConfig.model_validate({"ingestion": {"subscription_batch_size": 2}})
    cfg = cfg.model_copy(update={"venue": cfg.venue.model_copy(update={"request_timeout_s": 0.02})})
    monkeypatch.setattr(
        "selective_options.collectors.discover_and_record", AsyncMock(return_value=registry)
    )
    queued = deque()

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            request = json.loads(raw)
            if request["id"] == 1:
                queued.append(
                    json.dumps({"id": 1, "jsonrpc": "2.0", "result": request["params"]["channels"]})
                )
            elif failure == "malformed_ack":
                queued.append(json.dumps({"id": 3, "jsonrpc": "2.0", "result": []}))
            elif failure == "SINK_PUBLISH":
                queued.append(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": "subscription",
                            "params": {
                                "channel": "deribit_price_index.btc_usd",
                                "data": {"timestamp": 1000, "price": 100, "index_name": "btc_usd"},
                            },
                        }
                    )
                )

        async def recv(self):
            if queued:
                return queued.popleft()
            await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.collectors.connect", lambda *args, **kwargs: Socket())
    received = []

    async def sink(row):
        if row.kind == "index":
            await asyncio.Event().wait()
        received.append(row)
        if row.payload().get("reason") == "STREAM_DISCONNECTED":
            raise asyncio.CancelledError

    collector = DeribitCollector(cfg, sink)
    if failure == "malformed_ack":
        with pytest.raises(IntegrityError, match="incomplete subscription"):
            await collector.run()
    else:
        with pytest.raises(asyncio.CancelledError):
            await collector.run()
        broken = received[-1].payload()
        assert broken["error_type"] == "TimeoutError"
        assert broken["phase"] == "SUBSCRIPTION_RESPONSE"
        assert broken["receive_phase"] == failure
        assert broken["request_id"] == 3 and broken["requested_channels"] == 2
        assert broken["acknowledged_channels"] == 2 and broken["acknowledged_batches"] == 1
        assert broken["total_channels"] == 4
        assert broken["notification_frames"] == int(failure == "SINK_PUBLISH")
        assert broken["published_stream_events"] == 0
    assert (
        sum(row.payload().get("reason") == "STREAM_SUBSCRIPTION_BATCH_ACK" for row in received) == 1
    )
    assert not any(row.payload().get("reason") == "STREAM_SUBSCRIBED" for row in received)
