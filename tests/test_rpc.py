import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from websockets.asyncio.client import connect as real_connect
from websockets.asyncio.server import serve

from selective_options.config import AppConfig, BucketConfig, RateConfig, VenueConfig
from selective_options.errors import IntegrityError, RpcError, TradingUnavailable, TransportError
from selective_options.models import RpcEnvelope
from selective_options.rate_limit import EndpointLimiter, TokenBucket
from selective_options.rpc import WebSocketRpc


def client():
    return WebSocketRpc(VenueConfig(), EndpointLimiter(RateConfig()))


async def test_real_local_websocket_transport(monkeypatch):
    async def handler(socket):
        async for raw in socket:
            request = json.loads(raw)
            await socket.send(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": 123}))

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        monkeypatch.setattr(
            "selective_options.rpc.connect",
            lambda *a, **kw: real_connect(f"ws://127.0.0.1:{port}", **kw),
        )
        async with client() as rpc:
            assert await rpc.call("public/get_time", {}) == 123
            assert await asyncio.gather(
                rpc.call("public/get_time", {}), rpc.call("public/get_time", {})
            ) == [123, 123]


@pytest.mark.parametrize(
    "method",
    [
        "private/buy",
        "private/sell",
        "private/withdraw",
        "private/enable_cancel_on_disconnect",
        "private/cancel_all",
    ],
)
async def test_no_order_or_wallet_path(method):
    with pytest.raises(TradingUnavailable):
        await client().call(method, {})


async def test_connection_required():
    with pytest.raises(TransportError):
        await client().call("public/get_time", {})


async def test_handshake_failure(monkeypatch):
    monkeypatch.setattr("selective_options.rpc.connect", AsyncMock(side_effect=OSError("refused")))
    with pytest.raises(TransportError):
        async with client():
            pass


async def test_production_rejected_before_connect():
    config = AppConfig.model_validate(
        {
            "system": {"mode": "live"},
            "venue": {"key_profile": "production", "ws_url": "wss://www.deribit.com/ws/api/v2"},
        }
    )
    with pytest.raises(TradingUnavailable):
        async with WebSocketRpc(config.venue, EndpointLimiter(config.rate_limits)):
            pass


@pytest.mark.parametrize(
    "response",
    ["bad-json", '{"jsonrpc":"2.0","id":2,"result":1}', '{"jsonrpc":"1.0","id":1,"result":1}'],
)
async def test_malformed_or_mismatched_response_invalidates_connection(response):
    rpc = client()
    socket = AsyncMock()
    socket.recv.return_value = response
    rpc._socket = socket
    with pytest.raises(IntegrityError):
        await rpc.call("public/get_time", {})
    socket.close.assert_awaited_once()
    assert rpc._socket is None


@pytest.mark.parametrize("error", [OSError("dropped"), TimeoutError(), asyncio.CancelledError()])
async def test_failed_call_cannot_reuse_socket(error):
    rpc = client()
    socket = AsyncMock()
    socket.recv.side_effect = error
    rpc._socket = socket
    expected = (
        asyncio.CancelledError if isinstance(error, asyncio.CancelledError) else TransportError
    )
    with pytest.raises(expected):
        await rpc.call("public/get_time", {})
    assert rpc._socket is None


@pytest.mark.parametrize("code", [-32602, 10028])
async def test_rpc_error_keeps_only_safe_metadata(code):
    rpc = client()
    rpc._socket = AsyncMock()
    rpc._socket.recv.return_value = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": code,
                "message": "sensitive-server-echo",
                "data": {"param": "currency"},
            },
        }
    )
    with pytest.raises(RpcError) as caught:
        await rpc.call("public/get_instruments", {})
    assert "sensitive" not in str(caught.value)
    assert caught.value.parameter == "currency"
    assert (rpc._socket is None) == (code == 10028)


@pytest.mark.parametrize(
    "payload",
    [
        {"jsonrpc": "2.0", "id": 1},
        {"jsonrpc": "2.0", "id": 1, "result": 1, "error": {"code": 1, "message": "bad"}},
    ],
)
def test_envelope_requires_one_payload(payload):
    with pytest.raises(ValidationError):
        RpcEnvelope.model_validate(payload)


async def test_limiter_waits_using_monotonic_time():
    now, delays = [0.0], []

    async def sleep(delay):
        delays.append(delay)
        now[0] += delay

    bucket = TokenBucket(
        BucketConfig(capacity=10, refill_per_s=5, cost=10), clock=lambda: now[0], sleep=sleep
    )
    await bucket.acquire(10)
    await bucket.acquire(5)
    assert delays == [1]
    for value in (0, 11, float("nan")):
        with pytest.raises(ValueError):
            await bucket.acquire(value)


async def test_endpoint_buckets_independent():
    limiter = EndpointLimiter(RateConfig())
    await limiter.acquire("public/subscribe")
    await limiter.acquire("public/get_instruments")
    assert limiter._buckets["ordinary"]._tokens == 5000
    assert limiter._buckets["subscriptions"]._tokens == 27000
