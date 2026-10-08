import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.eth_gas import URL, EthereumGasCollector, check_progress, gas_event
from selective_options.store import DuckStore

HASH = "0x" + "ab" * 32
PARENT = "0x" + "cd" * 32


def block(**changes):
    return {
        "number": "0x64",
        "hash": HASH,
        "parentHash": PARENT,
        "timestamp": "0x3e8",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0xf4240",
        "baseFeePerGas": "0x20000000000001",
        "transactions": ["0x" + "12" * 32],
        **changes,
    }


def test_exact_units_and_stable_repeat_identity():
    first = gas_event(block(), 1100000, 1800000)
    repeat = gas_event(block(), 1101000, 1800000)
    assert first.event_id == repeat.event_id
    assert first.event_time_ms == 1000000 and first.observed_at_ms == 1100000
    assert first.payload()["base_fee_per_gas_wei"] == "9007199254740993"
    assert first.payload()["gas_used"] == "1000000"
    assert first.payload()["requested_block_tag"] == "finalized"
    assert first.payload()["execution_quote"] is False
    check_progress(None, first)
    check_progress(first, repeat)


@pytest.mark.parametrize(
    "changes",
    [
        {"number": "0x01"},
        {"number": "0x"},
        {"number": "0x" + "1" * 65},
        {"number": hex(2**64)},
        {"baseFeePerGas": 2},
        {"baseFeePerGas": "-1"},
        {"hash": "0x123"},
        {"hash": None},
        {"transactions": [{"hash": HASH}]},
        {"transactions": [HASH, HASH.upper().replace("0X", "0x")]},
        {"gasLimit": "0x0"},
        {"gasUsed": hex(30_000_001)},
        {"timestamp": "0x0"},
        {"timestamp": hex(3000)},
    ],
)
def test_bad_blocks(changes):
    with pytest.raises((IntegrityError, ValidationError)):
        gas_event(block(**changes), 1100000, 1800000)


@pytest.mark.parametrize("receipt,age", [(2800001, 1800000), (1100000, 0)])
def test_stale_and_invalid_age(receipt, age):
    with pytest.raises(IntegrityError):
        gas_event(block(), receipt, age)


@pytest.mark.parametrize(
    "changes",
    [
        {"number": "0x63"},
        {"hash": PARENT},
        {"gasUsed": "0x1"},
        {"number": "0x65", "timestamp": "0x3e8"},
        {"number": "0x65", "timestamp": "0x3e9"},
        {"number": "0x65", "timestamp": "0x3e9", "hash": PARENT, "parentHash": PARENT},
    ],
)
def test_finalized_progress_conflicts(changes):
    with pytest.raises(IntegrityError):
        check_progress(
            gas_event(block(), 1100000, 1800000), gas_event(block(**changes), 1101000, 1800000)
        )


def test_linked_successor_and_sampled_gap():
    previous = gas_event(block(), 1100000, 1800000)
    for number, parent in [(101, HASH), (120, PARENT)]:
        check_progress(
            previous,
            gas_event(
                block(
                    number=hex(number), hash="0x" + "ef" * 32, parentHash=parent, timestamp="0x400"
                ),
                1101000,
                1800000,
            ),
        )


async def test_pit_and_dedup(tmp_path):
    store = await DuckStore.open(tmp_path / "gas.duckdb")
    first = gas_event(block(), 1100000, 1800000)
    try:
        await store.append([first])
        await store.append([gas_event(block(), 1101000, 1800000)])
        assert not (await store.snapshot("ETH", 1099999, "public_reference")).rows()
        assert (await store.snapshot("ETH", 1100000, "public_reference")).rows() == (first,)
    finally:
        await store.close()


@pytest.fixture
def collector():
    return EthereumGasCollector(AppConfig(), AsyncMock())


async def test_success_then_invalid_chain_or_clock(collector):
    collector.request = AsyncMock(side_effect=[("0x1", 1099999), (block(), 1100000)])
    assert await collector.collect(None)
    previous = collector.previous
    assert [c.args[0].kind for c in collector.sink.await_args_list] == ["on_chain", "feed_health"]
    for responses in [[("0x2", 1099999)], [("0x1", 1100001), (block(), 1100000)]]:
        collector.sink.reset_mock()
        collector.request = AsyncMock(side_effect=responses)
        assert not await collector.collect(None)
        assert collector.previous == previous
        collector.sink.assert_awaited_once()
        assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


async def test_sink_failure_does_not_advance_and_cancel(collector):
    collector.request = AsyncMock(side_effect=[("0x1", 1099999), (block(), 1100000)])
    collector.sink.side_effect = IntegrityError("store")
    with pytest.raises(IntegrityError):
        await collector.collect(None)
    assert collector.previous is None
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (TimeoutError(), "TRANSPORT_UNAVAILABLE"),
    ],
)
async def test_outage(collector, error, reason):
    collector.request = AsyncMock(side_effect=error)
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == reason


@pytest.mark.parametrize(
    "provider,source",
    [
        ("publicnode", "ethereum_publicnode"),
        ("cloudflare", "ethereum_cloudflare"),
        ("drpc", "ethereum_drpc"),
    ],
)
async def test_provider_provenance_and_identity(provider, source):
    collector = EthereumGasCollector(AppConfig(), AsyncMock(), provider=provider)
    collector.request = AsyncMock(side_effect=[("0x1", 1099999), (block(), 1100000)])
    assert await collector.collect(None)
    assert collector.previous.source == source
    assert all(call.args[0].source == source for call in collector.sink.await_args_list)
    assert collector.previous == gas_event(block(), 1100000, 1800000, provider=provider)
    assert (collector.previous.event_id == gas_event(block(), 1100000, 1800000).event_id) is (
        provider == "publicnode"
    )


def test_provider_rejects_arbitrary_endpoints():
    for provider in ["https://private.invalid/?key=secret", "invalid", "ethereum_drpc"]:
        with pytest.raises(ValueError):
            EthereumGasCollector(AppConfig(), AsyncMock(), provider=provider)
        with pytest.raises(ValueError):
            gas_event(block(), 1100000, 1800000, provider=provider)


class Response:
    def __init__(self, status=200, raw=None):
        self.status, self.content = status, self
        self.body = (
            raw
            if isinstance(raw, bytes)
            else json.dumps(
                raw if raw is not None else {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
            ).encode()
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        yield self.body


class Session:
    def __init__(self, response, method="eth_chainId"):
        self.response, self.method = response, method

    def post(self, url, **kwargs):
        assert url == URL and kwargs["allow_redirects"] is False
        assert kwargs["json"] == {
            "jsonrpc": "2.0",
            "id": 1 if self.method == "eth_chainId" else 2,
            "method": self.method,
            "params": [] if self.method == "eth_chainId" else ["finalized", False],
        }
        return self.response


@pytest.mark.parametrize(
    "status,reason",
    [
        (401, "ACCESS_DENIED"),
        (403, "ACCESS_DENIED"),
        (451, "ACCESS_DENIED"),
        (429, "RATE_LIMITED"),
        (302, "HTTP_UNAVAILABLE"),
        (500, "HTTP_UNAVAILABLE"),
    ],
)
async def test_http_status(collector, status, reason):
    collector.response_received = AsyncMock()
    with pytest.raises(FeedUnavailable) as exc:
        await collector.request(Session(Response(status)), "eth_chainId")
    assert exc.value.reason == reason
    collector.response_received.assert_awaited_once()
    assert collector.response_received.await_args.args[:3] == (
        "eth_chainId",
        status,
        Response(status).body,
    )


@pytest.mark.parametrize(
    "raw",
    [
        {"jsonrpc": "1.0", "id": 1, "result": "0x1"},
        {"jsonrpc": "2.0", "id": True, "result": "0x1"},
        {"jsonrpc": "2.0", "id": 2, "result": "0x1"},
        {"jsonrpc": "2.0", "id": 1},
        {"jsonrpc": "2.0", "id": 1, "result": None},
    ],
)
async def test_rpc_identity_and_result(collector, raw):
    with pytest.raises(IntegrityError):
        await collector.request(Session(Response(raw=raw)), "eth_chainId")


async def test_request_success_limits_error_and_write_denial(collector):
    assert (await collector.request(Session(Response()), "eth_chainId"))[0] == "0x1"
    response = Response(raw={"jsonrpc": "2.0", "id": 2, "result": block()})
    assert (
        await collector.request(Session(response, "eth_getBlockByNumber"), "eth_getBlockByNumber")
    )[0] == block()
    with pytest.raises(IntegrityError):
        await collector.request(Session(Response(raw=b" " * 4_000_001)), "eth_chainId")
    with pytest.raises(FeedUnavailable):
        await collector.request(
            Session(Response(raw={"jsonrpc": "2.0", "id": 1, "error": {"code": -1}})), "eth_chainId"
        )
    with pytest.raises(ValueError):
        await collector.request(None, "eth_sendRawTransaction")


@pytest.mark.parametrize("ok,delay", [(True, 120), (False, 600)])
async def test_poll_backoff(collector, monkeypatch, ok, delay):
    collector.collect = AsyncMock(return_value=ok)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.eth_gas.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(delay)


@pytest.mark.parametrize(
    "enabled,asset,expected", [(False, "ETH", False), (True, "ETH", True), (True, "BTC", False)]
)
async def test_capture_opt_in(monkeypatch, tmp_path, enabled, asset, expected):
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    calls = []

    async def run(self):
        calls.append(True)
        await self.health(False, "ACCESS_DENIED", 403)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(EthereumGasCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": [asset]},
            "ingestion": {
                "eth_gas_enabled": enabled,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert bool(calls) == expected and report["rows"] == int(expected)


async def test_repeat_health_binds_original_retained_receipt_and_latest_response(
    collector, monkeypatch
):
    from selective_options.eth_gas_quality import eth_gas_quality

    collector.request = AsyncMock(
        side_effect=[("0x1", 1099999), (block(), 1100000), ("0x1", 1100999), (block(), 1101000)]
    )
    monkeypatch.setattr("selective_options.eth_gas.now_ms", lambda: 1101050)
    assert await collector.collect(None)
    retained = collector.previous
    assert await collector.collect(None)
    calls = [call.args[0] for call in collector.sink.await_args_list]
    assert calls[0] == calls[2] == retained
    assert collector.previous is retained
    assert calls[3].payload()["accepted_event_id"] == retained.event_id
    assert calls[3].payload()["accepted_observed_at_ms"] == 1100000
    assert calls[3].payload()["response_received_at_ms"] == 1101000
    # Distinct latest response binding differentiates same-millisecond poll health.
    assert calls[1].event_id != calls[3].event_id
    result = eth_gas_quality(
        [retained, calls[3]],
        provider="publicnode",
        as_of_ms=1101050,
        max_poll_age_ms=100,
        max_block_age_ms=200000,
    )
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["response_age_ms"] == 50 and result["observation_age_ms"] == 1050


@pytest.mark.parametrize("repeat", [True, False])
async def test_receipt_regression_fails_before_sink_and_preserves_previous(collector, repeat):
    previous = gas_event(block(), 1100000, 1800000)
    candidate = (
        block()
        if repeat
        else block(number="0x65", timestamp=hex(1010), hash="0x" + "ef" * 32, parentHash=HASH)
    )
    current = gas_event(candidate, 1050000, 1800000)
    with pytest.raises(IntegrityError, match="ETH_RECEIPT_CLOCK_REGRESSION"):
        check_progress(previous, current)
    collector.previous = previous
    collector.request = AsyncMock(side_effect=[("0x1", 1049999), (candidate, 1050000)])
    assert not await collector.collect(None)
    assert collector.previous is previous
    collector.sink.assert_awaited_once()
    status = collector.sink.await_args.args[0]
    assert status.kind == "feed_health" and status.payload()["ok"] is False
    assert status.payload()["reason"] == "INVALID_DATA"
    assert status.payload()["accepted_event_id"] is None
    assert status.payload()["response_received_at_ms"] == 1050000


async def test_gas_sink_must_accept_before_bound_health_and_health_failure_is_not_commit_proof(
    collector,
):
    accepted = []

    async def sink(event):
        if event.kind == "feed_health":
            assert accepted and accepted[0].kind == "on_chain"
            assert event.payload()["accepted_event_id"] == accepted[0].event_id
            raise IntegrityError("health delivery failed")
        accepted.append(event)

    collector.sink = sink
    collector.request = AsyncMock(side_effect=[("0x1", 1099999), (block(), 1100000)])
    with pytest.raises(IntegrityError, match="health delivery failed"):
        await collector.collect(None)
    assert len(accepted) == 1 and collector.previous == accepted[0]
    from selective_options.eth_gas_quality import eth_gas_quality

    assert (
        eth_gas_quality(
            accepted,
            provider="publicnode",
            as_of_ms=1100050,
            max_poll_age_ms=1000,
            max_block_age_ms=200000,
        )["blocker"]
        == "ETH_HEALTH_MISSING"
    )
