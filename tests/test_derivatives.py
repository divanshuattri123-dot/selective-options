import asyncio
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig
from selective_options.derivatives import DerivativesCollector, volatility_events
from selective_options.errors import IntegrityError, TransportError
from selective_options.events import PointInTimeSnapshot
from selective_options.rate_limit import EndpointLimiter


def future(**changes):
    return {
        "instrument_name": "opaque-venue-id",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "settlement_currency": "BTC",
        "kind": "future",
        "settlement_period": "perpetual",
        "is_active": True,
        "contract_size": 10,
        "instrument_type": "reversed",
        "price_index": "btc_usd",
        **changes,
    }


def ticker(**changes):
    return {
        "instrument_name": "opaque-venue-id",
        "timestamp": 1000,
        "mark_price": 100,
        "index_price": 100,
        "open_interest": 500,
        "current_funding": 0,
        "funding_8h": -0.0001,
        "best_bid_price": 99,
        "best_ask_price": 101,
        **changes,
    }


async def test_discovery_preserves_units_and_zero_negative_funding():
    config = AppConfig.model_validate({"system": {"assets": ["BTC", "SOL"]}})
    sink = AsyncMock()
    rpc = AsyncMock()
    rpc.call.side_effect = [
        [
            future(),
            future(instrument_name="dated", settlement_period="month"),
            future(instrument_name="inactive", is_active=False),
            future(instrument_name="other", base_currency="ETH"),
        ],
        ticker(),
        {"data": [], "continuation": None},
    ]
    await DerivativesCollector(config, sink, EndpointLimiter(config.rate_limits)).collect(rpc)
    rows = [call.args[0] for call in sink.await_args_list]
    funding = next(row for row in rows if row.kind == "funding")
    assert funding.payload()["current_funding"] == 0
    assert funding.payload()["funding_8h"] == -0.0001
    assert funding.payload()["settlement_currency"] == "BTC"
    assert funding.environment == "testnet"
    assert rpc.call.await_args_list[1].args[1]["instrument_name"] == "opaque-venue-id"
    assert {row.payload().get("reason") for row in rows if row.kind == "status"} == {
        "PERPETUAL_UNAVAILABLE",
        "VOLATILITY_INDEX_UNAVAILABLE",
        "VOLATILITY_INDEX_NOT_CONFIGURED",
    }


@pytest.mark.parametrize(
    "change", [{"instrument_name": "wrong"}, {"best_bid_price": 102}, {"mark_price": float("nan")}]
)
async def test_invalid_tickers_fail(change):
    config = AppConfig.model_validate({"system": {"assets": ["BTC"]}})
    rpc, sink = AsyncMock(), AsyncMock()
    rpc.call.side_effect = [[future()], ticker(**change)]
    with pytest.raises((IntegrityError, ValidationError)):
        await DerivativesCollector(config, sink, EndpointLimiter(config.rate_limits)).collect(rpc)
    sink.assert_not_awaited()


async def test_missing_funding_not_fabricated_and_duplicate_discovery():
    config = AppConfig.model_validate({"system": {"assets": ["BTC"]}})
    sink, rpc = AsyncMock(), AsyncMock()
    rpc.call.side_effect = [[future()], ticker(funding_8h=None), {"data": [], "continuation": None}]
    collector = DerivativesCollector(config, sink, EndpointLimiter(config.rate_limits))
    await collector.collect(rpc)
    assert "funding" not in {call.args[0].kind for call in sink.await_args_list}
    assert "FUNDING_UNAVAILABLE" in {
        call.args[0].payload().get("reason") for call in sink.await_args_list
    }
    rpc.call.side_effect = [[future(), future()]]
    with pytest.raises(IntegrityError, match="duplicate"):
        await collector.collect(rpc)


def test_closed_candle_and_observation_time_prevent_lookahead():
    raw = {"data": [[60000, 10, 12, 9, 11], [120000, 11, 12, 10, 11]], "continuation": None}
    rows = volatility_events(raw, "BTC", 60000, 150000, 180000)
    assert len(rows) == 1 and rows[0].event_time_ms == 120000
    assert rows[0].payload()["closed"] is True
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        PointInTimeSnapshot(150000, "BTC", "testnet", tuple(rows))
    assert PointInTimeSnapshot(180000, "BTC", "testnet", tuple(rows)).rows()
    assert not volatility_events(raw, "BTC", 180000, 240000, 240000)


@pytest.mark.parametrize(
    "raw",
    [
        {"data": [], "continuation": 120000},
        {"data": [[60001, 10, 12, 9, 11]], "continuation": None},
        {"data": [[60000, 10, 12, 9, 11]] * 2, "continuation": None},
        {"data": [[60000, 10, 9, 8, 11]], "continuation": None},
        {"data": [[60000, 10, float("inf"), 8, 11]], "continuation": None},
    ],
)
def test_invalid_or_truncated_candles_fail(raw):
    with pytest.raises((IntegrityError, ValidationError)):
        volatility_events(raw, "BTC", 60000, 120000, 180000)


async def test_network_reconnect_and_cancel(monkeypatch):
    entered = asyncio.Event()
    attempts = []

    class Connection:
        def __init__(self, *args):
            pass

        async def __aenter__(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise TransportError("test disconnect")
            return AsyncMock()

        async def __aexit__(self, *args):
            pass

    async def collect(rpc):
        entered.set()

    monkeypatch.setattr("selective_options.derivatives.WebSocketRpc", Connection)
    config = AppConfig.model_validate({"ingestion": {"derivatives_poll_interval_s": 0.001}})
    sink = AsyncMock()
    collector = DerivativesCollector(config, sink, EndpointLimiter(config.rate_limits))
    monkeypatch.setattr(collector, "collect", collect)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(entered.wait(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert sink.await_args.args[0].payload()["reason"] == "DERIVATIVES_DISCONNECTED"
