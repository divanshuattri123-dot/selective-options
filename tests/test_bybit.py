import asyncio
import json
from unittest.mock import AsyncMock

import aiohttp
import pytest
from pydantic import ValidationError

from selective_options.bybit import (
    MAX_BYTES,
    BybitCollector,
    Envelope,
    FeedUnavailable,
    Instrument,
    snapshot_events,
)
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot
from selective_options.store import DuckStore


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_opt_in(monkeypatch, tmp_path, enabled):
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    started = []

    async def run(self):
        started.append(True)
        await self.health("BTC", False, "PERPETUAL_UNAVAILABLE")
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(BybitCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "bybit_enabled": enabled,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert bool(started) is enabled
    assert report["rows"] == int(enabled)
    assert report["ready_for_signals"] is False


def instrument(**changes):
    return dict(
        symbol="BTCUSDT",
        baseCoin="BTC",
        quoteCoin="USDT",
        settleCoin="USDT",
        contractType="LinearPerpetual",
        status="Trading",
        fundingInterval=480,
        isPreListing=False,
        **changes,
    )


def ticker(**changes):
    return {
        "symbol": "BTCUSDT",
        "markPrice": "100",
        "indexPrice": "99",
        "bid1Price": "99",
        "ask1Price": "101",
        "openInterest": "123",
        "openInterestValue": "12300",
        "fundingRate": "-0.0001",
        "nextFundingTime": "30000000",
        "fundingIntervalHour": "8",
        **changes,
    }


def envelope(rows, cursor=None, timestamp=10000):
    result = {"category": "linear", "list": rows}
    if cursor is not None:
        result["nextPageCursor"] = cursor
    return Envelope(retCode=0, time=timestamp, result=result)


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.bybit.asyncio.sleep", AsyncMock())
    config = AppConfig.model_validate({"system": {"assets": ["BTC"]}})
    return BybitCollector(config, AsyncMock())


async def test_snapshot_units_and_pit_storage(tmp_path):
    rows = snapshot_events(Instrument(**instrument()), 9000, envelope([ticker()]), 11000)
    assert [r.kind for r in rows] == ["perpetual", "funding"]
    assert rows[0].payload()["open_interest_units"] == "BTC"
    assert rows[0].payload()["open_interest_sides"] == "both"
    assert rows[0].payload()["price_units"] == "USDT"
    assert rows[1].payload()["funding_rate"] == -0.0001
    assert rows[1].payload()["funding_interval_minutes"] == 480
    assert all(r.event_time_ms == r.observed_at_ms == 11000 for r in rows)
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        PointInTimeSnapshot(10999, "BTC", "public_reference", tuple(rows))
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        assert not (await store.snapshot("BTC", 10999, "public_reference")).rows()
        assert len((await store.snapshot("BTC", 11000, "public_reference")).rows()) == 2
    finally:
        await store.close()


@pytest.mark.parametrize(
    "change",
    [
        {"symbol": "ETHUSDT"},
        {"bid1Price": "102"},
        {"markPrice": "NaN"},
        {"fundingRate": ""},
        {"openInterest": "-1"},
        {"fundingIntervalHour": "4"},
        {"nextFundingTime": "10000"},
        {"indexPrice": "0"},
    ],
)
def test_invalid_snapshot_rejected(change):
    with pytest.raises((IntegrityError, ValidationError)):
        snapshot_events(Instrument(**instrument()), 9000, envelope([ticker(**change)]), 11000)


def test_empty_duplicate_future_discovery_and_zero_funding():
    inst = Instrument(**instrument())
    for rows in ([], [ticker(), ticker()]):
        with pytest.raises(IntegrityError):
            snapshot_events(inst, 9000, envelope(rows), 11000)
    with pytest.raises(IntegrityError):
        snapshot_events(inst, 12000, envelope([ticker()]), 11000)
    assert (
        snapshot_events(inst, 9000, envelope([ticker(fundingRate="0")]), 11000)[1].payload()[
            "funding_rate"
        ]
        == 0
    )


async def test_discovery_filters_and_paginates(collector):
    excluded = [
        {**instrument(), "symbol": f"ALT{i}", **change}
        for i, change in enumerate(
            [
                {"contractType": "InversePerpetual"},
                {"status": "Settling"},
                {"quoteCoin": "USDC"},
                {"settleCoin": "BTC"},
                {"isPreListing": True},
                {
                    "symbol": "BTCUSDT-02OCT26",
                    "contractType": "LinearFutures",
                    "fundingInterval": 0,
                },
            ]
        )
    ]
    collector.request = AsyncMock(
        side_effect=[
            (envelope(excluded, "next"), 10000),
            (envelope([instrument()], ""), 11000),
        ]
    )
    inst, received = await collector.discover(None, "BTC")
    assert inst.symbol == "BTCUSDT" and received == 11000
    assert collector.request.await_args.args[2]["cursor"] == "next"


@pytest.mark.parametrize(
    "pages",
    [
        [envelope([instrument(), instrument()])],
        [envelope([{**instrument(), "baseCoin": "ETH"}])],
        [envelope([instrument(), {**instrument(), "symbol": "ALT"}])],
        [envelope([], "repeat"), envelope([], "repeat")],
        [envelope([], str(i)) for i in range(5)],
    ],
)
async def test_bad_discovery(collector, pages):
    collector.request = AsyncMock(side_effect=[(p, 11000) for p in pages])
    with pytest.raises(IntegrityError):
        await collector.discover(None, "BTC")


async def test_collect_good_missing_and_invalid(collector):
    collector.request = AsyncMock(
        side_effect=[
            (envelope([instrument()]), 10000),
            (envelope([ticker()]), 11000),
        ]
    )
    assert await collector.collect(None)
    rows = [c.args[0] for c in collector.sink.await_args_list]
    assert [r.kind for r in rows] == ["perpetual", "funding", "feed_health"]
    collector.sink.reset_mock()
    collector.request = AsyncMock(return_value=(envelope([]), 10000))
    assert await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == "PERPETUAL_UNAVAILABLE"
    collector.request = AsyncMock(side_effect=IntegrityError("invalid"))
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (aiohttp.ClientConnectionError(), "TRANSPORT_UNAVAILABLE"),
        (TimeoutError(), "TRANSPORT_UNAVAILABLE"),
    ],
)
async def test_unavailable_stops_remaining_requests(collector, error, reason):
    collector.request = AsyncMock(side_effect=error)
    assert not await collector.collect(None)
    assert collector.request.await_count == 1
    assert collector.sink.await_args.args[0].payload()["reason"] == reason


async def test_sink_failure_and_cancel_propagate(collector):
    collector.request = AsyncMock(
        side_effect=[
            (envelope([instrument()]), 10000),
            (envelope([ticker()]), 11000),
        ]
    )
    collector.sink.side_effect = IntegrityError("storage")
    with pytest.raises(IntegrityError, match="storage"):
        await collector.collect(None)
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)


class Response:
    def __init__(self, code=200, body=None):
        self.status = code
        self.body = body if body is not None else envelope([]).model_dump_json().encode()
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        yield self.body


class Session:
    def __init__(self, response):
        self.response = response

    def get(self, url, **kwargs):
        assert url == "https://api.bybit.com/v5/market/tickers"
        assert kwargs["allow_redirects"] is False
        assert set(kwargs) == {"params", "allow_redirects"}
        return self.response


@pytest.mark.parametrize(
    "code,reason",
    [
        (403, "ACCESS_DENIED"),
        (451, "ACCESS_DENIED"),
        (401, "ACCESS_DENIED"),
        (429, "RATE_LIMITED"),
        (500, "HTTP_UNAVAILABLE"),
        (302, "HTTP_UNAVAILABLE"),
    ],
)
async def test_http_denial_no_redirect(collector, code, reason):
    with pytest.raises(FeedUnavailable) as exc:
        await collector.request(Session(Response(code)), "tickers", {})
    assert exc.value.reason == reason and exc.value.code == code


async def test_request_limits_clock_api_and_json(collector, monkeypatch):
    monkeypatch.setattr("selective_options.bybit.now_ms", lambda: 11000)
    parsed, received = await collector.request(Session(Response()), "tickers", {})
    assert parsed.time == 10000 and received == 11000
    for body in [
        b"x" * (MAX_BYTES + 1),
        envelope([], timestamp=14000).model_dump_json().encode(),
        b"{}",
    ]:
        with pytest.raises((ValidationError, IntegrityError)):
            await collector.request(Session(Response(body=body)), "tickers", {})
    api_error = json.dumps({"retCode": 10006, "time": 10000, "result": {}}).encode()
    with pytest.raises(FeedUnavailable) as exc:
        await collector.request(Session(Response(body=api_error)), "tickers", {})
    assert exc.value.code == 10006
    monkeypatch.setattr("selective_options.bybit.now_ms", lambda: 90000)
    with pytest.raises(IntegrityError, match="STALENESS"):
        await collector.request(Session(Response()), "tickers", {})


@pytest.mark.parametrize("healthy,delay", [(True, 30), (False, 600)])
async def test_run_backoff_and_cancellation(collector, monkeypatch, healthy, delay):
    collector.collect = AsyncMock(return_value=healthy)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.bybit.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(delay)
