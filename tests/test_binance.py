import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.binance import (
    BinanceCollector,
    FundingInfo,
    Instrument,
    PublicationFailure,
    funding_metadata,
    snapshots,
)
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot


def instrument(**changes):
    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        **changes,
    }


def mark(**changes):
    return {
        "symbol": "BTCUSDT",
        "markPrice": "100",
        "indexPrice": "101",
        "lastFundingRate": "-0.0001",
        "nextFundingTime": 20000,
        "time": 10000,
        **changes,
    }


def oi(**changes):
    return {"symbol": "BTCUSDT", "openInterest": "123", "time": 9000, **changes}


def metadata(**changes):
    return {
        "symbol": "BTCUSDT",
        "adjustedFundingRateCap": "0.01",
        "adjustedFundingRateFloor": "-0.01",
        "fundingIntervalHours": 4,
        **changes,
    }


@pytest.mark.parametrize("hours", [1, 4, 8])
def test_explicit_interval_and_metadata_availability(hours):
    info = funding_metadata([metadata(fundingIntervalHours=hours)])["BTCUSDT"]
    rows = snapshots(Instrument(**instrument()), mark(), 11000, oi(), 12000, info, 10500)
    payload = rows[1].payload()
    assert payload["funding_interval_minutes"] == hours * 60
    assert payload["funding_metadata_observed_at_ms"] == 10500
    assert payload["funding_interval_basis"] == "explicit_fundingInfo"
    assert payload["adjusted_rate_cap"] == 0.01
    assert payload["adjusted_rate_floor"] == -0.01
    # Source time precedes metadata receipt: visibility still requires the later mark receipt.
    with pytest.raises(IntegrityError):
        PointInTimeSnapshot(10999, "BTC", "public_reference", (rows[1],))
    assert PointInTimeSnapshot(11000, "BTC", "public_reference", (rows[1],)).rows()


@pytest.mark.parametrize(
    "raw",
    [
        [metadata(), metadata()],
        [metadata(adjustedFundingRateFloor="0.02")],
        [metadata(adjustedFundingRateCap="NaN")],
        [metadata(fundingIntervalHours=0)],
        [metadata(fundingIntervalHours=1.5)],
        {},
    ],
)
def test_invalid_funding_metadata(raw):
    with pytest.raises((IntegrityError, ValidationError)):
        funding_metadata(raw)


@pytest.mark.parametrize(
    "symbol,received",
    [
        ("OTHER", 10000),
        ("BTCUSDT", None),
        ("BTCUSDT", 0),
        ("BTCUSDT", 11001),
    ],
)
def test_metadata_identity_and_receipt_required(symbol, received):
    with pytest.raises(IntegrityError, match="METADATA_IDENTITY_OR_AVAILABILITY"):
        snapshots(
            Instrument(**instrument()),
            mark(),
            11000,
            oi(),
            12000,
            FundingInfo(**metadata(symbol=symbol)),
            received,
        )


async def test_metadata_refetched_without_stale_fallback(collector):
    for info, expected in [([metadata()], 240), ([], None)]:
        collector.sink.reset_mock()
        collector.request = AsyncMock(
            side_effect=[
                ({"symbols": [instrument()]}, 10000),
                (info, 10001),
                (mark(), 11000),
                (oi(), 12000),
            ]
        )
        assert await collector.collect(None)
        assert collector.request.await_args_list[1].args[1:] == ("fundingInfo", {})
        row = collector.sink.await_args_list[1].args[0]
        assert row.payload()["funding_interval_minutes"] == expected
        if expected is None:
            assert row.payload()["funding_interval_basis"] == "NOT_RETURNED_NO_DEFAULT_ASSUMED"
            assert row.payload()["adjusted_rate_cap"] is None
            assert row.payload()["funding_metadata_observed_at_ms"] is None


@pytest.mark.parametrize(
    "result,reason",
    [
        (FeedUnavailable("RATE_LIMITED", 429), "RATE_LIMITED"),
        (([metadata(), metadata()], 10001), "INVALID_DATA"),
    ],
)
async def test_metadata_failure_prevents_market_publication(collector, result, reason):
    collector.request = AsyncMock(side_effect=[({"symbols": [instrument()]}, 10000), result])
    assert not await collector.collect(None)
    assert collector.request.await_count == 2
    collector.sink.assert_awaited_once()
    event = collector.sink.await_args.args[0]
    assert event.kind == "feed_health" and event.asset == "GLOBAL"
    assert event.payload()["reason"] == reason


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.binance.asyncio.sleep", AsyncMock())
    return BinanceCollector(AppConfig.model_validate({"system": {"assets": ["BTC"]}}), AsyncMock())


def test_separate_timestamps_and_native_semantics():
    rows = snapshots(Instrument(**instrument()), mark(), 11000, oi(), 12000)
    assert [r.kind for r in rows] == ["perpetual", "funding", "open_interest"]
    assert [r.observed_at_ms for r in rows] == [11000, 11000, 12000]
    assert rows[1].payload()["funding_interval_minutes"] is None
    assert rows[1].payload()["last_funding_rate"] == -0.0001
    assert rows[2].payload()["side_convention"] == "NOT_VERIFIED"
    with pytest.raises(IntegrityError):
        PointInTimeSnapshot(11500, "BTC", "public_reference", tuple(rows))
    assert PointInTimeSnapshot(12000, "BTC", "public_reference", tuple(rows)).rows()


@pytest.mark.parametrize(
    "change",
    [
        {"symbol": "wrong"},
        {"markPrice": "NaN"},
        {"indexPrice": 0},
        {"time": 14000},
        {"nextFundingTime": 9999},
    ],
)
def test_bad_mark(change):
    with pytest.raises((IntegrityError, ValidationError)):
        snapshots(Instrument(**instrument()), mark(**change), 11000, oi(), 12000)


@pytest.mark.parametrize("change", [{"symbol": "wrong"}, {"openInterest": -1}, {"time": 1}])
def test_bad_oi(change):
    with pytest.raises((IntegrityError, ValidationError)):
        snapshots(
            Instrument(**instrument()),
            mark(time=100000, nextFundingTime=110000),
            100000,
            oi(**change),
            100000,
        )


async def test_discovery_filters_and_health(collector):
    excluded = [
        instrument(symbol="X" + str(i), **c)
        for i, c in enumerate(
            [
                {"status": "HALT"},
                {"contractType": "CURRENT_QUARTER"},
                {"quoteAsset": "USDC"},
                {"marginAsset": "BTC"},
            ]
        )
    ]
    collector.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument(), *excluded]}, 10000),
            ([], 10001),
            (mark(), 11000),
            (oi(), 12000),
        ]
    )
    assert await collector.collect(None)
    assert [c.args[0].kind for c in collector.sink.await_args_list] == [
        "perpetual",
        "funding",
        "open_interest",
        "feed_health",
    ]
    collector.request = AsyncMock(return_value=({"symbols": excluded}, 10000))
    assert await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == "PERPETUAL_UNAVAILABLE"


@pytest.mark.parametrize(
    "rows", [[instrument(), instrument()], [instrument(), instrument(symbol="OTHER")]]
)
async def test_invalid_discovery(collector, rows):
    collector.request = AsyncMock(return_value=({"symbols": rows}, 10000))
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (TimeoutError(), "TRANSPORT_UNAVAILABLE"),
    ],
)
async def test_source_outage(collector, error, reason):
    collector.request = AsyncMock(side_effect=error)
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == reason


async def test_publication_and_cancel(collector):
    collector.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument()]}, 10000),
            ([], 10001),
            (mark(), 11000),
            (oi(), 12000),
        ]
    )
    collector.sink.side_effect = IntegrityError("store")
    with pytest.raises(PublicationFailure):
        await collector.collect(None)
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)


class Response:
    def __init__(self, code=200, body=b"{}"):
        self.status, self.body, self.content = code, body, self

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
        assert url == "https://fapi.binance.com/fapi/v1/exchangeInfo"
        assert kwargs == {"params": {}, "allow_redirects": False}
        return self.response


@pytest.mark.parametrize(
    "code,reason",
    [
        (401, "ACCESS_DENIED"),
        (403, "ACCESS_DENIED"),
        (451, "ACCESS_DENIED"),
        (418, "RATE_LIMITED"),
        (429, "RATE_LIMITED"),
        (302, "HTTP_UNAVAILABLE"),
        (500, "HTTP_UNAVAILABLE"),
    ],
)
async def test_http_errors(collector, code, reason):
    with pytest.raises(FeedUnavailable) as e:
        await collector.request(Session(Response(code)), "exchangeInfo", {})
    assert e.value.reason == reason and e.value.code == code


async def test_http_valid_api_error_size_and_allowlist(collector):
    raw, received = await collector.request(Session(Response()), "exchangeInfo", {})
    assert raw == {} and received > 0
    with pytest.raises(FeedUnavailable):
        await collector.request(
            Session(Response(body=json.dumps({"code": -1003}).encode())), "exchangeInfo", {}
        )
    with pytest.raises(IntegrityError):
        await collector.request(Session(Response(body=b" " * 4000001)), "exchangeInfo", {})
    with pytest.raises(ValueError):
        await collector.request(None, "order", {})


@pytest.mark.parametrize("ok,delay", [(True, 30), (False, 600)])
async def test_poll_backoff(collector, monkeypatch, ok, delay):
    collector.collect = AsyncMock(return_value=ok)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.binance.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(delay)


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_opt_in(monkeypatch, tmp_path, enabled):
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    calls = []

    async def run(self):
        calls.append(True)
        await self.health("BTC", False, "PERPETUAL_UNAVAILABLE")
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(BinanceCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "binance_enabled": enabled,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert bool(calls) == enabled and report["rows"] == int(enabled)
