import asyncio
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.bybit import BybitCollector, Envelope, FeedUnavailable, Instrument
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.funding_history import collect_funding_history, parse_funding_page
from selective_options.store import DuckStore


def instrument(**changes):
    return Instrument.model_validate(
        {
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "contractType": "LinearPerpetual",
            "status": "Trading",
            "fundingInterval": 480,
            "isPreListing": False,
            **changes,
        }
    )


def row(ts=8000, rate="-0.0001000", symbol="BTCUSDT"):
    return {"symbol": symbol, "fundingRateTimestamp": str(ts), "fundingRate": rate}


def response(rows, **changes):
    return Envelope.model_validate(
        {"retCode": 0, "time": 10000, "result": {"category": "linear", "list": rows}, **changes}
    )


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.funding_history.now_ms", lambda: 20000)
    monkeypatch.setattr("selective_options.funding_history.asyncio.sleep", AsyncMock())
    result = BybitCollector(AppConfig(), AsyncMock())
    result.discover = AsyncMock(return_value=(instrument(), 10000))
    result.request = AsyncMock()
    return result


async def test_pagination_receipts_decimal_identity_and_store(collector, tmp_path):
    collector.request.side_effect = [
        (response([row(9000), row(7000, "0")]), 11000),
        (response([row(3000, "0.000123456789123456789")]), 12000),
        (response([]), 13000),
    ]
    plan = await collect_funding_history(
        collector, None, "BTC", start_ms=2000, end_ms=9500, page_size=2
    )
    assert plan.pages == 3 and plan.termination == "EMPTY_PAGE"
    assert [c.args[2]["endTime"] for c in collector.request.await_args_list] == [
        "9500",
        "6999",
        "2999",
    ]
    assert [e.observed_at_ms for e in plan.events] == [11000, 11000, 12000]
    assert [e.payload()["funding_rate"] for e in plan.events] == [
        "-0.0001",
        "0",
        "0.000123456789123456789",
    ]
    assert all(e.payload()["funding_interval_minutes"] is None for e in plan.events)
    collector.sink.assert_not_awaited()
    repeated = parse_funding_page(
        instrument(fundingInterval=60), response([row(9000, "-0.0001")]), 14000, 2000, 9500, 2
    )[0]
    assert repeated.event_id == plan.events[0].event_id
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(list(plan.events))
        await store.append([repeated])
        assert not (await store.snapshot("BTC", 10000, "public_reference")).rows()
        assert len((await store.snapshot("BTC", 11000, "public_reference")).rows()) == 2
        assert len((await store.snapshot("BTC", 12000, "public_reference")).rows()) == 3
    finally:
        await store.close()


@pytest.mark.parametrize(
    "bad",
    [
        [row(), row()],
        [row(7000), row(8000)],
        [row(8000, symbol="ETHUSDT")],
        [row(999)],
        [row(9501)],
        [row(rate="NaN")],
        [row(rate="Infinity")],
        [row(rate="1e100000")],
        [row(rate="1e-100000")],
    ],
)
def test_bad_pages_fail_before_publication(bad):
    with pytest.raises((ValidationError, IntegrityError)):
        parse_funding_page(instrument(), response(bad), 11000, 1000, 9500, 2)


def test_future_time_page_bound_and_wrong_contract():
    with pytest.raises(IntegrityError):
        parse_funding_page(instrument(), response([row()]), 7000, 1000, 9500, 2)
    with pytest.raises(IntegrityError):
        parse_funding_page(instrument(), response([row()], time=7000), 11000, 1000, 9500, 2)
    with pytest.raises(IntegrityError):
        parse_funding_page(instrument(), response([row(8000), row(7000)]), 11000, 1000, 9500, 1)
    with pytest.raises(IntegrityError):
        parse_funding_page(instrument(quoteCoin="USDC"), response([row()]), 11000, 1000, 9500, 2)


async def test_start_bound_and_empty_window(collector):
    collector.request.return_value = (response([row(1000, "-0.000")]), 11000)
    plan = await collect_funding_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    assert plan.termination == "START_BOUND_REACHED" and plan.pages == 1
    assert plan.events[0].payload()["funding_rate"] == "0"
    collector.request.return_value = (response([]), 11000)
    plan = await collect_funding_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    assert not plan.events and plan.termination == "EMPTY_PAGE"


async def test_truncation_and_nonadvancing_page(collector):
    collector.request.return_value = (response([row()]), 11000)
    with pytest.raises(IntegrityError, match="TRUNCATED"):
        await collect_funding_history(
            collector, None, "BTC", start_ms=1000, end_ms=9500, max_pages=1
        )
    with pytest.raises(IntegrityError, match="TIME_WINDOW"):
        await collect_funding_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.sink.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"start_ms": 0},
        {"start_ms": 10001},
        {"end_ms": 30000},
        {"page_size": 0},
        {"page_size": 201},
        {"max_pages": 0},
        {"max_pages": 21},
        {"timeout_s": float("nan")},
        {"timeout_s": 301},
        {"timeout_s": 0},
    ],
)
async def test_preflight_no_network(collector, changes):
    params = {"start_ms": 1000, "end_ms": 10000, **changes}
    with pytest.raises(ValueError):
        await collect_funding_history(collector, None, "BTC", **params)
    collector.discover.assert_not_awaited()


async def test_missing_asset_and_discovery(collector):
    with pytest.raises(ValueError):
        await collect_funding_history(collector, None, "NOT_CONFIGURED", start_ms=1000, end_ms=9500)
    collector.discover.return_value = (None, 10000)
    with pytest.raises(IntegrityError, match="UNAVAILABLE"):
        await collect_funding_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.request.assert_not_awaited()


@pytest.mark.parametrize("error", [FeedUnavailable("ACCESS_DENIED", 403), asyncio.CancelledError()])
async def test_access_error_and_cancel_leave_no_rows(collector, error):
    collector.request.side_effect = error
    with pytest.raises(type(error)):
        await collect_funding_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.sink.assert_not_awaited()


async def test_deadline_cancels_discovery(collector):
    finished = asyncio.Event()

    async def blocked(*args):
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    collector.discover.side_effect = blocked
    with pytest.raises(TimeoutError):
        await collect_funding_history(
            collector, None, "BTC", start_ms=1000, end_ms=9500, timeout_s=0.01
        )
    assert finished.is_set()
