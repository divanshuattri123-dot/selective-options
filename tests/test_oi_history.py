import asyncio
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.bybit import BybitCollector, Envelope, FeedUnavailable, Instrument
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.oi_history import collect_oi_history, parse_oi_page
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


def row(ts=8000, **changes):
    return {
        "timestamp": str(ts),
        "openInterest": "20.1200",
        "singleOpenInterest": "10.06",
        **changes,
    }


def response(rows, cursor="", **changes):
    return Envelope.model_validate(
        {
            "retCode": 0,
            "time": 10000,
            "result": {
                "category": "linear",
                "symbol": "BTCUSDT",
                "list": rows,
                "nextPageCursor": cursor,
                **changes,
            },
        }
    )


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.oi_history.now_ms", lambda: 20000)
    monkeypatch.setattr("selective_options.oi_history.asyncio.sleep", AsyncMock())
    result = BybitCollector(AppConfig(), AsyncMock())
    result.discover = AsyncMock(return_value=(instrument(), 10000))
    result.request = AsyncMock()
    return result


async def test_opaque_cursor_units_pit_and_repeat_identity(collector, tmp_path):
    cursor = "lastid%3D123%26lasttime%3D8000"
    collector.request.side_effect = [
        (response([row()], cursor), 11000),
        (response([row(7000, openInterest="0", singleOpenInterest=None)]), 12000),
    ]
    plan = await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500, page_size=2)
    assert plan.termination == "CURSOR_EXHAUSTED" and plan.pages == 2
    assert collector.request.await_args.args[2]["cursor"] == cursor
    assert collector.request.await_args.args[2]["endTime"] == "9500"
    assert plan.events[0].payload()["open_interest"] == "20.12"
    assert plan.events[0].payload()["open_interest_units"] == "BTC"
    assert plan.events[0].payload()["single_open_interest"] == "10.06"
    assert plan.events[1].payload()["open_interest"] == "0"
    assert plan.events[1].payload()["single_open_interest"] is None
    assert plan.events[0].payload()["sampling_interval_s"] == 300
    repeated = parse_oi_page(
        instrument(), response([row(openInterest="20.12")]), 14000, 1000, 9500, 2, "5min"
    )[0][0]
    assert repeated.event_id == plan.events[0].event_id
    different = parse_oi_page(instrument(), response([row()]), 14000, 1000, 9500, 2, "1h")[0][0]
    assert different.event_id != repeated.event_id
    collector.sink.assert_not_awaited()
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(list(plan.events))
        await store.append([repeated])
        assert not (await store.snapshot("BTC", 10000, "public_reference")).rows()
        assert (
            len((await store.snapshot("BTC", 11000, "public_reference")).rows(kind="open_interest"))
            == 1
        )
        assert len((await store.snapshot("BTC", 12000, "public_reference")).rows()) == 2
    finally:
        await store.close()


@pytest.mark.parametrize(
    "rows",
    [
        [row(), row()],
        [row(7000), row(8000)],
        [row(999)],
        [row(9501)],
        [row(openInterest="NaN")],
        [row(openInterest="-1")],
        [row(openInterest="1e100000")],
        [row(openInterest="1e-100000")],
        [row(singleOpenInterest="21")],
        [row(singleOpenInterest="-1")],
        [row(singleOpenInterest="Infinity")],
    ],
)
def test_bad_data(rows):
    with pytest.raises((IntegrityError, ValidationError)):
        parse_oi_page(instrument(), response(rows), 11000, 1000, 9500, 2, "5min")


@pytest.mark.parametrize(
    "page",
    [
        response([], "next"),
        response([row()], symbol="ETHUSDT"),
        response([row(), row(7000)], "next"),
    ],
)
def test_bad_page_identity_size_or_empty_continuation(page):
    with pytest.raises(IntegrityError):
        parse_oi_page(instrument(), page, 11000, 1000, 9500, 1, "5min")


def test_contract_and_source_receipt_time():
    with pytest.raises(IntegrityError):
        parse_oi_page(instrument(settleCoin="BTC"), response([row()]), 11000, 1000, 9500, 2, "5min")
    with pytest.raises(IntegrityError):
        parse_oi_page(instrument(), response([row()]), 7000, 1000, 9500, 2, "5min")
    with pytest.raises(IntegrityError):
        parse_oi_page(
            instrument(),
            response([row()]).model_copy(update={"time": 7000}),
            11000,
            1000,
            9500,
            2,
            "5min",
        )


async def test_cursor_repetition_page_limit_and_overlap(collector):
    collector.request.side_effect = [
        (response([row()], "same"), 11000),
        (response([row(7000)], "same"), 12000),
    ]
    with pytest.raises(IntegrityError, match="REPEATED_CURSOR"):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.request.side_effect = None
    collector.request.return_value = (response([row()], "next"), 11000)
    with pytest.raises(IntegrityError, match="TRUNCATED"):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500, max_pages=1)
    with pytest.raises(IntegrityError, match="TIME_WINDOW"):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.sink.assert_not_awaited()


async def test_boundary_empty_missing(collector):
    collector.request.return_value = (response([row(1000)], "more"), 11000)
    assert (
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    ).termination == "START_BOUND_REACHED"
    collector.request.return_value = (response([]), 11000)
    assert not (await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)).events
    collector.discover.return_value = (None, 10000)
    with pytest.raises(IntegrityError, match="UNAVAILABLE"):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)


@pytest.mark.parametrize(
    "changes",
    [
        {"start_ms": 0},
        {"end_ms": 30000},
        {"interval": "bad"},
        {"page_size": 201},
        {"max_pages": 21},
        {"timeout_s": float("nan")},
        {"timeout_s": 301},
    ],
)
async def test_preflight(collector, changes):
    with pytest.raises(ValueError):
        await collect_oi_history(
            collector, None, "BTC", **{"start_ms": 1000, "end_ms": 9500, **changes}
        )
    collector.discover.assert_not_awaited()


@pytest.mark.parametrize("error", [FeedUnavailable("ACCESS_DENIED", 403), asyncio.CancelledError()])
async def test_access_and_cancellation(collector, error):
    collector.request.side_effect = error
    with pytest.raises(type(error)):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500)
    collector.sink.assert_not_awaited()


async def test_deadline_cleanup(collector):
    closed = asyncio.Event()

    async def blocked(*args):
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    collector.discover.side_effect = blocked
    with pytest.raises(TimeoutError):
        await collect_oi_history(collector, None, "BTC", start_ms=1000, end_ms=9500, timeout_s=0.01)
    assert closed.is_set()
