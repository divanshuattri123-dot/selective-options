import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from scripts import verify_binance_funding_history as verifier
from selective_options.binance import BinanceCollector, Instrument
from selective_options.binance_funding_history import collect_binance_funding_history, parse_page
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore


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


def row(ts=4000, **changes):
    return {
        "symbol": "BTCUSDT",
        "fundingRate": "-0.00010000",
        "fundingTime": ts,
        "markPrice": "67890.123456789012345678",
        "rateType": "Regular",
        **changes,
    }


def parse(rows, **changes):
    return parse_page(
        Instrument(**instrument()),
        rows,
        **{"received": 11000, "start_ms": 1000, "end_ms": 9500, "page_size": 2, **changes},
    )


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.binance_funding_history.now_ms", lambda: 10000)
    monkeypatch.setattr("selective_options.binance_funding_history.asyncio.sleep", AsyncMock())
    result = BinanceCollector(
        AppConfig.model_validate({"system": {"assets": ["BTC"]}}), AsyncMock()
    )
    result.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument()]}, 10000),
            ([row(1000), row(4000)], 11000),
            ([row(8000, rateType="Special")], 12000),
            ([], 13000),
        ]
    )
    return result


async def collect(collector, **changes):
    return await collect_binance_funding_history(
        collector, None, "BTC", **{"start_ms": 1000, "end_ms": 9500, "page_size": 2, **changes}
    )


async def test_ascending_pages_exact_values_receipts_and_storage(collector, tmp_path):
    plan = await collect(collector)
    assert plan.pages == 3 and plan.termination == "EMPTY_PAGE"
    assert [e.event_time_ms for e in plan.events] == [1000, 4000, 8000]
    assert [e.observed_at_ms for e in plan.events] == [11000, 11000, 12000]
    assert [c.args[2]["startTime"] for c in collector.request.await_args_list[1:]] == [
        "1000",
        "4001",
        "8001",
    ]
    first = plan.events[0]
    assert first.payload()["funding_rate"] == "-0.00010000"
    assert first.payload()["mark_price_at_charge"] == "67890.123456789012345678"
    assert plan.events[-1].payload()["reported_rate_type"] == "Special"
    assert first.payload()["funding_interval_minutes"] is None
    assert first.payload()["observation_received_at_ms"] == 11000
    collector.sink.assert_not_awaited()
    later = parse([row(1000)], received=12500)[0]
    assert later.event_id != first.event_id
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(list(plan.events) * 2 + [later])
        assert not (await store.snapshot("BTC", 10999, "public_reference")).rows()
        assert len((await store.snapshot("BTC", 11000, "public_reference")).rows()) == 2
        assert len((await store.snapshot("BTC", 12000, "public_reference")).rows()) == 3
        assert len((await store.snapshot("BTC", 12500, "public_reference")).rows()) == 4
        assert await store.pending_delivery_count() == 4
    finally:
        await store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"fundingRate": 0.1},
        {"fundingRate": "NaN"},
        {"fundingRate": "1e-4"},
        {"fundingRate": "0."},
        {"fundingRate": "1" * 31},
        {"fundingRate": "0." + "1" * 19},
        {"fundingTime": True},
        {"fundingTime": 0},
        {"markPrice": "0"},
        {"markPrice": "-1"},
        {"markPrice": 1.0},
        {"rateType": "Unknown"},
        {"symbol": "ETHUSDT"},
    ],
)
def test_strict_fields_and_scope(changes):
    with pytest.raises((ValidationError, IntegrityError)):
        parse([row(**changes)])


@pytest.mark.parametrize(
    "rows",
    [
        [row(), row()],
        [row(8000), row(4000)],
        [row(999)],
        [row(9501)],
        [row(4000), row(5000), row(6000)],
    ],
)
def test_order_window_duplicates_and_size(rows):
    with pytest.raises(IntegrityError):
        parse(rows)


@pytest.mark.parametrize(
    "changes",
    [
        {"quoteAsset": "USDC"},
        {"marginAsset": "USD"},
        {"contractType": "CURRENT_QUARTER"},
        {"status": "SETTLING"},
        {"symbol": "OTHER"},
    ],
)
def test_instrument_scope(changes):
    with pytest.raises(IntegrityError, match="INSTRUMENT_SCOPE"):
        parse_page(Instrument(**instrument(**changes)), [], 11000, 1000, 9500, 2)


@pytest.mark.parametrize(
    "changes",
    [
        {"received": True},
        {"received": 0},
        {"start_ms": 0},
        {"end_ms": 999},
        {"page_size": 0},
        {"page_size": 1001},
    ],
)
def test_invalid_page_clocks_and_budget(changes):
    with pytest.raises(IntegrityError, match="RECEIPT_CLOCK"):
        parse([], **changes)


def test_future_charge_absent_type_and_signed_zero():
    with pytest.raises(IntegrityError, match="OUTSIDE_WINDOW"):
        parse([row(9000)], received=8999)
    raw = row(fundingRate="-0.000000", rateType=None)
    assert parse([raw])[0].payload()["funding_rate"] == "-0.000000"
    del raw["rateType"]
    assert parse([raw])[0].payload()["reported_rate_type"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"start_ms": True},
        {"start_ms": 0},
        {"end_ms": 10001},
        {"start_ms": 9501},
        {"page_size": 1001},
        {"page_size": False},
        {"max_pages": 0},
        {"max_pages": 21},
        {"max_pages": True},
        {"timeout_s": float("nan")},
        {"timeout_s": float("inf")},
        {"timeout_s": 0},
        {"timeout_s": 301},
        {"timeout_s": True},
    ],
)
async def test_invalid_request_has_no_network(collector, changes):
    with pytest.raises(ValueError):
        await collect(collector, **changes)
    collector.request.assert_not_awaited()


async def test_window_span_and_unconfigured_asset(collector, monkeypatch):
    monkeypatch.setattr("selective_options.binance_funding_history.now_ms", lambda: 4_000_000_000)
    with pytest.raises(ValueError):
        await collect(collector, start_ms=1000, end_ms=3_000_000_000)
    with pytest.raises(ValueError):
        await collect_binance_funding_history(collector, None, "ETH", start_ms=1000, end_ms=9500)
    collector.request.assert_not_awaited()


@pytest.mark.parametrize(
    "symbols,clock,blocker",
    [
        ([], 10000, "DISCOVERY_SCOPE"),
        ([instrument(), instrument()], 10000, "DUPLICATE_DISCOVERY"),
        ([instrument(), instrument(symbol="OTHER")], 10000, "DISCOVERY_SCOPE"),
        ([instrument(symbol="OTHER")], 10000, "DISCOVERY_SCOPE"),
        ([instrument()], 0, "DISCOVERY_CLOCK"),
    ],
)
async def test_discovery_refusal_before_history(collector, symbols, clock, blocker):
    collector.request = AsyncMock(return_value=({"symbols": symbols}, clock))
    with pytest.raises(IntegrityError, match=blocker):
        await collect(collector)
    assert collector.request.await_count == 1


async def test_empty_and_exact_end_termination(collector):
    collector.request = AsyncMock(side_effect=[({"symbols": [instrument()]}, 10000), ([], 11000)])
    plan = await collect(collector)
    assert plan.termination == "EMPTY_PAGE" and not plan.events
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 10000), ([row(9500)], 11000)]
    )
    plan = await collect(collector)
    assert plan.termination == "END_BOUND_REACHED" and len(plan.events) == 1


async def test_truncation_and_nonadvancing_pages_never_publish(collector):
    with pytest.raises(IntegrityError, match="TRUNCATED"):
        await collect(collector, max_pages=1)
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 10000), ([row()], 11000), ([row()], 12000)]
    )
    with pytest.raises(IntegrityError, match="OUTSIDE_WINDOW"):
        await collect(collector)
    collector.sink.assert_not_awaited()


@pytest.mark.parametrize("receipt", [9999, True])
async def test_receipt_regression(collector, receipt):
    collector.request = AsyncMock(side_effect=[({"symbols": [instrument()]}, 10000), ([], receipt)])
    with pytest.raises(IntegrityError, match="RECEIPT_REGRESSION"):
        await collect(collector)


@pytest.mark.parametrize("error", [FeedUnavailable("ACCESS_DENIED", 403), asyncio.CancelledError()])
async def test_errors_cancel_staged_history(collector, error):
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 10000), ([row()], 11000), error]
    )
    with pytest.raises(type(error)):
        await collect(collector)
    collector.sink.assert_not_awaited()


async def test_timeout_drains_discovery(collector):
    stopped = asyncio.Event()

    async def wait(*args):
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    collector.request = wait
    with pytest.raises(TimeoutError):
        await collect(collector, timeout_s=0.01)
    assert stopped.is_set()


async def test_public_route_with_no_headers():
    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def iter_chunked(self, size):
            yield json.dumps([row()]).encode()

        @property
        def content(self):
            return self

    class Session:
        def get(self, url, **kwargs):
            assert url == "https://fapi.binance.com/fapi/v1/fundingRate"
            assert kwargs == {"params": {"symbol": "BTCUSDT"}, "allow_redirects": False}
            return Response()

    collector = BinanceCollector(AppConfig(), AsyncMock())
    raw, received = await collector.request(Session(), "fundingRate", {"symbol": "BTCUSDT"})
    assert raw == [row()] and received > 0


@pytest.mark.parametrize("mode", ["success", "empty", "denied", "truncated"])
async def test_verifier_reports_real_attempts_and_no_partial_history(tmp_path, monkeypatch, mode):
    async def request(self, session, endpoint, params):
        from selective_options.collectors import now_ms

        receipt = now_ms()
        if mode == "denied":
            raise FeedUnavailable("ACCESS_DENIED", 403)
        if endpoint == "exchangeInfo":
            return {"symbols": [instrument()]}, receipt
        start, end = int(params["startTime"]), int(params["endTime"])
        if mode == "empty":
            return [], receipt
        if mode == "truncated":
            return [row(start)], receipt
        return [row(end)], receipt

    monkeypatch.setattr(BinanceCollector, "request", request)
    output = tmp_path / "probe"
    if mode in {"denied", "truncated"}:
        with pytest.raises(FeedUnavailable if mode == "denied" else IntegrityError):
            await verifier.verify(output, page_size=2, max_pages=1)
    else:
        await verifier.verify(output, page_size=2, max_pages=1)
    report = json.loads((output / "report.json").read_text())
    assert report["public_requests_attempted"] == (1 if mode == "denied" else 2)
    assert report["public_responses_accepted"] == (0 if mode == "denied" else 2)
    assert report["rows"] == report["pit_checks"] == (1 if mode == "success" else 0)
    assert report["repeat_deduplication_verified"] == (mode == "success")
    assert report["archive_exact_match"] and report["trading_enabled"] is False
    assert report["funding_revision_view"]["state"] == (
        "REVISION_VIEW_VERIFIED" if mode == "success" else "MISSING"
    )
    assert report["funding_revision_view"]["selected_charges"] == (1 if mode == "success" else 0)


async def test_verifier_invalid_hours_before_output(tmp_path):
    with pytest.raises(ValueError):
        await verifier.verify(tmp_path / "invalid", hours=0)
    assert not (tmp_path / "invalid").exists()
