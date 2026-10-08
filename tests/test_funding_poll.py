import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest
from pydantic import ValidationError

from selective_options import funding_poll
from selective_options.binance_funding_history import BinanceFundingArchive
from selective_options.binance_funding_view import funding_history_at
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.funding_poll import BinanceFundingCollector
from selective_options.ingestion import capture
from selective_options.store import DuckStore
from tests.test_binance_funding_history import instrument, row
from tests.test_binance_funding_view import event

CLOCK = 180_000_000


def config(**changes):
    return AppConfig.model_validate({"system": {"assets": ["BTC"]}, "ingestion": changes})


def plan(events=(), end=CLOCK):
    return BinanceFundingArchive(tuple(events), 2, end - 48 * 3_600_000, end, "EMPTY_PAGE")


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr(funding_poll, "now_ms", lambda: CLOCK)
    sink = AsyncMock()
    collector = BinanceFundingCollector(config(), sink)
    return collector


async def test_poll_revisions_replay_and_health_persist(collector, monkeypatch, tmp_path):
    first = event(CLOCK - 10000, CLOCK - 9000)
    second = event(CLOCK - 10000, CLOCK - 8000, rate="-0.00020000")
    fetch = AsyncMock(side_effect=[plan([first]), plan([second], end=CLOCK + 1000)])
    monkeypatch.setattr(funding_poll, "collect_binance_funding_history", fetch)
    assert await collector.collect(None)
    monkeypatch.setattr(funding_poll, "now_ms", lambda: CLOCK + 1000)
    assert await collector.collect(None)
    published = [call.args[0] for call in collector.sink.await_args_list]
    assert [e.kind for e in published] == ["funding", "feed_health", "funding", "feed_health"]
    assert published[1].payload()["ok"] is True
    assert published[1].payload()["observations"] == 1
    assert published[1].source == "binance_history" and published[1].symbol == "BTCUSDT"
    assert fetch.call_args.kwargs == dict(
        start_ms=CLOCK + 1000 - 48 * 3_600_000,
        end_ms=CLOCK + 1000,
        page_size=200,
        max_pages=10,
        timeout_s=90,
    )
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(published)
        await store.append(published)
        rows = (await store.snapshot("BTC", CLOCK + 1000, "public_reference")).rows()
        assert len(rows) == 4 and await store.pending_delivery_count() == 4
        assert funding_history_at(
            rows,
            asset="BTC",
            symbol="BTCUSDT",
            start_ms=1,
            end_ms=CLOCK,
            as_of_ms=first.observed_at_ms,
        ) == (first,)
        assert funding_history_at(
            rows, asset="BTC", symbol="BTCUSDT", start_ms=1, end_ms=CLOCK, as_of_ms=CLOCK
        ) == (second,)
    finally:
        await store.close()


async def test_empty_poll_explicit_failure(collector, monkeypatch):
    monkeypatch.setattr(
        funding_poll, "collect_binance_funding_history", AsyncMock(return_value=plan())
    )
    assert not await collector.collect(None)
    health = collector.sink.call_args.args[0]
    assert health.payload()["ok"] is False
    assert health.payload()["reason"] == "NO_SETTLEMENTS_RETURNED"
    assert health.payload()["observations"] == 0


async def test_wrong_plan_scope_fails_before_market_publication(collector, monkeypatch):
    monkeypatch.setattr(
        funding_poll,
        "collect_binance_funding_history",
        AsyncMock(return_value=plan([event(CLOCK - 1000, CLOCK)], end=CLOCK - 1)),
    )
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    assert collector.sink.call_args.args[0].kind == "feed_health"


async def test_invalid_source_row_is_failure_health_not_a_prefix(collector, monkeypatch):
    monkeypatch.setattr("selective_options.binance_funding_history.now_ms", lambda: CLOCK)
    collector.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument()]}, CLOCK),
            ([row(CLOCK - 1000, markPrice="bad")], CLOCK),
        ]
    )
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    assert collector.sink.call_args.args[0].payload()["reason"] == "INVALID_DATA"


async def test_cancel_during_sink_can_leave_rows_but_no_completed_health(collector, monkeypatch):
    events = [event(CLOCK - 2000, CLOCK), event(CLOCK - 1000, CLOCK)]
    monkeypatch.setattr(
        funding_poll, "collect_binance_funding_history", AsyncMock(return_value=plan(events))
    )
    collector.sink.side_effect = [None, asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)
    assert collector.sink.call_count == 2
    assert all(c.args[0].kind == "funding" for c in collector.sink.call_args_list)


@pytest.mark.parametrize(
    "error,reason,code",
    [
        (FeedUnavailable("ACCESS_DENIED", 451), "ACCESS_DENIED", 451),
        (FeedUnavailable("RATE_LIMITED", 429), "RATE_LIMITED", 429),
        (aiohttp.ClientError(), "TRANSPORT_UNAVAILABLE", 0),
        (OSError(), "TRANSPORT_UNAVAILABLE", 0),
        (TimeoutError(), "TRANSPORT_UNAVAILABLE", 0),
        (IntegrityError("TRUNCATED"), "INVALID_DATA", 0),
    ],
)
async def test_failed_staging_only_publishes_failure_health(
    collector, monkeypatch, error, reason, code
):
    monkeypatch.setattr(
        funding_poll, "collect_binance_funding_history", AsyncMock(side_effect=error)
    )
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    health = collector.sink.call_args.args[0]
    assert health.kind == "feed_health"
    assert health.payload()["ok"] is False and health.payload()["reason"] == reason
    assert health.payload()["code"] == code


async def test_real_truncated_fetch_does_not_publish_prefix(collector, monkeypatch):
    monkeypatch.setattr("selective_options.binance_funding_history.now_ms", lambda: CLOCK)
    collector.config = config(binance_funding_history_max_pages=1)
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, CLOCK), ([row(CLOCK - 1000)], CLOCK)]
    )
    assert not await collector.collect(None)
    assert collector.sink.call_count == 1
    assert collector.sink.call_args.args[0].kind == "feed_health"


@pytest.mark.parametrize("failure_at", [0, 1])
async def test_sink_failure_propagates_without_source_relabelling(
    collector, monkeypatch, failure_at
):
    monkeypatch.setattr(
        funding_poll,
        "collect_binance_funding_history",
        AsyncMock(return_value=plan([event(CLOCK - 10000, CLOCK - 9000)])),
    )
    collector.sink.side_effect = [None] * failure_at + [IntegrityError("STORAGE_FAILURE")]
    with pytest.raises(IntegrityError, match="STORAGE_FAILURE"):
        await collector.collect(None)
    assert collector.sink.call_count == failure_at + 1


async def test_cancelled_staging_propagates_without_health(collector, monkeypatch):
    monkeypatch.setattr(
        funding_poll,
        "collect_binance_funding_history",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)
    collector.sink.assert_not_awaited()


async def test_one_asset_failure_does_not_skip_next(monkeypatch):
    monkeypatch.setattr(funding_poll, "now_ms", lambda: CLOCK)
    collector = BinanceFundingCollector(
        AppConfig.model_validate({"system": {"assets": ["BTC", "ETH"]}}), AsyncMock()
    )
    fetch = AsyncMock(side_effect=[IntegrityError("BAD_BTC_SCOPE"), plan()])
    monkeypatch.setattr(funding_poll, "collect_binance_funding_history", fetch)
    assert not await collector.collect(None)
    assert [c.args[2] for c in fetch.call_args_list] == ["BTC", "ETH"]
    assert [c.args[0].asset for c in collector.sink.call_args_list] == ["BTC", "ETH"]


@pytest.mark.parametrize(
    "error",
    [FeedUnavailable("RATE_LIMITED", 429), FeedUnavailable("ACCESS_DENIED", 451), TimeoutError()],
)
async def test_venue_refusal_aborts_cycle_before_next_asset(monkeypatch, error):
    monkeypatch.setattr(funding_poll, "now_ms", lambda: CLOCK)
    collector = BinanceFundingCollector(
        AppConfig.model_validate({"system": {"assets": ["BTC", "ETH"]}}), AsyncMock()
    )
    fetch = AsyncMock(side_effect=error)
    monkeypatch.setattr(funding_poll, "collect_binance_funding_history", fetch)
    assert not await collector.collect(None)
    fetch.assert_awaited_once()
    collector.sink.assert_awaited_once()


@pytest.mark.parametrize("clocks", [[CLOCK, CLOCK - 1, CLOCK], [CLOCK, CLOCK, CLOCK - 1]])
async def test_clock_regression_never_emits_success(collector, monkeypatch, clocks):
    monkeypatch.setattr(funding_poll, "now_ms", iter(clocks).__next__)
    monkeypatch.setattr(
        funding_poll,
        "collect_binance_funding_history",
        AsyncMock(return_value=plan([event(CLOCK - 1000, CLOCK)])),
    )
    if clocks[1] < CLOCK:
        assert not await collector.collect(None)
        assert collector.sink.call_args.args[0].payload()["ok"] is False
    else:
        with pytest.raises(IntegrityError, match="HEALTH_CLOCK_REGRESSION"):
            await collector.collect(None)
        assert collector.sink.call_count == 1 and collector.sink.call_args.args[0].kind == "funding"


@pytest.mark.parametrize("ok", [True, False])
async def test_run_waits_full_configured_interval_after_either_result(collector, monkeypatch, ok):
    collector.collect = AsyncMock(return_value=ok)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(funding_poll.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(900)


@pytest.mark.parametrize(
    "field,value",
    [
        ("binance_funding_history_hours", 0),
        ("binance_funding_history_hours", 721),
        ("binance_funding_history_hours", True),
        ("binance_funding_history_page_size", 0),
        ("binance_funding_history_page_size", 1001),
        ("binance_funding_history_max_pages", 0),
        ("binance_funding_history_max_pages", 21),
        ("binance_funding_history_timeout_s", 0),
        ("binance_funding_history_timeout_s", 301),
        ("binance_funding_history_timeout_s", float("nan")),
        ("binance_funding_history_poll_interval_s", 599),
        ("binance_funding_history_poll_interval_s", float("inf")),
    ],
)
def test_config_bounds(field, value):
    with pytest.raises(ValidationError):
        config(**{field: value})


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_opt_in_report_storage_and_health_requirement(monkeypatch, tmp_path, enabled):
    async def idle(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    calls = []

    async def run(self):
        calls.append(True)
        current = funding_poll.now_ms()
        await self.sink(event(current - 1000, current))
        await self.poll_health(
            "BTC", current - 48 * 3_600_000, current, True, "BOUNDED_HISTORY_ACCEPTED", plan=plan()
        )
        await idle()

    monkeypatch.setattr(BinanceFundingCollector, "run", run)
    cfg = config(
        binance_funding_history_enabled=enabled,
        reference_sources=[],
        derivatives_enabled=False,
        spot_trades_enabled=False,
        policy_news_sources=[],
        flush_interval_s=0.001,
    )
    report = await capture(cfg, tmp_path / "capture", 0.1)
    assert bool(calls) == enabled
    assert report["rows"] == 2 * int(enabled)
    assert len(report["funding_revision_views"]) == int(enabled)
    if enabled:
        assert report["funding_revision_views"][0]["selected_charges"] == 1
        health = report["funding_poll_health"][0]["latest_observations"]
        assert len(health) == 1 and '"ok":true' in health[0]["payload_json"]
    assert report["trading_enabled"] is False
