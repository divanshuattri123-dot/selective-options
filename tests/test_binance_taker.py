import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.binance import Instrument
from selective_options.binance_taker import BinanceTakerCollector, taker_events
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore


def instrument(asset="BTC", **changes):
    return dict(
        symbol=asset + "USDT",
        baseAsset=asset,
        quoteAsset="USDT",
        marginAsset="USDT",
        contractType="PERPETUAL",
        status="TRADING",
        **changes,
    )


def sample(timestamp=600_000, **changes):
    return {
        "timestamp": timestamp,
        "buyVol": "123456789012345678901.000001",
        "sellVol": "2.3000",
        "buySellRatio": "1.5586",
        **changes,
    }


def parse(rows, received=1_000_000, discovered=950_000, **changes):
    raw = instrument()
    raw.update(changes)
    return taker_events(Instrument(**raw), rows, received, discovered)


async def test_period_end_receipt_precision_repeat_and_revision(tmp_path):
    rows = parse([sample(600_000), sample(900_000)])
    assert len(rows) == 1 and rows[0].event_time_ms == 900_000
    payload = rows[0].payload()
    assert payload["buy_volume"] == "123456789012345678901.000001"
    assert payload["sell_volume"] == "2.3000" and payload["buy_sell_ratio"] == "1.5586"
    assert payload["period_start_ms"] == 600_000 and payload["period_end_ms"] == 900_000
    assert payload["normalization_status"] == "BLOCKED_PENDING_UNIT_VERIFICATION"
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        assert not (await store.snapshot("BTC", 999_999, "public_reference")).rows()
        assert (await store.snapshot("BTC", 1_000_000, "public_reference")).rows() == tuple(rows)
        later = parse([sample()], 1_100_000, 1_050_000)
        assert later[0].event_id != rows[0].event_id
        await store.append(later)
        await store.append(later)
        assert (await store.snapshot("BTC", 1_100_000, "public_reference")).rows() == tuple(
            rows + later
        )
        changed = parse([sample(buyVol="9.0000")], 1_100_001, 1_050_000)
        assert changed[0].event_id != rows[0].event_id
        await store.append(changed)
        assert (await store.snapshot("BTC", 1_100_000, "public_reference")).rows() == tuple(
            rows + later
        )
        assert len((await store.snapshot("BTC", 1_100_001, "public_reference")).rows()) == 3
    finally:
        await store.close()


def test_sort_identical_overlap_zero_and_exact_period_boundary():
    rows = parse(
        [
            sample(600_000, buyVol="0", sellVol="0", buySellRatio="0"),
            sample(300_000),
            sample(300_000),
        ],
        900_000,
        800_000,
    )
    assert [r.event_time_ms for r in rows] == [600_000, 900_000]
    assert rows[-1].payload()["buy_volume"] == "0"


@pytest.mark.parametrize(
    "raw",
    [
        [],
        [sample()] * 4,
        {},
        [sample(timestamp=True)],
        [sample(timestamp="600000")],
        [sample(timestamp=0)],
        [sample(buyVol=3)],
        [sample(buyVol="-1")],
        [sample(sellVol="NaN")],
        [sample(sellVol="1e10")],
        [sample(buySellRatio="Infinity")],
        [sample(buyVol="1" * 31)],
        [sample(buyVol="1." + "2" * 19)],
        [sample(1_000_001)],
        [sample(), sample(buyVol="4")],
    ],
)
def test_invalid_response_refused(raw):
    with pytest.raises((IntegrityError, ValidationError)):
        parse(raw)


@pytest.mark.parametrize(
    "changes",
    [
        {"contractType": "CURRENT_QUARTER"},
        {"status": "SETTLING"},
        {"marginAsset": "BTC"},
        {"quoteAsset": "USDC"},
        {"symbol": "OTHER"},
    ],
)
def test_incompatible_instrument_refused(changes):
    with pytest.raises(IntegrityError, match="SCOPE"):
        parse([sample()], **changes)


@pytest.mark.parametrize("discovered", [0, 1_000_001])
def test_discovery_must_precede_sample_receipt(discovered):
    with pytest.raises(IntegrityError, match="RECEIPT_ORDER"):
        parse([sample()], discovered=discovered)


def test_stale_sample_and_only_forming_response_refused():
    with pytest.raises(IntegrityError, match="STALE"):
        parse([sample()], received=2_000_001)
    with pytest.raises(FeedUnavailable, match="NO_ENDED_PERIOD"):
        parse([sample(900_000)])


def test_saved_public_response_accepts_oldest_sample_with_fresh_latest_end():
    from pathlib import Path

    responses = json.loads(Path("evidence/binance-taker-public-01/responses.json").read_text())
    response = responses[-1]
    rows = taker_events(
        Instrument(**instrument()),
        response["response"],
        response["observed_at_ms"],
        responses[0]["observed_at_ms"],
    )
    assert len(rows) == 3
    assert rows[0].event_time_ms == response["response"][0]["timestamp"] + 300_000
    assert response["observed_at_ms"] - rows[-1].event_time_ms <= 600_000
    with pytest.raises(IntegrityError, match="LATEST_PERIOD_STALE"):
        parse([sample()], received=1_500_001)


@pytest.fixture
def collector():
    config = AppConfig.model_validate({"system": {"assets": ["BTC", "ETH"]}})
    return BinanceTakerCollector(config, AsyncMock())


async def test_cycle_validates_all_assets_before_publishing_and_reports_missing(collector):
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 950_000), ([sample()], 1_000_000)]
    )
    assert not await collector.collect(None)
    assert collector.request.await_args_list[1].args[1:] == (
        "takerlongshortRatio",
        {"symbol": "BTCUSDT", "period": "5m", "limit": "3"},
    )
    assert [c.args[0].kind for c in collector.sink.await_args_list] == [
        "taker_flow",
        "feed_health",
        "feed_health",
    ]
    assert collector.sink.await_args.args[0].asset == "ETH"
    assert collector.sink.await_args.args[0].payload()["reason"] == "INSTRUMENT_UNAVAILABLE"
    collector.sink.reset_mock()
    collector.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument(), instrument("ETH")]}, 950_000),
            ([sample()], 1_000_000),
            ([sample(buyVol="bad")], 1_000_001),
        ]
    )
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    assert collector.sink.await_args.args[0].kind == "feed_health"
    assert collector.sink.await_args.args[0].asset == "ETH"


async def test_success_all_assets_and_excluded_discovery(collector):
    collector.request = AsyncMock(
        side_effect=[
            ({"symbols": [instrument(), instrument("ETH")]}, 950_000),
            ([sample()], 1_000_000),
            ([sample()], 1_000_001),
        ]
    )
    assert await collector.collect(None)
    collector.sink.reset_mock()
    wrong = instrument()
    wrong["contractType"] = "CURRENT_QUARTER"
    collector.request = AsyncMock(return_value=({"symbols": [wrong]}, 950_000))
    assert not await collector.collect(None)
    assert collector.request.await_count == 1
    assert all(
        c.args[0].payload()["reason"] == "INSTRUMENT_UNAVAILABLE"
        for c in collector.sink.await_args_list
    )


@pytest.mark.parametrize(
    "rows", [[instrument(), instrument()], [instrument(), {**instrument(), "symbol": "OTHER"}]]
)
async def test_bad_discovery_refused(collector, rows):
    collector.request = AsyncMock(return_value=({"symbols": rows}, 950_000))
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


@pytest.mark.parametrize(
    "error,reason",
    [(FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"), (TimeoutError(), "TimeoutError")],
)
async def test_unavailable_and_cancel_propagation(collector, error, reason):
    collector.request = AsyncMock(side_effect=error)
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["reason"] == reason
    collector.sink.reset_mock()
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)
    collector.sink.assert_not_awaited()


async def test_sink_failure_is_not_source_failure(collector):
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 950_000), ([sample()], 1_000_000)]
    )
    collector.sink.side_effect = IntegrityError("writer failure")
    with pytest.raises(IntegrityError, match="writer failure"):
        await collector.collect(None)
    collector.sink.assert_awaited_once()


class Response:
    status = 200
    content = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        yield json.dumps([sample()]).encode()


class Session:
    def get(self, url, **kwargs):
        assert url == "https://fapi.binance.com/futures/data/takerlongshortRatio"
        assert kwargs == {
            "params": {"symbol": "BTCUSDT", "period": "5m", "limit": "3"},
            "allow_redirects": False,
        }
        response = Response()
        response.content = response
        return response


async def test_public_endpoint_routing(collector):
    raw, received = await collector.request(
        Session(), "takerlongshortRatio", {"symbol": "BTCUSDT", "period": "5m", "limit": "3"}
    )
    assert raw == [sample()] and received > 0


@pytest.mark.parametrize(
    "ok,interval,delay", [(True, 300, 300), (False, 300, 600), (False, 900, 900)]
)
async def test_bounded_poll_backoff(collector, monkeypatch, ok, interval, delay):
    collector.config = AppConfig.model_validate(
        {"ingestion": {"binance_taker_poll_interval_s": interval}}
    )
    collector.collect = AsyncMock(return_value=ok)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.binance_taker.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(delay)


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_opt_in(tmp_path, monkeypatch, enabled):
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    calls = []

    async def run(self):
        calls.append(True)
        await self.health("BTC", False, "ACCESS_DENIED", 403)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(BinanceTakerCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "binance_taker_enabled": enabled,
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


@pytest.mark.parametrize("interval", [299, float("inf"), float("nan")])
def test_config_minimum_and_finite_interval(interval):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {"binance_taker_poll_interval_s": interval}})
