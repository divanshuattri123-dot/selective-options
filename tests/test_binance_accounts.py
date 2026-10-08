import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.binance import Instrument
from selective_options.binance_accounts import BinanceAccountCollector, account_events
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


def sample(timestamp=900_000, **changes):
    return {
        "symbol": "BTCUSDT",
        "timestamp": timestamp,
        "longShortRatio": "0.1960",
        "longAccount": "0.6622",
        "shortAccount": "0.3378",
        **changes,
    }


def parse(rows, received=1_000_000, discovered=950_000, **changes):
    return account_events(Instrument(**instrument(**changes)), rows, received, discovered)


async def test_reported_fields_and_period_end_pit_revisions(tmp_path):
    row = parse([sample()])[0]
    payload = row.payload()
    # The official example is inconsistent: retain each named field, never silently repair it.
    assert payload["reported_longShortRatio"] == "0.1960"
    assert (
        payload["reported_longAccount"] == "0.6622" and payload["reported_shortAccount"] == "0.3378"
    )
    assert row.event_time_ms == 900_000 and payload["period_start_ms"] == 600_000
    assert payload["semantics_status"] == "REPORTED_FIELDS_PRESERVED_DOCUMENTATION_INCONSISTENCIES"
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([row])
        assert not (await store.snapshot("BTC", 999_999, "public_reference")).rows()
        assert (await store.snapshot("BTC", 1_000_000, "public_reference")).rows() == (row,)
        repeated = parse([sample()], 1_100_000, 1_050_000)[0]
        assert repeated.event_id != row.event_id
        await store.append([repeated])
        await store.append([repeated])
        assert (await store.snapshot("BTC", 1_100_000, "public_reference")).rows() == (
            row,
            repeated,
        )
        revised = parse([sample(longShortRatio="1.9600")], 1_100_001)[0]
        await store.append([revised])
        assert revised.event_id != row.event_id
        assert (await store.snapshot("BTC", 1_100_000, "public_reference")).rows() == (
            row,
            repeated,
        )
        assert len((await store.snapshot("BTC", 1_100_001, "public_reference")).rows()) == 3
    finally:
        await store.close()


def test_sorted_overlap_zero_and_receipt_boundary():
    rows = parse([sample(), sample(600_000), sample(600_000)], 900_000, 800_000)
    assert [r.event_time_ms for r in rows] == [600_000, 900_000]
    zeros = parse([sample(longAccount="0", shortAccount="0", longShortRatio="0")])
    assert zeros[0].payload()["reported_longShortRatio"] == "0"


@pytest.mark.parametrize(
    "raw",
    [
        [],
        {},
        [sample()] * 4,
        [sample(symbol="ETHUSDT")],
        [sample(timestamp=True)],
        [sample(timestamp=300_000)],
        [sample(longAccount="1.00001")],
        [sample(shortAccount="-1")],
        [sample(longShortRatio="NaN")],
        [sample(longAccount=0.5)],
        [sample(timestamp=1_000_001)],
        [sample(), sample(longAccount="0.5")],
        [sample(399_999)],
    ],
)
def test_bad_response_refused(raw):
    with pytest.raises((IntegrityError, ValidationError)):
        parse(raw)


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "SETTLING"},
        {"contractType": "CURRENT_QUARTER"},
        {"quoteAsset": "USDC"},
        {"marginAsset": "BTC"},
        {"symbol": "OTHER"},
    ],
)
def test_bad_instrument_scope(changes):
    with pytest.raises(IntegrityError, match="SCOPE"):
        parse([sample()], **changes)


@pytest.mark.parametrize("discovered", [0, 1_000_001])
def test_discovery_clock_order(discovered):
    with pytest.raises(IntegrityError, match="RECEIPT_ORDER"):
        parse([sample()], discovered=discovered)


def test_old_neighbor_bound_and_latest_age_are_independent():
    assert len(parse([sample(500_000), sample()], received=1_000_000)) == 2
    with pytest.raises(IntegrityError, match="STALE_OR_FUTURE"):
        parse([sample()], received=3_000_001)


async def test_global_collection_hooks_and_source_identity():
    config = AppConfig.model_validate(
        {"system": {"assets": ["BTC"]}, "ingestion": {"binance_account_ratio_poll_interval_s": 900}}
    )
    collector = BinanceAccountCollector(config, AsyncMock())
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 950_000), ([sample()], 1_000_000)]
    )
    assert await collector.collect(None)
    assert collector.request.await_args_list[1].args[1:] == (
        "globalLongShortAccountRatio",
        {"symbol": "BTCUSDT", "period": "5m", "limit": "3"},
    )
    assert collector.poll_interval() == 900
    emitted = [call.args[0] for call in collector.sink.await_args_list]
    assert [e.kind for e in emitted] == ["account_ratio", "feed_health"]
    assert all(e.source == "binance_accounts" for e in emitted)
    collector.sink.reset_mock()
    collector.sink.side_effect = IntegrityError("storage failure")
    collector.request = AsyncMock(
        side_effect=[({"symbols": [instrument()]}, 950_000), ([sample()], 1_000_000)]
    )
    with pytest.raises(IntegrityError, match="storage failure"):
        await collector.collect(None)
    collector.sink.assert_awaited_once()


async def test_allowlist_route_and_private_denial():
    collector = BinanceAccountCollector(AppConfig(), AsyncMock())

    class Response:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def iter_chunked(self, size):
            yield json.dumps([sample()]).encode()

    class Session:
        def get(self, url, **kwargs):
            assert url == "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
            assert kwargs["allow_redirects"] is False
            assert "headers" not in kwargs
            r = Response()
            r.content = r
            return r

    raw, _ = await collector.request(Session(), "globalLongShortAccountRatio", {})
    assert raw == [sample()]
    with pytest.raises(ValueError):
        await collector.request(None, "topLongShortAccountRatio", {})


@pytest.mark.parametrize("enabled", [False, True])
async def test_capture_switch(tmp_path, monkeypatch, enabled):
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
    monkeypatch.setattr(BinanceAccountCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "binance_account_ratio_enabled": enabled,
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


@pytest.mark.parametrize("interval", [299, float("nan"), float("inf")])
def test_config_poll_bounds(interval):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {"binance_account_ratio_poll_interval_s": interval}})
