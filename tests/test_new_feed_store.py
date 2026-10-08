"""Capture/store wiring for opt-in supply, real-yield and lossy liquidation inputs."""

import asyncio
import importlib
import threading
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import make_event
from selective_options.ingestion import capture
from selective_options.store import DuckStore

FEEDS = [
    (
        "stablecoin_supply",
        "StablecoinSupplyCollector",
        "ethereum_drpc",
        "GLOBAL",
        "ETH_MAINNET_STABLECOIN_SUPPLY",
    ),
    ("tips_yield", "TipsYieldCollector", "fred_tips", "GLOBAL", "DFII10"),
    ("liquidations", "LiquidationCollector", "binance_liquidations", "BTC", "BTCUSDT"),
]


def native_health(source, asset, symbol, clock=1000):
    return make_event(
        source=source,
        environment="public_reference",
        asset=asset,
        symbol=symbol,
        kind="feed_health",
        event_time_ms=clock,
        observed_at_ms=clock,
        payload={"fixture_origin": "MALFORMED_HEALTH_MUST_BLOCK"},
    )


def assess(feed, rows, clock, max_rows=10000):
    function = getattr(importlib.import_module("selective_options." + feed), feed + "_quality")
    kwargs = {"as_of_ms": clock, "max_rows": max_rows}
    if feed == "liquidations":
        kwargs["asset"] = "BTC"
    return function(rows, **kwargs)


async def query(store, feed, clock, max_rows=10000):
    function = getattr(store, feed + "_quality")
    kwargs = {"as_of_ms": clock, "max_rows": max_rows}
    if feed == "liquidations":
        kwargs["asset"] = "BTC"
    return await function(**kwargs)


@pytest.mark.parametrize("feed,cls,source,asset,symbol", FEEDS)
async def test_scoped_queries_match_pure_with_both_clocks_and_exact_fetch_bound(
    tmp_path, monkeypatch, feed, cls, source, asset, symbol
):
    row = native_health(source, asset, symbol)
    future_receipt = row.model_copy(update={"event_id": "future-receipt", "observed_at_ms": 1001})
    future_source = row.model_copy(update={"event_id": "future-source", "event_time_ms": 1001})
    noise = [native_health(source, asset, "OTHER", i + 1) for i in range(100)]
    rows = [row, future_receipt, future_source, *noise]
    store = await DuckStore.open(tmp_path / "events.duckdb")
    calls = []
    select = store._select

    def tracked(sql, params):
        values = select(sql, params)
        calls.append((params[-1], len(values)))
        return values

    monkeypatch.setattr(store, "_select", tracked)
    try:
        await store.append(rows, exact=True)
        assert await query(store, feed, 999, 1) == assess(feed, rows, 999, 1)
        assert calls[-1] == (2, 0)
        assert await query(store, feed, 1000, 1) == assess(feed, rows, 1000, 1)
        assert calls[-1] == (2, 1)
        result = await query(store, feed, 1001, 1)
        assert result == assess(feed, rows, 1001, 1)
        assert result["state"] == "BLOCKED"
        assert calls[-1] == (2, 2)
    finally:
        monkeypatch.setattr(store, "_select", select)
        assert await store.pending_delivery_count() == len(rows)
        await store.close()


@pytest.mark.parametrize("feed,cls,source,asset,symbol", FEEDS)
@pytest.mark.parametrize("changes", [{"as_of_ms": True}, {"max_rows": True}, {"max_rows": 100001}])
async def test_invalid_policies_refuse_before_query(
    tmp_path, monkeypatch, feed, cls, source, asset, symbol, changes
):
    store = await DuckStore.open(tmp_path / "events.duckdb")

    def forbidden(*args):
        raise AssertionError("invalid policy accessed SQL")

    monkeypatch.setattr(store, "_select", forbidden)
    kwargs = {"as_of_ms": 1000, **changes}
    if feed == "liquidations":
        kwargs["asset"] = "BTC"
    try:
        with pytest.raises(ValueError, match="policy"):
            await getattr(store, feed + "_quality")(**kwargs)
    finally:
        await store.close()


async def test_shared_bounded_query_cancellation_drains_before_append_and_close(
    tmp_path, monkeypatch
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    started, release = threading.Event(), threading.Event()
    select = store._select

    def blocked(sql, params):
        started.set()
        assert release.wait(5)
        return select(sql, params)

    monkeypatch.setattr(store, "_select", blocked)
    task = asyncio.create_task(store.tips_yield_quality(1000))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        append = asyncio.create_task(store.append([native_health("fred_tips", "GLOBAL", "DFII10")]))
        close = asyncio.create_task(store.close())
        await asyncio.sleep(0)
        assert not task.done() and not append.done() and not close.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await append
        await close
    finally:
        release.set()
        await store.close()


@pytest.mark.parametrize("feed,cls,source,asset,symbol", FEEDS)
async def test_opt_in_producer_and_quality_report_fail_closed(
    tmp_path, monkeypatch, feed, cls, source, asset, symbol
):
    async def idle(*args):
        await asyncio.Event().wait()

    async def produce(self):
        await self.sink(native_health(source, asset, symbol, now_ms()))
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(
        getattr(importlib.import_module("selective_options." + feed), cls), "run", produce
    )
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
                feed + "_enabled": True,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    result = report[feed + "_quality"]
    if feed == "liquidations":
        result = result[0]
    assert result["state"] == "BLOCKED"
    assert result["trading_approval"] is False
    assert report["trading_enabled"] is False and report["ready_for_signals"] is False
    assert any(item["source"] == source for item in report["freshness"])


def test_new_feeds_default_disabled():
    config = AppConfig()
    assert not config.ingestion.stablecoin_supply_enabled
    assert not config.ingestion.tips_yield_enabled
    assert not config.ingestion.liquidations_enabled


@pytest.mark.parametrize(
    "name,value",
    [
        ("stablecoin_supply_poll_interval_s", float("inf")),
        ("stablecoin_supply_max_block_age_s", 0),
        ("tips_yield_poll_interval_s", float("nan")),
        ("tips_yield_history_days", True),
        ("tips_yield_history_days", 367),
    ],
)
def test_new_feed_policy_config_rejects_nonfinite_bool_and_excessive_window(name, value):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {name: value}})


def current_fixture_pair(feed):
    """Explicit native/clock fixtures exercise positive capture wiring, never availability."""
    clock = now_ms() - 20
    if feed == "stablecoin_supply":
        from selective_options.stablecoin_supply import stablecoin_poll_health, supply_event

        header = {
            "number": "0x64",
            "hash": "0x" + "11" * 32,
            "parentHash": "0x" + "22" * 32,
            "timestamp": hex((clock - 300000) // 1000),
        }

        def word(v):
            return "0x" + f"{v:064x}"

        values = ["0x1", header, word(1000000), word(6), word(2000000), word(6), header]
        native = supply_event([(value, clock - 6 + i) for i, value in enumerate(values)], 1800000)
        healthy = stablecoin_poll_health(
            clock + 1,
            ok=True,
            reason="SUPPLY_SNAPSHOT_OBSERVED",
            accepted_event_id=native.event_id,
            accepted_observed_at_ms=native.observed_at_ms,
            response_received_at_ms=native.observed_at_ms,
        )
        return [native, healthy]
    if feed == "tips_yield":
        from selective_options.tips_yield import tips_yield_health, tips_yield_snapshot

        day = datetime.fromtimestamp(clock / 1000, UTC).date()
        body = f"observation_date,DFII10\n{day.isoformat()},2.30\n".encode()
        native = tips_yield_snapshot(body, day, day, clock)
        return [
            native,
            tips_yield_health(
                clock + 1,
                snapshot_event_id=native.event_id,
                response_received_at_ms=clock,
                http_status=200,
            ),
        ]
    from selective_options.liquidations import health_event, streams_for

    return [
        health_event(
            "BTC",
            clock,
            state="CONNECTED",
            ok=True,
            reason="SUBSCRIPTION_ACKNOWLEDGED",
            session_id="a" * 32,
            session_started_at_ms=clock - 1,
            streams=streams_for(["BTC"]),
            ignored_unrequested_frames=0,
            accepted_event_id=None,
            accepted_observed_at_ms=None,
        )
    ]


@pytest.mark.parametrize("feed,cls,source,asset,symbol", FEEDS)
async def test_canonical_fixture_success_in_capture_reports_still_cannot_advance_milestone(
    tmp_path, monkeypatch, feed, cls, source, asset, symbol
):
    rows = current_fixture_pair(feed)

    async def idle(*args):
        await asyncio.Event().wait()

    async def produce(self):
        for row in rows:
            await self.sink(row)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(
        getattr(importlib.import_module("selective_options." + feed), cls), "run", produce
    )
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
                feed + "_enabled": True,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    result = report[feed + "_quality"]
    if feed == "liquidations":
        result = result[0]
        assert result["observation_state"] == "UNOBSERVED"
    assert result["state"] != "BLOCKED"
    assert result["trading_approval"] is False
    assert not report["trading_enabled"] and not report["ready_for_signals"]
