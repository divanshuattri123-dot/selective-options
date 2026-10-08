import json
import subprocess
import sys
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig
from selective_options.events import MarketEvent, make_event
from selective_options.quality import FreshnessMonitor
from selective_options.recovery import replay_pending, replay_store
from selective_options.store import DuckStore


def event(index=0):
    return make_event(
        source="source",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=1000 + index,
        observed_at_ms=2000 + index,
        payload={"price": 100},
    )


async def test_durable_outbox_survives_restart_and_failed_delivery(tmp_path):
    path = tmp_path / "state.duckdb"
    store = await DuckStore.open(path)
    await store.append([event(i) for i in range(4)])
    await store.close()
    store = await DuckStore.open(path)
    assert await store.pending_delivery_count() == 4
    mirror = AsyncMock()
    mirror.publish_batch.side_effect = ConnectionError("simulated Redis outage")
    with pytest.raises(ConnectionError):
        await replay_pending(store, mirror, 2)
    assert await store.pending_delivery_count() == 4
    mirror.publish_batch.side_effect = None
    assert await replay_pending(store, mirror, 2) == 4
    assert await store.pending_delivery_count() == 0
    await store.append([event()])
    assert await store.pending_delivery_count() == 0
    await store.acknowledge_delivery([])
    with pytest.raises(ValueError):
        await store.pending_delivery(0)
    await store.close()
    reopened = await DuckStore.open(path)
    assert await reopened.pending_delivery_count() == 0
    await reopened.close()


async def test_crash_after_publish_before_ack_replays_same_identity(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "state.duckdb")
    await store.append([event()])
    mirror = AsyncMock()
    original = store.acknowledge_delivery
    monkeypatch.setattr(
        store, "acknowledge_delivery", AsyncMock(side_effect=RuntimeError("crash before ACK"))
    )
    with pytest.raises(RuntimeError):
        await replay_pending(store, mirror, 500)
    monkeypatch.setattr(store, "acknowledge_delivery", original)
    assert await replay_pending(store, mirror, 500) == 1
    assert (
        mirror.publish_batch.await_args_list[0].args[0][0].event_id
        == mirror.publish_batch.await_args_list[1].args[0][0].event_id
    )
    assert len((await store.snapshot("BTC", 9999, "public_reference")).rows()) == 1
    await store.close()


async def test_actual_abrupt_process_exit_rolls_back_uncommitted_rows(tmp_path):
    path = tmp_path / "crash.duckdb"
    # A child exits without closing DuckDB, after one committed event and an uncommitted row.
    script = """
import asyncio,os,sys
from pathlib import Path
from selective_options.store import DuckStore
from selective_options.events import make_event
async def main():
 s=await DuckStore.open(Path(sys.argv[1]))
 e=make_event(source='test',environment='testnet',asset='BTC',symbol='x',kind='spot',event_time_ms=1000,observed_at_ms=1000,payload={'price':1})
 await s.append([e])
 s._connection.execute('BEGIN TRANSACTION')
 s._connection.execute("INSERT INTO events SELECT 'uncommitted',source,environment,asset,symbol,kind,event_time_ms,observed_at_ms,timestamp_kind,schema_version,payload_json FROM events")
 os._exit(23)
asyncio.run(main())
"""
    import asyncio

    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c", script, str(path)], capture_output=True, timeout=20
    )
    assert result.returncode == 23, result.stderr.decode()
    store = await DuckStore.open(path)
    assert sum(row["count"] for row in await store.counts()) == 1
    assert await store.pending_delivery_count() == 1
    await store.close()


async def test_replay_cli_service_checks_and_closes(tmp_path, monkeypatch):
    with pytest.raises(ValueError):
        await replay_store(AppConfig(), tmp_path / "missing")
    config = AppConfig.model_validate({"ingestion": {"redis_url": "redis://localhost:6379"}})
    with pytest.raises(FileNotFoundError):
        await replay_store(config, tmp_path / "missing")
    path = tmp_path / "state.duckdb"
    store = await DuckStore.open(path)
    await store.append([event()])
    await store.close()
    mirror = AsyncMock()
    monkeypatch.setattr("selective_options.recovery.RedisMirror", lambda url: mirror)
    report = await replay_store(config, path)
    assert report["events_delivered"] == 1 and report["pending_events"] == 0
    assert report["delivery"] == "AT_LEAST_ONCE"
    mirror.close.assert_awaited_once()


def test_selected_option_leg_freshness_is_not_hidden_by_busy_contract():
    monitor = FreshnessMonitor(5)
    subscription = make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=1000,
        observed_at_ms=1000,
        payload={
            "reason": "STREAM_SUBSCRIBED",
            "instruments": [
                {"asset": "BTC", "symbol": "stale"},
                {"asset": "BTC", "symbol": "busy"},
                {"asset": "BTC", "symbol": "absent"},
            ],
        },
    )
    monitor.observe(subscription)
    for symbol, t in [("stale", 1000), ("busy", 1999)]:
        monitor.observe(
            make_event(
                source="deribit",
                environment="testnet",
                asset="BTC",
                symbol=symbol,
                kind="book",
                event_time_ms=t,
                observed_at_ms=t,
                payload={"bids": [], "asks": []},
            )
        )
    result = {(row["symbol"], row["kind"]): row for row in monitor.option_freshness(2000, 500)}
    assert result["stale", "book"]["state"] == "STALE"
    assert result["busy", "book"]["state"] == "FRESH"
    assert result["absent", "book"]["state"] == "MISSING"
    assert result["busy", "book"]["quote_state"] == "ONE_SIDED_OR_EMPTY"
    assert monitor.option_freshness(900, 500)[0]["state"] in {"MISSING", "CLOCK_SKEW"}


@pytest.mark.parametrize(
    "change", [{"closed": False}, {"close_time_ms": 1200000}, {"low": 120}, {"timeframe_s": 301}]
)
def test_persisted_bars_cannot_disguise_forming_or_future_data(change):
    payload = {
        "open_time_ms": 300000,
        "close_time_ms": 600000,
        "timeframe_s": 300,
        "open": 100,
        "high": 110,
        "low": 90,
        "close": 105,
        "volume": 1,
        "trade_count": 1,
        "closed": True,
        "complete": True,
    }
    with pytest.raises(ValidationError):
        MarketEvent(
            event_id="x",
            source="kraken",
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USD",
            kind="bar",
            event_time_ms=600000,
            observed_at_ms=605000,
            timestamp_kind="source",
            payload_json=json.dumps(payload | change),
        )


def test_valid_bar_close_cannot_be_backdated_as_an_earlier_event():
    payload = {
        "open_time_ms": 300000,
        "close_time_ms": 600000,
        "timeframe_s": 300,
        "open": 100,
        "high": 110,
        "low": 90,
        "close": 105,
        "volume": 1,
        "trade_count": 1,
        "closed": True,
        "complete": True,
    }
    with pytest.raises(ValidationError, match="close time"):
        make_event(
            source="kraken",
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USD",
            kind="bar",
            event_time_ms=300000,
            observed_at_ms=305000,
            payload=payload,
        )
