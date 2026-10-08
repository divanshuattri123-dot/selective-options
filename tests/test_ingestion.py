import asyncio
import json
import threading
from unittest.mock import AsyncMock

import duckdb
import pytest
from pydantic import ValidationError

from selective_options.bus import EventBus, HotBuffer, RedisMirror
from selective_options.collectors import (
    ChannelRegistry,
    DeribitCollector,
    Notification,
    SpotCollector,
)
from selective_options.config import AppConfig, load_config
from selective_options.errors import IntegrityError, TradingUnavailable
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.ingestion import capture, consume
from selective_options.quality import FeedRequirement, FreshnessMonitor
from selective_options.store import DuckStore, complete_thread


def event(t=1000, observed=1000, source="deribit", environment="testnet", **kwargs):
    return make_event(
        source=source,
        environment=environment,
        asset="BTC",
        symbol="BTC-X",
        kind="spot",
        event_time_ms=t,
        observed_at_ms=observed,
        payload=kwargs or {"price": 100},
    )


def notification(channel, data):
    return Notification.model_validate(
        {"jsonrpc": "2.0", "method": "subscription", "params": {"channel": channel, "data": data}}
    )


def test_event_immutable_and_identity():
    first = event()
    assert first.event_id == event(observed=2000).event_id
    assert first.event_id != event(price=101).event_id
    first.payload()["price"] = 500
    assert first.payload()["price"] == 100
    with pytest.raises(ValidationError):
        first.asset = "ETH"
    with pytest.raises(ValueError):
        event(price=float("nan"))


@pytest.mark.parametrize(
    "row", [event(observed=2000), event(t=2000), event(environment="public_reference")]
)
def test_snapshot_rejects_future_or_other_environment(row):
    with pytest.raises(IntegrityError):
        PointInTimeSnapshot(1000, "BTC", "testnet", (row,))


async def test_pit_late_data_dedup_and_parquet(tmp_path):
    store = await DuckStore.open(tmp_path / "events.db")
    try:
        await store.append(
            [event(), event(t=500, observed=3000), event(environment="public_reference")]
        )
        await store.append([event(observed=4000)])
        assert len((await store.snapshot("BTC", 1000)).rows()) == 1
        assert len((await store.snapshot("BTC", 3000)).rows(source="deribit", kind="spot")) == 2
        assert len((await store.snapshot("BTC", 3000, "public_reference")).rows()) == 1
        assert not (await store.snapshot("ETH", 9999)).rows()
        path = tmp_path / "quote's snapshot.parquet"
        assert await store.export_parquet(path) == 3
        with duckdb.connect() as db:
            assert (
                db.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] == 3
            )
        with pytest.raises(FileExistsError):
            await store.export_parquet(path)
        with pytest.raises(IntegrityError):
            await store.append([event(t=10000)])
        await store.append([])
    finally:
        await store.close()
        await store.close()


async def test_database_batch_rollback(tmp_path):
    store = await DuckStore.open(tmp_path / "events.db")
    await store.append([event()])
    bad = event(t=2000, observed=2000).model_copy(update={"source": None})
    with pytest.raises(duckdb.ConstraintException):
        await store.append([event(t=1500, observed=1500), bad])
    assert sum(row["count"] for row in await store.counts()) == 1
    await store.close()


async def test_cancel_waits_for_database_thread():
    entered, release = threading.Event(), threading.Event()

    def worker():
        entered.set()
        release.wait(2)

    task = asyncio.create_task(complete_thread(worker))
    await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_backpressure_and_durable_drain(tmp_path):
    bus = EventBus(1, 0.001)
    await bus.publish(event())
    with pytest.raises(IntegrityError, match="BACKPRESSURE"):
        await bus.publish(event(t=2000))
    stopped = asyncio.Event()
    stopped.set()
    hot, monitor = HotBuffer(1), FreshnessMonitor(5000)
    store = await DuckStore.open(tmp_path / "events.db")
    config = AppConfig.model_validate({"ingestion": {"flush_interval_s": 0.001}})
    mirror = AsyncMock()
    await consume(config, bus, store, hot, monitor, stopped, mirror)
    await bus.queue.join()
    mirror.publish_batch.assert_awaited_once()
    hot.append(event(t=2000, observed=2000))
    assert len(hot.recent("deribit", "BTC", "spot")) == 1
    assert not hot.recent("none", "BTC", "spot")
    await store.close()


def test_quality_late_packets_and_divergence():
    monitor = FreshnessMonitor(10)
    monitor.observe(event(t=500, observed=1000))
    monitor.observe(event(t=100, observed=2000))
    req = [
        FeedRequirement("deribit", "BTC", "spot", 500),
        FeedRequirement("news", "BTC", "spot", 500),
        FeedRequirement("macro", "BTC", "spot", 500, "NOT_CONFIGURED"),
    ]
    assert [r["state"] for r in monitor.check(req, 2000)] == ["STALE", "MISSING", "NOT_CONFIGURED"]
    assert monitor.check(req, 400)[0]["state"] == "CLOCK_SKEW"
    assert monitor.divergence("BTC", 1000, 500, 25)["state"] == "INSUFFICIENT_SOURCES"
    for source, price in [("coinbase", 100), ("kraken", 101)]:
        monitor.observe(event(source=source, environment="public_reference", price=price))
    assert monitor.divergence("BTC", 1000, 500, 25)["state"] == "PRICE_DIVERGENCE"
    assert monitor.divergence("BTC", 1000, 500, 200)["state"] == "AGREE"
    assert monitor.divergence("BTC", 3000, 500, 25)["state"] == "INSUFFICIENT_SOURCES"


def test_book_snapshots_and_gap_detection():
    registry = ChannelRegistry()
    registry.add_instrument("BTC", "BTC-X", "btc_usd")
    body = {
        "instrument_name": "BTC-X",
        "timestamp": 1000,
        "change_id": 10,
        "bids": [[1, 2]],
        "asks": [[2, 3]],
    }

    def parse(**changes):
        return registry.normalize(notification("book.BTC-X.none.20.100ms", body | changes), 1000)

    assert parse()[0].kind == "book"
    assert parse(change_id=20)  # complete snapshots may skip native changes
    for changes in (
        {"change_id": 19},
        {"change_id": 22, "prev_change_id": 19},
        {"bids": [[3, 1]]},
        {"asks": [[-1, 1]]},
        {"instrument_name": "OTHER"},
    ):
        with pytest.raises(IntegrityError):
            parse(**changes)
    with pytest.raises(IntegrityError):
        registry.normalize(notification("unknown", {}), 1000)


def test_trades_index_ticker(ticker, instrument):
    registry = ChannelRegistry()
    symbol = instrument["instrument_name"]
    registry.add_instrument("BTC", symbol, "btc_usd")
    assert (
        registry.normalize(notification(f"ticker.{symbol}.100ms", ticker), ticker["timestamp"])[
            0
        ].kind
        == "ticker"
    )
    assert registry.normalize(
        notification(
            "deribit_price_index.btc_usd",
            {"index_name": "btc_usd", "price": 100, "timestamp": 1000},
        ),
        1000,
    )
    body = {
        "instrument_name": symbol,
        "timestamp": 1000,
        "trade_seq": 10,
        "trade_id": "a",
        "price": 0.01,
        "amount": 1,
        "direction": "buy",
    }
    assert registry.normalize(notification(f"trades.{symbol}.100ms", [body]), 1000)
    assert not registry.normalize(notification(f"trades.{symbol}.100ms", [body]), 1000)
    with pytest.raises(IntegrityError, match="SEQUENCE_GAP"):
        registry.normalize(notification(f"trades.{symbol}.100ms", [body | {"trade_seq": 12}]), 1000)


@pytest.mark.parametrize("source", ["coinbase", "kraken"])
async def test_dynamic_spot_discovery_and_poll(monkeypatch, source):
    collector = SpotCollector(AppConfig(), AsyncMock(), source)
    product = (
        [
            {
                "id": "discovered-id",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "status": "online",
            }
        ]
        if source == "coinbase"
        else {"error": [], "result": {"native": {"altname": "discovered-id", "wsname": "XBT/USD"}}}
    )
    quote = (
        {"price": "100", "time": "2026-09-20T00:00:00Z", "bid": "99", "ask": "101"}
        if source == "coinbase"
        else {"error": [], "result": {"native": {"b": ["99", "1"], "a": ["101", "1"]}}}
    )
    get = AsyncMock(side_effect=[product, quote])
    monkeypatch.setattr("selective_options.collectors.get_json", get)
    assert await collector.discover(None) == {"BTC": "discovered-id"}
    row = await collector.poll(None, "BTC", "discovered-id")
    assert row.payload()["price"] == 100
    assert row.environment == "public_reference"
    assert row.timestamp_kind == ("source" if source == "coinbase" else "receipt")


async def test_capture_normal_shutdown_and_failure(monkeypatch, tmp_path):
    async def produce(self):
        await self.sink(event())
        await asyncio.Event().wait()

    async def chains(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(DeribitCollector, "run", produce)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", chains)
    config = AppConfig.model_validate(
        {
            "ingestion": {
                "reference_sources": [],
                "flush_interval_s": 0.001,
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
            }
        }
    )
    report = await capture(config, tmp_path / "ok", 0.02)
    assert report["rows"] == 1 and report["status"] == "CAPTURE_FINISHED"
    assert report["ready_for_signals"] is False

    async def fail(self):
        raise IntegrityError("deliberate test")

    monkeypatch.setattr(DeribitCollector, "run", fail)
    with pytest.raises(IntegrityError):
        await capture(config, tmp_path / "bad", 0.02)
    assert json.loads((tmp_path / "bad/report.json").read_text())["status"] == "FAILED"
    with pytest.raises(ValueError):
        await capture(config, tmp_path / "invalid", float("nan"))
    production = load_config(__import__("pathlib").Path("config"), "production")
    with pytest.raises(TradingUnavailable):
        await capture(production, tmp_path / "prod", 0.01)
    with pytest.raises(TradingUnavailable):
        DeribitCollector(production, AsyncMock())


async def test_redis_mirror_uses_transaction_and_no_trim(monkeypatch):
    class Pipe:
        def __init__(self):
            self.rows = []
            self.execute = AsyncMock()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def xadd(self, *args, **kwargs):
            self.rows.append((args, kwargs))

    pipe = Pipe()

    class Client:
        aclose = AsyncMock()

        def pipeline(self, transaction):
            assert transaction
            return pipe

    client = Client()
    monkeypatch.setattr("selective_options.bus.Redis.from_url", lambda *a, **kw: client)
    mirror = RedisMirror("redis://localhost:6379")
    await mirror.publish_batch([event()])
    assert len(pipe.rows) == 1 and not pipe.rows[0][1]
    pipe.execute.assert_awaited_once()
    await mirror.close()


@pytest.mark.parametrize("derivatives,spot", [(True, False), (False, True)])
async def test_volatility_freshness_tracks_derivatives_setting(
    monkeypatch, tmp_path, derivatives, spot
):
    async def idle(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(DeribitCollector, "run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr("selective_options.ingestion.DerivativesCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.SpotTradeCollector.run", idle)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "derivatives_enabled": derivatives,
                "spot_trades_enabled": spot,
                "reference_sources": [],
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.01)
    kinds = {row["kind"] for row in report["freshness"]}
    assert ("volatility_index" in kinds) is derivatives
    assert ("trade" in kinds) is spot


async def test_stream_disconnect_resubscribes_and_accepts_interleaved_data(monkeypatch):
    from collections import deque

    from selective_options.errors import TransportError

    registry = ChannelRegistry()
    registry.channels["deribit_price_index.btc_usd"] = ("BTC", "btc_usd", "index")
    discovers = AsyncMock(side_effect=[TransportError("retry"), registry, registry])
    monkeypatch.setattr("selective_options.collectors.discover_and_record", discovers)
    rows, subscribed = [], asyncio.Event()
    sockets = []

    class Socket:
        def __init__(self):
            self.messages = deque()
            sockets.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            request = json.loads(raw)
            self.messages.append(
                notification(
                    "deribit_price_index.btc_usd",
                    {"price": 100, "timestamp": 1000, "index_name": "btc_usd"},
                ).model_dump_json()
            )
            self.messages.append(
                json.dumps(
                    {"jsonrpc": "2.0", "id": request["id"], "result": request["params"]["channels"]}
                )
            )

        async def recv(self):
            if self.messages:
                return self.messages.popleft()
            if len(sockets) == 1:
                raise OSError("disconnect")
            await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.collectors.connect", lambda *a, **kw: Socket())

    async def sink(row):
        rows.append(row)
        if len(sockets) == 2 and row.payload().get("reason") == "STREAM_SUBSCRIBED":
            subscribed.set()

    config = AppConfig.model_validate({"ingestion": {"reconnect_delay_s": 0.001}})
    collector = DeribitCollector(config, sink)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(subscribed.wait(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert collector.connections == 2
    assert len([row for row in rows if row.kind == "index"]) == 2
    assert len([row for row in rows if row.payload().get("reason") == "STREAM_DISCONNECTED"]) == 2


@pytest.mark.parametrize(
    "body,error",
    [
        ({"jsonrpc": "2.0", "id": 9, "result": []}, IntegrityError),
        ({"jsonrpc": "2.0", "id": 1, "error": {"code": 10028, "message": "rate limit"}}, Exception),
    ],
)
async def test_stream_rejects_bad_acks(body, error):
    socket = AsyncMock()
    socket.recv.return_value = json.dumps(body)
    collector = DeribitCollector(AppConfig(), AsyncMock())
    with pytest.raises(error):
        await collector._receive(socket, ChannelRegistry(), 1)


async def test_discovery_records_complete_chain_but_caps_stream(monkeypatch, instrument, summary):
    from selective_options.collectors import discover_and_record

    # Use the adapter's real result model, then exercise recording and stream selection.
    from selective_options.models import BookSummary, ChainSnapshot, DiscoveryResult, Instrument
    from selective_options.rate_limit import EndpointLimiter
    from selective_options.venue import ChainRow

    parsed = Instrument.model_validate(instrument)
    chain = ChainSnapshot(
        asset="BTC",
        observed_at_ms=summary["creation_timestamp"],
        discovery=DiscoveryResult(
            asset="BTC", query_currency="BTC", fallback_used=False, instruments=(parsed,)
        ),
        rows=(ChainRow(instrument=parsed, summary=BookSummary.model_validate(summary)),),
    )

    class Connection:
        def __init__(self, *args):
            pass

        async def __aenter__(self):
            return None

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("selective_options.collectors.WebSocketRpc", Connection)
    monkeypatch.setattr(
        "selective_options.collectors.DeribitAdapter.option_chain", AsyncMock(return_value=chain)
    )
    sink = AsyncMock()
    config = AppConfig.model_validate({"system": {"assets": ["BTC"]}})
    registry = await discover_and_record(config, sink, EndpointLimiter(config.rate_limits))
    assert len(registry.channels) == 4
    assert {call.args[0].kind for call in sink.await_args_list} == {
        "instrument",
        "chain_summary",
        "status",
    }
