import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from selective_options.bars import TradeBars
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.spot_trades import KrakenTrade, SpotTradeCollector, TradeSequence


def tick(timestamp=301000, price=100, qty=0.1, trade_id=1, symbol="BTC/USD"):
    return make_event(
        source="kraken",
        environment="public_reference",
        asset="BTC",
        symbol=symbol,
        kind="trade",
        event_time_ms=timestamp,
        observed_at_ms=timestamp + 100,
        payload={"price": price, "qty": qty, "trade_id": trade_id},
    )


def wire(trade_id=1, timestamp=301000, **changes):
    return {
        "symbol": "BTC/USD",
        "side": "buy",
        "price": 100,
        "qty": 0.1,
        "trade_id": trade_id,
        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(),
        **changes,
    }


def test_bar_close_lateness_and_pit():
    builder = TradeBars((300, 900), 300000, 5000)
    builder.add(tick())
    builder.add(tick(302000, price=110, qty=0.2, trade_id=2))
    builder.add(tick(303000, price=95, qty=0.3, trade_id=3))
    assert not builder.close_ready(604999)
    rows = builder.close_ready(605000)
    assert len(rows) == 1
    bar = rows[0].payload()
    assert (bar["open"], bar["high"], bar["low"], bar["close"]) == (100, 110, 95, 95)
    assert bar["volume"] == 0.6 and bar["trade_count"] == 3
    assert bar["complete"] is True and bar["closed"] is True
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        PointInTimeSnapshot(600000, "BTC", "public_reference", tuple(rows))
    assert PointInTimeSnapshot(605000, "BTC", "public_reference", tuple(rows)).rows()
    assert not builder.close_ready(606000)


def test_partial_first_bar_gap_and_no_empty_bars():
    builder = TradeBars((300,), 302000, 0)
    builder.add(tick())
    assert builder.close_ready(600000)[0].payload()["complete"] is False
    assert not builder.close_ready(900000)
    builder.add(tick(901000, trade_id=2))
    builder.mark_gap(1000000)
    assert builder.close_ready(1200000)[0].payload()["complete"] is False
    builder.add(tick(1201000, trade_id=3))
    assert builder.close_ready(1500000)[0].payload()["complete"] is True


def test_bar_order_is_event_time_then_native_id_and_fail_closed():
    builder = TradeBars((300,), 300000, 5000)
    builder.add(tick(303000, price=110, trade_id=3))
    builder.add(tick(301000, price=90, trade_id=1))
    builder.add(tick(303000, price=120, trade_id=4))
    row = builder.close_ready(605000)[0]
    assert row.payload()["open"] == 90 and row.payload()["close"] == 120
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        builder.add(tick(303000, trade_id=5))
    with pytest.raises(IntegrityError, match="backwards"):
        builder.close_ready(600000)
    with pytest.raises(IntegrityError, match="mismatch"):
        builder.add(tick(606000, symbol="ETH/USD"))
    with pytest.raises(IntegrityError, match="must be a trade"):
        builder.add(tick().model_copy(update={"kind": "spot"}))


@pytest.mark.parametrize("frames,late", [((), 0), ((0,), 0), ((300, 300), 0), ((300,), -1)])
def test_bad_bar_config(frames, late):
    with pytest.raises(ValueError):
        TradeBars(frames, 1000, late)


def test_trade_sequence_dedup_conflict_gap_and_bounded_cache():
    sequence = TradeSequence(capacity=2)
    one = KrakenTrade.model_validate(wire())
    assert sequence.accept(one)
    assert not sequence.accept(one)
    with pytest.raises(IntegrityError, match="CONFLICTING"):
        sequence.accept(KrakenTrade.model_validate(wire(price=999)))
    with pytest.raises(IntegrityError, match="SEQUENCE_GAP"):
        sequence.accept(KrakenTrade.model_validate(wire(trade_id=3)))
    assert sequence.accept(KrakenTrade.model_validate(wire(trade_id=2)))
    assert sequence.accept(KrakenTrade.model_validate(wire(trade_id=3)))
    assert len(sequence.seen) == 2


async def test_collector_snapshot_and_live_ticks_aggregate_without_duplicates():
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink)
    collector.symbols = {"BTC/USD": "BTC"}
    await collector._trades(
        {"channel": "trade", "type": "snapshot", "data": [wire(2, 302000), wire()]}, 303000
    )
    await collector._trades(
        {"channel": "trade", "type": "update", "data": [wire(2, 302000), wire(3, 304000)]}, 305000
    )
    await collector._flush(605000)
    rows = [call.args[0] for call in sink.await_args_list]
    assert len([row for row in rows if row.kind == "trade"]) == 3
    bar = next(row for row in rows if row.kind == "bar")
    assert bar.payload()["trade_count"] == 3 and not bar.payload()["complete"]
    # A reconnect snapshot older than closed bars is retained raw, never silently injected.
    await collector._trades(
        {"channel": "trade", "type": "snapshot", "data": [wire(4, 306000)]}, 606000
    )
    assert sink.await_args.args[0].payload()["reason"] == "HISTORICAL_TRADE_RAW_ONLY"


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "OTHER"},
        {"timestamp": "2026-01-01T00:00:00"},
        {"timestamp": "2099-01-01T00:00:00Z"},
    ],
)
async def test_invalid_trade_identity_or_time_fails(changes):
    collector = SpotTradeCollector(AppConfig(), AsyncMock())
    collector.symbols = {"BTC/USD": "BTC"}
    with pytest.raises(IntegrityError):
        await collector._trades(
            {"channel": "trade", "type": "update", "data": [wire() | changes]}, 303000
        )


async def test_metadata_discovery_does_not_construct_instrument_names():
    collector = SpotTradeCollector(AppConfig(), AsyncMock())
    socket = AsyncMock()
    socket.recv.side_effect = [
        json.dumps({"channel": "heartbeat"}),
        json.dumps(
            {
                "channel": "instrument",
                "type": "snapshot",
                "data": {
                    "pairs": [
                        {"symbol": "opaque", "base": "BTC", "quote": "USD", "status": "online"},
                        {
                            "symbol": "offline",
                            "base": "ETH",
                            "quote": "USD",
                            "status": "cancel_only",
                        },
                    ]
                },
            }
        ),
        json.dumps(
            {
                "method": "subscribe",
                "req_id": 1,
                "success": True,
                "result": {"channel": "instrument"},
            }
        ),
    ]
    await collector._discover(socket)
    assert collector.symbols == {"opaque": "BTC"}
    request = json.loads(socket.send.await_args.args[0])
    assert request["params"]["channel"] == "instrument"


async def test_stream_acknowledgements_heartbeat_and_cancellation(monkeypatch):
    ready = asyncio.Event()
    messages = [
        json.dumps(
            {
                "method": "subscribe",
                "req_id": 2,
                "success": True,
                "result": {"channel": "trade", "symbol": "BTC/USD"},
            }
        ),
        json.dumps({"channel": "trade", "type": "snapshot", "data": [wire()]}),
    ]

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            if messages:
                return messages.pop(0)
            ready.set()
            await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.spot_trades.connect", lambda *a, **k: Socket())
    monkeypatch.setattr("selective_options.spot_trades.now_ms", lambda: 303000)
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink)

    async def discover(socket):
        collector.symbols = {"BTC/USD": "BTC"}

    monkeypatch.setattr(collector, "_discover", discover)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(ready.wait(), 2)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert any(call.args[0].kind == "trade" for call in sink.await_args_list)
