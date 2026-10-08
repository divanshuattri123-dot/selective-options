"""Independent batch-refusal checks through the actual Kraken collector."""

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError, TradeGap
from selective_options.spot_trades import SpotTradeCollector


def wire(symbol="BTC/USD", trade_id=1, timestamp=601000, **changes):
    return {
        "symbol": symbol,
        "side": "buy",
        "price": 100,
        "qty": 1,
        "trade_id": trade_id,
        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat()
        if isinstance(timestamp, int)
        else timestamp,
        **changes,
    }


def message(rows, kind="update"):
    return {"channel": "trade", "type": kind, "data": rows}


def state(collector):
    """Compare live values, including untouched symbols and all bar timeframes."""
    return {
        "sequences": {
            symbol: (sequence.last, tuple(sequence.seen.items()), sequence.capacity)
            for symbol, sequence in collector.sequences.items()
        },
        "last_events": dict(collector.last_events),
        "bars": {symbol: deepcopy(vars(builder)) for symbol, builder in collector.bars.items()},
        "awaiting_snapshot": set(collector.awaiting_snapshot),
        "repairing": set(collector.repairing),
        "symbols": dict(collector.symbols),
        "pending_frames": deepcopy(tuple(collector.pending_frames)),
    }


async def seeded(*, symbols=("BTC/USD", "ETH/USD")):
    emissions = []

    async def sink(event):
        emissions.append(event)

    collector = SpotTradeCollector(
        AppConfig(ingestion={"bar_timeframes_s": [300, 900], "bar_lateness_s": 0}), sink
    )
    collector.symbols = {"BTC/USD": "BTC", "ETH/USD": "ETH", "SOL/USD": "SOL"}
    await collector._trades(
        message(
            [wire(symbol, timestamp=601000 + index * 100) for index, symbol in enumerate(symbols)],
            "snapshot",
        ),
        602000,
    )
    await collector._flush(650000)
    emissions.clear()
    return collector, emissions


@pytest.mark.parametrize("layout", ["same_symbol", "interleaved_symbols", "new_symbol_prefix"])
async def test_later_late_update_emits_zero_and_preserves_every_live_frontier(layout):
    collector, emissions = await seeded()
    if layout == "same_symbol":
        rows = [wire(trade_id=2, timestamp=651000), wire(trade_id=3, timestamp=649000)]
    elif layout == "interleaved_symbols":
        rows = [
            wire(trade_id=2, timestamp=651000),
            wire("ETH/USD", 2, 651100),
            wire(trade_id=3, timestamp=649000),
        ]
    else:
        rows = [wire("SOL/USD", 1, 651000), wire("ETH/USD", 2, 649000)]
    before = state(collector)
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        await collector._trades(message(rows), 652000)
    assert emissions == []
    assert state(collector) == before
    # A validation refusal does not poison transport or consume the native IDs.
    await collector._trades(
        message([wire(trade_id=2, timestamp=651000), wire(trade_id=3, timestamp=651100)]), 652000
    )
    assert [event.payload()["trade_id"] for event in emissions] == [2, 3]
    assert collector.sequences["BTC/USD"].last == 3
    await collector._flush(900000)
    closed = [event for event in emissions if event.kind == "bar"]
    assert len(closed) == 4
    assert {event.payload()["trade_count"] for event in closed if event.asset == "BTC"} == {3}
    assert all(event.payload()["complete"] is False for event in closed)


@pytest.mark.parametrize(
    "later",
    [
        wire(trade_id=3, timestamp=658001),
        wire(trade_id=3, timestamp=0),
        wire(trade_id=3, timestamp=-1000),
        wire(trade_id=3, price=0),
        wire(trade_id=3, qty=-1),
        wire(trade_id=3, price=float("inf")),
        wire(trade_id=3, qty=float("nan")),
        wire(trade_id=3, side="unknown"),
        wire(trade_id=3, timestamp="2026-01-01T00:00:00"),
        wire(trade_id=3, symbol="UNREQUESTED"),
    ],
)
async def test_later_invalid_native_record_cannot_publish_or_mutate_valid_prefix(later):
    collector, emissions = await seeded()
    before = state(collector)
    with pytest.raises((IntegrityError, ValidationError)):
        await collector._trades(message([wire(trade_id=2, timestamp=651000), later]), 652000)
    assert emissions == []
    assert state(collector) == before


@pytest.mark.parametrize("later", ["gap", "conflict", "repairing", "awaiting_snapshot"])
async def test_later_sequence_or_recovery_refusal_keeps_interleaved_prefix_unaccepted(later):
    collector, emissions = await seeded()
    if later == "gap":
        second = wire("ETH/USD", 3, 651100)
    elif later == "conflict":
        second = wire("ETH/USD", 1, 601100, price=101)
    else:
        second = wire("ETH/USD", 2, 651100)
        (collector.repairing if later == "repairing" else collector.awaiting_snapshot).add(
            "ETH/USD"
        )
    before = state(collector)
    with pytest.raises((IntegrityError, TradeGap)):
        await collector._trades(message([wire(trade_id=2, timestamp=651000), second]), 652000)
    assert emissions == []
    assert state(collector) == before


async def test_historical_snapshot_raw_prefix_and_later_bar_scope_error_emit_nothing():
    collector, emissions = await seeded()
    collector.awaiting_snapshot.update({"BTC/USD", "ETH/USD"})
    collector.bars["ETH/USD"].identity = ("other", "public_reference", "ETH", "ETH/USD")
    before = state(collector)
    rows = [wire(trade_id=2, timestamp=649000), wire("ETH/USD", 2, 651100)]
    with pytest.raises(IntegrityError, match="mismatch"):
        await collector._trades(message(rows, "snapshot"), 652000)
    assert emissions == []  # Neither historical raw trade nor its health/status prefix escaped.
    assert state(collector) == before


async def test_recovery_overlap_then_late_later_update_preserves_committed_raw_only_state():
    collector, emissions = await seeded()
    collector.awaiting_snapshot.add("BTC/USD")
    await collector._trades(
        message([wire(), wire(trade_id=2, timestamp=649000)], "snapshot"), 652000
    )
    assert [event.payload()["trade_id"] for event in emissions if event.kind == "trade"] == [2]
    assert any(event.payload().get("reason") == "HISTORICAL_TRADE_RAW_ONLY" for event in emissions)
    emissions.clear()
    before = state(collector)
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        await collector._trades(
            message([wire(trade_id=3, timestamp=651000), wire(trade_id=4, timestamp=649500)]),
            652000,
        )
    assert emissions == []
    assert state(collector) == before
    assert collector.last_events["BTC/USD"].payload()["trade_id"] == 2


async def test_partial_recovery_snapshot_deduplicates_overlap_and_keeps_other_book_suspended():
    collector, emissions = await seeded()
    collector.awaiting_snapshot.update({"BTC/USD", "ETH/USD"})
    eth_before = deepcopy(vars(collector.bars["ETH/USD"]))
    btc_before = deepcopy(vars(collector.bars["BTC/USD"]))
    # Unobserved older prefix is skipped; observed overlap is deduplicated; only
    # newly observed historical ID 2 is emitted, preserving its actual receipt.
    await collector._trades(
        message(
            [wire(trade_id=2, timestamp=649000), wire(trade_id=0, timestamp=500000), wire()],
            "snapshot",
        ),
        652000,
    )
    trades = [event for event in emissions if event.kind == "trade"]
    assert len(trades) == 1 and trades[0].payload()["trade_id"] == 2
    assert trades[0].event_time_ms == 649000 and trades[0].observed_at_ms == 652000
    assert vars(collector.bars["BTC/USD"]) == btc_before
    assert vars(collector.bars["ETH/USD"]) == eth_before
    assert collector.awaiting_snapshot == {"ETH/USD"}
    reasons = [event.payload().get("reason") for event in emissions if event.kind == "status"]
    assert reasons == ["HISTORICAL_TRADE_RAW_ONLY", "TRADE_SEQUENCE_RECOVERED"]
    emissions.clear()
    await collector._flush(900000)
    assert {event.symbol for event in emissions} == {"BTC/USD"}
    assert len(emissions) == 2 and all(event.payload()["trade_count"] == 1 for event in emissions)
    assert vars(collector.bars["ETH/USD"]) == eth_before


async def test_invalid_second_book_snapshot_cannot_clear_first_recovery_scope():
    collector, emissions = await seeded()
    collector.awaiting_snapshot.update({"BTC/USD", "ETH/USD"})
    before = state(collector)
    with pytest.raises(TradeGap):
        await collector._trades(
            message(
                [wire(), wire(trade_id=2, timestamp=651000), wire("ETH/USD", 3, 651100)], "snapshot"
            ),
            652000,
        )
    assert emissions == []
    assert state(collector) == before


@pytest.mark.parametrize("invalid", ["backward_watermark", "ohlc", "volume", "count", "interval"])
async def test_later_symbol_flush_validation_emits_no_earlier_bars_or_watermark_mutation(invalid):
    collector, emissions = await seeded()
    builder = collector.bars["ETH/USD"]
    bar = builder.buckets[(300000, 600000)]
    if invalid == "backward_watermark":
        builder.watermark_ms = 900001
    elif invalid == "ohlc":
        bar.high = 99
    elif invalid == "volume":
        bar.volume = Decimal(-1)
    elif invalid == "count":
        bar.count = 0
    else:
        bar.opening_ms = 599999
    before = state(collector)
    with pytest.raises((IntegrityError, ValidationError)):
        await collector._flush(900000)
    assert emissions == []
    assert state(collector) == before


@pytest.mark.parametrize("corrupt_suspended", [False, True])
async def test_flush_skips_awaiting_and_repairing_books_without_touching_their_buckets(
    corrupt_suspended,
):
    collector, emissions = await seeded(symbols=("BTC/USD", "ETH/USD", "SOL/USD"))
    collector.awaiting_snapshot.add("ETH/USD")
    collector.repairing.add("SOL/USD")
    if corrupt_suspended:
        collector.bars["ETH/USD"].buckets[(300000, 600000)].high = 99
        collector.bars["SOL/USD"].watermark_ms = 900001
    before = state(collector)
    await collector._flush(900000)
    after = state(collector)
    assert len(emissions) == 2 and {event.symbol for event in emissions} == {"BTC/USD"}
    assert after["bars"]["ETH/USD"] == before["bars"]["ETH/USD"]
    assert after["bars"]["SOL/USD"] == before["bars"]["SOL/USD"]
    assert after["sequences"] == before["sequences"]
    assert after["last_events"] == before["last_events"]
    assert after["awaiting_snapshot"] == before["awaiting_snapshot"]
    assert after["repairing"] == before["repairing"]
    assert collector.bars["BTC/USD"].watermark_ms == 900000
    assert collector.bars["BTC/USD"].buckets == {}


async def test_duplicate_only_snapshot_preserves_bars_and_reports_only_mentioned_book_recovery():
    collector, emissions = await seeded()
    collector.awaiting_snapshot.update({"BTC/USD", "ETH/USD"})
    before = state(collector)
    await collector._trades(message([wire()], "snapshot"), 652000)
    assert len(emissions) == 1 and emissions[0].payload()["reason"] == "TRADE_SEQUENCE_RECOVERED"
    assert emissions[0].payload()["recovered_trades"] == 0
    after = state(collector)
    assert after["bars"] == before["bars"]
    assert after["sequences"] == before["sequences"]
    assert after["last_events"] == before["last_events"]
    assert collector.awaiting_snapshot == {"ETH/USD"}
