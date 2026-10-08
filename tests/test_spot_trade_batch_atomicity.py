"""Synthetic batch validation and ambiguous publication checks; no network calls."""

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from selective_options import spot_trades as module
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError, TradeGap


def wire(identifier, timestamp=301000, symbol="BTC/USD", **changes):
    return {
        "symbol": symbol,
        "side": "buy",
        "price": 100.0,
        "qty": 0.1,
        "trade_id": identifier,
        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(),
        **changes,
    }


def message(*rows, kind="update"):
    return {"channel": "trade", "type": kind, "data": list(rows)}


def frontier(collector):
    return deepcopy(
        {
            "sequences": {
                symbol: vars(sequence) for symbol, sequence in collector.sequences.items()
            },
            "last_events": collector.last_events,
            "bars": {symbol: vars(builder) for symbol, builder in collector.bars.items()},
            "awaiting_snapshot": collector.awaiting_snapshot,
            "repairing": collector.repairing,
        }
    )


async def seeded(*, two=False):
    emitted = []

    async def sink(event):
        emitted.append(event)

    config = AppConfig.model_validate(
        {"ingestion": {"bar_timeframes_s": [300, 900], "bar_lateness_s": 0}}
    )
    collector = module.SpotTradeCollector(config, sink)
    collector.symbols = {"BTC/USD": "BTC", "ETH/USD": "ETH"} if two else {"BTC/USD": "BTC"}
    rows = [wire(1)]
    if two:
        rows.append(wire(1, symbol="ETH/USD"))
    await collector._trades(message(*rows, kind="snapshot"), 303000)
    emitted.clear()
    return collector, emitted


async def test_later_late_trade_cannot_mutate_or_emit_earlier_valid_trade():
    collector, emitted = await seeded()
    await collector._flush(903000)
    emitted.clear()
    before = frontier(collector)
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        await collector._trades(message(wire(2, 905000), wire(3, 902000)), 906000)
    assert emitted == [] and frontier(collector) == before
    assert collector._emission_failed is False


async def test_interleaved_symbols_reject_entire_batch_when_later_symbol_is_late():
    collector, emitted = await seeded(two=True)
    collector.bars["ETH/USD"].watermark_ms = 602000
    before = frontier(collector)
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        await collector._trades(
            message(wire(2, 605000), wire(2, 601000, symbol="ETH/USD"), wire(3, 605500)), 606000
        )
    assert emitted == [] and frontier(collector) == before


async def test_later_timeframe_failure_does_not_change_earlier_timeframe_accumulator():
    collector, emitted = await seeded()
    builder = collector.bars["BTC/USD"]
    builder.buckets[(900000, 0)].volume = None
    before = frontier(collector)
    with pytest.raises(TypeError):
        await collector._trades(message(wire(2, 302000)), 303000)
    assert emitted == [] and frontier(collector) == before
    assert builder.buckets[(300000, 300000)].count == 1


@pytest.mark.parametrize("case", ["conflict", "gap", "future", "unrequested", "timezone"])
async def test_all_symbol_sequence_and_clock_validation_precedes_any_emission(case):
    collector, emitted = await seeded(two=True)
    later = wire(2, 302000, symbol="ETH/USD")
    if case == "conflict":
        later = wire(1, symbol="ETH/USD", price=999)
    elif case == "gap":
        later = wire(3, 302000, symbol="ETH/USD")
    elif case == "future":
        later = wire(2, 400000, symbol="ETH/USD")
    elif case == "unrequested":
        later = wire(2, 302000, symbol="UNKNOWN")
    else:
        later = wire(2, symbol="ETH/USD")
        later["timestamp"] = "1970-01-01T00:05:02"
    before = frontier(collector)
    with pytest.raises(IntegrityError):
        await collector._trades(message(wire(2, 302000), later), 303000)
    assert emitted == [] and frontier(collector) == before


async def test_success_installs_all_candidates_only_after_every_sink_completion():
    collector, _ = await seeded(two=True)
    collector.awaiting_snapshot = {"BTC/USD", "ETH/USD"}
    before = frontier(collector)
    emitted = []

    async def sink(event):
        assert frontier(collector) == before
        emitted.append(event)
        await asyncio.sleep(0)
        assert frontier(collector) == before

    collector.sink = sink
    await collector._trades(
        message(wire(2, 302000, symbol="ETH/USD"), wire(2, 302000), kind="snapshot"), 303000
    )
    assert [event.kind for event in emitted] == ["trade", "trade", "status", "status"]
    assert all(sequence.last == 2 for sequence in collector.sequences.values())
    assert collector.awaiting_snapshot == set()
    assert all(
        bucket.count == 2
        for builder in collector.bars.values()
        for bucket in builder.buckets.values()
    )
    assert collector._emission_failed is False


async def test_historical_snapshot_raw_only_preserves_closed_watermark_and_status_order():
    collector, emitted = await seeded()
    await collector._flush(903000)
    emitted.clear()
    collector.awaiting_snapshot = {"BTC/USD"}
    prior_bars = deepcopy(vars(collector.bars["BTC/USD"]))
    await collector._trades(message(wire(2, 302000), kind="snapshot"), 604000)
    assert [event.kind for event in emitted] == ["trade", "status", "status"]
    assert [event.payload().get("reason") for event in emitted[1:]] == [
        "HISTORICAL_TRADE_RAW_ONLY",
        "TRADE_SEQUENCE_RECOVERED",
    ]
    assert vars(collector.bars["BTC/USD"]) == prior_bars
    assert collector.sequences["BTC/USD"].last == 2 and not collector.awaiting_snapshot


async def test_historical_prefix_status_is_not_published_when_later_builder_fails():
    collector, emitted = await seeded(two=True)
    collector.bars["BTC/USD"].watermark_ms = 600000
    collector.bars["ETH/USD"].identity = ("wrong", "public_reference", "ETH", "ETH/USD")
    collector.awaiting_snapshot = {"BTC/USD", "ETH/USD"}
    before = frontier(collector)
    with pytest.raises(IntegrityError, match="mismatch"):
        await collector._trades(
            message(wire(2, 302000), wire(2, 602000, symbol="ETH/USD"), kind="snapshot"), 603000
        )
    assert emitted == [] and frontier(collector) == before


@pytest.mark.parametrize("failure", [OSError, TimeoutError, RuntimeError, IntegrityError])
@pytest.mark.parametrize("fail_at", [0, 1, 3])
async def test_sink_failure_keeps_entire_frontier_and_stops_collector_after_possible_prefix(
    failure, fail_at
):
    collector, _ = await seeded(two=True)
    collector.awaiting_snapshot = {"BTC/USD", "ETH/USD"}
    before = frontier(collector)
    delivered = []
    problem = failure("synthetic sink failure")

    async def sink(event):
        delivered.append(event)
        if len(delivered) - 1 == fail_at:
            raise problem

    collector.sink = sink
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED") as raised:
        await collector._trades(
            message(wire(2, 302000), wire(2, 302000, symbol="ETH/USD"), kind="snapshot"), 303000
        )
    assert raised.value.__cause__ is problem
    assert len(delivered) == fail_at + 1
    assert frontier(collector) == before and collector._emission_failed is True
    with pytest.raises(IntegrityError, match="OUTCOME_UNKNOWN"):
        await collector._trades(message(wire(2, 302000)), 303000)
    with pytest.raises(IntegrityError, match="OUTCOME_UNKNOWN"):
        await collector._flush(903000)
    assert len(delivered) == fail_at + 1


@pytest.mark.parametrize("fail_at", [0, 1, 3])
async def test_cancel_during_trade_sink_keeps_frontier_and_ambiguous_prefix(fail_at):
    collector, _ = await seeded(two=True)
    collector.awaiting_snapshot = {"BTC/USD", "ETH/USD"}
    before = frontier(collector)
    delivered = []
    entered = asyncio.Event()

    async def sink(event):
        delivered.append(event)
        if len(delivered) - 1 == fail_at:
            entered.set()
            await asyncio.Event().wait()

    collector.sink = sink
    task = asyncio.create_task(
        collector._trades(
            message(wire(2, 302000), wire(2, 302000, symbol="ETH/USD"), kind="snapshot"), 303000
        )
    )
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(delivered) == fail_at + 1 and frontier(collector) == before
    assert collector._emission_failed is True


async def test_flush_later_symbol_backwards_clock_preserves_all_earlier_buckets():
    collector, emitted = await seeded(two=True)
    collector.bars["ETH/USD"].watermark_ms = 1000000
    before = frontier(collector)
    with pytest.raises(IntegrityError, match="backwards"):
        await collector._flush(903000)
    assert emitted == [] and frontier(collector) == before


async def test_flush_later_symbol_invalid_payload_is_validated_before_earlier_bar_emission():
    collector, emitted = await seeded(two=True)
    collector.bars["ETH/USD"].buckets[(900000, 0)].volume = Decimal("1e10000")
    before = frontier(collector)
    with pytest.raises(ValueError):
        await collector._flush(903000)
    assert emitted == [] and frontier(collector) == before


async def test_flush_revalidates_copy_updated_bad_event_before_any_sink(monkeypatch):
    collector, emitted = await seeded(two=True)
    original = module.TradeBars.close_ready

    def bad(builder, received):
        events = original(builder, received)
        if builder.identity[-1] == "ETH/USD":
            events[-1] = events[-1].model_copy(update={"event_time_ms": 999})
        return events

    monkeypatch.setattr(module.TradeBars, "close_ready", bad)
    before = frontier(collector)
    with pytest.raises(ValueError):
        await collector._flush(903000)
    assert emitted == [] and frontier(collector) == before


@pytest.mark.parametrize("suspension", ["awaiting_snapshot", "repairing"])
async def test_flush_skips_suspended_symbol_without_clock_or_bucket_changes(suspension):
    collector, emitted = await seeded(two=True)
    getattr(collector, suspension).add("ETH/USD")
    collector.bars["ETH/USD"].watermark_ms = 1000000
    original = collector.bars["ETH/USD"]
    before = deepcopy(vars(original))
    await collector._flush(903000)
    assert len(emitted) == 2 and all(event.symbol == "BTC/USD" for event in emitted)
    assert collector.bars["ETH/USD"] is original and vars(original) == before
    assert collector.bars["BTC/USD"].buckets == {}
    assert collector.bars["BTC/USD"].watermark_ms == 903000


@pytest.mark.parametrize("failure", [OSError, TimeoutError, RuntimeError])
@pytest.mark.parametrize("fail_at", [0, 2, 3])
async def test_flush_sink_failure_retains_all_multiframe_buckets_after_possible_delivery(
    failure, fail_at
):
    collector, _ = await seeded(two=True)
    before = frontier(collector)
    delivered = []

    async def sink(event):
        delivered.append(event)
        assert frontier(collector) == before
        if len(delivered) - 1 == fail_at:
            raise failure("synthetic partial flush")

    collector.sink = sink
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_FAILED"):
        await collector._flush(903000)
    assert len(delivered) == fail_at + 1 and frontier(collector) == before
    assert collector._emission_failed is True


@pytest.mark.parametrize("fail_at", [0, 2, 3])
async def test_flush_sink_cancellation_retains_all_multiframe_buckets(fail_at):
    collector, _ = await seeded(two=True)
    before = frontier(collector)
    entered = asyncio.Event()
    delivered = []

    async def sink(event):
        delivered.append(event)
        if len(delivered) - 1 == fail_at:
            entered.set()
            await asyncio.Event().wait()

    collector.sink = sink
    task = asyncio.create_task(collector._flush(903000))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert frontier(collector) == before and len(delivered) == fail_at + 1
    assert collector._emission_failed is True


async def test_no_output_flush_installs_watermark_only_after_successful_validation():
    collector, emitted = await seeded(two=True)
    before = frontier(collector)
    await collector._flush(304000)
    assert emitted == []
    for symbol, builder in collector.bars.items():
        assert builder.watermark_ms == 304000
        assert builder.buckets == before["bars"][symbol]["buckets"]


async def test_sequence_gap_remains_repairable_validation_failure_not_sink_poison():
    collector, emitted = await seeded()
    before = frontier(collector)
    with pytest.raises(TradeGap):
        await collector._trades(message(wire(3, 303000)), 304000)
    assert emitted == [] and frontier(collector) == before and collector._emission_failed is False
    await collector._trades(message(wire(2, 302000), wire(3, 303000)), 304000)
    assert collector.sequences["BTC/USD"].last == 3 and len(emitted) == 2
