import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from selective_options.errors import IntegrityError
from selective_options.spot_trades import KrakenTrade, trade_event
from selective_options.trade_backfill import RestTrade
from selective_options.trade_reconciliation import (
    RecoveryBuffer,
    TradeIdentity,
    reconcile_while_receiving,
)


def ws(identifier=3, observed=500000, timestamp=None, **changes):
    trade = KrakenTrade(
        symbol="BTC/USD",
        side="buy",
        price=100,
        qty=0.1,
        trade_id=identifier,
        timestamp=timestamp or datetime.fromtimestamp(300 + identifier, UTC),
    )
    return trade_event(trade.model_copy(update=changes), "BTC", observed)


def rest(identifier=4, observed=600000, **changes):
    trade = RestTrade.model_validate(
        dict(
            price="100.00000",
            qty="0.10000000",
            timestamp=str(300 + identifier),
            side="buy",
            trade_id=identifier,
        )
        | changes
    )
    return trade.event(ws(), observed)


def test_cross_transport_plan_keeps_earliest_receipt_and_all_raw_rows():
    buffer = RecoveryBuffer(ws(), 5)
    earliest = ws(4, observed=550000)
    later = rest(4, timestamp="304.0000004")
    fifth = rest(5)
    for row in (earliest, later, fifth, ws(5, observed=610000)):
        buffer.add(row)
    plan = buffer.finish(620000)
    assert plan.selected == (earliest, fifth)
    assert len(plan.raw) == 4 and plan.through_id == 5
    assert [row.observed_at_ms for row in plan.selected] == [550000, 600000]
    with pytest.raises(IntegrityError, match="NOT_YET_VERIFIED"):
        plan.ready_rows(619999)
    assert plan.ready_rows(620000) == plan.selected
    with pytest.raises(IntegrityError, match="UNAVAILABLE"):
        buffer.add(ws(6))
    with pytest.raises(IntegrityError, match="UNAVAILABLE"):
        buffer.finish(630000)


@pytest.mark.parametrize(
    "change", [{"price": "101"}, {"qty": "0.2"}, {"side": "sell"}, {"timestamp": "304.000002"}]
)
def test_conflicts_poison_attempt(change):
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(ws(4, observed=550000))
    with pytest.raises(IntegrityError, match="CONFLICTING"):
        buffer.add(rest(4, **change))
    with pytest.raises(IntegrityError, match="UNAVAILABLE"):
        buffer.finish(650000)


def test_timestamp_tolerance_does_not_chain_through_another_transport():
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(ws(4, observed=550000))
    buffer.add(rest(4, timestamp="304.0000009"))
    with pytest.raises(IntegrityError, match="CONFLICTING"):
        buffer.add(ws(4, observed=610000, timestamp=datetime.fromtimestamp(304.000001, UTC)))


@pytest.mark.parametrize("max_events,max_bytes", [(1, 100000), (100, 1)])
def test_event_and_byte_limits_even_for_duplicates(max_events, max_bytes):
    buffer = RecoveryBuffer(ws(), 4, max_events=max_events, max_bytes=max_bytes)
    with pytest.raises(IntegrityError, match="OVERFLOW"):
        buffer.add(ws(4))
        buffer.add(ws(4))
    assert buffer.failed


@pytest.mark.parametrize("identifiers", [(4, 6), (4,), ()])
def test_missing_tail_or_middle_cannot_release(identifiers):
    buffer = RecoveryBuffer(ws(), 5)
    for identifier in identifiers:
        buffer.add(rest(identifier))
    with pytest.raises(IntegrityError, match="INCOMPLETE"):
        buffer.finish(650000)
    assert buffer.failed


def test_buffered_live_tail_must_also_be_contiguous():
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(rest(4))
    buffer.add(ws(6, observed=610000))
    with pytest.raises(IntegrityError, match="INCOMPLETE"):
        buffer.finish(650000)


def test_later_live_updates_extend_verified_high_water_mark():
    buffer = RecoveryBuffer(ws(), 4)
    for identifier in (6, 4, 5):
        buffer.add(ws(identifier, observed=610000))
    assert buffer.finish(620000).through_id == 6


def test_old_prefix_is_retained_but_not_claimed_as_recovered():
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(ws(2, observed=600000))
    buffer.add(rest(3))
    buffer.add(rest(4))
    plan = buffer.finish(650000)
    assert len(plan.raw) == 3 and [r.payload()["trade_id"] for r in plan.selected] == [4]


@pytest.mark.parametrize(
    "event,error",
    [
        (ws(4).model_copy(update={"asset": "ETH"}), "IDENTITY_MISMATCH"),
        (ws(4).model_copy(update={"event_id": "changed"}), "IDENTITY_MISMATCH"),
        (ws(4).model_copy(update={"environment": "testnet"}), "INVALID_EVENT"),
        (ws(4).model_copy(update={"source": "other"}), "UNSUPPORTED_SOURCE"),
        (ws(4, observed=499999), "OBSERVATION_BEFORE"),
        (ws(4, observed=500000, timestamp=datetime.fromtimestamp(999999, UTC)), "CLOCK_SKEW"),
    ],
)
def test_invalid_provenance_and_clocks_fail(event, error):
    buffer = RecoveryBuffer(ws(), 4)
    with pytest.raises(IntegrityError, match=error):
        buffer.add(event)


def test_valid_other_book_and_conflicting_anchor_rejected():
    buffer = RecoveryBuffer(ws(), 4)
    other = trade_event(
        KrakenTrade(
            symbol="ETH/USD",
            side="buy",
            price=100,
            qty=0.1,
            trade_id=4,
            timestamp=datetime.fromtimestamp(304, UTC),
        ),
        "ETH",
        600000,
    )
    with pytest.raises(IntegrityError, match="BOOK_MISMATCH"):
        buffer.add(other)
    buffer = RecoveryBuffer(ws(), 4)
    with pytest.raises(IntegrityError, match="ANCHOR_CONFLICT"):
        buffer.add(rest(3, price="999"))


def test_verification_cannot_be_backdated_and_selection_is_order_independent():
    first = RecoveryBuffer(ws(), 4)
    first.add(rest(4))
    with pytest.raises(IntegrityError, match="BACKDATED"):
        first.finish(599999)
    a, b = RecoveryBuffer(ws(), 4), RecoveryBuffer(ws(), 4)
    rows = (ws(4, observed=600000), rest(4))
    for row in rows:
        a.add(row)
    for row in reversed(rows):
        b.add(row)
    assert a.finish(650000).selected == b.finish(650000).selected


@pytest.mark.parametrize(
    "params",
    [
        {"through_id": 3},
        {"through_id": 4, "max_events": 0},
        {"through_id": 4, "max_bytes": 0},
        {"through_id": 4, "max_skew_ms": -1},
    ],
)
def test_invalid_budgets(params):
    with pytest.raises(ValueError):
        RecoveryBuffer(ws(), **params)


async def test_receiver_runs_during_fetch_and_handoff_preserves_receipt():
    arrived = asyncio.Event()
    receiver_stopped = asyncio.Event()
    count = 0

    async def receive():
        nonlocal count
        count += 1
        if count == 1:
            arrived.set()
            return ws(5, observed=550000)
        try:
            await asyncio.Event().wait()
        finally:
            receiver_stopped.set()

    async def recover():
        await arrived.wait()
        return [rest(4), rest(5)]

    plan = await reconcile_while_receiving(
        RecoveryBuffer(ws(), 5), receive, recover, clock=lambda: 620000
    )
    assert receiver_stopped.is_set()
    assert [r.observed_at_ms for r in plan.selected] == [600000, 550000]
    assert plan.verified_at_ms == 620000


async def test_overflow_cancels_inflight_recovery():
    stopped = asyncio.Event()

    async def receive():
        return ws(4, observed=550000)

    async def recover():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    buffer = RecoveryBuffer(ws(), 4, max_events=2)
    with pytest.raises(IntegrityError, match="OVERFLOW"):
        await reconcile_while_receiving(buffer, receive, recover)
    assert stopped.is_set() and buffer.failed


@pytest.mark.parametrize("failure", ["receive", "fetch", "deadline", "cancel"])
async def test_all_exit_paths_clean_up_tasks(failure):
    received = asyncio.Event()
    fetched = asyncio.Event()

    async def receive():
        try:
            if failure == "receive":
                raise OSError("lost transport")
            await asyncio.Event().wait()
        finally:
            received.set()

    async def recover():
        try:
            if failure == "fetch":
                raise IntegrityError("failed coverage")
            await asyncio.Event().wait()
        finally:
            fetched.set()

    buffer = RecoveryBuffer(ws(), 4)
    task = asyncio.create_task(reconcile_while_receiving(buffer, receive, recover, timeout_s=0.02))
    if failure == "cancel":
        await asyncio.sleep(0.001)
        task.cancel()
    with pytest.raises((OSError, IntegrityError, TimeoutError, asyncio.CancelledError)):
        await task
    assert received.is_set() and fetched.is_set() and buffer.failed


@pytest.mark.parametrize("deadline", [0, -1, float("nan"), float("inf")])
async def test_invalid_deadline_never_starts_tasks(deadline):
    async def forbidden():
        raise AssertionError("must not run")

    with pytest.raises(ValueError):
        await reconcile_while_receiving(RecoveryBuffer(ws(), 4), forbidden, forbidden, deadline)


def test_rest_decimal_precision_is_compared_without_float_rounding():
    assert TradeIdentity.read(rest(4, timestamp="304.000000001")).timestamp == Decimal(
        "304.000000001"
    )
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(rest(4, qty="0.10000000000000000001"))
    with pytest.raises(IntegrityError, match="CONFLICTING"):
        buffer.add(ws(4, observed=610000))


def test_allowed_clock_skew_still_cannot_leak_future_source_data():
    buffer = RecoveryBuffer(ws(), 4)
    buffer.add(ws(4, observed=600000, timestamp=datetime.fromtimestamp(601, UTC)))
    plan = buffer.finish(600000)
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        plan.ready_rows(600000)
    assert plan.ready_rows(601000)


async def test_async_recovery_waits_for_actual_clock_before_future_source_publication():
    loop = asyncio.get_running_loop()
    start = loop.time()

    def clock():
        return 600000 + int((loop.time() - start) * 1000)

    async def receive():
        await asyncio.Event().wait()

    async def recover():
        return [rest(4, observed=600000, timestamp="600.03")]

    result = await reconcile_while_receiving(RecoveryBuffer(ws(), 4), receive, recover, clock=clock)
    assert result.verified_at_ms >= 600030
    assert result.ready_rows(result.verified_at_ms)[0].observed_at_ms == 600000


async def test_clock_wait_keeps_original_deadline_and_invalidates_failed_attempt():
    async def receive():
        await asyncio.Event().wait()

    async def recover():
        return [rest(4, observed=600000, timestamp="601")]

    buffer = RecoveryBuffer(ws(), 4)
    with pytest.raises(TimeoutError):
        await reconcile_while_receiving(
            buffer, receive, recover, timeout_s=0.02, clock=lambda: 600000
        )
    assert buffer.failed and not buffer.closed


def test_naive_websocket_timestamp_cannot_enter_recovery():
    event = ws(4, timestamp=datetime(2026, 1, 1))
    with pytest.raises(IntegrityError, match="WITHOUT_TIMEZONE"):
        TradeIdentity.read(event)
