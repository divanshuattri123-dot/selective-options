"""Suspend one book and activate only committed recovery after consumer delivery."""

from .bus import EventBus
from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent
from .recovery_handoff import RecoveredSequence
from .recovery_journal import load_committed_recovery
from .spot_trades import SpotTradeCollector
from .store import DuckStore
from .trade_reconciliation import RecoveryPlan, TradeIdentity


def _unchanged(collector: SpotTradeCollector, anchor: MarketEvent) -> None:
    sequence = collector.sequences.get(anchor.symbol)
    if (
        anchor.symbol not in collector.repairing
        or collector.last_events.get(anchor.symbol) != anchor
        or sequence is None
        or sequence.last != TradeIdentity.read(anchor).native_id
    ):
        raise IntegrityError("RECOVERY_ANCHOR_STATE_CHANGED")


async def begin_gap_repair(
    collector: SpotTradeCollector, bus: EventBus, store: DuckStore, symbol: str
) -> MarketEvent:
    async with collector.bar_checkpoint_lock:
        if symbol in collector.repairing:
            raise IntegrityError("SPOT_BOOK_RECOVERY_IN_PROGRESS")
        anchor = collector.last_events.get(symbol)
        if anchor is None or collector.symbols.get(symbol) != anchor.asset:
            raise IntegrityError("RECOVERY_NO_BOOK_ANCHOR")
        # Suspension precedes any repair I/O or publication. Waiting for an
        # existing checkpoint gate cannot mutate its frozen builder/ledger.
        collector.repairing.add(symbol)
        builder = collector.bars.get(symbol)
        if builder is not None:
            builder.mark_gap(now_ms())
        _unchanged(collector, anchor)
    await bus.flush()
    if await store.read_events([anchor.event_id]) != (anchor,):
        raise IntegrityError("RECOVERY_ANCHOR_NOT_COMMITTED_EXACTLY")
    _unchanged(collector, anchor)
    return anchor


async def activate_gap_repair(
    collector: SpotTradeCollector,
    bus: EventBus,
    store: DuckStore,
    anchor: MarketEvent,
    marker_id: str,
) -> RecoveryPlan:
    # Only one installation can cross an await at a time; a second attempt must
    # recheck suspension/anchor after acquiring the lock.
    async with collector.recovery_lock:
        _unchanged(collector, anchor)
        saved_anchor, plan = await load_committed_recovery(store, marker_id, now_ms())
        if saved_anchor != anchor:
            raise IntegrityError("RECOVERY_MARKER_ANCHOR_MISMATCH")
        sequence = RecoveredSequence(plan)
        _unchanged(collector, anchor)
        # Raw records are already durable/outboxed. Normal consumers receive the
        # same immutable rows; Redis remains at-least-once with event-ID dedup.
        # Prefix/anchor rows were processed before suspension and aren't replayed.
        rows = [row for row in plan.raw if TradeIdentity.read(row).native_id > plan.anchor_id]
        for row in sorted(rows, key=lambda event: (event.observed_at_ms, event.event_id)):
            await bus.publish(row)
        marker = await store.read_events([marker_id])
        if len(marker) != 1:
            raise IntegrityError("RECOVERY_MARKER_MISSING")
        await bus.publish(marker[0])
        await bus.flush()
        async with collector.bar_checkpoint_lock:
            _unchanged(collector, anchor)
            builder = collector.bars.get(anchor.symbol)
            if builder is not None:
                builder.mark_gap(now_ms())
            # No await during installation. Missing trades do not enter bar builders.
            collector.sequences[anchor.symbol] = sequence
            collector.last_events[anchor.symbol] = plan.selected[-1]
            collector.awaiting_snapshot.discard(anchor.symbol)
            collector.repairing.remove(anchor.symbol)
        return plan
