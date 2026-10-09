"""Explicit stopped or mutation-gated bar save and verified seed installation.

The stopped API requires drained producers and writer; the active API owns the
collector mutation gate and FIFO writer barrier. Both bind the prospective
consumed-input ledger to committed rows, without guessing membership from stored
historical trades or certifying whole-source/crash recovery.
"""

from pathlib import Path

from .bar_checkpoint import (
    BarCheckpointResume,
    CheckpointBook,
    create_bar_checkpoint,
    validate_bar_resume,
)
from .bar_state import decode_bar_state, encode_bar_state, restore_bar_state
from .bus import EventBus
from .collectors import now_ms
from .errors import IntegrityError
from .spot_trades import SpotTradeCollector, TradeSequence, stored_trade
from .store import DuckStore


def _collector_ready(collector: SpotTradeCollector) -> None:
    if not collector.track_bar_inputs:
        raise IntegrityError("BAR_CHECKPOINT_TRACKING_DISABLED")
    if collector._emission_failed:
        raise IntegrityError("SPOT_EMISSION_OUTCOME_UNKNOWN")
    if (
        collector.repairing
        or collector.awaiting_snapshot
        or collector.pending_frames
        or collector.recovery_lock.locked()
    ):
        raise IntegrityError("BAR_CHECKPOINT_UNRESOLVED_RECOVERY")
    if not collector.bars or set(collector.bars) != set(collector.bar_inputs):
        raise IntegrityError("BAR_CHECKPOINT_MISSING_CONSUMPTION_LEDGER")
    if set(collector.sequences) != set(collector.bars) or set(collector.last_events) != set(
        collector.bars
    ):
        raise IntegrityError("BAR_CHECKPOINT_AMBIGUOUS_BOOK_FRONTIER")
    for symbol in collector.bars:
        tail = collector.last_events[symbol]
        if (
            tail.symbol != symbol
            or collector.symbols.get(symbol) != tail.asset
            or collector.sequences[symbol].last != stored_trade(tail).trade_id
        ):
            raise IntegrityError("BAR_CHECKPOINT_SEQUENCE_TAIL_MISMATCH")


def _collector_books(collector: SpotTradeCollector, as_of_ms: int) -> tuple[CheckpointBook, ...]:
    _collector_ready(collector)
    books = []
    for symbol, builder in sorted(collector.bars.items()):
        tail = collector.last_events[symbol]
        books.append(
            CheckpointBook(
                state_blob=encode_bar_state(builder, as_of_ms),
                input_events=collector.bar_inputs[symbol],
                tail_event=tail,
            )
        )
    return tuple(books)


async def save_collector_bar_checkpoint(
    collector: SpotTradeCollector, store: DuckStore, output: Path, as_of_ms: int
) -> dict[str, object]:
    """Save only after stopped producers and a successful durable writer drain."""
    return await create_bar_checkpoint(
        store,
        output,
        _collector_books(collector, as_of_ms),
        as_of_ms,
        max_events=collector.bar_input_limit,
    )


async def save_active_collector_bar_checkpoint(
    collector: SpotTradeCollector, bus: EventBus, store: DuckStore, output: Path
) -> dict[str, object]:
    """Freeze complete local batches, flush the writer, then save exact consumed inputs.

    Lock order is collector gate -> FIFO barrier -> store lock. This first bounded
    implementation holds the gate through immutable validation/publication too;
    it makes no producer throughput or lock-duration guarantee. The writer stays
    available, and the existing checkpoint worker drains before releasing the
    gate on cancellation. Feed health and whole-source/crash recovery remain unproven.
    """
    async with collector.bar_checkpoint_lock:
        # Recheck after acquisition; suspension/poison may have changed while
        # waiting for a complete producer emission and frontier installation.
        _collector_ready(collector)
        await bus.flush()
        cutoff = now_ms()
        books = _collector_books(collector, cutoff)
        return await create_bar_checkpoint(
            store, output, books, cutoff, max_events=collector.bar_input_limit
        )


def install_bar_resume(
    collector: SpotTradeCollector, resume: BarCheckpointResume, restart_ms: int
) -> None:
    """Atomically install AFTER capture verifies the closed copied seed database.

    Pure model validation here is not durable verification. Dynamic discovery and
    an actual recovery snapshot are still mandatory before accepting live updates.
    """
    if not collector.track_bar_inputs:
        raise IntegrityError("BAR_CHECKPOINT_TRACKING_DISABLED")
    if (
        collector.connections
        or collector.symbols
        or collector.sequences
        or collector.bars
        or collector.bar_inputs
        or collector.last_events
        or collector.checkpoint_assets
        or collector.awaiting_snapshot
        or collector.repairing
        or collector.pending_frames
        or collector.recovery_lock.locked()
        or collector.bar_checkpoint_lock.locked()
        or collector._emission_failed
    ):
        raise IntegrityError("BAR_CHECKPOINT_COLLECTOR_MUST_BE_FRESH")
    clean = validate_bar_resume(
        resume,
        restart_ms,
        collector.config.system.assets,
        collector.config.ingestion.bar_timeframes_s,
        int(collector.config.ingestion.bar_lateness_s * 1000),
        max_events=collector.bar_input_limit,
    )
    bars = {}
    sequences = {}
    inputs = {}
    tails = {}
    assets = {}
    for book in clean.books:
        state = decode_bar_state(book.state_blob)
        assert state.scope is not None
        symbol = state.scope.symbol
        if symbol in bars:
            raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_SYMBOL")
        bars[symbol] = restore_bar_state(
            book.state_blob,
            restart_ms,
            timeframes_s=collector.config.ingestion.bar_timeframes_s,
            lateness_ms=int(collector.config.ingestion.bar_lateness_s * 1000),
        )
        sequence = TradeSequence()
        sequence.accept(stored_trade(book.tail_event))
        sequences[symbol] = sequence
        inputs[symbol] = book.input_events
        tails[symbol] = book.tail_event
        assets[symbol] = state.scope.asset
    # No await or publication occurs while these validated frontiers are installed.
    collector.bars = bars
    collector.sequences = sequences
    collector.bar_inputs = inputs
    collector.last_events = tails
    collector.checkpoint_assets = assets
    collector.awaiting_snapshot = set(bars)
