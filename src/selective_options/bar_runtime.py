"""Explicit stopped-capture bar save and already-verified seed installation.

Callers stop and drain every producer and writer before saving. This layer binds
the collector's prospective consumed-input ledger to the committed checkpoint;
it never guesses membership from arbitrary stored historical trades.
"""

from pathlib import Path

from .bar_checkpoint import (
    BarCheckpointResume,
    CheckpointBook,
    create_bar_checkpoint,
    validate_bar_resume,
)
from .bar_state import decode_bar_state, encode_bar_state, restore_bar_state
from .errors import IntegrityError
from .spot_trades import SpotTradeCollector, TradeSequence, stored_trade
from .store import DuckStore


async def save_collector_bar_checkpoint(
    collector: SpotTradeCollector, store: DuckStore, output: Path, as_of_ms: int
) -> dict[str, object]:
    """Save only after stopped producers and a successful durable writer drain."""
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
    books = []
    for symbol, builder in sorted(collector.bars.items()):
        tail = collector.last_events[symbol]
        if (
            tail.symbol != symbol
            or collector.symbols.get(symbol) != tail.asset
            or collector.sequences[symbol].last != stored_trade(tail).trade_id
        ):
            raise IntegrityError("BAR_CHECKPOINT_SEQUENCE_TAIL_MISMATCH")
        books.append(
            CheckpointBook(
                state_blob=encode_bar_state(builder, as_of_ms),
                input_events=collector.bar_inputs[symbol],
                tail_event=tail,
            )
        )
    return await create_bar_checkpoint(
        store,
        output,
        tuple(books),
        as_of_ms,
        max_events=collector.bar_input_limit,
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
