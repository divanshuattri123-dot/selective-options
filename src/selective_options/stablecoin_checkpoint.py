"""Explicit read-only restart anchors from health-bound stablecoin poll revisions."""

from pathlib import Path

import duckdb
from pydantic import ValidationError

from .errors import IntegrityError
from .events import MarketEvent
from .stablecoin_supply import SOURCE, SYMBOL, stablecoin_supply_quality
from .store import complete_thread


def _load_bound_pair(
    database: Path, as_of_ms: int, max_rows: int, max_poll_age_ms: int, max_block_age_ms: int
) -> tuple[MarketEvent, MarketEvent]:
    # Validate policy before opening a database; neither path creation nor mutation is allowed.
    stablecoin_supply_quality(
        [],
        as_of_ms=as_of_ms,
        max_rows=max_rows,
        max_poll_age_ms=max_poll_age_ms,
        max_block_age_ms=max_block_age_ms,
    )
    if not database.is_file():
        raise FileNotFoundError("stablecoin checkpoint database does not exist")
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events WHERE source=? AND environment='public_reference' "
            "AND asset='GLOBAL' AND symbol=? AND kind IN ('on_chain','feed_health') "
            "AND event_time_ms<=? AND observed_at_ms<=? "
            "ORDER BY observed_at_ms, event_time_ms, event_id LIMIT ?",
            [SOURCE, SYMBOL, as_of_ms, as_of_ms, max_rows + 1],
        )
        values = cursor.fetchall()
        if len(values) > max_rows:
            raise IntegrityError("STABLECOIN_CHECKPOINT_OVER_BUDGET")
        columns = [entry[0] for entry in cursor.description]
        try:
            rows = [
                MarketEvent.model_validate(dict(zip(columns, value, strict=True)))
                for value in values
            ]
        except ValidationError as exc:
            raise IntegrityError("STABLECOIN_CHECKPOINT_INVALID_ROW") from exc
    quality = stablecoin_supply_quality(
        rows,
        as_of_ms=as_of_ms,
        max_rows=max_rows,
        max_poll_age_ms=max_poll_age_ms,
        max_block_age_ms=max_block_age_ms,
    )
    if quality["state"] != "POLL_OBSERVED_VERIFIED":
        raise IntegrityError(str(quality["blocker"]))
    selected = [row for row in rows if row.event_id == quality["accepted_event_id"]]
    if len(selected) != 1:
        raise IntegrityError("STABLECOIN_CHECKPOINT_REFERENCE_AMBIGUOUS")
    health = [row for row in rows if row.event_id == quality["health_event_id"]]
    if len(health) != 1:
        raise IntegrityError("STABLECOIN_CHECKPOINT_HEALTH_AMBIGUOUS")
    return selected[0], health[0]


def _load(database: Path, as_of_ms: int, max_rows: int) -> MarketEvent:
    return _load_bound_pair(database, as_of_ms, max_rows, 1_200_000, 1_800_000)[0]


async def load_stablecoin_checkpoint(
    database: Path, as_of_ms: int, max_rows: int = 10_000
) -> MarketEvent:
    """Select the latest eligible healthy bound revision; drain and close on cancellation.

    Both clocks and the fixed pair scope are filtered before the overflow sentinel. All
    eligible history within the budget is checked; older healthy rows never conceal a
    current failure. Default quality freshness applies at the caller's explicit time.
    This bounds returned rows, not storage-engine scans, execution time or memory.
    """
    return await complete_thread(_load, database, as_of_ms, max_rows)


async def load_stablecoin_resume(
    database: Path,
    as_of_ms: int,
    max_rows: int = 10_000,
    *,
    max_poll_age_ms: int = 1_200_000,
    max_block_age_ms: int = 1_800_000,
) -> tuple[MarketEvent, MarketEvent]:
    """Read one consistent audited anchor and its exact successful health for explicit copy.

    Both records retain their original receipts. All eligible source history is validated
    before selection, including failures. Copying this pair is a minimal restart anchor,
    not complete history, current transport health or delivery acknowledgement recovery.
    """
    return await complete_thread(
        _load_bound_pair, database, as_of_ms, max_rows, max_poll_age_ms, max_block_age_ms
    )
