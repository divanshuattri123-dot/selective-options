"""Bounded read-only committed Binance funding seeds; keep all observed revisions."""

from pathlib import Path

import duckdb
from pydantic import ValidationError

from .binance_funding_view import funding_history_at
from .errors import IntegrityError
from .events import MarketEvent
from .store import complete_thread


def _load(
    path: Path, asset: str, symbol: str, start_ms: int, end_ms: int, as_of_ms: int, max_rows: int
) -> tuple[MarketEvent, ...]:
    if not path.is_file():
        raise FileNotFoundError("funding seed requires an existing committed database")
    try:
        with duckdb.connect(str(path), read_only=True) as db:
            result = db.execute(
                "SELECT * FROM events WHERE source='binance_history' AND environment='public_reference' "
                "AND asset=? AND symbol=? AND kind='funding' AND event_time_ms BETWEEN ? AND ? "
                "AND event_time_ms<=? AND observed_at_ms<=? ORDER BY event_time_ms, observed_at_ms, event_id LIMIT ?",
                [asset, symbol, start_ms, end_ms, as_of_ms, as_of_ms, max_rows + 1],
            )
            columns = [c[0] for c in result.description]
            values = result.fetchall()
        if len(values) > max_rows:
            raise IntegrityError("FUNDING_SEED_OVER_BUDGET")
        rows = tuple(MarketEvent.model_validate(dict(zip(columns, r, strict=True))) for r in values)
        selected = funding_history_at(
            rows,
            asset=asset,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            as_of_ms=as_of_ms,
            max_rows=max_rows,
        )
        if not selected:
            raise IntegrityError("FUNDING_SEED_MISSING")
        return rows
    except (duckdb.Error, ValidationError) as exc:
        raise IntegrityError("FUNDING_SEED_SCHEMA_OR_ROW_INVALID") from exc


async def load_funding_seed(
    database: Path,
    *,
    asset: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    as_of_ms: int,
    max_rows: int = 10_000,
) -> tuple[MarketEvent, ...]:
    """Validate a closed committed source before creating output or fetching new data."""
    # Validate caller policy before touching the filesystem.
    funding_history_at(
        (),
        asset=asset,
        symbol=symbol,
        start_ms=start_ms,
        end_ms=end_ms,
        as_of_ms=as_of_ms,
        max_rows=max_rows,
    )
    return await complete_thread(
        _load, database, asset, symbol, start_ms, end_ms, as_of_ms, max_rows
    )
