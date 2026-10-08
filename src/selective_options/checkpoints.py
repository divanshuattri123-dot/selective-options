"""Recover bounded Kraken sequence tails from committed events, never a ahead-of-data cursor."""

from pathlib import Path

import duckdb

from .errors import IntegrityError
from .events import MarketEvent
from .spot_trades import TradeSequence, stored_trade, trade_event
from .store import complete_thread


def _load(path: Path, assets: tuple[str, ...]) -> tuple[MarketEvent, ...]:
    if not path.is_file():
        raise FileNotFoundError("checkpoint database does not exist")
    # Read-only prevents migration or mutation of the prior capture's evidence.
    connection = duckdb.connect(str(path), read_only=True)
    try:
        cursor = connection.execute(
            """SELECT * EXCLUDE (sequence_rank) FROM (
                SELECT *, dense_rank() OVER (
                    PARTITION BY symbol ORDER BY CAST(json_extract_string(payload_json, '$.trade_id') AS BIGINT) DESC
                ) AS sequence_rank FROM events
                WHERE source='kraken' AND environment='public_reference' AND kind='trade'
                AND asset IN (SELECT unnest(?))
            ) WHERE sequence_rank<=50
            ORDER BY symbol, CAST(json_extract_string(payload_json, '$.trade_id') AS BIGINT)""",
            [list(assets)],
        )
        columns = [item[0] for item in cursor.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        )
    finally:
        connection.close()
    if not rows:
        raise IntegrityError("NO_KRAKEN_TRADE_CHECKPOINTS")
    sequences: dict[str, TradeSequence] = {}
    identities: dict[str, str] = {}
    for event in rows:
        trade = stored_trade(event)
        if (
            trade.timestamp.tzinfo is None
            or trade_event(trade, event.asset, event.observed_at_ms) != event
        ):
            raise IntegrityError("CHECKPOINT_EVENT_IDENTITY_MISMATCH")
        if identities.setdefault(event.symbol, event.asset) != event.asset:
            raise IntegrityError("CHECKPOINT_ASSET_CONFLICT")
        if not sequences.setdefault(event.symbol, TradeSequence()).accept(trade):
            raise IntegrityError("DUPLICATE_CHECKPOINT_TRADE")
    return rows


async def load_trade_checkpoints(path: Path, assets: tuple[str, ...]) -> tuple[MarketEvent, ...]:
    """Up to 50 contiguous trades per book, with original first-observation times."""
    return await complete_thread(_load, path, assets)
