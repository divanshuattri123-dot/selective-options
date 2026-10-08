"""Replay saved public REST and WebSocket evidence through the asynchronous recovery buffer."""

import argparse
import asyncio
import json
from pathlib import Path

import duckdb

from selective_options.archive import restore_archive, seal_archive
from selective_options.checkpoints import load_trade_checkpoints
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.recovery_journal import load_recovery, publish_recovery
from selective_options.store import DuckStore, complete_thread
from selective_options.trade_reconciliation import (
    RecoveryBuffer,
    TradeIdentity,
    reconcile_while_receiving,
)


def read(path: Path, symbol: str, source: str, after: int) -> list[MarketEvent]:
    with duckdb.connect(str(path), read_only=True) as connection:
        cursor = connection.execute(
            """SELECT * FROM events WHERE source=? AND symbol=?
            AND kind='trade' AND environment='public_reference'
            AND CAST(json_extract_string(payload_json, '$.trade_id') AS BIGINT)>?
            ORDER BY observed_at_ms, event_id LIMIT 20001""",
            [source, symbol, after],
        )
        columns = [row[0] for row in cursor.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        ]
    if len(rows) > 20000:
        raise ValueError("evidence exceeds replay budget")
    return rows


async def verify(
    checkpoint: Path,
    rest_database: Path,
    websocket_database: Path,
    symbol: str,
    output: Path,
    durable: bool = False,
) -> None:
    seeds = await load_trade_checkpoints(checkpoint, AppConfig().system.assets)
    anchor = [row for row in seeds if row.symbol == symbol][-1]
    after = TradeIdentity.read(anchor).native_id
    rest = await complete_thread(read, rest_database, symbol, "kraken_rest", after)
    websocket = await complete_thread(read, websocket_database, symbol, "kraken", after)
    if not rest:
        raise ValueError("REST evidence is required")
    through = max(TradeIdentity.read(row).native_id for row in rest)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    queued = iter(websocket)
    received = asyncio.Event()

    async def receive() -> MarketEvent:
        row = next(queued, None)
        if row is not None:
            return row
        received.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def fetch() -> list[MarketEvent]:
        await received.wait()
        return rest

    plan = await reconcile_while_receiving(RecoveryBuffer(anchor, through), receive, fetch)
    originals = {row.event_id: row for row in rest + websocket}
    assert all(originals[row.event_id] == row for row in plan.selected)
    distribution: dict[str, int] = {}
    for row in plan.selected:
        distribution[row.source] = distribution.get(row.source, 0) + 1
    store = await DuckStore.open(output / "events.duckdb")
    marker: MarketEvent | None = None
    try:
        if durable:
            marker = await publish_recovery(store, anchor, plan)
        else:
            await store.append(list(plan.raw))
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
    finally:
        await store.close()
    if marker is not None:
        loaded = await load_recovery(output / "events.duckdb", marker.event_id, plan.verified_at_ms)
        assert loaded.selected == plan.selected
        await restore_archive(output / "events.parquet", output / "restored")
        restored = await load_recovery(
            output / "restored/events.duckdb", marker.event_id, plan.verified_at_ms
        )
        assert restored == loaded
    report = {
        "status": "REPLAY_VERIFIED",
        "evidence_type": "saved real public data replay; no new network capture",
        "symbol": symbol,
        "anchor_id": plan.anchor_id,
        "through_id": plan.through_id,
        "verified_at_ms": plan.verified_at_ms,
        "raw_rows": len(plan.raw),
        "database_rows": count,
        "durable_marker_id": marker.event_id if marker else None,
        "restart_and_archive_restore_verified": marker is not None,
        "selected_rows": len(plan.selected),
        "selected_sources": distribution,
        "selected_event_ids": [row.event_id for row in plan.selected],
        "original_receipt_times_preserved": True,
        "parquet_sha256": manifest.sha256,
        "capture_integration_enabled": False,
        "trading_enabled": False,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "selected_event_ids"}, indent=2
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint-database", type=Path, required=True)
    parser.add_argument("--rest-database", type=Path, required=True)
    parser.add_argument("--websocket-database", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--durable",
        action="store_true",
        help="Atomically publish, reopen, and archive-restore the verified plan",
    )
    args = parser.parse_args()
    asyncio.run(
        verify(
            args.checkpoint_database,
            args.rest_database,
            args.websocket_database,
            args.symbol,
            args.output,
            args.durable,
        )
    )
