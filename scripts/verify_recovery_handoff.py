"""Verify startup handoff and duplicate identities using saved public evidence only."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.recovery_handoff import handoff_recovery
from selective_options.recovery_journal import load_recovery_evidence, publish_recovery
from selective_options.spot_trades import SpotTradeCollector, stored_trade
from selective_options.store import DuckStore


async def verify(database: Path, marker_id: str, output: Path) -> None:
    before = hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest()
    anchor, plan = await load_recovery_evidence(database, marker_id, now_ms())
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    destination = output / "events.duckdb"
    store = await DuckStore.open(destination)
    try:
        marker = await publish_recovery(store, anchor, plan)
        assert marker.event_id == marker_id
        rows = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", rows)
    finally:
        await store.close()
    emitted: list[MarketEvent] = []

    async def sink(event: MarketEvent) -> None:
        emitted.append(event)

    collector = SpotTradeCollector(AppConfig(), sink)
    loaded = await handoff_recovery(collector, destination, marker_id)
    assert loaded == plan
    sequence = collector.sequences[anchor.symbol]
    checked = 0
    for event in plan.raw:
        if event.source == "kraken" and event.payload()["trade_id"] in sequence.seen:
            assert not sequence.accept(stored_trade(event))
            checked += 1
    assert checked > 0 and sequence.last == plan.through_id
    assert not emitted and not collector.bars
    assert collector.awaiting_snapshot == {anchor.symbol}
    assert hashlib.sha256(await asyncio.to_thread(database.read_bytes)).hexdigest() == before
    report = {
        "build_version": __version__,
        "status": "SAVED_PUBLIC_HANDOFF_VERIFIED",
        "evidence_type": "saved public data replay; no network connection or new trades",
        "source_database_sha256": before,
        "marker_id": marker_id,
        "database_rows": rows,
        "selected_rows": len(plan.selected),
        "through_id": plan.through_id,
        "saved_websocket_duplicates_verified": checked,
        "original_records_and_receipt_times_preserved": True,
        "destination_commit_and_reload_verified": True,
        "runtime_awaiting_snapshot": sorted(collector.awaiting_snapshot),
        "bar_state_empty": True,
        "live_continuity_verified": False,
        "automatic_gap_fetch_enabled": False,
        "trading_enabled": False,
        "parquet_sha256": archive.sha256,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text,
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.database, args.marker, args.output))
