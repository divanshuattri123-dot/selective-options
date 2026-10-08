"""Offline public-evidence replay through collector suspension and runtime activation."""

import argparse
import asyncio
import json
from pathlib import Path

from pydantic import JsonValue

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.bus import EventBus, HotBuffer
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import TradeGap
from selective_options.events import MarketEvent
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.recovery_journal import load_recovery_evidence, publish_recovery
from selective_options.recovery_runtime import activate_gap_repair, begin_gap_repair
from selective_options.spot_trades import SpotTradeCollector, stored_trade
from selective_options.store import DuckStore
from selective_options.trade_reconciliation import TradeIdentity


def message(event: MarketEvent, kind: str) -> dict[str, JsonValue]:
    return {"channel": "trade", "type": kind, "data": [stored_trade(event).model_dump(mode="json")]}


async def verify(database: Path, marker_id: str, output: Path) -> None:
    anchor, saved = await load_recovery_evidence(database, marker_id, now_ms())
    # Intentionally omit an earlier saved trade to exercise the real gap check.
    trigger = next(
        row
        for row in saved.raw
        if row.source == "kraken" and TradeIdentity.read(row).native_id > saved.anchor_id + 1
    )
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig.model_validate({"ingestion": {"flush_interval_s": 0.01}})
    bus, hot = EventBus(1000, 10), HotBuffer(1000)
    store = await DuckStore.open(output / "events.duckdb")
    stopped = asyncio.Event()
    writer = asyncio.create_task(
        consume(config, bus, store, hot, FreshnessMonitor(5000), stopped, None)
    )
    collector = SpotTradeCollector(config, bus.publish)
    collector.symbols = {anchor.symbol: anchor.asset}
    try:
        await collector._trades(message(anchor, "snapshot"), anchor.observed_at_ms)
        try:
            await collector._trades(message(trigger, "update"), trigger.observed_at_ms)
        except TradeGap as gap:
            assert gap.symbol == anchor.symbol and gap.expected_id == saved.anchor_id + 1
        else:
            raise AssertionError("saved discontinuity was not detected")
        committed_anchor = await begin_gap_repair(collector, bus, store, anchor.symbol)
        assert committed_anchor == anchor
        assert collector.sequences[anchor.symbol].last == saved.anchor_id
        marker = await publish_recovery(store, anchor, saved)
        installed = await activate_gap_repair(collector, bus, store, anchor, marker.event_id)
        assert installed == saved
        assert collector.sequences[anchor.symbol].last == saved.through_id
        assert not collector.repairing
        assert all(
            not bar.complete and bar.count == 1
            for bar in collector.bars[anchor.symbol].buckets.values()
        )
        # Saved overlap must be recognized without publishing/recounting it.
        await collector._trades(message(trigger, "update"), trigger.observed_at_ms)
        await bus.flush()
        assert collector.last_events[anchor.symbol] == saved.selected[-1]
        delivered = (
            *hot.recent("kraken", anchor.asset, "trade"),
            *hot.recent("kraken_rest", anchor.asset, "trade"),
        )
        assert all(row in delivered for row in saved.raw)
        assert await store.read_events([anchor.event_id]) == (anchor,)
    except BaseException:
        stopped.set()
        await asyncio.gather(writer, return_exceptions=True)
        await store.close()
        raise
    else:
        stopped.set()
        try:
            await writer
        except BaseException:
            await store.close()
            raise
    try:
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        report = {
            "build_version": __version__,
            "status": "OFFLINE_RUNTIME_ACTIVATION_VERIFIED",
            "evidence_type": "saved public events with intentional omission; no live network or Redis server",
            "anchor_id": saved.anchor_id,
            "through_id": saved.through_id,
            "selected_rows": len(saved.selected),
            "raw_rows": len(saved.raw),
            "database_rows": count,
            "marker_id": marker.event_id,
            "durable_anchor_barrier_verified": True,
            "consumer_delivery_before_activation_verified": True,
            "saved_overlap_deduplicated": True,
            "active_bars_partial_without_recovery_recount": True,
            "redis_pending": await store.pending_delivery_count(),
            "automatic_socket_routing_enabled": False,
            "trading_enabled": False,
            "parquet_sha256": archive.sha256,
        }
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2))
    finally:
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.database, args.marker, args.output))
