"""Offline coordinator replay: original HTTP pages and saved WebSocket receipt times."""

import argparse
import asyncio
import json
from pathlib import Path
from unittest.mock import patch

import aiohttp
from verify_reconciliation import read

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.checkpoints import load_trade_checkpoints
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.gap_recovery import recover_trade_gap
from selective_options.recovery_journal import load_committed_recovery, load_recovery
from selective_options.store import DuckStore, complete_thread
from selective_options.trade_backfill import parse_page
from selective_options.trade_reconciliation import TradeIdentity


async def verify(
    checkpoint: Path,
    rest_database: Path,
    websocket_database: Path,
    pages_directory: Path,
    symbol: str,
    output: Path,
) -> None:
    config = AppConfig.model_validate({"ingestion": {"kraken_backfill_page_size": 50}})
    seeds = await load_trade_checkpoints(checkpoint, config.system.assets)
    anchor = [row for row in seeds if row.symbol == symbol][-1]
    after = TradeIdentity.read(anchor).native_id
    rest = await complete_thread(read, rest_database, symbol, "kraken_rest", after)
    websocket = await complete_thread(read, websocket_database, symbol, "kraken", after)
    by_id = {TradeIdentity.read(row).native_id: row for row in rest}
    through = max(by_id)
    queued = iter(websocket)
    drained = asyncio.Event()
    page_count = 0
    returned: set[int] = set()

    async def receive() -> MarketEvent:
        row = next(queued, None)
        if row is not None:
            return row
        drained.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def saved_page(
        session: aiohttp.ClientSession, requested: str, cursor: str, limit: int
    ) -> tuple[bytes, int]:
        nonlocal page_count
        await drained.wait()
        assert requested == symbol
        page_count += 1
        body = await asyncio.to_thread((pages_directory / f"page-{page_count:03}.json").read_bytes)
        trades, _ = parse_page(body, symbol, limit)
        new_ids = {row.trade_id for row in trades} & by_id.keys() - returned
        observations = {by_id[key].observed_at_ms for key in new_ids}
        assert len(observations) == 1, "saved page must have one original receipt time"
        returned.update(new_ids)
        return body, observations.pop()

    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    try:
        await store.append([anchor])
        # Only transport is replaced. Actual parsing/pagination, reconciliation,
        # publication and open-writer reload execute unchanged; no network is used.
        with patch("selective_options.trade_backfill.fetch_page", saved_page):
            result = await recover_trade_gap(
                config, store, anchor, through, receive, output / "attempt"
            )
        assert returned == by_id.keys()
        assert len(result.plan.selected) == through - after
        assert (
            await load_committed_recovery(store, result.marker.event_id, result.plan.verified_at_ms)
        )[1] == result.plan
        for event in result.plan.raw:
            assert event in rest or event in websocket
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
    finally:
        await store.close()
    assert (
        await load_recovery(
            output / "events.duckdb", result.marker.event_id, result.plan.verified_at_ms
        )
        == result.plan
    )
    report = {
        "build_version": __version__,
        "status": "OFFLINE_GAP_COORDINATOR_VERIFIED",
        "evidence_type": "saved public HTTP pages and WebSocket records; mocked transport, no network",
        "pages": page_count,
        "database_rows": count,
        "raw_rows": len(result.plan.raw),
        "selected_rows": len(result.plan.selected),
        "buffered_websocket_rows": len(websocket),
        "through_id": result.plan.through_id,
        "marker_id": result.marker.event_id,
        "original_raw_records_and_receipts_preserved": True,
        "open_writer_reload_and_closed_database_reload_verified": True,
        "runtime_activated": False,
        "automatic_capture_repair_enabled": False,
        "trading_enabled": False,
        "parquet_sha256": manifest.sha256,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in (
        "checkpoint-database",
        "rest-database",
        "websocket-database",
        "pages-directory",
        "output",
    ):
        parser.add_argument("--" + flag, type=Path, required=True)
    parser.add_argument("--symbol", default="BTC/USD")
    args = parser.parse_args()
    asyncio.run(
        verify(
            args.checkpoint_database,
            args.rest_database,
            args.websocket_database,
            args.pages_directory,
            args.symbol,
            args.output,
        )
    )
