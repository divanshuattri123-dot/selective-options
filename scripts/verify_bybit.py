"""One bounded public Bybit collection with saved responses, PIT and archive checks."""

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp
import duckdb

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.bybit import BybitCollector, Envelope, PublicEndpoint
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.store import DuckStore


class RecordingCollector(BybitCollector):
    def __init__(self, config: AppConfig, store: DuckStore, output: Path) -> None:
        self.rows: list[MarketEvent] = []
        self.responses: list[dict[str, object]] = []
        self.store, self.output = store, output
        super().__init__(config, self.record)

    async def record(self, event: MarketEvent) -> None:
        await self.store.append([event])
        self.rows.append(event)

    async def request(
        self,
        session: aiohttp.ClientSession,
        endpoint: PublicEndpoint,
        params: dict[str, str],
    ) -> tuple[Envelope, int]:
        response, received = await super().request(session, endpoint, params)
        self.responses.append(
            {
                "endpoint": endpoint,
                "params": params,
                "observed_at_ms": received,
                "response": response.model_dump(mode="json"),
            }
        )
        return response, received


async def verify(output: Path) -> None:
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    config = AppConfig.model_validate({"ingestion": {"bybit_enabled": True}})
    collector = RecordingCollector(config, store, output)
    started = now_ms()
    failure: BaseException | None = None
    try:
        try:
            async with (
                asyncio.timeout(90),
                aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session,
            ):
                await collector.collect(session)
        except BaseException as exc:
            failure = exc
        rows = collector.rows
        snapshots = [r for r in rows if r.kind in {"perpetual", "funding"}]
        for row in snapshots:
            before = await store.snapshot(row.asset, row.observed_at_ms - 1, "public_reference")
            at_receipt = await store.snapshot(row.asset, row.observed_at_ms, "public_reference")
            assert row.event_id not in {e.event_id for e in before.rows()}
            assert row.event_id in {e.event_id for e in at_receipt.rows()}
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        with duckdb.connect() as db:
            result = db.execute("SELECT * FROM read_parquet(?)", [str(output / "events.parquet")])
            columns = [c[0] for c in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
                for r in result.fetchall()
            ]
        assert {r.event_id: r for r in restored} == {r.event_id: r for r in rows}
        health = [{"asset": r.asset, **r.payload()} for r in rows if r.kind == "feed_health"]
        all_assets_accepted = len(health) == len(config.system.assets) and all(
            r["ok"] for r in health
        )
        report = {
            "version": __version__,
            "started_at_ms": started,
            "ended_at_ms": now_ms(),
            "status": "FAILED"
            if failure
            else ("PUBLIC_SNAPSHOTS_VERIFIED" if all_assets_accepted else "PARTIAL_OR_UNAVAILABLE"),
            "failure_type": type(failure).__name__ if failure else None,
            "rows": count,
            "counts": await store.counts(),
            "source_health": health,
            "pit_checks": len(snapshots),
            "archive_exact_match": True,
            "parquet_sha256": archive.sha256,
            "public_responses": len(collector.responses),
            "publication_time_available": False,
            "timestamp_basis": "receipt",
            "orders_submitted": 0,
            "trading_enabled": False,
            "paper_trading_started": False,
        }
        await asyncio.to_thread(
            (output / "responses.json").write_text,
            json.dumps(collector.responses, indent=2) + "\n",
            encoding="utf-8",
        )
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2))
    finally:
        await store.close()
    if failure:
        raise failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.output))
