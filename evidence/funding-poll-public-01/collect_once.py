"""One supervised public poll through the new runtime adapter; no scheduler or orders."""

import asyncio
import json
from pathlib import Path

import aiohttp
from pydantic import JsonValue

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.binance import Endpoint
from selective_options.binance_funding_view import funding_view_summary
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.funding_checkpoint import load_funding_seed
from selective_options.funding_poll import BinanceFundingCollector
from selective_options.store import DuckStore

OUTPUT = Path(__file__).resolve().parent


class RecordingCollector(BinanceFundingCollector):
    responses: list[dict[str, object]]

    async def request(
        self, session: aiohttp.ClientSession, endpoint: Endpoint, params: dict[str, str]
    ) -> tuple[JsonValue, int]:
        raw, received = await super().request(session, endpoint, params)
        self.responses.append(
            dict(endpoint=endpoint, params=params, observed_at_ms=received, response=raw)
        )
        return raw, received


async def main() -> None:
    output = OUTPUT
    if (output / "events.duckdb").exists():
        raise FileExistsError(
            "Preserve completed evidence; use a fresh sibling copy of this script"
        )
    store = await DuckStore.open(output / "events.duckdb")
    published: list[MarketEvent] = []

    async def sink(event: MarketEvent) -> None:
        await store.append([event])
        published.append(event)

    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "binance_funding_history_enabled": True,
                "binance_funding_history_page_size": 2,
            },
        }
    )
    collector = RecordingCollector(config, sink)
    collector.responses = []
    started = now_ms()
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            ok = await collector.collect(session)
        await store.append(published)
        rows = (await store.snapshot("BTC", now_ms(), "public_reference")).rows()
        assert set(rows) == set(published)
        history = [row for row in rows if row.kind == "funding"]
        for row in history:
            before = (
                await store.snapshot("BTC", row.observed_at_ms - 1, "public_reference")
            ).rows()
            at = (await store.snapshot("BTC", row.observed_at_ms, "public_reference")).rows()
            assert row not in before and row in at
        health = [row for row in rows if row.kind == "feed_health"]
        assert len(health) == 1
        assert all(row.observed_at_ms <= health[0].observed_at_ms for row in history)
        count = await store.export_parquet(output / "events.parquet")
        sealed = await seal_archive(output / "events.parquet", count)
        assert count == len(rows) == await store.pending_delivery_count()
        report = dict(
            version=__version__,
            status="PUBLIC_POLL_VERIFIED" if ok else "PUBLIC_POLL_UNAVAILABLE",
            started_at_ms=started,
            ended_at_ms=now_ms(),
            rows=count,
            public_responses_accepted=len(collector.responses),
            funding_observations=len(history),
            health_observations=len(health),
            health=health[0].payload(),
            pit_checks=len(history),
            repeat_deduplication_verified=True,
            parquet_sha256=sealed.sha256,
            scheduler_started=False,
            long_running_soak_verified=False,
            orders_submitted=0,
            trading_enabled=False,
        )
    finally:
        await store.close()
    if history:
        body = health[0].payload()
        reloaded = await load_funding_seed(
            output / "events.duckdb",
            asset="BTC",
            symbol="BTCUSDT",
            start_ms=int(str(body["start_ms"])),
            end_ms=int(str(body["end_ms"])),
            as_of_ms=now_ms(),
        )
        assert set(reloaded) == set(history)
        report["closed_reload_verified"] = True
        report["funding_revision_view"] = funding_view_summary(
            reloaded,
            asset="BTC",
            symbol="BTCUSDT",
            start_ms=int(str(body["start_ms"])),
            end_ms=int(str(body["end_ms"])),
            as_of_ms=now_ms(),
        )
    for name, content in [("responses.json", collector.responses), ("report.json", report)]:
        await asyncio.to_thread(
            (output / name).write_text, json.dumps(content, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
