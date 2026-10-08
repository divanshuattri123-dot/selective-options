"""One finite public SOL network poll with PIT, replay, archive and closed reload checks."""

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp
import duckdb
from pydantic import JsonValue

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.solana_network import Method, SolanaNetworkCollector
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore, complete_thread


class RecordingCollector(SolanaNetworkCollector):
    def __init__(self, store: DuckStore) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.results: list[dict[str, object]] = []
        self.requests_attempted = 0
        super().__init__(AppConfig.model_validate({"system": {"assets": ["SOL"]}}), self.record)

    async def record(self, event: MarketEvent) -> None:
        await self.store.append([event])
        self.rows.append(event)

    async def request(
        self, session: aiohttp.ClientSession, method: Method, slot: int | None = None
    ) -> tuple[JsonValue, int]:
        self.requests_attempted += 1
        result, received = await super().request(session, method, slot)
        self.results.append(
            {
                "method": method,
                "slot_argument": slot,
                "observed_at_ms": received,
                "validated_result": result,
            }
        )
        return result, received


def read_saved(output: Path) -> tuple[tuple[MarketEvent, ...], tuple[MarketEvent, ...], int]:
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [c[0] for c in result.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        outbox = db.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered = FALSE"
        ).fetchone()
        assert outbox is not None
    with duckdb.connect() as db:
        result = db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(output / "events.parquet")]
        )
        columns = [c[0] for c in result.description]
        archive = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
    return rows, archive, int(outbox[0])


async def verify(output: Path) -> dict[str, object]:
    await complete_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(store)
    report: dict[str, object] = {
        "version": __version__,
        "started_at_ms": now_ms(),
        "orders_submitted": 0,
        "trading_enabled": False,
        "paper_trading_started": False,
        "scope": "one recent performance sample and returned vote-account activated stake",
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            accepted = await collector.collect(session)
        assert len(collector.rows) == (3 if accepted else 1)
        for event in collector.rows:
            at = max(event.event_time_ms, event.observed_at_ms)
            assert event not in (await store.snapshot("SOL", at - 1, "public_reference")).rows()
            assert event in (await store.snapshot("SOL", at, "public_reference")).rows()
        await store.append(collector.rows)
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        health = collector.rows[-1]
        snapshot = (await store.snapshot("SOL", health.observed_at_ms, "public_reference")).rows()
        quality = network_quality(snapshot, health.observed_at_ms)
        assert quality["state"] == ("NETWORK_SAMPLE_VERIFIED" if accepted else "BLOCKED")
        if not accepted:
            assert quality["blocker"] == "SOL_FEED_UNAVAILABLE"
        await store.close()
        rows, exported, pending = await complete_thread(read_saved, output)
        assert set(rows) == set(collector.rows) and rows == exported
        assert count == pending == len(rows)
        assert network_quality(rows, health.observed_at_ms) == quality
        report.update(
            status="PUBLIC_NETWORK_SAMPLE_VERIFIED" if accepted else "SOURCE_UNAVAILABLE",
            rows=count,
            pending_outbox_rows=pending,
            public_requests_attempted=collector.requests_attempted,
            accepted_rpc_results=len(collector.results),
            pit_checks=len(collector.rows) * 2,
            repeat_deduplication_verified=True,
            archive_exact_match=True,
            closed_reload_verified=True,
            parquet_sha256=archive.sha256,
            quality=quality,
            health=health.payload(),
        )
    except BaseException as exc:
        report.update(status="FAILED", failure_type=type(exc).__name__)
        raise
    finally:
        await store.close()
        report["ended_at_ms"] = now_ms()
        await complete_thread(
            (output / "results.json").write_text,
            json.dumps(collector.results, indent=2) + "\n",
            encoding="utf-8",
        )
        await complete_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.output))
