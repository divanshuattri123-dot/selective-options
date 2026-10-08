"""Fetch and verify a bounded public settled-funding archive; no orders or credentials."""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Literal

import aiohttp
import duckdb

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.bybit import BybitCollector, Envelope, FeedUnavailable, PublicEndpoint
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.funding_history import collect_funding_history
from selective_options.oi_history import Interval, collect_oi_history
from selective_options.store import DuckStore


class RecordingCollector(BybitCollector):
    def __init__(self, config: AppConfig) -> None:
        self.responses: list[dict[str, object]] = []
        super().__init__(config, self.unused_sink)

    async def unused_sink(self, event: MarketEvent) -> None:
        raise RuntimeError("history collection must stage rows before publication")

    async def request(
        self, session: aiohttp.ClientSession, endpoint: PublicEndpoint, params: dict[str, str]
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


async def verify(
    output: Path,
    asset: str,
    hours: int,
    page_size: int,
    max_pages: int,
    *,
    kind: Literal["funding", "open_interest"] = "funding",
    interval: Interval = "5min",
) -> None:
    if not 1 <= hours <= 720:
        raise ValueError("hours must be 1..720")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(AppConfig.model_validate({"system": {"assets": [asset]}}))
    end = now_ms()
    start = end - hours * 3_600_000
    failure: BaseException | None = None
    report: dict[str, object] = {
        "version": __version__,
        "asset": asset,
        "kind": kind,
        "sampling_interval": interval if kind == "open_interest" else None,
        "start_ms": start,
        "end_ms": end,
        "page_size": page_size,
        "max_pages": max_pages,
        "orders_submitted": 0,
        "trading_enabled": False,
        "paper_trading_started": False,
        "historical_schedule_completeness": "NOT_PROVEN",
        "pit_checks": 0,
    }
    try:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                plan = (
                    await collect_funding_history(
                        collector,
                        session,
                        asset,
                        start_ms=start,
                        end_ms=end,
                        page_size=page_size,
                        max_pages=max_pages,
                    )
                    if kind == "funding"
                    else await collect_oi_history(
                        collector,
                        session,
                        asset,
                        start_ms=start,
                        end_ms=end,
                        interval=interval,
                        page_size=page_size,
                        max_pages=max_pages,
                    )
                )
            await store.append(list(plan.events))
            # Re-observing identical settlements must not create new economic events.
            await store.append(
                [e.model_copy(update={"observed_at_ms": e.observed_at_ms + 1}) for e in plan.events]
            )
            for row in plan.events:
                before = await store.snapshot(asset, row.observed_at_ms - 1, "public_reference")
                at = await store.snapshot(asset, row.observed_at_ms, "public_reference")
                assert row.event_id not in {e.event_id for e in before.rows()}
                assert row in at.rows()
            report.update(
                status="ARCHIVE_VERIFIED"
                if plan.events
                else ("NO_SETTLEMENTS_RETURNED" if kind == "funding" else "NO_OI_SAMPLES_RETURNED"),
                pages=plan.pages,
                termination=plan.termination,
                pit_checks=len(plan.events),
            )
        except BaseException as exc:
            failure = exc
            report.update(
                status="FAILED",
                failure_type=type(exc).__name__,
                failure_reason=exc.reason
                if isinstance(exc, FeedUnavailable)
                else str(exc)
                if isinstance(exc, IntegrityError)
                else None,
                failure_code=exc.code if isinstance(exc, FeedUnavailable) else None,
            )
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        all_rows = (await store.snapshot(asset, now_ms(), "public_reference")).rows()
        with duckdb.connect() as db:
            result = db.execute("SELECT * FROM read_parquet(?)", [str(output / "events.parquet")])
            columns = [c[0] for c in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
                for r in result.fetchall()
            ]
        assert {e.event_id: e for e in all_rows} == {e.event_id: e for e in restored}
        report.update(
            rows=count,
            archive_exact_match=True,
            parquet_sha256=archive.sha256,
            public_responses=len(collector.responses),
            ended_at_ms=now_ms(),
        )
        for name, content in (("responses.json", collector.responses), ("report.json", report)):
            await asyncio.to_thread(
                (output / name).write_text, json.dumps(content, indent=2) + "\n", encoding="utf-8"
            )
        print(json.dumps(report, indent=2))
    finally:
        await store.close()
    if failure:
        raise failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--asset", choices=("BTC", "ETH", "SOL", "PAXG"), default="BTC")
    parser.add_argument("--hours", type=int, default=48)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(verify(args.output, args.asset, args.hours, args.page_size, args.max_pages))
