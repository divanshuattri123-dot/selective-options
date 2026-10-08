"""One bounded official BEA calendar fetch with raw evidence and PIT/archive checks."""

import argparse
import asyncio
import json
from pathlib import Path

import aiohttp
import duckdb

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.bea_calendar import URL, calendar_at, fetch_calendar, parse_calendar
from selective_options.collectors import now_ms
from selective_options.events import MarketEvent
from selective_options.store import DuckStore


async def verify(output: Path) -> None:
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    report: dict[str, object] = {
        "version": __version__,
        "source_url": URL,
        "started_at_ms": now_ms(),
        "orders_submitted": 0,
        "trading_enabled": False,
        "paper_trading_started": False,
    }
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            body, received = await fetch_calendar(session)
        await asyncio.to_thread((output / "response.json").write_bytes, body)
        report["observed_at_ms"] = received
        event = parse_calendar(body, received)
        await store.append([event])
        await store.append([event])
        before = await store.snapshot("GLOBAL", received - 1, "public_reference")
        available = await store.snapshot("GLOBAL", received, "public_reference")
        assert len(available.rows()) == 1
        assert calendar_at(before.rows(), received - 1) is None
        assert calendar_at(available.rows(), received) == event
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        with duckdb.connect() as db:
            result = db.execute("SELECT * FROM read_parquet(?)", [str(output / "events.parquet")])
            columns = [c[0] for c in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
                for r in result.fetchall()
            ]
        assert restored == [event]
        payload = event.payload()
        releases = payload["releases"]
        assert isinstance(releases, list)
        upcoming = [
            r
            for r in releases
            if isinstance(r, dict)
            and isinstance(r.get("scheduled_at_ms"), int)
            and int(str(r["scheduled_at_ms"])) >= received
        ]
        report.update(
            status="PUBLIC_CALENDAR_VERIFIED",
            rows=count,
            group_count=payload["group_count"],
            release_count=len(releases),
            upcoming_count=len(upcoming),
            upcoming_personal_income_and_outlays=[
                r for r in upcoming if r.get("title") == "Personal Income and Outlays"
            ],
            duplicates_removed=payload["duplicate_entries_removed"],
            receipt_visibility_verified=True,
            repeat_deduplication_verified=True,
            archive_exact_match=True,
            parquet_sha256=archive.sha256,
            publication_time_available=False,
            coverage=payload["coverage"],
        )
    except BaseException as exc:
        report.update(
            status="FAILED_OR_UNAVAILABLE",
            failure_type=type(exc).__name__,
            reason=getattr(exc, "reason", None),
            code=getattr(exc, "code", None),
        )
        raise
    finally:
        report["ended_at_ms"] = now_ms()
        await store.close()
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(verify(args.output))
