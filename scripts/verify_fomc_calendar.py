"""One bounded official FOMC schedule poll with raw response, PIT, health and archive checks."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

import aiohttp
import duckdb

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.events import MarketEvent, make_event
from selective_options.fomc_calendar import (
    MAX_AGE_MS,
    SOURCE,
    SYMBOL,
    URL,
    CalendarPayload,
    calendar_at,
    calendar_quality,
    fetch_calendar,
    parse_calendar,
)
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
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"User-Agent": "SelectiveOptionsResearch/0.3 official FOMC calendar metadata"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            body, received = await fetch_calendar(session)
        await asyncio.to_thread((output / "response.html").write_bytes, body)
        event = parse_calendar(body, received)
        await store.append([event])
        await store.append([event])
        before = await store.snapshot("GLOBAL", received - 1, "public_reference")
        after = await store.snapshot("GLOBAL", received, "public_reference")
        assert calendar_at(before.rows(), received - 1) is None
        assert calendar_at(after.rows(), received) == event
        latest = now_ms()
        health = make_event(
            source=SOURCE,
            environment="public_reference",
            asset="GLOBAL",
            symbol=SYMBOL,
            kind="feed_health",
            event_time_ms=latest,
            observed_at_ms=latest,
            timestamp_kind="receipt",
            payload={"ok": True, "reason": "SCHEDULE_SNAPSHOT_ACCEPTED", "code": 0},
        )
        await store.append([health])
        quality_rows = await store.snapshot("GLOBAL", latest, "public_reference")
        quality = calendar_quality(quality_rows.rows(), latest, MAX_AGE_MS)
        assert quality["state"] == "OBSERVED_SCOPE_AVAILABLE"
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        with duckdb.connect() as connection:
            result = connection.execute(
                "SELECT * FROM read_parquet(?)", [str(output / "events.parquet")]
            )
            columns = [column[0] for column in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
                for row in result.fetchall()
            ]
        assert restored == [event, health]
        payload = CalendarPayload.model_validate(event.payload())
        entries = payload.entries
        report.update(
            status="PUBLIC_FOMC_CALENDAR_VERIFIED",
            response_received_at_ms=received,
            response_bytes=len(body),
            response_sha256=hashlib.sha256(body).hexdigest(),
            snapshot_event_id=event.event_id,
            snapshot_receipt_visibility_verified=True,
            exact_replay_deduplicated=True,
            schedule_entry_count=len(entries),
            listed_years=sorted({entry.year for entry in entries}),
            future_events=quality["upcoming_events"],
            quality=quality,
            persisted_rows=count,
            outbox_pending_after_capture=await store.pending_delivery_count(),
            archive_exact_match=True,
            parquet_sha256=archive.sha256,
            receipt_clock_is_schedule_availability=True,
            exact_meeting_times_available=False,
            unscheduled_meeting_coverage=False,
            release_values_available=False,
            consensus_available=False,
            trading_approval=False,
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
    asyncio.run(verify(parser.parse_args().output))
