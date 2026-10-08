"""One bounded real Fed RSS collector poll; raw parsed entity and exact PIT storage."""

import asyncio
import hashlib
import json
from pathlib import Path

import aiohttp
import duckdb

from selective_options import __version__
from selective_options import news
from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.news_quality import policy_news_quality
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parent


async def main():
    output = ROOT / "run"
    output.mkdir(exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    rows, response = [], {}
    original_parse = news.parse_feed

    def record_parse(body, source, received):
        assert not response, "one real poll only"
        response.update(
            source=source,
            observed_at_ms=received,
            bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            entity_basis="Decoded HTTP entity bytes passed unchanged to the production parser",
        )
        (output / "response.xml").write_bytes(body)
        return original_parse(body, source, received)

    async def sink(row):
        await store.append([row], exact=True)
        rows.append(row)

    collector = news.PolicyNewsCollector(AppConfig(), sink, "federal_reserve")
    failure = None
    try:
        news.parse_feed = record_parse
        try:
            async with (
                asyncio.timeout(25),
                aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session,
            ):
                await collector.poll(session)
        except Exception as exc:
            failure = type(exc).__name__
            await collector._health(False, error_type=failure)
        finally:
            news.parse_feed = original_parse
        cutoff = now_ms()
        view = await store.policy_news_quality("federal_reserve", cutoff)
        assert view == policy_news_quality(rows, source="federal_reserve", as_of_ms=cutoff)
        for row in rows:
            at = max(row.observed_at_ms, row.event_time_ms)
            assert row.event_id not in {
                value.event_id
                for value in (await store.snapshot("GLOBAL", at - 1, "public_reference")).rows()
            }
            assert row.event_id in {
                value.event_id
                for value in (await store.snapshot("GLOBAL", at, "public_reference")).rows()
            }
        await store.append(rows, exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        assert count == len(rows) == await store.pending_delivery_count()
    finally:
        news.parse_feed = original_parse
        await store.close()
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as db:
        native = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        assert (
            native
            == db.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(output / "events.parquet")]
            ).fetchall()
        )
    reopened = await DuckStore.open(output / "events.duckdb")
    try:
        assert await reopened.policy_news_quality("federal_reserve", cutoff) == view
        assert await reopened.pending_delivery_count() == count
    finally:
        await reopened.close()
    result = {
        "version": __version__,
        "status": "PUBLIC_BOUND_RSS_POLL_VERIFIED"
        if view["state"] == "POLL_OBSERVED_VERIFIED"
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "source": "federal_reserve",
        "feed_url": news.FEEDS["federal_reserve"],
        "real_public_polls_attempted": 1,
        "failure_type": failure,
        "response": response,
        "quality": view,
        "rows": count,
        "exact_canonical_store_view": True,
        "pit_event_checks": len(rows),
        "exact_insert_duplicate_replay": True,
        "parquet_exact_match": True,
        "parquet_sha256": manifest.sha256,
        "closed_reload_exact_match": True,
        "outbox_pending_rows": count,
        "tls_verification": "default; no override",
        "network_deadline_s": 25,
        "native_data_or_timestamps_rewritten": False,
        "fixture_poll_health": False,
        "first_publication_independently_verified": False,
        "continuous_news_or_recovery_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": result["status"],
                "rows": count,
                "quality_state": view["state"],
                "headline_state": view["headline_state"],
                "failure_type": failure,
            }
        )
    )


asyncio.run(main())
