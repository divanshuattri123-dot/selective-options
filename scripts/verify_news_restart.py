"""Offline saved Fed HTTP entity replay and durable closed-store restart proof."""

import argparse
import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Literal, Self, cast
from unittest.mock import patch

import aiohttp
import duckdb
from pydantic import StrictInt, TypeAdapter

from selective_options import news
from selective_options.archive import _verify_archive, file_digest, seal_archive
from selective_options.collectors import Sink
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.news import FEEDS, NewsPollHealth, PolicyNewsCollector, parse_feed
from selective_options.news_quality import policy_news_quality
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE_NAMES = (
    "response.xml",
    "events.duckdb",
    "events.parquet",
    "events.parquet.manifest.json",
    "report.json",
)
MAX_SOURCE_ROWS = 256
MAX_SOURCE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class SavedEvidence:
    body: bytes
    rows: tuple[MarketEvent, ...]
    health: MarketEvent
    hashes: dict[str, str]


def hashes(source: Path) -> dict[str, str]:
    result = {}
    for name in SOURCE_NAMES:
        path = source / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_SOURCE_BYTES:
            raise ValueError("missing, linked or over-budget saved source file")
        result[name] = file_digest(path)
    return result


def read_rows(database: Path, archive: Path | None = None) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [MAX_SOURCE_ROWS + 1]
        )
        columns = [entry[0] for entry in cursor.description]
        values = cursor.fetchall()
        if len(values) > MAX_SOURCE_ROWS:
            raise IntegrityError("NEWS_RESTART_SOURCE_ROW_BUDGET")
        if archive is not None:
            archived = connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id LIMIT ?",
                [str(archive), MAX_SOURCE_ROWS + 1],
            ).fetchall()
            if values != archived:
                raise IntegrityError("NEWS_RESTART_DATABASE_ARCHIVE_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, value, strict=True))) for value in values
        )


def preflight(source: Path, output: Path) -> SavedEvidence:
    source, output = source.resolve(), output.resolve()
    scratch = (ROOT / "work" / "news-restart-0338").resolve()
    if output != (ROOT / "evidence" / "news-restart-01").resolve() and not output.is_relative_to(
        scratch
    ):
        raise ValueError(
            "output must be news-restart-01 or within workspace work/news-restart-0338"
        )
    if (
        output.exists()
        or output == source
        or source.is_relative_to(output)
        or output.is_relative_to(source)
    ):
        raise FileExistsError("refusing existing output or overlapping source")
    before = hashes(source)
    if (source / "events.duckdb.wal").exists():
        raise IntegrityError("NEWS_RESTART_SOURCE_NOT_CLOSED")
    manifest = _verify_archive(source / "events.parquet")
    if not 1 <= manifest.rows <= MAX_SOURCE_ROWS - 3:
        raise IntegrityError("NEWS_RESTART_SOURCE_ROW_BUDGET")
    body = (source / "response.xml").read_bytes()
    if len(body) > 1_048_576:
        raise IntegrityError("NEWS_FEED_TOO_LARGE")
    rows = read_rows(source / "events.duckdb", source / "events.parquet")
    if len(rows) != manifest.rows or any(
        (row.source, row.environment, row.asset, row.symbol)
        != ("federal_reserve", "public_reference", "GLOBAL", "POLICY_NEWS")
        or row.kind not in ("news", "feed_health")
        for row in rows
    ):
        raise IntegrityError("NEWS_RESTART_SOURCE_SCOPE_OR_COUNT")
    health = [row for row in rows if row.kind == "feed_health"]
    if len(health) != 1:
        raise IntegrityError("NEWS_RESTART_EXPECTED_ONE_ORIGINAL_POLL")
    status = NewsPollHealth.model_validate(health[0].payload())
    if status.response_received_at_ms is None:
        raise IntegrityError("NEWS_RESTART_ORIGINAL_RECEIPT_MISSING")
    parsed, rejected = parse_feed(body, "federal_reserve", status.response_received_at_ms)
    if rejected or {row.event_id: row for row in parsed} != {
        row.event_id: row for row in rows if row.kind == "news"
    }:
        raise IntegrityError("NEWS_RESTART_RAW_ENTITY_OR_ORIGINAL_RECEIPT_MISMATCH")
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if (
        not isinstance(report, dict)
        or report.get("source") != "federal_reserve"
        or report.get("fixture_poll_health") is not False
        or report.get("real_public_polls_attempted") != 1
    ):
        raise IntegrityError("NEWS_RESTART_ORIGINAL_PROVENANCE")
    response = report.get("response")
    if (
        not isinstance(response, dict)
        or response.get("sha256") != before["response.xml"]
        or response.get("bytes") != len(body)
        or response.get("observed_at_ms") != status.response_received_at_ms
    ):
        raise IntegrityError("NEWS_RESTART_ORIGINAL_ENTITY_REPORT_MISMATCH")
    quality = report.get("quality")
    if not isinstance(quality, dict):
        raise IntegrityError("NEWS_RESTART_ORIGINAL_QUALITY_MISSING")
    cutoff = TypeAdapter(StrictInt).validate_python(quality.get("as_of_ms"))
    validated = policy_news_quality(rows, source="federal_reserve", as_of_ms=cutoff)
    if validated != quality or validated["state"] != "POLL_OBSERVED_VERIFIED":
        raise IntegrityError("NEWS_RESTART_ORIGINAL_QUALITY_INVALID")
    if hashes(source) != before:
        raise IntegrityError("NEWS_RESTART_SOURCE_CHANGED_DURING_PREFLIGHT")
    return SavedEvidence(body, rows, health[0], before)


class SavedContent:
    def __init__(self, body: bytes) -> None:
        self.body = body

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        if size != 65536:
            raise AssertionError("unexpected production collector chunk bound")
        for start in range(0, len(self.body), size):
            yield self.body[start : start + size]


class SavedResponse:
    def __init__(self, body: bytes) -> None:
        self.content = SavedContent(body)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        return False

    def raise_for_status(self) -> None:
        return None


class SavedSession:
    """One explicit mocked HTTP entity; never constructs a real network session."""

    def __init__(self, body: bytes, transport_failure: bool) -> None:
        self.body, self.transport_failure = body, transport_failure
        self.calls = 0

    def get(self, url: str) -> SavedResponse:
        self.calls += 1
        if self.calls != 1 or url != FEEDS["federal_reserve"]:
            raise AssertionError("offline replay attempted unexpected request scope/count")
        if self.transport_failure:
            raise aiohttp.ClientConnectionError("Labelled synthetic offline transport outage")
        return SavedResponse(self.body)


async def replay_poll(
    body: bytes, sink: Sink, response_ms: int, health_ms: int, *, transport_failure: bool = False
) -> tuple[MarketEvent, ...]:
    if (
        any(type(value) is not int or value <= 0 for value in (response_ms, health_ms))
        or response_ms > health_ms
    ):
        raise ValueError("positive ordered fixture receipt clocks required")
    emitted = []

    async def record(row: MarketEvent) -> None:
        await sink(row)
        emitted.append(row)

    collector = PolicyNewsCollector(AppConfig(), record, "federal_reserve")
    session = SavedSession(body, transport_failure)
    clocks = [health_ms] if transport_failure else [response_ms, health_ms]
    with patch.object(news, "now_ms", side_effect=clocks):
        try:
            await collector.poll(cast(aiohttp.ClientSession, session))
        except aiohttp.ClientConnectionError as exc:
            await collector._health(False, error_type=type(exc).__name__)
    if session.calls != 1:
        raise AssertionError("exactly one mocked request required")
    return tuple(emitted)


async def assert_view(
    store: DuckStore, at: int, expected: tuple[MarketEvent, ...]
) -> dict[str, object]:
    snapshot = await store.snapshot("GLOBAL", at, "public_reference")
    available = tuple(row for row in expected if max(row.event_time_ms, row.observed_at_ms) <= at)
    if set(snapshot.rows()) != set(available):
        raise IntegrityError("NEWS_RESTART_PIT_ROWS_MISMATCH")
    canonical = policy_news_quality(available, source="federal_reserve", as_of_ms=at)
    stored = await store.policy_news_quality("federal_reserve", at)
    if stored != canonical:
        raise IntegrityError("NEWS_RESTART_CANONICAL_STORE_VIEW_MISMATCH")
    return stored


async def export_checked(store: DuckStore, path: Path) -> None:
    count = await store.export_parquet(path)
    await seal_archive(path, count)


async def verify(source: Path, output: Path) -> dict[str, object]:
    source, output = await asyncio.gather(
        asyncio.to_thread(source.resolve), asyncio.to_thread(output.resolve)
    )
    saved = await complete_thread(preflight, source, output)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    source_status = NewsPollHealth.model_validate(saved.health.payload())
    assert source_status.response_received_at_ms is not None
    base = saved.health.observed_at_ms
    stage_times = {
        "repeat": (base + 1000, base + 1010),
        "failure": (base + 2000, base + 2000),
        "recovery": (base + 3000, base + 3010),
    }
    database = output / "events.duckdb"
    store = await DuckStore.open(database)
    expected = saved.rows
    scenarios: dict[str, object] = {}
    fixture_health: dict[str, object] = {}
    original_news = tuple(row for row in saved.rows if row.kind == "news")
    acknowledged = saved.health.event_id
    try:

        async def initial_sink(row: MarketEvent) -> None:
            await store.append([row], exact=True)

        first = await replay_poll(
            saved.body, initial_sink, source_status.response_received_at_ms, base
        )
        if set(first) != set(saved.rows):
            raise IntegrityError("NEWS_RESTART_FIRST_PRODUCTION_REPLAY_MISMATCH")
        for row in saved.rows:
            at = max(row.event_time_ms, row.observed_at_ms)
            for instant in (at - 1, at):
                await assert_view(store, instant, expected)
        scenarios["before_original_response"] = await assert_view(
            store, source_status.response_received_at_ms - 1, expected
        )
        scenarios["at_original_response"] = await assert_view(
            store, source_status.response_received_at_ms, expected
        )
        scenarios["at_original_health"] = await assert_view(store, base, expected)
        await store.acknowledge_delivery([acknowledged])

        async def repeated_sink(row: MarketEvent) -> None:
            # Stable metadata identities deduplicate to their original receipts.
            await store.append([row], exact=row.kind == "feed_health")

        repeated = await replay_poll(saved.body, repeated_sink, *stage_times["repeat"])
        expected += (repeated[-1],)
        fixture_health["repeat"] = repeated[-1].model_dump(mode="json")
        scenarios["before_repeat_health"] = await assert_view(
            store, stage_times["repeat"][1] - 1, expected
        )
        scenarios["at_repeat_health"] = await assert_view(store, stage_times["repeat"][1], expected)
        await export_checked(store, output / "before-restart.parquet")
    finally:
        await store.close()
    if set(await complete_thread(read_rows, database, output / "before-restart.parquet")) != set(
        expected
    ):
        raise IntegrityError("NEWS_RESTART_FIRST_CLOSED_RELOAD_MISMATCH")
    store = await DuckStore.open(database)
    try:
        scenarios["reopened_repeat"] = await assert_view(store, stage_times["repeat"][1], expected)
        pending_before_failure = {
            row.event_id for row in await store.pending_delivery(MAX_SOURCE_ROWS)
        }
        if pending_before_failure != {row.event_id for row in expected} - {acknowledged}:
            raise IntegrityError("NEWS_RESTART_OUTBOX_FIRST_REOPEN_MISMATCH")

        async def failed_sink(row: MarketEvent) -> None:
            await store.append([row], exact=True)

        failure = await replay_poll(
            saved.body, failed_sink, *stage_times["failure"], transport_failure=True
        )
        expected += failure
        fixture_health["failure"] = failure[-1].model_dump(mode="json")
        scenarios["before_failure"] = await assert_view(
            store, stage_times["failure"][1] - 1, expected
        )
        scenarios["at_failure"] = await assert_view(store, stage_times["failure"][1], expected)
        if cast(dict[str, object], scenarios["at_failure"])["blocker"] != "NEWS_FEED_UNAVAILABLE":
            raise IntegrityError("NEWS_RESTART_FAILURE_DID_NOT_BLOCK")
        await export_checked(store, output / "failure.parquet")
    finally:
        await store.close()
    if set(await complete_thread(read_rows, database, output / "failure.parquet")) != set(expected):
        raise IntegrityError("NEWS_RESTART_FAILED_CLOSED_RELOAD_MISMATCH")
    store = await DuckStore.open(database)
    try:
        scenarios["reopened_failure"] = await assert_view(
            store, stage_times["failure"][1], expected
        )

        async def recovery_sink(row: MarketEvent) -> None:
            await store.append([row], exact=row.kind == "feed_health")

        recovery = await replay_poll(saved.body, recovery_sink, *stage_times["recovery"])
        expected += (recovery[-1],)
        fixture_health["recovery"] = recovery[-1].model_dump(mode="json")
        scenarios["at_recovery_response_before_health"] = await assert_view(
            store, stage_times["recovery"][0], expected
        )
        scenarios["before_recovery_health"] = await assert_view(
            store, stage_times["recovery"][1] - 1, expected
        )
        recovered = await assert_view(store, stage_times["recovery"][1], expected)
        scenarios["at_recovery_health"] = recovered
        if recovered["state"] != "POLL_OBSERVED_VERIFIED" or recovered["first_observed_at_ms"] != [
            source_status.response_received_at_ms
        ] * len(original_news):
            raise IntegrityError("NEWS_RESTART_RECOVERY_OR_ORIGINAL_RECEIPTS_MISMATCH")
        if any(
            recovered[field] is not False
            for field in (
                "trading_approval",
                "complete_feed_available",
                "all_news_complete",
                "first_publication_independently_verified",
            )
        ):
            raise IntegrityError("NEWS_RESTART_OVERCLAIM")
        await export_checked(store, output / "events.parquet")
    finally:
        await store.close()
    if set(await complete_thread(read_rows, database, output / "events.parquet")) != set(expected):
        raise IntegrityError("NEWS_RESTART_FINAL_CLOSED_RELOAD_MISMATCH")
    store = await DuckStore.open(database)
    try:
        scenarios["reopened_recovery"] = await assert_view(
            store, stage_times["recovery"][1], expected
        )
        pending = {row.event_id for row in await store.pending_delivery(MAX_SOURCE_ROWS)}
        if pending != {row.event_id for row in expected} - {acknowledged}:
            raise IntegrityError("NEWS_RESTART_OUTBOX_FINAL_REOPEN_MISMATCH")
        actual_news = await store.read_events([row.event_id for row in original_news])
        if set(actual_news) != set(original_news):
            raise IntegrityError("NEWS_RESTART_ORIGINAL_METADATA_REWRITTEN")
    finally:
        await store.close()
    after = await complete_thread(hashes, source)
    if after != saved.hashes:
        raise IntegrityError("NEWS_RESTART_SOURCE_CHANGED")
    report: dict[str, object] = {
        "status": "OFFLINE_SAVED_PUBLIC_RESTART_VERIFIED",
        "source": str(source),
        "provenance": "Original public Fed entity and canonical records unchanged. First offline HTTP replay uses original recorded clocks; repeat, transport failure and recovery clocks/outcomes are synthetic fixtures. No new public request or live outage/recovery claim.",
        "source_hashes_before": saved.hashes,
        "source_hashes_after": after,
        "source_files_unchanged": True,
        "native_records": len(saved.rows),
        "original_headlines": len(original_news),
        "mocked_http_attempts": 4,
        "real_public_requests": 0,
        "synthetic_health_records": fixture_health,
        "simulated_poll_clocks": {name: list(clocks) for name, clocks in stage_times.items()},
        "persisted_records": len(expected),
        "metadata_first_receipts_preserved": True,
        "exact_closed_reloads": 3,
        "outbox_pending_before_failure": len(pending_before_failure),
        "outbox_pending_after_final_reopen": len(pending),
        "local_acknowledged_event_id": acknowledged,
        "redis_delivery_claim": False,
        "pit_checks": 2 * len(saved.rows),
        "quality_scenarios": scenarios,
        "three_archives_exact": True,
        "all_news_complete": False,
        "milestone_2_complete": False,
        "first_publication_independently_verified": False,
        "trading_approval": False,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "evidence/news-quality-public-01/run")
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/news-restart-01")
    args = parser.parse_args()
    report = asyncio.run(verify(args.source.resolve(), args.output.resolve()))
    print(
        json.dumps(
            {
                field: report[field]
                for field in (
                    "status",
                    "native_records",
                    "persisted_records",
                    "source_files_unchanged",
                    "real_public_requests",
                    "exact_closed_reloads",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
