"""Saved native policy metadata replay plus clearly labelled synthetic poll outcomes."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import cast

import duckdb

from selective_options.events import MarketEvent
from selective_options.news import NewsSource, news_poll_health
from selective_options.news_quality import policy_news_quality
from selective_options.store import DuckStore


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1_048_576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_native(path: Path) -> list[MarketEvent]:
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            "SELECT * FROM read_parquet(?) WHERE symbol='POLICY_NEWS' AND source IN ('federal_reserve','ecb') ORDER BY observed_at_ms,event_id LIMIT 10001",
            [str(path)],
        )
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        if len(rows) > 10000:
            raise ValueError("saved native record budget exceeded")
        return [MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in rows]
    finally:
        connection.close()


async def verify(root: Path, destination: Path) -> dict[str, object]:
    original = root / "evidence" / "ingestion-pilot-05"
    paths = [
        original / name
        for name in ("events.duckdb", "events.parquet", "events.parquet.manifest.json")
    ]
    before_hashes = {str(path.relative_to(root)): sha256(path) for path in paths}
    native = read_native(original / "events.parquet")
    if len(native) != 17:
        raise ValueError("expected saved pilot-05 policy RSS evidence")
    fed = [row for row in native if row.source == "federal_reserve"]
    headlines = [row for row in fed if row.kind == "news"]
    native_as_of = max(row.observed_at_ms for row in native)
    sources: tuple[NewsSource, ...] = ("federal_reserve", "ecb")
    native_reports = {
        source: policy_news_quality(native, source=source, as_of_ms=native_as_of)
        for source in sources
    }
    assert native_reports["federal_reserve"]["blocker"] == "NEWS_HEALTH_UNBOUND"
    assert native_reports["ecb"]["blocker"] == "NEWS_FEED_UNAVAILABLE"
    base = native_as_of + 1000
    # These health rows did not come from public HTTP. They bind saved native
    # metadata only to exercise outage/recovery and storage behavior offline.
    success = news_poll_health(
        "federal_reserve",
        base,
        response_received_at_ms=base - 1,
        accepted_event_ids=tuple(row.event_id for row in headlines),
    )
    failure = news_poll_health("federal_reserve", base + 1000, error_type="LabelledFixtureOutage")
    recovery = news_poll_health(
        "federal_reserve",
        base + 2000,
        response_received_at_ms=base + 1999,
        accepted_event_ids=tuple(row.event_id for row in headlines),
    )
    empty = news_poll_health("federal_reserve", base + 3000, response_received_at_ms=base + 2999)
    records = [*native, success, failure, recovery, empty]
    await asyncio.to_thread(destination.mkdir, parents=True, exist_ok=False)
    (destination / "native-records.json").write_text(
        json.dumps([row.model_dump(mode="json") for row in native], indent=2) + "\n",
        encoding="utf-8",
    )
    (destination / "labelled-fixture-health.json").write_text(
        json.dumps(
            {
                "provenance": "Synthetic offline health; no new public poll",
                "rows": [
                    row.model_dump(mode="json") for row in (success, failure, recovery, empty)
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    store = await DuckStore.open(destination / "events.duckdb")
    scenarios: dict[str, object] = {}
    try:
        await store.append(records, exact=True)
        assert await store.pending_delivery_count() == len(records)
        points = {
            "before_first_observation": min(row.observed_at_ms for row in headlines) - 1,
            "at_original_observation": min(row.observed_at_ms for row in headlines),
            "before_bound_fixture_poll": base - 1,
            "at_bound_fixture_poll": base,
            "before_fixture_failure": base + 999,
            "at_fixture_failure": base + 1000,
            "before_fixture_recovery": base + 1999,
            "at_fixture_recovery": base + 2000,
            "at_fixture_empty_poll": base + 3000,
            "at_exact_poll_age_limit": base + 2999 + 600000,
            "one_ms_after_poll_age_limit": base + 2999 + 600001,
        }
        for name, instant in points.items():
            snapshot = await store.snapshot("GLOBAL", instant, "public_reference")
            scenarios[name] = policy_news_quality(
                snapshot.rows(), source="federal_reserve", as_of_ms=instant
            )
            assert all(
                row.event_time_ms <= instant and row.observed_at_ms <= instant
                for row in snapshot.rows()
            )
        assert (
            cast(dict[str, object], scenarios["at_fixture_failure"])["blocker"]
            == "NEWS_FEED_UNAVAILABLE"
        )
        assert (
            cast(dict[str, object], scenarios["before_fixture_recovery"])["blocker"]
            == "NEWS_FEED_UNAVAILABLE"
        )
        assert (
            cast(dict[str, object], scenarios["at_fixture_recovery"])["state"]
            == "POLL_OBSERVED_VERIFIED"
        )
        assert (
            cast(dict[str, object], scenarios["at_fixture_empty_poll"])["headline_state"]
            == "EMPTY_POLL"
        )
        assert (
            cast(dict[str, object], scenarios["at_exact_poll_age_limit"])["state"]
            == "POLL_OBSERVED_VERIFIED"
        )
        assert (
            cast(dict[str, object], scenarios["one_ms_after_poll_age_limit"])["blocker"]
            == "NEWS_POLL_STALE"
        )
        await store.export_parquet(destination / "events.parquet")
    finally:
        await store.close()
    store = await DuckStore.open(destination / "events.duckdb")
    try:
        snapshot = await store.snapshot("GLOBAL", base + 3000, "public_reference")
        assert {row.event_id: row for row in snapshot.rows()} == {
            row.event_id: row for row in records
        }
        reopened = policy_news_quality(
            snapshot.rows(), source="federal_reserve", as_of_ms=base + 3000
        )
        assert reopened == scenarios["at_fixture_empty_poll"]
        assert await store.pending_delivery_count() == len(records)
        await store.acknowledge_delivery([failure.event_id])
        assert await store.pending_delivery_count() == len(records) - 1
    finally:
        await store.close()
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            "SELECT * FROM read_parquet(?)", [str(destination / "events.parquet")]
        )
        columns = [column[0] for column in cursor.description]
        parquet = [
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        ]
        assert {row.event_id: row for row in parquet} == {row.event_id: row for row in records}
    finally:
        connection.close()
    after_hashes = {str(path.relative_to(root)): sha256(path) for path in paths}
    assert before_hashes == after_hashes
    report: dict[str, object] = {
        "provenance": "Saved pilot-05 native public RSS metadata; all bound health, outage, recovery and empty-poll outcomes are explicitly synthetic offline fixtures. No new public poll.",
        "original_hashes_before": before_hashes,
        "original_hashes_after": after_hashes,
        "original_files_unchanged": True,
        "native_record_count": len(native),
        "native_fed_headline_count": len(headlines),
        "synthetic_health_count": 4,
        "persisted_count": len(records),
        "native_as_of": native_reports,
        "fixture_pit_scenarios": scenarios,
        "duckdb_parquet_exact": True,
        "closed_reopen_exact": True,
        "outbox_reopen_pending": len(records),
        "outbox_after_acknowledging_fixture_failure": len(records) - 1,
        "original_receipts_preserved": True,
        "limits": "Metadata only; publisher time is unverified first publication. Legacy native success is unbound and ECB native health unavailable. Fixtures prove implementation behavior, not live recovery or feed availability. No classification, features, complete news universe, M2 completeness or trading approval.",
    }
    (destination / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("evidence/news-quality-01"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = asyncio.run(verify(root, args.output.resolve()))
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "native_record_count",
                    "synthetic_health_count",
                    "persisted_count",
                    "duckdb_parquet_exact",
                    "closed_reopen_exact",
                    "original_files_unchanged",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
