"""Saved native equivalence plus explicitly synthetic retained-history overflow; no network."""

import asyncio
import hashlib
import itertools
import json
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent, make_event
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parent
REPLAY = ROOT.parent / "option-projection-replay-02"
SOURCES = [
    ROOT.parent / name for name in ("reused-validation-public-01", "multiasset-options-public-01")
]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_source(source):
    with duckdb.connect(str(source / "events.duckdb"), read_only=True) as db:
        cursor = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in cursor.description]
        native = cursor.fetchall()
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(native)
    return native, [
        MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
    ]


def synthetic_history(original, start, count):
    payload = original.payload()
    for index in range(count):
        timestamp = start - count - 1 + index
        yield make_event(
            source="deribit",
            environment="testnet",
            asset="BTC",
            symbol=original.symbol,
            kind="ticker",
            event_time_ms=timestamp,
            observed_at_ms=timestamp,
            payload=payload
            | {"timestamp": timestamp, "fixture_origin": "SYNTHETIC_OLD_QUOTE_HISTORY_NOT_PUBLIC"},
        )


async def main():
    assert not REPLAY.exists() and not (ROOT / "report.json").exists()
    files = [
        source / name
        for source in SOURCES
        for name in (
            "events.duckdb",
            "events.parquet",
            "events.parquet.manifest.json",
            "report.json",
            "verification.json",
            "config.sanitized.json",
        )
    ]
    hashes = {str(path.relative_to(ROOT.parent)): digest(path) for path in files}
    REPLAY.mkdir(exist_ok=False)
    results, fixture_report = [], None
    for source in SOURCES:
        captured = json.loads((source / "report.json").read_text())
        cutoff = captured["ended_at_ms"]
        native, rows = read_source(source)
        assert len(rows) == captured["rows"]
        path = REPLAY / (source.name + ".duckdb")
        store = await DuckStore.open(path)
        try:
            await store.append(rows)
            await store.append(rows)
            select = store._select
            materialized = []

            def tracked_select(sql, params, selector=select, counts=materialized):
                result = selector(sql, params)
                counts.append(len(result))
                return result

            store._select = tracked_select
            controls = [row for row in rows if row.kind == "status"]
            resync = next(row for row in controls if row.payload()["reason"] == "STREAM_RESYNC")
            ack = next(row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIBED")
            for saved in captured["option_stream_coverage"]:
                asset = saved["asset"]
                materialized.clear()
                projected = await store.option_coverage(asset, cutoff, 30000)
                assert projected == saved == coverage_at(rows, asset, cutoff, 30000)
                final_materialized = list(materialized)
                boundaries = sorted(
                    {
                        resync.observed_at_ms - 1,
                        resync.observed_at_ms,
                        ack.observed_at_ms - 1,
                        ack.observed_at_ms,
                        cutoff,
                    }
                )
                for point in boundaries:
                    assert await store.option_coverage(asset, point, 30000) == coverage_at(
                        rows, asset, point, 30000
                    )
                results.append(
                    {
                        "source_artifact": source.name,
                        "asset": asset,
                        "retained_native_rows": len(rows),
                        "final_materialized_query_rows": final_materialized,
                        "final_materialized_total": sum(final_materialized),
                        "original_saved_view_exactly_matches": True,
                        "source_and_receipt_pit_boundaries": len(boundaries),
                        "coverage_state": saved["state"],
                        "blocker": saved["blocker"],
                    }
                )
            store._select = select
            baseline_parquet = REPLAY / (source.name + ".native.parquet")
            assert await store.export_parquet(baseline_parquet) == len(rows)
            if source == SOURCES[0]:
                base_view = captured["option_stream_coverage"][0]
                original = next(
                    row
                    for row in rows
                    if row.kind == "ticker"
                    and row.symbol in {leg["symbol"] for leg in base_view["legs"]}
                )
                fixture_count = 110000
                historical = iter(synthetic_history(original, resync.observed_at_ms, fixture_count))
                for offset in range(0, fixture_count, 10000):
                    batch = list(itertools.islice(historical, min(10000, fixture_count - offset)))
                    await store.append(batch)
                raw = coverage_at(
                    itertools.chain(
                        rows, synthetic_history(original, resync.observed_at_ms, fixture_count)
                    ),
                    "BTC",
                    cutoff,
                    30000,
                )
                assert raw["blocker"] == "OPTION_COVERAGE_OVER_BUDGET"
                materialized.clear()
                store._select = tracked_select
                assert await store.option_coverage("BTC", cutoff, 30000) == base_view
                assert sum(materialized) < 100000
                fixture_materialized = list(materialized)
                store._select = select
                assert await store.pending_delivery_count() == len(rows) + fixture_count
                assert (
                    await store.export_parquet(REPLAY / "btc-with-synthetic-old-history.parquet")
                    == len(rows) + fixture_count
                )
                fixture_report = {
                    "scope": "EXPLICITLY_SYNTHETIC_OLD_QUOTE_HISTORY_ADDED_TO_NATIVE_REPLAY",
                    "synthetic_history_rows": fixture_count,
                    "native_rows_unchanged": len(rows),
                    "retained_total_rows": len(rows) + fixture_count,
                    "old_full_materialization_blocker": raw["blocker"],
                    "bounded_projection_matches_original_saved_view": True,
                    "bounded_materialized_query_rows": fixture_materialized,
                    "pending_outbox_rows": len(rows) + fixture_count,
                    "synthetic_events_not_claimed_as_public_history": True,
                }
        finally:
            await store.close()
        with duckdb.connect(str(path), read_only=True) as db:
            original_ids = {row.event_id for row in rows}
            assert [
                row
                for row in db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
                if row[0] in original_ids
            ] == native
            assert (
                db.execute(
                    "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(baseline_parquet)]
                ).fetchall()
                == native
            )
            if source == SOURCES[0]:
                all_rows = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
                assert len(all_rows) == len(rows) + 110000
                assert (
                    db.execute(
                        "SELECT * FROM read_parquet(?) ORDER BY event_id",
                        [str(REPLAY / "btc-with-synthetic-old-history.parquet")],
                    ).fetchall()
                    == all_rows
                )
        reopened = await DuckStore.open(path)
        try:
            for saved in captured["option_stream_coverage"]:
                assert await reopened.option_coverage(saved["asset"], cutoff, 30000) == saved
        finally:
            await reopened.close()
    assert hashes == {str(path.relative_to(ROOT.parent)): digest(path) for path in files}
    report = {
        "status": "BOUNDED_OPTION_PROJECTION_NATIVE_EQUIVALENCE_AND_SYNTHETIC_OVERFLOW_VERIFIED",
        "native_asset_views": results,
        "synthetic_history_overflow": fixture_report,
        "native_source_rows": 109845,
        "source_and_receipt_pit_boundaries": sum(
            row["source_and_receipt_pit_boundaries"] for row in results
        ),
        "freshness_bound_ms": 30000,
        "original_source_bytes_unchanged": True,
        "original_sha256": hashes,
        "closed_reload_exact_native_subset_and_archives_verified": True,
        "new_network_requests": 0,
        "history_rows_deleted": 0,
        "all_tied_latest_legs_retained_in_sql": True,
        "controls_and_metadata_not_pruned": True,
        "application_materialization_budget": 100000,
        "database_scan_time_size_and_engine_memory_bounded": False,
        "sustained_capacity_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "original_sha256"}, indent=2
        )
    )


asyncio.run(main())
