"""Current-manifest metadata with preserved native/PIT and labelled synthetic old generations."""

import asyncio
import hashlib
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent, make_event
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parents[1]
REPLAY = ROOT.parent / "metadata-projection-replay-01"
SOURCES = [
    ROOT.parent / name for name in ("reused-validation-public-01", "multiasset-options-public-01")
]
EXPECTED_WHEEL = "1baa6dafea29a060a1c976645f9f2cb7db8ea478daee5e7c2a8bea95e7dc0af7"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def old_store():
    wheel = PROJECT / "../../work/dist/selective_options-0.3.35-py3-none-any.whl"
    assert digest(wheel) == EXPECTED_WHEEL
    target = PROJECT / "../../work/baseline_0335"
    target.mkdir(exist_ok=False)
    with zipfile.ZipFile(wheel) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.startswith("selective_options/") and name.endswith(".py")
        ]
        assert len(names) == 52 and archive.testzip() is None
        for name in names:
            assert len(Path(name).parts) == 2
            (target / Path(name).name).write_bytes(archive.read(name))
    spec = importlib.util.spec_from_file_location(
        "baseline0335", target / "__init__.py", submodule_search_locations=[str(target)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["baseline0335"] = module
    spec.loader.exec_module(module)
    return __import__("baseline0335.store", fromlist=["DuckStore"])


def synthetic_metadata(original, start, count):
    for index in range(count):
        timestamp = start - count - 1 + index
        yield make_event(
            source="deribit",
            environment="testnet",
            asset=original.asset,
            symbol=original.symbol,
            kind="instrument",
            event_time_ms=timestamp,
            observed_at_ms=timestamp,
            timestamp_kind="receipt",
            payload=original.payload(),
        )


async def main():
    assert not REPLAY.exists() and not (ROOT / "report.json").exists()
    legacy = old_store()
    originals = [
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
    original_hashes = {str(path.relative_to(ROOT.parent)): digest(path) for path in originals}
    REPLAY.mkdir(exist_ok=False)
    findings, fixture_report = [], None
    for source in SOURCES:
        capture = json.loads((source / "report.json").read_text())
        cutoff = capture["ended_at_ms"]
        with duckdb.connect(str(source / "events.duckdb"), read_only=True) as db:
            cursor = db.execute("SELECT * FROM events ORDER BY event_id")
            columns = [column[0] for column in cursor.description]
            native = cursor.fetchall()
            rows = [
                MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
            ]
        assert len(rows) == capture["rows"]
        path = REPLAY / (source.name + ".duckdb")
        store = await DuckStore.open(path)
        try:
            await store.append(rows, exact=True)
            await store.append(rows, exact=True)
            original_select, read_counts = store._select, []

            def tracked(sql, params, selector=original_select, counts=read_counts):
                result = selector(sql, params)
                counts.append(len(result))
                return result

            store._select = tracked
            resync = next(
                row
                for row in rows
                if row.kind == "status" and row.payload().get("reason") == "STREAM_RESYNC"
            )
            ack = next(
                row
                for row in rows
                if row.kind == "status" and row.payload().get("reason") == "STREAM_SUBSCRIBED"
            )
            for saved in capture["option_stream_coverage"]:
                asset = saved["asset"]
                read_counts.clear()
                assert (
                    await store.option_coverage(asset, cutoff, 30000)
                    == saved
                    == coverage_at(rows, asset, cutoff, 30000)
                )
                final_reads = list(read_counts)
                points = sorted(
                    {
                        resync.observed_at_ms - 1,
                        resync.observed_at_ms,
                        ack.observed_at_ms - 1,
                        ack.observed_at_ms,
                        cutoff,
                    }
                )
                for point in points:
                    assert await store.option_coverage(asset, point, 30000) == coverage_at(
                        rows, asset, point, 30000
                    )
                findings.append(
                    {
                        "asset": asset,
                        "source_artifact": source.name,
                        "native_rows": len(rows),
                        "materialized_query_rows": final_reads,
                        "materialized_total": sum(final_reads),
                        "exact_original_final_view": True,
                        "pit_boundaries": len(points),
                        "coverage_state": saved["state"],
                        "blocker": saved["blocker"],
                    }
                )
            store._select = original_select
            original_archive = REPLAY / (source.name + ".native.parquet")
            assert await store.export_parquet(original_archive) == len(rows)
            if source == SOURCES[0]:
                original = next(row for row in rows if row.kind == "instrument")
                synthetic_count = 110000
                pending = []
                fixture_start = min(row.observed_at_ms for row in rows) - 1000
                for row in synthetic_metadata(original, fixture_start, synthetic_count):
                    pending.append(row)
                    if len(pending) == 10000:
                        await store.append(pending)
                        pending.clear()
                assert not pending
                async with store._lock:
                    old_view = await complete_thread(
                        legacy.DuckStore._option_coverage,
                        store,
                        "BTC",
                        cutoff,
                        30000,
                        3600000,
                        100000,
                    )
                assert old_view["blocker"] == "OPTION_COVERAGE_OVER_BUDGET"
                read_counts.clear()
                store._select = tracked
                assert (
                    await store.option_coverage("BTC", cutoff, 30000)
                    == capture["option_stream_coverage"][0]
                )
                assert read_counts == [3, 1010, 2020]
                store._select = original_select
                assert await store.pending_delivery_count() == len(rows) + synthetic_count
                assert (
                    await store.export_parquet(REPLAY / "btc-with-synthetic-old-metadata.parquet")
                    == len(rows) + synthetic_count
                )
                fixture_report = {
                    "scope": "SYNTHETIC_OBSOLETE_METADATA_GENERATIONS_NOT_PUBLIC_HISTORY",
                    "synthetic_metadata_rows": synthetic_count,
                    "unchanged_native_rows": len(rows),
                    "retained_rows_and_pending_outbox": len(rows) + synthetic_count,
                    "old_0335_projection_blocker": old_view["blocker"],
                    "current_materialized_query_rows": list(read_counts),
                    "current_materialized_total": sum(read_counts),
                    "original_final_view_exactly_matches": True,
                    "native_payloads_cloned_only_for_labelled_fixture": True,
                }
        finally:
            await store.close()
        with duckdb.connect(str(path), read_only=True) as db:
            all_rows = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            source_ids = {row.event_id for row in rows}
            assert [row for row in all_rows if row[0] in source_ids] == native
            assert (
                db.execute(
                    "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(original_archive)]
                ).fetchall()
                == native
            )
            assert db.execute(
                "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
            ).fetchone()[0] == len(all_rows)
            if source == SOURCES[0]:
                assert len(all_rows) == len(rows) + 110000
                assert (
                    db.execute(
                        "SELECT * FROM read_parquet(?) ORDER BY event_id",
                        [str(REPLAY / "btc-with-synthetic-old-metadata.parquet")],
                    ).fetchall()
                    == all_rows
                )
        reopened = await DuckStore.open(path)
        try:
            for saved in capture["option_stream_coverage"]:
                assert await reopened.option_coverage(saved["asset"], cutoff, 30000) == saved
        finally:
            await reopened.close()
    assert original_hashes == {
        str(path.relative_to(ROOT.parent)): digest(path) for path in originals
    }
    result = {
        "status": "EXACT_CURRENT_METADATA_PROJECTION_NATIVE_PIT_AND_SYNTHETIC_HISTORY_VERIFIED",
        "native_asset_views": findings,
        "native_source_rows": 109845,
        "pit_boundaries": sum(row["pit_boundaries"] for row in findings),
        "synthetic_metadata_history": fixture_report,
        "baseline_0335_wheel_sha256": EXPECTED_WHEEL,
        "original_sha256": original_hashes,
        "original_bytes_unchanged": True,
        "closed_reload_exact_native_subset_and_archives": True,
        "new_network_requests": 0,
        "history_or_outbox_rows_deleted": 0,
        "stale_bounds_or_timestamps_relaxed": False,
        "all_eligible_control_integrity_checks_retained": True,
        "control_history_remains_budgeted": True,
        "database_scan_time_size_or_engine_memory_bounded": False,
        "sustained_capacity_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {key: value for key, value in result.items() if key != "original_sha256"}, indent=2
        )
    )


asyncio.run(main())
