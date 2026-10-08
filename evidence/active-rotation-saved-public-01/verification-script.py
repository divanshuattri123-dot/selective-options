"""Finite offline supervised capture with committed active rotation snapshots.

Native saved records retain original clocks. Collectors, scheduler ticks and wall
clock progression are explicitly mocked; no network or sustained history claim.
"""

import argparse
import asyncio
import json
import math
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from unittest.mock import patch

import duckdb
from verify_rotation import (
    independent_news,
    independent_supply,
    quality_views,
    read_rows,
    source_hashes,
)

from selective_options import active_rotation, archive_rotation, ingestion, rotation_runtime
from selective_options.archive import file_digest
from selective_options.bus import EventBus
from selective_options.collectors import DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.rotation_restore import restore_rotation
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = Path(__file__).resolve()
NATIVE_HELPER = ROOT / "scripts/verify_rotation.py"
CODE_FILES = (
    "active_rotation.py",
    "archive_rotation.py",
    "rotation_runtime.py",
    "ingestion.py",
    "config.py",
    "rotation_restore.py",
    "store.py",
    "bus.py",
)


@dataclass(frozen=True)
class Sources:
    rows: tuple[MarketEvent, ...]
    hashes: dict[str, dict[str, str]]
    facts: dict[str, object]


@dataclass
class ReplayState:
    clock: int
    bus: EventBus | None = None
    store: DuckStore | None = None
    initial_committed: asyncio.Event = field(default_factory=asyncio.Event)
    snapshot_closed: asyncio.Event = field(default_factory=asyncio.Event)
    later_committed: asyncio.Event = field(default_factory=asyncio.Event)
    second_rotated: asyncio.Event = field(default_factory=asyncio.Event)
    tail_committed: asyncio.Event = field(default_factory=asyncio.Event)
    barriers: list[dict[str, object]] = field(default_factory=list)
    active_calls: list[dict[str, object]] = field(default_factory=list)
    tick_count: int = 0
    closed_calls: int = 0
    later_commit_before_first_partition_validation: bool = False
    producer_cancelled: bool = False
    runtime_cancelled: bool = False


def preflight(news: Path, supply: Path, output: Path) -> Sources:
    scratch = (ROOT.parents[1] / "work/active-rotation-0343").resolve()
    destination = output.resolve()
    if destination != (
        ROOT / "evidence/active-rotation-saved-public-01"
    ).resolve() and not destination.is_relative_to(scratch):
        raise ValueError(
            "output must be active-rotation-saved-public-01 or within workspace scratch"
        )
    if destination.exists() or any(
        destination == path.resolve() or path.resolve().is_relative_to(destination)
        for path in (news, supply)
    ):
        raise FileExistsError("refusing to overwrite active or original evidence")
    before = {"news": source_hashes(news), "supply": source_hashes(supply)}
    news_rows = read_rows(news / "events.duckdb", news / "events.parquet")
    supply_rows = read_rows(supply / "events.duckdb", supply / "events.parquet")
    facts: dict[str, object] = {
        "news": independent_news(news, news_rows),
        "supply": independent_supply(supply, supply_rows),
    }
    return Sources(news_rows + supply_rows, before, facts)


def fixtures(native: tuple[MarketEvent, ...]) -> tuple[tuple[MarketEvent, ...], int, int, int]:
    first = max(max(row.event_time_ms, row.observed_at_ms) for row in native) + 1000
    second, ended = first + 4000, first + 5000
    clocks = (
        (first + 3000, first),
        (first, first + 3000),
        (first - 1000, second),
        (first - 500, ended),
    )
    events = tuple(
        make_event(
            source="active_rotation_offline_fixture",
            environment="testnet",
            asset="FIXTURE",
            symbol=f"ACTIVE_BOUNDARY_{index}",
            kind="status",
            event_time_ms=source_clock,
            observed_at_ms=receipt,
            payload={
                "provenance": "synthetic_offline_verification_fixture",
                "fixture_index": index,
                "purpose": "committed cutoff availability and post-snapshot producer progress",
            },
        )
        for index, (source_clock, receipt) in enumerate(clocks)
    )
    return events, first, second, ended


async def live_rows(store: DuckStore, identities: list[str]) -> tuple[MarketEvent, ...]:
    rows = await store.read_events(identities)
    return tuple(sorted(rows, key=lambda event: event.event_id))


async def live_outbox(store: DuckStore) -> list[dict[str, object]]:
    async with store._lock:
        return await complete_thread(
            store._select, "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id", []
        )


async def run_capture(
    output: Path,
    native: tuple[MarketEvent, ...],
    duration_s: float = 6.0,
    *,
    max_snapshots: int = 2,
    partition_ms: int = 86400000,
    max_partitions: int = 2,
    fail_after_snapshot: bool = False,
) -> tuple[dict[str, object], ReplayState, tuple[MarketEvent, ...]]:
    fixture, first, second, ended = fixtures(native)
    initial = (*native, *fixture[:2])
    all_events = tuple(sorted((*native, *fixture), key=lambda event: event.event_id))
    state = ReplayState(first)
    actual_active = active_rotation.rotate_active_store
    actual_closed = archive_rotation.rotate_archive
    native_id = native[0].event_id

    class ObservedBus(EventBus):
        def __init__(self, capacity: int, timeout_s: float) -> None:
            super().__init__(capacity, timeout_s)
            state.bus = self

        async def flush(self) -> None:
            queued = self.queue.qsize()
            await super().flush()
            state.barriers.append(
                {
                    "clock_ms": state.clock,
                    "queued_before": queued,
                    "queue_after": self.queue.qsize(),
                    "completed_after_storage": True,
                }
            )

    async def producer(collector: DeribitCollector) -> None:
        try:
            for row in initial:
                await collector.sink(row)
            assert state.bus is not None
            await state.bus.flush()
            state.initial_committed.set()
            await state.snapshot_closed.wait()
            if fail_after_snapshot:
                raise IntegrityError("SYNTHETIC_OFFLINE_COLLECTOR_FAILURE")
            state.clock = second
            await collector.sink(fixture[2])
            await state.bus.flush()
            state.later_committed.set()
            await state.second_rotated.wait()
            state.clock = ended
            await collector.sink(fixture[3])
            await state.bus.flush()
            state.tail_committed.set()
            await asyncio.Event().wait()
        finally:
            state.producer_cancelled = True

    async def idle_chains(*args: object) -> None:
        await asyncio.Event().wait()

    async def observe_closed(
        database: Path,
        destination: Path,
        as_of_ms: int,
        partition_ms: int = 86400000,
        batch_size: int = 500,
        max_partitions: int = 366,
    ) -> dict[str, object]:
        state.closed_calls += 1
        if state.closed_calls == 1:
            # The active snapshot is already closed, while the real consumer can
            # still commit a queued later record before partition validation.
            if read_rows(database) != tuple(sorted(native, key=lambda event: event.event_id)):
                raise IntegrityError("ACTIVE_PROOF_FIRST_CLOSED_SNAPSHOT_MISMATCH")
            state.snapshot_closed.set()
            await state.later_committed.wait()
            assert state.store is not None
            later = await state.store.read_events([fixture[2].event_id])
            state.later_commit_before_first_partition_validation = later == (fixture[2],)
            if not state.later_commit_before_first_partition_validation:
                raise IntegrityError("ACTIVE_PROOF_PRODUCER_CANNOT_COMMIT_AFTER_SNAPSHOT")
        return await actual_closed(
            database, destination, as_of_ms, partition_ms, batch_size, max_partitions
        )

    async def observe_active(
        store: DuckStore,
        destination: Path,
        as_of_ms: int,
        partition_ms: int = 86400000,
        batch_size: int = 500,
        max_partitions: int = 366,
    ) -> dict[str, object]:
        state.store = store
        if not state.active_calls:
            await store.acknowledge_delivery([native_id])
        before_rows = await live_rows(store, [row.event_id for row in all_events])
        expected = tuple(
            row
            for row in before_rows
            if row.event_time_ms <= as_of_ms and row.observed_at_ms <= as_of_ms
        )
        before_outbox = await live_outbox(store)
        result = await actual_active(
            store, destination, as_of_ms, partition_ms, batch_size, max_partitions
        )
        snapshot = read_rows(destination / "snapshot.duckdb")
        after_outbox = await live_outbox(store)
        retained = {str(row["event_id"]): row["delivered"] for row in after_outbox}
        if snapshot != expected or any(
            retained.get(str(row["event_id"])) != row["delivered"] for row in before_outbox
        ):
            raise IntegrityError("ACTIVE_PROOF_COHORT_OR_SOURCE_ACK_CHANGED")
        # Real store APIs still succeed; the production runtime has not closed it.
        await store.append([])
        state.active_calls.append(
            {
                "as_of_ms": as_of_ms,
                "directory": destination.name,
                "source_rows_at_snapshot_request": len(before_rows),
                "expected_event_ids": [row.event_id for row in expected],
                "snapshot_rows": len(snapshot),
                "source_outbox_before": before_outbox,
                "source_outbox_after": after_outbox,
                "store_open_after_rotation": not store._closed,
                "result": result,
            }
        )
        if len(state.active_calls) == 2:
            state.second_rotated.set()
        return result

    class ControlledRuntime(rotation_runtime.RotationRuntime):
        async def _wait_due(self) -> bool:
            # Only scheduling ticks are mocked. Production run(), FIFO commit
            # barriers, cutoff selection, active snapshot and export are used.
            if state.tick_count == 0:
                await state.initial_committed.wait()
            elif state.tick_count == 1:
                await state.later_committed.wait()
            else:
                await self.stopped.wait()
                return False
            state.tick_count += 1
            return True

        async def run(self) -> None:
            try:
                await super().run()
            finally:
                state.runtime_cancelled = True

    config = AppConfig.model_validate(
        {
            "ingestion": {
                "reference_sources": [],
                "policy_news_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "flush_interval_s": 0.005,
                "storage_batch_size": 3,
                "queue_capacity": 64,
                "queue_timeout_s": 15,
                "archive_rotation_interval_s": 1.0,
                "archive_rotation_partition_ms": partition_ms,
                "archive_rotation_max_partitions": max_partitions,
                "archive_rotation_max_snapshots": max_snapshots,
            }
        }
    )
    with (
        patch.object(ingestion, "EventBus", ObservedBus),
        patch.object(DeribitCollector, "run", producer),
        patch.object(ingestion, "poll_chains", idle_chains),
        patch.object(ingestion, "RotationRuntime", ControlledRuntime),
        patch.object(ingestion, "now_ms", lambda: state.clock),
        patch.object(rotation_runtime, "now_ms", lambda: state.clock),
        patch.object(rotation_runtime, "rotate_active_store", observe_active),
        patch.object(active_rotation, "rotate_archive", observe_closed),
    ):
        report = await ingestion.capture(config, output, duration_s=duration_s)
    if (
        len(state.active_calls) != 2
        or not state.tail_committed.is_set()
        or state.bus is None
        or not state.bus.queue.empty()
        or not state.producer_cancelled
        or not state.runtime_cancelled
    ):
        raise IntegrityError("ACTIVE_PROOF_CAPTURE_DRAIN_OR_ROTATIONS_INCOMPLETE")
    return report, state, all_events


async def verify(
    news: Path, supply: Path, output: Path, duration_s: float = 6.0
) -> dict[str, object]:
    if not math.isfinite(duration_s) or not 0 < duration_s <= 60:
        raise ValueError("offline capture duration must be finite, positive and at most 60 seconds")
    saved = await complete_thread(preflight, news, supply, output)
    destination = await complete_thread(output.resolve)
    before_code = {name: file_digest(ROOT / "src/selective_options" / name) for name in CODE_FILES}
    before_verifier, before_helper = file_digest(VERIFIER), file_digest(NATIVE_HELPER)
    legacy_zip = ROOT.parent / "selective-options-0.3.42.zip"
    before_legacy_zip = file_digest(legacy_zip)
    destination.mkdir(parents=True, exist_ok=False)
    capture_output = destination / "capture"
    capture_report, state, all_rows = await run_capture(capture_output, saved.rows, duration_s)
    source_rows = read_rows(capture_output / "events.duckdb", capture_output / "events.parquet")
    if (
        source_rows != all_rows
        or capture_report["status"] != "CAPTURE_FINISHED"
        or capture_report["rows"] != 32
        or capture_report["redis_delivery_pending"] != 31
    ):
        raise IntegrityError("ACTIVE_PROOF_FINAL_CAPTURE_NOT_EXACT")
    if capture_report["ready_for_signals"] is not False:
        raise IntegrityError("ACTIVE_PROOF_CAPTURE_READINESS")
    closed_source_hash = file_digest(capture_output / "events.duckdb")
    restored = []
    pit_checks = []
    quality_checks = []
    original_ids = {row.event_id for row in saved.rows}
    for index, call in enumerate(state.active_calls):
        cohort = tuple(
            row for row in all_rows if row.event_id in cast(list[str], call["expected_event_ids"])
        )
        active_dir = capture_output / "archive-rotations" / str(call["directory"])
        snapshot_rows = read_rows(active_dir / "snapshot.duckdb")
        if snapshot_rows != cohort:
            raise IntegrityError("ACTIVE_PROOF_CLOSED_SNAPSHOT_CHANGED")
        catalog = await complete_thread(
            archive_rotation.validate_rotation_catalog, active_dir / "rotation"
        )
        archive_rows: list[MarketEvent] = []
        with duckdb.connect() as connection:
            for part in catalog.parts:
                cursor = connection.execute(
                    "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                    [str(active_dir / "rotation" / part.file)],
                )
                columns = [entry[0] for entry in cursor.description]
                rows = [
                    MarketEvent.model_validate(dict(zip(columns, values, strict=True)))
                    for values in cursor.fetchall()
                ]
                if (
                    any(
                        not part.start_ms
                        <= max(row.event_time_ms, row.observed_at_ms)
                        < part.end_ms
                        for row in rows
                    )
                    or len(rows) != part.rows
                ):
                    raise IntegrityError("ACTIVE_PROOF_PARTITION_AVAILABILITY")
                archive_rows.extend(rows)
        if tuple(
            sorted(archive_rows, key=lambda row: row.event_id)
        ) != cohort or catalog.total_rows != len(cohort):
            raise IntegrityError("ACTIVE_PROOF_EXACT_CATALOG_PARTS")
        restore_dir = destination / f"restored-{index:02}"
        restore = await restore_rotation(
            active_dir / "rotation/catalog.json", restore_dir, batch_size=3
        )
        restored_rows = read_rows(restore_dir / "events.duckdb")
        if restored_rows != cohort or tuple(
            row for row in restored_rows if row.event_id in original_ids
        ) != tuple(sorted(saved.rows, key=lambda row: row.event_id)):
            raise IntegrityError("ACTIVE_PROOF_RESTORE_NATIVE_ROWS")
        with duckdb.connect(str(restore_dir / "events.duckdb"), read_only=True) as connection:
            if connection.execute(
                "SELECT count(*),count(*) FILTER(WHERE delivered) FROM delivery_outbox"
            ).fetchone() != (len(cohort), 0):
                raise IntegrityError("ACTIVE_PROOF_RESTORED_OUTBOX_NOT_PENDING")
            cutoffs = sorted(
                {
                    value
                    for row in cohort
                    for value in (
                        row.event_time_ms - 1,
                        row.event_time_ms,
                        row.observed_at_ms - 1,
                        row.observed_at_ms,
                    )
                    if value > 0
                }
                | {cast(int, call["as_of_ms"])}
            )
            for cutoff in cutoffs:
                actual = connection.execute(
                    "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                    [cutoff, cutoff],
                ).fetchall()
                expected = [
                    (row.event_id,)
                    for row in cohort
                    if row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff
                ]
                if actual != expected:
                    raise IntegrityError("ACTIVE_PROOF_RESTORED_PIT")
                pit_checks.append(
                    {
                        "rotation": index,
                        "as_of_ms": cutoff,
                        "available_rows": len(actual),
                        "event_ids": [identity[0] for identity in actual],
                    }
                )
        cutoffs = sorted(
            {row.observed_at_ms for row in saved.rows if row.kind == "feed_health"}
            | {cast(int, call["as_of_ms"])}
        )
        quality_checks.append(
            {
                "rotation": index,
                "checks": await quality_views(restore_dir / "events.duckdb", cutoffs, cohort),
            }
        )
        restored.append(
            {
                "rotation": index,
                "snapshot_rows": len(cohort),
                "snapshot_sha256": file_digest(active_dir / "snapshot.duckdb"),
                "catalog_sha256": file_digest(active_dir / "rotation/catalog.json"),
                "parts": len(catalog.parts),
                "restore": restore,
            }
        )
    after_source = {"news": source_hashes(news), "supply": source_hashes(supply)}
    after_code = {name: file_digest(ROOT / "src/selective_options" / name) for name in CODE_FILES}
    if (
        after_source != saved.hashes
        or after_code != before_code
        or file_digest(VERIFIER) != before_verifier
        or file_digest(NATIVE_HELPER) != before_helper
        or file_digest(capture_output / "events.duckdb") != closed_source_hash
        or file_digest(legacy_zip) != before_legacy_zip
    ):
        raise IntegrityError("ACTIVE_PROOF_SOURCE_OR_CODE_CHANGED")
    fixture_rows = [row.model_dump() for row in all_rows if row.event_id not in original_ids]
    report: dict[str, object] = {
        "status": "OFFLINE_SUPERVISED_ACTIVE_ROTATION_VERIFIED",
        "real_public_requests_attempted": 0,
        "transport_provenance": "mocked_collectors_replaying_saved_native_records",
        "clock_provenance": "finite synthetic scheduling ticks and wall-clock progression; not elapsed market validation",
        "saved_native_records": 28,
        "synthetic_records": 4,
        "captured_records": 32,
        "capture_report": capture_report,
        "native_facts": saved.facts,
        "periodic_rotations": state.active_calls,
        "restores": restored,
        "pit_checks": pit_checks,
        "shared_quality_checks": quality_checks,
        "commit_barriers": state.barriers,
        "synthetic_events": fixture_rows,
        "later_commit_before_first_partition_validation": state.later_commit_before_first_partition_validation,
        "queue_empty_after_shutdown": state.bus is not None and state.bus.queue.empty(),
        "producer_cancelled_and_drained": state.producer_cancelled,
        "runtime_cancelled_and_drained": state.runtime_cancelled,
        "source_hashes_before": saved.hashes,
        "source_hashes_after": after_source,
        "code_sha256_before": before_code,
        "code_sha256_after": after_code,
        "verifier_sha256": before_verifier,
        "native_helper_sha256": before_helper,
        "closed_capture_sha256": closed_source_hash,
        "legacy_release_zip": legacy_zip.name,
        "legacy_release_sha256_before": before_legacy_zip,
        "legacy_release_sha256_after": file_digest(legacy_zip),
        "redis_delivery_claimed": False,
        "continuous_history_complete": False,
        "actual_provider_outage_recovery_verified": False,
        "seven_day_validation_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
        "limitation": "Two finite cumulative committed active-store snapshots under mocked collectors/ticks. Native clocks are unchanged. No purge, complete market history, actual sustained collection, Redis acknowledgement delivery or seven-day validation is established.",
    }
    (destination / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(VERIFIER, destination / "verification-script.py")
    (destination / "README.md").write_text(
        "# Offline supervised active rotation\n\nZero network requests. Actual capture supervision, bounded queue, production writer, FIFO commit barriers, periodic-runtime loop, active snapshots, sealed partitions and restore run with mocked collectors and scheduling ticks. Native news26 and supply2 rows retain original clocks, IDs and native facts. Four later/future-clock records are explicitly synthetic. First snapshot preserves28 eligible records from30 committed rows; a queued later31st record commits after snapshot closure and before first partition validation, proving the source store is open and unlocked. Second cumulative snapshot preserves31 records. A later32nd record is committed and included in final closed capture. One native source acknowledgement is introduced through the actual storage API for separation testing, never claimed as Redis delivery; each restored cohort queues every row. Synthetic clocks/ticks are not real elapsed market validation. No purge, complete history, actual provider recovery, sustained collection or seven-day validation is proved.\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=ROOT / "evidence/crypto-news-public-02")
    parser.add_argument(
        "--supply", type=Path, default=ROOT / "evidence/stablecoin-supply-public-01"
    )
    parser.add_argument(
        "--output", type=Path, default=ROOT / "evidence/active-rotation-saved-public-01"
    )
    parser.add_argument("--duration-s", type=float, default=6.0)
    args = parser.parse_args()
    report = asyncio.run(verify(args.news, args.supply, args.output, args.duration_s))
    print(
        json.dumps(
            {
                "status": report["status"],
                "rotations": len(cast(list[object], report["periodic_rotations"])),
                "capture_rows": report["captured_records"],
                "public_requests": 0,
            }
        )
    )


if __name__ == "__main__":
    main()
