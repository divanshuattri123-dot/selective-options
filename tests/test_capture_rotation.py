import asyncio
import json
import threading

import duckdb
import pytest

from selective_options import active_rotation, ingestion, rotation_runtime
from selective_options.archive_rotation import validate_rotation_catalog
from selective_options.bus import EventBus
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.ingestion import capture
from selective_options.rotation_restore import restore_rotation
from selective_options.rotation_runtime import RotationRuntime
from selective_options.store import DuckStore


def config(**changes):
    return AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
            **changes,
        },
    )


def event(index):
    clock = now_ms() - 100
    return make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=clock,
        observed_at_ms=clock,
        payload={"price": 100 + index},
    )


async def idle(*args):
    await asyncio.Event().wait()


def disable_network(monkeypatch, producer=idle):
    monkeypatch.setattr(ingestion.DeribitCollector, "run", producer)
    monkeypatch.setattr(ingestion, "poll_chains", idle)


async def test_default_capture_creates_no_runtime_task_barrier_or_rotation_files(
    tmp_path, monkeypatch
):
    disable_network(monkeypatch)

    async def forbidden(*args):
        raise AssertionError("disabled archive task created")

    monkeypatch.setattr(RotationRuntime, "run", forbidden)
    monkeypatch.setattr(EventBus, "flush", forbidden)
    output = tmp_path / "capture"
    report = await capture(config(), output, 0.03)
    assert report["status"] == "CAPTURE_FINISHED"
    assert report["archive_rotation"]["status"] == "DISABLED"
    assert report["archive_rotation"]["completed_snapshots"] == 0
    assert not (output / "archive-rotations").exists()
    assert report["trading_enabled"] is report["milestone_2_complete"] is False


async def test_two_real_committed_snapshots_while_later_events_continue_then_normal_shutdown(
    tmp_path, monkeypatch
):
    first_finished = asyncio.Event()
    second_queued = asyncio.Event()
    second_finished = asyncio.Event()
    third_queued = asyncio.Event()
    rows = [event(i) for i in range(3)]

    async def produce(self):
        await self.sink(rows[0])
        await first_finished.wait()
        await self.sink(rows[1])
        second_queued.set()
        await second_finished.wait()
        await self.sink(rows[2])
        third_queued.set()
        await idle()

    ticks = 0

    async def synthetic_due(self):
        nonlocal ticks
        ticks += 1
        if ticks == 1:
            return True
        if ticks == 2:
            await second_queued.wait()
            return True
        await self.stopped.wait()
        return False

    original = rotation_runtime.rotate_active_store

    async def rotate(*args, **kwargs):
        report = await original(*args, **kwargs)
        assert not args[0]._closed
        (first_finished if not first_finished.is_set() else second_finished).set()
        return report

    disable_network(monkeypatch, produce)
    monkeypatch.setattr(RotationRuntime, "_wait_due", synthetic_due)
    monkeypatch.setattr(rotation_runtime, "rotate_active_store", rotate)
    output = tmp_path / "capture"
    report = await capture(
        config(archive_rotation_interval_s=1, archive_rotation_max_snapshots=2), output, 2
    )
    assert third_queued.is_set()
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 3
    assert report["archive_rotation"]["status"] == "STOPPED"
    snapshots = report["archive_rotation"]["snapshots"]
    assert [item["rows"] for item in snapshots] == [1, 2]
    assert report["archive_rotation"]["attempts"] == 2
    for index, snapshot in enumerate(snapshots):
        directory = output / snapshot["directory"]
        catalog = validate_rotation_catalog(directory / "rotation")
        assert catalog.total_rows == index + 1
        restored = tmp_path / f"restore-{index}"
        proof = await restore_rotation(directory / "rotation" / "catalog.json", restored)
        assert proof["rows"] == index + 1
        store = await DuckStore.open(restored / "events.duckdb")
        try:
            saved = (await store.snapshot("BTC", snapshot["as_of_ms"], "public_reference")).rows()
            assert {row.event_id: row for row in saved} == {
                row.event_id: row for row in rows[: index + 1]
            }
            assert await store.pending_delivery_count() == index + 1
        finally:
            await store.close()
    assert all(snapshot["history_complete"] is False for snapshot in snapshots)
    assert report["ready_for_signals"] is report["trading_enabled"] is False


async def test_next_due_tick_exhausts_total_budget_and_persists_failed_capture_report(
    tmp_path, monkeypatch
):
    async def produce(self):
        await self.sink(event(0))
        await idle()

    async def due(self):
        await asyncio.sleep(0)
        return True

    disable_network(monkeypatch, produce)
    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="SNAPSHOT_BUDGET_EXHAUSTED"):
        await capture(
            config(archive_rotation_interval_s=1, archive_rotation_max_snapshots=1), output, 5
        )
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["archive_rotation"]["completed_snapshots"] == 1
    assert report["archive_rotation"]["attempts"] == 1
    assert not (output / "archive-rotations" / "snapshot-000001").exists()
    with duckdb.connect(str(output / "events.duckdb")) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone() == (1,)


async def test_rotation_error_propagates_and_records_failed_attempt_before_closing_store(
    tmp_path, monkeypatch
):
    disable_network(monkeypatch)

    async def due(self):
        return True

    async def failure(store, output, cutoff, **options):
        assert not store._closed
        raise RuntimeError("simulated checkpoint failure")

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(rotation_runtime, "rotate_active_store", failure)
    output = tmp_path / "capture"
    with pytest.raises(RuntimeError, match="checkpoint failure"):
        await capture(config(archive_rotation_interval_s=1), output, 5)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["archive_rotation"]["last_attempt"]["status"] == "FAILED"
    assert report["archive_rotation"]["completed_snapshots"] == 0
    with duckdb.connect(str(output / "events.duckdb")) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone() == (0,)


async def test_unexpected_normal_archive_worker_exit_fails_capture(tmp_path, monkeypatch):
    disable_network(monkeypatch)

    async def early(self):
        return

    monkeypatch.setattr(RotationRuntime, "run", early)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="archive worker exited"):
        await capture(config(archive_rotation_interval_s=1), output, 5)
    assert json.loads((output / "report.json").read_text())["status"] == "FAILED"


async def test_capture_does_not_wait_future_long_rotation_interval_during_shutdown(
    tmp_path, monkeypatch
):
    disable_network(monkeypatch)
    output = tmp_path / "capture"
    report = await asyncio.wait_for(
        capture(config(archive_rotation_interval_s=86400), output, 0.03), 1
    )
    assert report["archive_rotation"]["status"] == "STOPPED"
    assert report["archive_rotation"]["attempts"] == 0


async def test_repeated_capture_cancellation_drains_real_snapshot_worker_before_store_close(
    tmp_path, monkeypatch
):
    entered = threading.Event()
    release = threading.Event()
    closed = []
    original = active_rotation._snapshot
    close = DuckStore.close

    def blocked(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    async def closing(self):
        assert not entered.is_set() or release.is_set()
        closed.append(True)
        await close(self)

    async def due(self):
        return True

    async def produce(self):
        await self.sink(event(0))
        await idle()

    disable_network(monkeypatch, produce)
    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(active_rotation, "_snapshot", blocked)
    monkeypatch.setattr(DuckStore, "close", closing)
    output = tmp_path / "capture"
    task = asyncio.create_task(capture(config(archive_rotation_interval_s=1), output, 10))
    for _ in range(500):
        if entered.is_set():
            break
        await asyncio.sleep(0.01)
    assert entered.is_set()
    task.cancel()
    await asyncio.sleep(0.03)
    task.cancel()
    await asyncio.sleep(0.03)
    assert not task.done() and not closed
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["archive_rotation"]["last_attempt"]["status"] == "CANCELLED"
    assert report["archive_rotation"]["completed_snapshots"] == 0
    snapshot = output / "archive-rotations" / "snapshot-000000"
    assert not (snapshot / "active-rotation.json").exists()
    assert (snapshot / "active-rotation-failed.json").exists()
    with duckdb.connect(str(output / "events.duckdb")) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone() == (1,)


async def test_later_backdated_rows_commit_during_partition_validation_without_changing_snapshot(
    tmp_path, monkeypatch
):
    partition_entered = asyncio.Event()
    later_committed = asyncio.Event()
    rows = [event(i) for i in range(2)]
    original = active_rotation.rotate_archive
    ticks = 0

    async def due(self):
        nonlocal ticks
        ticks += 1
        if ticks == 1:
            return True
        await self.stopped.wait()
        return False

    async def produce(self):
        await self.sink(rows[0])
        await partition_entered.wait()
        await self.sink(rows[1])
        await self.sink.__self__.flush()
        later_committed.set()
        await idle()

    async def partition(*args, **kwargs):
        partition_entered.set()
        await asyncio.wait_for(later_committed.wait(), 1)
        return await original(*args, **kwargs)

    disable_network(monkeypatch, produce)
    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(active_rotation, "rotate_archive", partition)
    output = tmp_path / "capture"
    report = await capture(config(archive_rotation_interval_s=1), output, 1)
    assert later_committed.is_set() and report["rows"] == 2
    snapshot = report["archive_rotation"]["snapshots"][0]
    assert snapshot["rows"] == 1
    assert all(max(row.event_time_ms, row.observed_at_ms) <= snapshot["as_of_ms"] for row in rows)
    with duckdb.connect(
        str(output / snapshot["directory"] / "snapshot.duckdb"), read_only=True
    ) as db:
        assert db.execute("SELECT event_id FROM events").fetchall() == [(rows[0].event_id,)]


async def test_repeated_capture_cancellation_at_real_commit_barrier_drains_writer_before_close(
    tmp_path, monkeypatch
):
    entered = threading.Event()
    release = threading.Event()
    barrier_entered = asyncio.Event()
    closed = []
    insert = DuckStore._insert
    close = DuckStore.close
    flush = EventBus.flush

    def blocked(self, rows, exact=False):
        insert(self, rows, exact)
        entered.set()
        assert release.wait(5)

    async def barrier(self):
        barrier_entered.set()
        await flush(self)

    async def closing(self):
        assert not entered.is_set() or release.is_set()
        closed.append(True)
        await close(self)

    async def due(self):
        return True

    async def produce(self):
        await self.sink(event(0))
        await idle()

    disable_network(monkeypatch, produce)
    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(EventBus, "flush", barrier)
    monkeypatch.setattr(DuckStore, "_insert", blocked)
    monkeypatch.setattr(DuckStore, "close", closing)
    output = tmp_path / "capture"
    task = asyncio.create_task(capture(config(archive_rotation_interval_s=1), output, 10))
    await barrier_entered.wait()
    for _ in range(500):
        if entered.is_set():
            break
        await asyncio.sleep(0.01)
    assert entered.is_set()
    task.cancel()
    await asyncio.sleep(0.03)
    task.cancel()
    await asyncio.sleep(0.03)
    assert not task.done() and not closed
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["archive_rotation"]["attempts"] == 0
    assert report["rows"] == 1
    assert not (output / "archive-rotations").exists()
