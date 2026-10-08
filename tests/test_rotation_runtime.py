import asyncio
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options import rotation_runtime as module
from selective_options.bus import EventBus
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.rotation_runtime import RotationRuntime, drain_shutdown
from selective_options.store import DuckStore


def config(**changes):
    return AppConfig(ingestion={"archive_rotation_interval_s": 1, **changes})


def completed(cutoff, **changes):
    return {
        "status": "ACTIVE_ROTATION_VERIFIED",
        "as_of_ms": cutoff,
        "rows": 1,
        "parts": 1,
        "history_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
        **changes,
    }


async def test_disabled_runtime_has_no_timer_barrier_or_files(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)
    bus.flush = AsyncMock(side_effect=AssertionError("disabled barrier"))
    runtime = RotationRuntime(AppConfig(), bus, store, tmp_path, asyncio.Event())
    await runtime.run()
    assert runtime.summary()["status"] == "DISABLED"
    assert runtime.summary()["completed_snapshots"] == 0
    assert not (tmp_path / "archive-rotations").exists()
    await store.close()


async def test_barrier_precedes_cutoff_and_core_exact_options_and_new_basename(
    tmp_path, monkeypatch
):
    order = []
    stopped = asyncio.Event()
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)

    async def flush():
        order.append("committed")

    async def due(self):
        return not stopped.is_set()

    def clock():
        assert order == ["committed"]
        order.append("cutoff")
        return 9000

    async def rotate(actual_store, output, cutoff, **options):
        assert actual_store is store
        assert output == tmp_path / "archive-rotations" / "snapshot-000000"
        assert output.parent.is_dir() and not output.exists()
        assert cutoff == 9000
        assert options == {"partition_ms": 1000, "batch_size": 7, "max_partitions": 2}
        order.append("snapshot")
        stopped.set()
        return completed(cutoff)

    bus.flush = flush
    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(module, "now_ms", clock)
    monkeypatch.setattr(module, "rotate_active_store", rotate)
    runtime = RotationRuntime(
        config(
            archive_rotation_partition_ms=1000,
            archive_rotation_max_partitions=2,
            storage_batch_size=7,
        ),
        bus,
        store,
        tmp_path,
        stopped,
    )
    await runtime.run()
    summary = runtime.summary()
    assert order == ["committed", "cutoff", "snapshot"]
    assert summary["status"] == "STOPPED"
    assert summary["attempts"] == summary["completed_snapshots"] == 1
    assert summary["snapshots"][0]["directory"] == "archive-rotations/snapshot-000000"
    summary["snapshots"].clear()
    assert runtime.summary()["completed_snapshots"] == 1
    assert "Later commits can contain earlier clocks" in runtime.summary()["scope"]
    await store.close()


async def test_total_snapshot_budget_refuses_next_tick_before_barrier_or_extra_file(
    tmp_path, monkeypatch
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)
    bus.flush = AsyncMock()
    calls = []

    async def due(self):
        await asyncio.sleep(0)
        return True

    async def rotate(actual_store, output, cutoff, **options):
        calls.append(output.name)
        output.mkdir()
        return completed(cutoff)

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(module, "rotate_active_store", rotate)
    runtime = RotationRuntime(
        config(archive_rotation_max_snapshots=2), bus, store, tmp_path, asyncio.Event()
    )
    with pytest.raises(IntegrityError, match="SNAPSHOT_BUDGET_EXHAUSTED"):
        await runtime.run()
    assert calls == ["snapshot-000000", "snapshot-000001"]
    assert bus.flush.await_count == 2
    assert runtime.summary()["status"] == "FAILED"
    assert runtime.summary()["completed_snapshots"] == 2
    assert not (tmp_path / "archive-rotations" / "snapshot-000002").exists()
    await store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "PARTIAL"},
        {"as_of_ms": 1},
        {"history_complete": True},
        {"ready_for_signals": True},
        {"trading_enabled": True},
        {"trading_enabled": 0},
    ],
)
async def test_invalid_core_completion_never_enters_completed_snapshot_list(
    tmp_path, monkeypatch, changes
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)
    bus.flush = AsyncMock()

    async def due(self):
        return True

    async def rotate(actual_store, output, cutoff, **options):
        return completed(cutoff, **changes)

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(module, "rotate_active_store", rotate)
    runtime = RotationRuntime(config(), bus, store, tmp_path, asyncio.Event())
    with pytest.raises(IntegrityError, match="INVALID_COMPLETION"):
        await runtime.run()
    summary = runtime.summary()
    assert summary["completed_snapshots"] == 0 and summary["attempts"] == 1
    assert summary["last_attempt"]["status"] == "FAILED"
    await store.close()


async def test_requested_stop_during_long_interval_returns_promptly_without_core(
    tmp_path, monkeypatch
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    stopped = asyncio.Event()
    core = AsyncMock(side_effect=AssertionError("stop reached core"))
    monkeypatch.setattr(module, "rotate_active_store", core)
    runtime = RotationRuntime(
        config(archive_rotation_interval_s=86400), EventBus(10, 1), store, tmp_path, stopped
    )
    task = asyncio.create_task(runtime.run())
    await asyncio.sleep(0)
    stopped.set()
    await asyncio.wait_for(task, 0.5)
    assert runtime.summary()["status"] == "STOPPED"
    core.assert_not_awaited()
    await store.close()


async def test_real_interval_timeout_triggers_due_without_fabricated_clock(tmp_path):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    runtime = RotationRuntime(config(), EventBus(10, 1), store, tmp_path, asyncio.Event())
    before = asyncio.get_running_loop().time()
    assert await runtime._wait_due()
    assert asyncio.get_running_loop().time() - before >= 0.9
    await store.close()


@pytest.mark.parametrize("requested", [False, True])
async def test_barrier_cancellation_state_distinguishes_requested_stop(
    tmp_path, monkeypatch, requested
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    entered = asyncio.Event()
    stopped = asyncio.Event()
    bus = EventBus(10, 1)

    async def due(self):
        return True

    async def flush():
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    bus.flush = flush
    runtime = RotationRuntime(config(), bus, store, tmp_path, stopped)
    task = asyncio.create_task(runtime.run())
    await entered.wait()
    if requested:
        stopped.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.summary()["status"] == ("STOPPED" if requested else "FAILED")
    assert runtime.summary()["attempts"] == 0
    await store.close()


async def test_stop_after_successful_barrier_does_not_allocate_snapshot(tmp_path, monkeypatch):
    stopped = asyncio.Event()
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)
    bus.flush = AsyncMock(side_effect=stopped.set)

    async def due(self):
        return True

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    runtime = RotationRuntime(config(), bus, store, tmp_path, stopped)
    await runtime.run()
    assert runtime.summary()["attempts"] == 0
    assert not (tmp_path / "archive-rotations").exists()
    await store.close()


async def test_shutdown_drain_retains_repeated_cancellation_until_cleanup_finishes():
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = []

    async def cleanup():
        entered.set()
        await release.wait()
        finished.append(True)

    task = asyncio.create_task(drain_shutdown(cleanup()))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    assert isinstance(await task, asyncio.CancelledError)
    assert finished == [True]


@pytest.mark.parametrize(
    "name,value",
    [
        ("archive_rotation_interval_s", True),
        ("archive_rotation_interval_s", 0),
        ("archive_rotation_interval_s", float("inf")),
        ("archive_rotation_interval_s", float("nan")),
        ("archive_rotation_interval_s", 86401),
        ("archive_rotation_partition_ms", True),
        ("archive_rotation_partition_ms", 1.5),
        ("archive_rotation_partition_ms", "1000"),
        ("archive_rotation_partition_ms", 0),
        ("archive_rotation_partition_ms", 86400001),
        ("archive_rotation_max_partitions", True),
        ("archive_rotation_max_partitions", 1.1),
        ("archive_rotation_max_partitions", 0),
        ("archive_rotation_max_partitions", 1001),
        ("archive_rotation_max_snapshots", True),
        ("archive_rotation_max_snapshots", 1.1),
        ("archive_rotation_max_snapshots", 0),
        ("archive_rotation_max_snapshots", 1001),
    ],
)
def test_strict_bounded_runtime_config(name, value):
    with pytest.raises(ValidationError):
        AppConfig(ingestion={name: value})
