"""Deterministic worker gates exercise cancellation without timing-based SQL races."""

import asyncio
import threading

import duckdb
import pytest

import selective_options.archive as archive_module
import selective_options.store as store_module
from selective_options.archive import restore_archive, seal_archive
from selective_options.events import make_event
from selective_options.store import DuckStore, acquire_thread_resource, complete_thread


async def entered(event):
    assert await asyncio.to_thread(event.wait, 5), "worker did not reach gate"


async def cancel_twice(task):
    task.cancel("first interruption")
    await asyncio.sleep(0)
    task.cancel("second interruption")
    await asyncio.sleep(0)
    assert not task.done(), "caller escaped while worker was still running"


@pytest.mark.parametrize("fails", [False, True])
async def test_repeated_cancel_drains_worker_and_retains_failure_cause(fails):
    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def work():
        started.set()
        assert release.wait(5)
        finished.set()
        if fails:
            raise ValueError("worker failed")
        return 42

    task = asyncio.create_task(complete_thread(work))
    try:
        await entered(started)
        await cancel_twice(task)
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task
    assert finished.is_set()
    assert (
        isinstance(caught.value.__cause__, ValueError) if fails else caught.value.__cause__ is None
    )


async def test_worker_cancelled_error_does_not_spin():
    def work():
        raise asyncio.CancelledError("worker cancellation")

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(complete_thread(work), 5)


async def test_resource_cleanup_is_drained_even_when_cancelled_again():
    acquiring, release_acquire = threading.Event(), threading.Event()
    closing, release_close, closed = threading.Event(), threading.Event(), threading.Event()
    resource = object()

    def acquire():
        acquiring.set()
        assert release_acquire.wait(5)
        return resource

    def close(value):
        assert value is resource
        closing.set()
        assert release_close.wait(5)
        closed.set()

    task = asyncio.create_task(acquire_thread_resource(acquire, close))
    try:
        await entered(acquiring)
        await cancel_twice(task)
        release_acquire.set()
        await entered(closing)
        await cancel_twice(task)
    finally:
        release_acquire.set()
        release_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


async def test_acquisition_failure_has_no_resource_to_release():
    released = []

    def fail():
        raise ValueError("acquire failed")

    with pytest.raises(ValueError, match="acquire failed"):
        await acquire_thread_resource(fail, released.append)
    assert released == []


async def test_cancelled_store_open_closes_actual_connection(tmp_path, monkeypatch):
    original = DuckStore._open_connection
    started, release = threading.Event(), threading.Event()
    connections = []

    def open_gated(path):
        connection = original(path)
        connections.append(connection)
        started.set()
        assert release.wait(5)
        return connection

    monkeypatch.setattr(DuckStore, "_open_connection", staticmethod(open_gated))
    path = tmp_path / "cancelled.duckdb"
    task = asyncio.create_task(DuckStore.open(path))
    try:
        await entered(started)
        await cancel_twice(task)
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].execute("SELECT 1")
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone() == (0,)


async def test_schema_failure_closes_partial_connection(tmp_path, monkeypatch):
    class Connection:
        closed = False

        def execute(self, sql):
            raise RuntimeError("schema failed")

        def close(self):
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(store_module.duckdb, "connect", lambda *_: connection)
    with pytest.raises(RuntimeError, match="schema failed"):
        await DuckStore.open(tmp_path / "failed.duckdb")
    assert connection.closed


async def test_cancelled_append_holds_lock_until_commit_then_close(tmp_path, monkeypatch):
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    row = make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=1000,
        observed_at_ms=2000,
        payload={"price": 100},
    )
    started, release = threading.Event(), threading.Event()
    original = store._insert

    def insert_gated(rows, exact=False):
        started.set()
        assert release.wait(5)
        original(rows, exact)

    monkeypatch.setattr(store, "_insert", insert_gated)
    task = asyncio.create_task(store.append([row]))
    closer = None
    try:
        await entered(started)
        await cancel_twice(task)
        closer = asyncio.create_task(store.close())
        await asyncio.sleep(0)
        assert not closer.done()
        assert store._lock.locked()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await closer
    reopened = await DuckStore.open(path)
    try:
        assert (await reopened.snapshot("BTC", 2000, "public_reference")).rows() == (row,)
        assert not (await reopened.snapshot("BTC", 1999, "public_reference")).rows()
        assert await reopened.pending_delivery_count() == 1
    finally:
        await reopened.close()


async def test_cancelled_restore_reader_closes_before_output_creation(tmp_path, monkeypatch):
    source = await DuckStore.open(tmp_path / "source.duckdb")
    archive = tmp_path / "empty.parquet"
    count = await source.export_parquet(archive)
    await source.close()
    await seal_archive(archive, count)
    original = archive_module._open_reader
    started, release = threading.Event(), threading.Event()
    connections = []

    def open_gated(*args):
        connection = original(*args)
        connections.append(connection)
        started.set()
        assert release.wait(5)
        return connection

    monkeypatch.setattr(archive_module, "_open_reader", open_gated)
    output = tmp_path / "restore"
    task = asyncio.create_task(restore_archive(archive, output))
    try:
        await entered(started)
        await cancel_twice(task)
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not output.exists()
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        connections[0].execute("SELECT 1")
