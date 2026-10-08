import asyncio
import json
import threading
from pathlib import Path

import duckdb
import pytest

from selective_options import active_rotation as module
from selective_options.archive import file_digest
from selective_options.archive_rotation import validate_rotation_catalog
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.rotation_restore import restore_rotation
from selective_options.store import DuckStore, complete_thread


def event(source_time, receipt_time, number=0):
    return make_event(
        source="fixture-native",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=source_time,
        observed_at_ms=receipt_time,
        native_id=str(number),
        payload={"native": "000123.4500", "number": number},
    )


async def source(tmp_path, rows=None):
    store = await DuckStore.open(tmp_path / "quote's active source.duckdb")
    rows = rows if rows is not None else [event(999, 1001), event(2999, 2999, 1)]
    await store.append(rows)
    await store.acknowledge_delivery([row.event_id for row in rows])
    return store, rows


async def test_open_store_exact_snapshot_rotation_restore_preserves_source_and_acks(tmp_path):
    store, rows = await source(
        tmp_path,
        [event(999, 1001), event(2999, 2999, 1), event(3001, 2999, 2), event(2999, 4001, 3)],
    )
    try:
        output = tmp_path / "quote's active rotation"
        report = await module.rotate_active_store(store, output, 3000, 1000, 1, 2)
        assert report["status"] == "ACTIVE_ROTATION_VERIFIED" and report["rows"] == 2
        assert report["parts"] == 2 and report["later_commits_included"] is False
        assert report["producer_lock_released_before_partition_validation"] is True
        for name in (
            "source_database_hash_claimed",
            "delivery_acknowledgements_copied",
            "source_history_modified",
            "history_complete",
            "current_feed_health_verified",
            "ready_for_signals",
            "trading_enabled",
        ):
            assert report[name] is False
        assert json.loads((output / "active-rotation.json").read_text()) == report
        catalog = validate_rotation_catalog(output / "rotation")
        assert (
            report["snapshot_sha256"]
            == catalog.source_sha256
            == file_digest(output / "snapshot.duckdb")
        )
        assert report["catalog_sha256"] == file_digest(output / "rotation/catalog.json")
        with duckdb.connect(str(output / "snapshot.duckdb"), read_only=True) as reader:
            assert reader.execute("SHOW TABLES").fetchall() == [("events",)]
            assert reader.execute("SELECT * FROM events ORDER BY event_time_ms").fetchall() == [
                tuple(row.model_dump().values()) for row in rows[:2]
            ]
        restored_report = await restore_rotation(
            output / "rotation/catalog.json", tmp_path / "restored", batch_size=1
        )
        assert restored_report["rows"] == 2 and restored_report["redis_delivery_pending"] == 2
        assert await store.pending_delivery_count() == 0
        assert (await store.snapshot("BTC", 5000, "public_reference")).rows() == tuple(
            sorted(rows, key=lambda row: (row.event_time_ms, row.observed_at_ms, row.event_id))
        )
        assert not store._closed and not store._lock.locked()
        aliases = await complete_thread(
            lambda: store._connection.execute(
                "SELECT database_name FROM duckdb_databases()"
            ).fetchall()
        )
        assert (module._ALIAS,) not in aliases
    finally:
        await store.close()


async def test_writer_blocked_during_snapshot_then_allowed_during_part_validation(
    tmp_path, monkeypatch
):
    store, rows = await source(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original_schema = module._schema

    def pause(connection, query, params):
        if query == "SELECT * FROM main.events LIMIT 0":
            entered.set()
            assert release.wait(5)
        return original_schema(connection, query, params)

    monkeypatch.setattr(module, "_schema", pause)
    original_rotation = module.rotate_archive
    later = event(1000, 1500, 9)
    pending_writer = None

    async def validation(*args):
        assert not store._lock.locked()
        await pending_writer
        assert await store.pending_delivery_count() == 1
        return await original_rotation(*args)

    monkeypatch.setattr(module, "rotate_archive", validation)
    try:
        task = asyncio.create_task(
            module.rotate_active_store(store, tmp_path / "first", 3000, 1000, 1, 2)
        )
        assert await asyncio.to_thread(entered.wait, 5)
        assert store._lock.locked()
        pending_writer = asyncio.create_task(store.append([later]))
        await asyncio.sleep(0.02)
        assert not pending_writer.done()
        release.set()
        report = await task
        assert report["rows"] == 2
        assert (await store.snapshot("BTC", 3000, "public_reference")).rows() == tuple(
            sorted(
                [*rows, later],
                key=lambda row: (row.event_time_ms, row.observed_at_ms, row.event_id),
            )
        )
        monkeypatch.setattr(module, "_schema", original_schema)
        monkeypatch.setattr(module, "rotate_archive", original_rotation)
        second = await module.rotate_active_store(store, tmp_path / "second", 3000, 1000, 1, 2)
        assert second["rows"] == 3
        assert (
            second["as_of_ms"] == report["as_of_ms"]
            and second["snapshot_sha256"] != report["snapshot_sha256"]
        )
    finally:
        release.set()
        await store.close()


async def test_preflight_partition_overflow_has_no_destination_or_source_mutation(tmp_path):
    store, rows = await source(tmp_path)
    try:
        with pytest.raises(IntegrityError, match="PARTITION_BUDGET"):
            await module.rotate_active_store(store, tmp_path / "over", 3000, 1000, max_partitions=1)
        assert not (tmp_path / "over").exists()
        assert await store.pending_delivery_count() == 0
        assert (await store.snapshot("BTC", 3000, "public_reference")).rows() == tuple(rows)
        await store.append([event(3000, 3000, 3)])
    finally:
        await store.close()


@pytest.mark.parametrize(
    "name,value",
    [
        (name, value)
        for name in ("as_of_ms", "partition_ms", "batch_size", "max_partitions")
        for value in (True, 0, -1, 1.0, "1", None)
    ]
    + [
        ("as_of_ms", 1 << 63),
        ("partition_ms", 86400001),
        ("batch_size", 10001),
        ("max_partitions", 1001),
    ],
)
async def test_invalid_policy_before_lock_or_io(tmp_path, monkeypatch, name, value):
    store, _ = await source(tmp_path)
    policy = {"as_of_ms": 3000, "partition_ms": 1000, "batch_size": 1, "max_partitions": 2}
    policy[name] = value

    def forbidden(*args):
        raise AssertionError("worker must not run")

    monkeypatch.setattr(module, "_snapshot", forbidden)
    try:
        with pytest.raises(ValueError):
            await module.rotate_active_store(store, tmp_path / "untouched", **policy)
        assert not (tmp_path / "untouched").exists() and not store._lock.locked()
    finally:
        await store.close()


@pytest.mark.parametrize(
    "case", ["closed", "existing", "missing_parent", "glob", "traversal", "symlink"]
)
async def test_store_and_destination_preconditions(tmp_path, monkeypatch, case):
    store, _ = await source(tmp_path)
    output = tmp_path / "out"
    if case == "closed":
        await store.close()
    elif case == "existing":
        output.mkdir()
        (output / "marker").write_text("preserve")
    elif case == "missing_parent":
        output = tmp_path / "missing" / "out"
    elif case == "glob":
        output = tmp_path / "*.rotation"
    elif case == "traversal":
        output = tmp_path / "x" / ".." / "out"
    elif case == "symlink":
        original = Path.is_symlink
        monkeypatch.setattr(Path, "is_symlink", lambda self: self == output or original(self))
    try:
        with pytest.raises((ValueError, FileNotFoundError, FileExistsError, IntegrityError)):
            await module.rotate_active_store(store, output, 3000)
        if case == "existing":
            assert (output / "marker").read_text() == "preserve"
        else:
            assert not output.exists()
    finally:
        await store.close()


class FailOnce:
    def __init__(self, connection, needle):
        self.connection = connection
        self.needle = needle
        self.failed = False

    def execute(self, query, *args):
        if self.needle in query and not self.failed:
            self.failed = True
            raise RuntimeError("synthetic worker failure")
        return self.connection.execute(query, *args)

    def __getattr__(self, name):
        return getattr(self.connection, name)


@pytest.mark.parametrize(
    "needle",
    [
        "CREATE TABLE active_rotation_snapshot.events",
        "EXCEPT ALL",
        "COMMIT",
        "DETACH active_rotation_snapshot",
    ],
)
async def test_transaction_and_attachment_cleanup_on_worker_failure(tmp_path, needle):
    store, rows = await source(tmp_path)
    connection = store._connection
    store._connection = FailOnce(connection, needle)
    try:
        with pytest.raises(RuntimeError, match="synthetic"):
            await module.rotate_active_store(store, tmp_path / "failed", 3000)
        assert (module._ALIAS,) not in connection.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
        assert not (tmp_path / "failed/active-rotation.json").exists()
        assert (
            json.loads((tmp_path / "failed/active-rotation-failed.json").read_text())["status"]
            == "FAILED"
        )
        store._connection = connection
        assert (await store.snapshot("BTC", 3000, "public_reference")).rows() == tuple(rows)
        assert await store.pending_delivery_count() == 0
        await store.append([event(3000, 3000, 99)])
    finally:
        store._connection = connection
        await store.close()


async def test_unmanaged_transaction_refused_without_rollback(tmp_path):
    store, _ = await source(tmp_path)
    store._connection.execute("BEGIN TRANSACTION")
    store._connection.execute("CREATE TABLE private_pending(x INTEGER)")
    try:
        with pytest.raises(IntegrityError, match="SOURCE_TRANSACTION_ALREADY_OPEN"):
            await module.rotate_active_store(store, tmp_path / "bad", 3000)
        assert not (tmp_path / "bad").exists()
        assert store._connection.execute("SELECT count(*) FROM private_pending").fetchone() == (0,)
        store._connection.execute("ROLLBACK")
        assert ("private_pending",) not in store._connection.execute("SHOW TABLES").fetchall()
    finally:
        await store.close()


async def test_existing_alias_is_not_detached_or_overwritten(tmp_path):
    store, _ = await source(tmp_path)
    path = str(tmp_path / "external.duckdb").replace("'", "''")
    store._connection.execute(f"ATTACH '{path}' AS {module._ALIAS}")
    try:
        with pytest.raises(IntegrityError, match="ALIAS_ALREADY"):
            await module.rotate_active_store(store, tmp_path / "no", 3000)
        assert (module._ALIAS,) in store._connection.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
        assert not (tmp_path / "no").exists()
    finally:
        store._connection.execute(f"DETACH {module._ALIAS}")
        await store.close()


async def test_repeated_cancellation_during_snapshot_drains_and_releases_writer(
    tmp_path, monkeypatch
):
    store, rows = await source(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = module._snapshot

    def paused(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(module, "_snapshot", paused)
    try:
        task = asyncio.create_task(module.rotate_active_store(store, tmp_path / "cancelled", 3000))
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done() and store._lock.locked()
        writer = asyncio.create_task(store.append([event(3000, 3000, 9)]))
        assert not writer.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await writer
        assert not store._lock.locked()
        assert (module._ALIAS,) not in store._connection.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
        assert (tmp_path / "cancelled/snapshot.duckdb").is_file()
        assert not (tmp_path / "cancelled/active-rotation.json").exists()
        assert (
            json.loads((tmp_path / "cancelled/active-rotation-failed.json").read_text())[
                "failure_type"
            ]
            == "CancelledError"
        )
        assert await store.pending_delivery_count() == 1
        assert len((await store.snapshot("BTC", 3000, "public_reference")).rows()) == len(rows) + 1
    finally:
        release.set()
        await store.close()


async def test_cancellation_after_success_worker_publication_revokes_active_report(
    tmp_path, monkeypatch
):
    store, _ = await source(tmp_path)
    entered, release = threading.Event(), threading.Event()
    original = module._publish

    def paused(*args):
        original(*args)
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(module, "_publish", paused)
    try:
        task = asyncio.create_task(
            module.rotate_active_store(store, tmp_path / "cancel-publish", 3000)
        )
        assert await asyncio.to_thread(entered.wait, 5)
        assert (tmp_path / "cancel-publish/active-rotation.json").exists()
        task.cancel()
        await asyncio.sleep(0.01)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not (tmp_path / "cancel-publish/active-rotation.json").exists()
        assert (
            json.loads((tmp_path / "cancel-publish/active-rotation-incomplete.json").read_text())[
                "status"
            ]
            == "PUBLICATION_REVOKED"
        )
        assert (tmp_path / "cancel-publish/active-rotation-failed.json").exists()
        assert validate_rotation_catalog(tmp_path / "cancel-publish/rotation").total_rows == 2
        await store.append([event(3000, 3000, 99)])
    finally:
        release.set()
        await store.close()


async def test_closed_rotation_failure_preserves_snapshot_and_active_store(tmp_path, monkeypatch):
    store, rows = await source(tmp_path)

    async def failed(*args):
        assert not store._lock.locked()
        raise IntegrityError("synthetic part validation failure")

    monkeypatch.setattr(module, "rotate_archive", failed)
    try:
        with pytest.raises(IntegrityError):
            await module.rotate_active_store(store, tmp_path / "rotation-failed", 3000)
        assert (tmp_path / "rotation-failed/snapshot.duckdb").is_file()
        assert not (tmp_path / "rotation-failed/active-rotation.json").exists()
        assert (
            json.loads((tmp_path / "rotation-failed/active-rotation-failed.json").read_text())[
                "status"
            ]
            == "FAILED"
        )
        assert (await store.snapshot("BTC", 3000, "public_reference")).rows() == tuple(rows)
        assert await store.pending_delivery_count() == 0
    finally:
        await store.close()


async def test_empty_open_store_snapshot_contains_events_only(tmp_path):
    store, _ = await source(tmp_path, [])
    try:
        report = await module.rotate_active_store(store, tmp_path / "empty", 1, max_partitions=1)
        assert report["rows"] == report["parts"] == 0
        with duckdb.connect(str(tmp_path / "empty/snapshot.duckdb"), read_only=True) as reader:
            assert reader.execute("SHOW TABLES").fetchall() == [("events",)]
        await store.append([event(1000, 1000)])
    finally:
        await store.close()


@pytest.mark.parametrize("case", ["catalog", "part", "resealed_part", "snapshot"])
async def test_post_rotation_mutation_cannot_publish_active_success(tmp_path, monkeypatch, case):
    store, rows = await source(tmp_path)
    original = module.rotate_archive

    async def mutate(snapshot, output, *args):
        catalog = await original(snapshot, output, *args)
        if case == "catalog":
            changed = json.loads((output / "catalog.json").read_text())
            changed["as_of_ms"] += 1
            (output / "catalog.json").write_text(json.dumps(changed))
        elif case == "part":
            with (output / catalog["parts"][0]["file"]).open("ab") as stream:
                stream.write(b"synthetic corruption")
        elif case == "snapshot":
            with snapshot.open("ab") as stream:
                stream.write(b"synthetic external snapshot change")
        else:
            from selective_options.archive import manifest_path, seal_archive

            part = output / catalog["parts"][0]["file"]
            replacement = output / "changed.parquet"
            with duckdb.connect() as connection:
                connection.execute(
                    'COPY (SELECT * REPLACE (\'{"native":"changed"}\' AS payload_json) FROM read_parquet($source)) TO $destination (FORMAT PARQUET, COMPRESSION ZSTD)',
                    {"source": str(part), "destination": str(replacement)},
                )
            part.unlink()
            replacement.rename(part)
            manifest_path(part).unlink()
            manifest = await seal_archive(part, catalog["parts"][0]["rows"])
            changed = json.loads((output / "catalog.json").read_text())
            changed["parts"][0].update(
                {
                    "bytes": manifest.bytes,
                    "sha256": manifest.sha256,
                    "manifest_sha256": file_digest(manifest_path(part)),
                }
            )
            (output / "catalog.json").write_text(json.dumps(changed))
            assert validate_rotation_catalog(output).total_rows == 2
        return catalog

    monkeypatch.setattr(module, "rotate_archive", mutate)
    try:
        with pytest.raises(IntegrityError):
            await module.rotate_active_store(store, tmp_path / "changed", 3000)
        assert not (tmp_path / "changed/active-rotation.json").exists()
        assert (tmp_path / "changed/active-rotation-failed.json").is_file()
        assert (await store.snapshot("BTC", 3000, "public_reference")).rows() == tuple(rows)
        assert await store.pending_delivery_count() == 0
        await store.append([event(3000, 3000, 77)])
    finally:
        await store.close()


async def test_catalog_changed_after_digest_is_rechecked_before_publication(tmp_path, monkeypatch):
    store, _ = await source(tmp_path)
    original = module.file_digest
    count = 0

    def mutate(path):
        nonlocal count
        digest = original(path)
        if path.name == "catalog.json":
            count += 1
            if count == 1:
                with path.open("a") as stream:
                    stream.write(" ")
        return digest

    monkeypatch.setattr(module, "file_digest", mutate)
    try:
        with pytest.raises(IntegrityError, match="ARTIFACT_CHANGED_BEFORE"):
            await module.rotate_active_store(store, tmp_path / "race", 3000)
        assert not (tmp_path / "race/active-rotation.json").exists()
        assert (tmp_path / "race/active-rotation-failed.json").is_file()
    finally:
        await store.close()


@pytest.mark.parametrize("case", ["schema", "duplicate_id"])
async def test_corrupt_source_preflight_refuses_before_destination(tmp_path, case):
    store, _ = await source(tmp_path)
    if case == "schema":
        store._connection.execute("ALTER TABLE events ADD COLUMN surprise INTEGER")
    else:
        store._connection.execute("CREATE TABLE copied AS SELECT * FROM events")
        store._connection.execute("UPDATE copied SET event_id=(SELECT min(event_id) FROM events)")
        store._connection.execute("DROP TABLE events")
        store._connection.execute("ALTER TABLE copied RENAME TO events")
    try:
        with pytest.raises(IntegrityError):
            await module.rotate_active_store(store, tmp_path / "bad-source", 3000)
        assert not (tmp_path / "bad-source").exists()
    finally:
        await store.close()


async def test_active_source_file_is_never_hashed(tmp_path, monkeypatch):
    store, _ = await source(tmp_path)
    original = module.file_digest
    calls = []

    def safe(path):
        calls.append(path)
        assert path != tmp_path / "quote's active source.duckdb"
        return original(path)

    monkeypatch.setattr(module, "file_digest", safe)
    try:
        report = await module.rotate_active_store(store, tmp_path / "hashes", 3000)
        assert report["source_database_hash_claimed"] is False
        assert {path.name for path in calls} == {"snapshot.duckdb", "catalog.json"}
    finally:
        await store.close()
