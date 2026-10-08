import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from pydantic import ValidationError

from selective_options import rotation_restore as module
from selective_options.archive import file_digest, manifest_path, seal_archive
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.rotation_restore import restore_rotation
from selective_options.store import DuckStore, complete_thread


async def rotation(tmp_path, *, empty=False):
    directory = tmp_path / "rotation"
    directory.mkdir()
    rows = (
        []
        if empty
        else [
            make_event(
                source="fixture",
                environment="public_reference",
                asset="BTC",
                symbol="BTC/USD",
                kind="spot",
                event_time_ms=1000 + index * 1000,
                observed_at_ms=1100 + index * 1000,
                payload={"price": 10 + index},
            )
            for index in range(3)
        ]
    )
    parts = []
    for index, row in enumerate(rows):
        path = directory / f"part-{index:06d}.parquet"
        store = await DuckStore.open(tmp_path / f"source-{index}.duckdb")
        await store.append([row])
        await store.acknowledge_delivery([row.event_id])
        count = await store.export_parquet(path)
        await store.close()
        manifest = await seal_archive(path, count)
        parts.append(
            {
                "index": index,
                "file": path.name,
                "manifest": manifest_path(path).name,
                "start_ms": 1000 + index * 1000,
                "end_ms": 2000 + index * 1000,
                "rows": count,
                "bytes": manifest.bytes,
                "sha256": manifest.sha256,
                "manifest_sha256": file_digest(manifest_path(path)),
            }
        )
    data = {
        "format_version": 1,
        "status": "ROTATION_VERIFIED",
        "as_of_ms": 9000,
        "partition_ms": 1000,
        "total_rows": len(rows),
        "source_sha256": "0" * 64,
        "source_scope": "all sources and environments in events; both clocks at or before cutoff",
        "delivery_acknowledgements_copied": False,
        "source_history_modified": False,
        "history_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
        "parts": parts,
    }
    catalog = directory / "catalog.json"
    catalog.write_text(json.dumps(data), encoding="utf-8")
    return catalog, data, rows


def rewrite(catalog, data):
    catalog.write_text(json.dumps(data), encoding="utf-8")


async def change_chunk(catalog, data, sql, *, count=1, index=0):
    path = catalog.parent / data["parts"][index]["file"]
    replacement = catalog.parent / "replacement.parquet"
    with duckdb.connect() as db:
        db.execute(
            f"COPY ({sql.replace('?', '$source')}) TO $destination (FORMAT PARQUET)",
            {"source": str(path), "destination": str(replacement)},
        )
    replacement.replace(path)
    manifest_path(path).unlink()
    manifest = await seal_archive(path, count)
    data["parts"][index].update(
        rows=count,
        bytes=manifest.bytes,
        sha256=manifest.sha256,
        manifest_sha256=file_digest(manifest_path(path)),
    )
    data["total_rows"] = sum(part["rows"] for part in data["parts"])
    rewrite(catalog, data)


async def assert_failed(output):
    assert not (output / "events.duckdb").exists()
    assert not (output / "restore.json").exists()
    assert json.loads((output / "restore-failed.json").read_text())["status"] == "FAILED"
    if (output / "restoring.duckdb").exists():
        with duckdb.connect(str(output / "restoring.duckdb")) as db:
            assert db.execute("SELECT count(*) FROM events").fetchone() is not None


async def test_roundtrip_preserves_native_clocks_and_exact_ids_all_delivery_requeued(tmp_path):
    catalog, _, rows = await rotation(tmp_path)
    before = {path.name: file_digest(path) for path in catalog.parent.iterdir()}
    output = tmp_path / "restored"
    report = await restore_rotation(catalog, output, batch_size=1)
    assert report["status"] == "ROTATION_RESTORE_FINISHED"
    assert report["rows"] == report["redis_delivery_pending"] == 3
    assert report["parts"] == 3 and report["trading_enabled"] is False
    assert report["history_complete"] is report["ready_for_signals"] is False
    assert before == {path.name: file_digest(path) for path in catalog.parent.iterdir()}
    assert not (output / "restoring.duckdb").exists()
    store = await DuckStore.open(output / "events.duckdb")
    try:
        for cutoff in [999, 1000, 1099, 1100, 1999, 2100, 3100, 9000]:
            expected = tuple(
                row for row in rows if max(row.event_time_ms, row.observed_at_ms) <= cutoff
            )
            assert (await store.snapshot("BTC", cutoff, "public_reference")).rows() == expected
        assert await store.pending_delivery_count() == 3
    finally:
        await store.close()
    with pytest.raises(ValueError):
        await restore_rotation(catalog, output)


async def test_empty_catalog_is_valid_empty_snapshot_not_history_approval(tmp_path):
    catalog, _, _ = await rotation(tmp_path, empty=True)
    output = tmp_path / "empty"
    report = await restore_rotation(catalog, output)
    assert report["rows"] == report["parts"] == report["redis_delivery_pending"] == 0
    store = await DuckStore.open(output / "events.duckdb")
    assert await store.pending_delivery_count() == 0
    await store.close()


@pytest.mark.parametrize(
    "name,value",
    [
        ("batch_size", 0),
        ("batch_size", -1),
        ("batch_size", True),
        ("batch_size", 1.2),
        ("batch_size", 100001),
        ("max_skew_ms", -1),
        ("max_skew_ms", True),
        ("max_skew_ms", 1.2),
        ("max_skew_ms", 86400001),
    ],
)
async def test_policy_is_strict_and_preflight_before_creation(tmp_path, name, value):
    catalog, _, _ = await rotation(tmp_path)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        await restore_rotation(catalog, output, **{name: value})
    assert not output.exists()


@pytest.mark.parametrize("case", ["missing", "glob", "traversal", "basename", "no_parent"])
async def test_explicit_paths_refuse_before_output(tmp_path, case):
    catalog, _, _ = await rotation(tmp_path)
    output = tmp_path / "invalid"
    if case == "missing":
        catalog = catalog.parent / "missing.json"
    elif case == "glob":
        catalog = catalog.parent / "*.json"
    elif case == "traversal":
        catalog = catalog.parent / ".." / "rotation" / "catalog.json"
    elif case == "basename":
        catalog.rename(catalog.parent / "other.json")
        catalog = catalog.parent / "other.json"
    else:
        output = tmp_path / "missing" / "invalid"
    with pytest.raises(ValueError):
        await restore_rotation(catalog, output)
    assert not output.exists()


@pytest.mark.parametrize(
    "case",
    [
        "extra",
        "traversal",
        "filename",
        "overlap",
        "count",
        "cutoff",
        "manifest",
        "missing",
        "checksum",
    ],
)
async def test_malicious_or_incomplete_catalog_never_publishes(tmp_path, case):
    catalog, data, _ = await rotation(tmp_path)
    part = data["parts"][0]
    if case == "extra":
        data["approval"] = True
    elif case == "traversal":
        part["file"] = "../part-000000.parquet"
    elif case == "filename":
        part["file"] = "substitute.parquet"
    elif case == "overlap":
        data["parts"][1]["start_ms"] = 1000
        data["parts"][1]["end_ms"] = 2000
    elif case == "count":
        data["total_rows"] = 4
    elif case == "cutoff":
        data["as_of_ms"] = 1
    elif case == "manifest":
        (catalog.parent / part["manifest"]).write_text("{}", encoding="utf-8")
    elif case == "missing":
        (catalog.parent / part["file"]).rename(catalog.parent / "unlisted.parquet")
    else:
        with (catalog.parent / part["file"]).open("ab") as stream:
            stream.write(b"corrupt")
    rewrite(catalog, data)
    output = tmp_path / "invalid"
    with pytest.raises((ValueError, IntegrityError, ValidationError, FileNotFoundError)):
        await restore_rotation(catalog, output)
    await assert_failed(output)


@pytest.mark.parametrize(
    "case",
    [
        "schema",
        "physical_type",
        "duplicate",
        "environment",
        "id",
        "payload",
        "skew",
        "partition",
        "future",
    ],
)
async def test_valid_hash_cannot_approve_invalid_chunk_or_native_timing(tmp_path, case):
    catalog, data, _ = await rotation(tmp_path)
    if case == "schema":
        sql = "SELECT *, 1 AS unexpected FROM read_parquet(?)"
    elif case == "physical_type":
        sql = "SELECT * REPLACE (cast(event_time_ms AS VARCHAR) AS event_time_ms) FROM read_parquet(?)"
    elif case == "duplicate":
        sql = "SELECT p.* FROM read_parquet(?) p CROSS JOIN range(2)"
    else:
        replace = {
            "environment": "'production' AS environment",
            "id": "'bad' AS event_id",
            "payload": "'[]' AS payload_json",
            "skew": "cast(999 AS BIGINT) AS observed_at_ms",
            "partition": "cast(999 AS BIGINT) AS observed_at_ms",
            "future": "cast(9500 AS BIGINT) AS observed_at_ms",
        }[case]
        if case == "partition":
            replace += ", cast(999 AS BIGINT) AS event_time_ms"
        sql = f"SELECT * REPLACE ({replace}) FROM read_parquet(?)"
    await change_chunk(catalog, data, sql, count=2 if case == "duplicate" else 1)
    output = tmp_path / "invalid"
    with pytest.raises((ValueError, IntegrityError, ValidationError)):
        await restore_rotation(catalog, output, max_skew_ms=0)
    await assert_failed(output)


async def test_cross_part_identical_id_detected_without_silent_dedup(tmp_path):
    catalog, data, rows = await rotation(tmp_path)
    await change_chunk(
        catalog,
        data,
        f"SELECT * REPLACE ('{rows[0].event_id}' AS event_id) FROM read_parquet(?)",
        index=1,
    )
    output = tmp_path / "invalid"
    with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
        await restore_rotation(catalog, output)
    await assert_failed(output)


@pytest.mark.parametrize("case", ["catalog", "manifest", "chunk"])
async def test_changes_during_restore_refuse_even_after_some_rows_committed(
    tmp_path, monkeypatch, case
):
    catalog, data, _ = await rotation(tmp_path)
    append = DuckStore.append
    changed = False

    async def tamper(self, rows, *, exact=False):
        nonlocal changed
        await append(self, rows, exact=exact)
        if not changed:
            changed = True
            if case == "catalog":
                catalog.write_text(catalog.read_text() + " ", encoding="utf-8")
            else:
                path = (
                    catalog.parent / data["parts"][0]["manifest" if case == "manifest" else "file"]
                )
                with path.open("ab") as stream:
                    stream.write(b"changed")

    monkeypatch.setattr(DuckStore, "append", tamper)
    output = tmp_path / "invalid"
    with pytest.raises((ValueError, IntegrityError)):
        await restore_rotation(catalog, output, batch_size=1)
    await assert_failed(output)


@pytest.mark.parametrize("phase", ["catalog", "reader", "batch", "publish"])
async def test_repeated_cancellation_drains_workers_closes_resources_and_revokes_publication(
    tmp_path, monkeypatch, phase
):
    catalog, _, _ = await rotation(tmp_path)
    output = tmp_path / "cancelled"
    entered = threading.Event()
    release = threading.Event()
    name = {
        "catalog": "validate_rotation_catalog",
        "reader": "_reader",
        "batch": "_batch",
        "publish": "_publish",
    }[phase]
    original = getattr(module, name)

    def blocked(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(module, name, blocked)
    task = asyncio.create_task(restore_rotation(catalog, output, batch_size=1))
    for _ in range(500):
        if entered.is_set():
            break
        await asyncio.sleep(0.01)
    assert entered.is_set()
    task.cancel()
    await asyncio.sleep(0.03)
    task.cancel()
    await asyncio.sleep(0.03)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await assert_failed(output)
    if phase == "publish":
        assert (
            json.loads((output / "restore-incomplete.json").read_text())["status"]
            == "PUBLICATION_REVOKED"
        )


@pytest.mark.parametrize("case", ["catalog", "manifest", "unlisted"])
async def test_duplicate_json_controls_and_unlisted_chunks_refuse(tmp_path, case):
    catalog, data, _ = await rotation(tmp_path)
    if case == "catalog":
        catalog.write_text(
            catalog.read_text().replace(
                '"format_version": 1', '"format_version": 1, "format_version": 1'
            ),
            encoding="utf-8",
        )
    elif case == "manifest":
        manifest = catalog.parent / data["parts"][0]["manifest"]
        manifest.write_text(
            manifest.read_text().replace('"rows": 1', '"rows": 1, "rows": 1'), encoding="utf-8"
        )
        data["parts"][0]["manifest_sha256"] = file_digest(manifest)
        rewrite(catalog, data)
    else:
        (catalog.parent / "part-000003.parquet").write_bytes(b"unlisted")
    output = tmp_path / "invalid"
    with pytest.raises(IntegrityError):
        await restore_rotation(catalog, output)
    await assert_failed(output)


@pytest.mark.parametrize("component", ["catalog", "parent", "output_parent"])
async def test_simulated_windows_reparse_components_refused_before_creation(
    tmp_path, monkeypatch, component
):
    catalog, _, _ = await rotation(tmp_path)
    target = {"catalog": catalog, "parent": catalog.parent, "output_parent": tmp_path}[component]
    lstat = Path.lstat

    def simulated(path):
        original = lstat(path)
        if path == target:
            return SimpleNamespace(st_mode=original.st_mode, st_file_attributes=0x400)
        return original

    monkeypatch.setattr(Path, "lstat", simulated)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError, match="junction"):
        await restore_rotation(catalog, output)
    assert not output.exists()


async def test_mutation_during_store_close_is_rechecked_before_publication(tmp_path, monkeypatch):
    catalog, _, _ = await rotation(tmp_path)
    close = DuckStore.close

    async def changed(self):
        await close(self)
        catalog.write_text(catalog.read_text() + " ", encoding="utf-8")

    monkeypatch.setattr(DuckStore, "close", changed)
    output = tmp_path / "invalid"
    with pytest.raises(IntegrityError, match="CHANGED_DURING"):
        await restore_rotation(catalog, output)
    await assert_failed(output)


async def test_partial_insert_failure_closes_staging_and_never_publishes_early_part(
    tmp_path, monkeypatch
):
    catalog, _, _ = await rotation(tmp_path)
    append = DuckStore.append
    calls = 0

    async def failure(self, rows, *, exact=False):
        nonlocal calls
        calls += 1
        assert not (tmp_path / "invalid" / "events.duckdb").exists()
        if calls == 2:
            raise RuntimeError("simulated second part storage failure")
        await append(self, rows, exact=exact)

    monkeypatch.setattr(DuckStore, "append", failure)
    output = tmp_path / "invalid"
    with pytest.raises(RuntimeError):
        await restore_rotation(catalog, output)
    await assert_failed(output)
    with duckdb.connect(str(output / "restoring.duckdb")) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone() == (1,)


@pytest.mark.parametrize("case", ["lost_row", "copied_ack", "misbound_outbox"])
async def test_global_stored_count_and_pending_outbox_required_before_publication(
    tmp_path, monkeypatch, case
):
    catalog, _, _ = await rotation(tmp_path)
    append = DuckStore.append
    first = True

    async def faulty(self, rows, *, exact=False):
        nonlocal first
        if first:
            first = False
            if case == "lost_row":
                return
            await append(self, rows, exact=exact)
            if case == "copied_ack":
                await self.acknowledge_delivery([row.event_id for row in rows])
            else:
                await complete_thread(
                    self._connection.execute,
                    "UPDATE delivery_outbox SET event_id=? WHERE event_id=?",
                    ["f" * 64, rows[0].event_id],
                )
        else:
            await append(self, rows, exact=exact)

    monkeypatch.setattr(DuckStore, "append", faulty)
    output = tmp_path / "invalid"
    with pytest.raises(IntegrityError, match="GLOBAL_COUNT"):
        await restore_rotation(catalog, output)
    await assert_failed(output)


async def test_cleanup_still_journals_failure_if_store_close_raises(tmp_path, monkeypatch):
    catalog, _, _ = await rotation(tmp_path)
    close = DuckStore.close

    async def failure(self):
        await close(self)
        raise RuntimeError("simulated closure failure")

    monkeypatch.setattr(DuckStore, "close", failure)
    output = tmp_path / "invalid"
    with pytest.raises(RuntimeError, match="closure failure"):
        await restore_rotation(catalog, output)
    await assert_failed(output)


async def test_output_inside_rotation_refused_without_altering_saved_catalog_directory(tmp_path):
    catalog, _, _ = await rotation(tmp_path)
    before = {path.name: file_digest(path) for path in catalog.parent.iterdir()}
    output = catalog.parent / "restored"
    with pytest.raises(ValueError, match="outside"):
        await restore_rotation(catalog, output)
    assert not output.exists()
    assert before == {path.name: file_digest(path) for path in catalog.parent.iterdir()}
