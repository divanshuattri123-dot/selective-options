import asyncio
import json
import threading

import duckdb
import pytest
from pydantic import ValidationError

from selective_options import archive_rotation as rotation
from selective_options.archive import file_digest, manifest_path, restore_archive
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.store import DuckStore


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
    database = tmp_path / "quote's source database.duckdb"
    rows = rows if rows is not None else [event(999, 1001), event(2999, 2999, 1)]
    store = await DuckStore.open(database)
    await store.append(rows)
    await store.close()
    with duckdb.connect(str(database)) as connection:
        connection.execute("UPDATE delivery_outbox SET delivered=TRUE")
    return database, rows


async def test_exact_cohort_boundaries_future_filter_and_source_immutability(tmp_path):
    database, rows = await source(
        tmp_path,
        [event(999, 1001), event(2999, 2999, 1), event(3001, 2999, 2), event(2999, 4001, 3)],
    )
    before = file_digest(database)
    output = tmp_path / "quote's rotation directory"
    report = await rotation.rotate_archive(database, output, 3000, 1000, 1, 2)
    assert report["status"] == "ROTATION_VERIFIED"
    assert report["total_rows"] == 2
    assert report["source_sha256"] == before == file_digest(database)
    for key in (
        "delivery_acknowledgements_copied",
        "source_history_modified",
        "history_complete",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert report[key] is False
    catalog = rotation.validate_rotation_catalog(output)
    assert [(part.start_ms, part.end_ms, part.rows) for part in catalog.parts] == [
        (1000, 2000, 1),
        (2000, 3000, 1),
    ]
    saved = []
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered"
        ).fetchone() == (4,)
        for part in catalog.parts:
            cursor = connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(output / part.file)]
            )
            saved.extend(cursor.fetchall())
    assert saved == [tuple(row.model_dump().values()) for row in rows[:2]]
    assert not database.with_suffix(".duckdb.wal").exists()
    assert file_digest(database) == before


async def test_empty_finite_cohort_has_no_chunks(tmp_path):
    database, _ = await source(tmp_path)
    output = tmp_path / "empty"
    result = await rotation.rotate_archive(database, output, 1, max_partitions=1)
    assert result["total_rows"] == 0 and result["parts"] == []
    assert {p.name for p in output.iterdir()} == {"catalog.json"}
    assert rotation.validate_rotation_catalog(output).parts == ()


async def test_integer_boundary_uses_availability_not_event_time(tmp_path):
    database, _ = await source(tmp_path, [event(1, 999), event(1, 1000, 1), event(1001, 1000, 2)])
    output = tmp_path / "bounds"
    result = await rotation.rotate_archive(database, output, 1001, 1000, 1, 2)
    assert [(part["start_ms"], part["rows"]) for part in result["parts"]] == [(0, 1), (1000, 2)]


async def test_partition_budget_refused_before_output_creation(tmp_path):
    database, _ = await source(tmp_path)
    output = tmp_path / "over-budget"
    before = file_digest(database)
    with pytest.raises(IntegrityError, match="PARTITION_BUDGET"):
        await rotation.rotate_archive(database, output, 3000, 1000, max_partitions=1)
    assert not output.exists() and file_digest(database) == before


@pytest.mark.parametrize(
    "name,value",
    [
        (name, value)
        for name in ("as_of_ms", "partition_ms", "batch_size", "max_partitions")
        for value in (True, False, 0, -1, 1.0, "1", None)
    ]
    + [
        ("as_of_ms", 1 << 63),
        ("partition_ms", 86_400_001),
        ("batch_size", 10001),
        ("max_partitions", 1001),
    ],
)
async def test_invalid_policy_before_database_or_destination_io(tmp_path, monkeypatch, name, value):
    def forbidden(*args, **kwargs):
        raise AssertionError("database must not open")

    monkeypatch.setattr(rotation.duckdb, "connect", forbidden)
    policy = {"as_of_ms": 3000, "partition_ms": 1000, "batch_size": 1, "max_partitions": 2}
    policy[name] = value
    with pytest.raises(ValueError):
        await rotation.rotate_archive(tmp_path / "missing.duckdb", tmp_path / "untouched", **policy)
    assert not (tmp_path / "untouched").exists()


@pytest.mark.parametrize(
    "case", ["missing", "glob", "traversal", "parent_missing", "existing", "wal"]
)
async def test_path_and_closed_source_preconditions(tmp_path, case):
    database, _ = await source(tmp_path)
    output = tmp_path / "rotation"
    if case == "missing":
        database = tmp_path / "missing.duckdb"
    elif case == "glob":
        output = tmp_path / "*.parquet"
    elif case == "traversal":
        output = tmp_path / "other" / ".." / "rotation"
    elif case == "parent_missing":
        output = tmp_path / "missing" / "rotation"
    elif case == "existing":
        output.mkdir()
        (output / "marker").write_text("preserve")
    elif case == "wal":
        database.with_suffix(".duckdb.wal").write_text("fake uncheckpointed WAL")
    with pytest.raises((ValueError, FileNotFoundError, FileExistsError, IntegrityError)):
        await rotation.rotate_archive(database, output, 3000)
    if case == "existing":
        assert (output / "marker").read_text() == "preserve"
    else:
        assert not output.exists()


async def test_incompatible_active_writer_is_not_interrupted(tmp_path):
    database, _ = await source(tmp_path)
    with duckdb.connect(str(database)) as writer:
        with pytest.raises((duckdb.ConnectionException, PermissionError)):
            await rotation.rotate_archive(database, tmp_path / "bad", 3000)
        assert writer.execute("SELECT count(*) FROM events").fetchone() == (2,)
    assert not (tmp_path / "bad").exists()


@pytest.mark.parametrize(
    "case", ["schema", "duplicate_across_parts", "invalid_kind", "invalid_payload"]
)
async def test_corrupt_source_never_publishes_catalog(tmp_path, case):
    database, _ = await source(tmp_path)
    with duckdb.connect(str(database)) as connection:
        if case == "schema":
            connection.execute("ALTER TABLE events ADD COLUMN surprise INTEGER")
        elif case == "duplicate_across_parts":
            connection.execute("CREATE TABLE copied AS SELECT * FROM events")
            connection.execute("UPDATE copied SET event_id=(SELECT min(event_id) FROM events)")
            connection.execute("DROP TABLE events")
            connection.execute("ALTER TABLE copied RENAME TO events")
        elif case == "invalid_kind":
            connection.execute("UPDATE events SET kind='unknown'")
        else:
            connection.execute("UPDATE events SET payload_json='not JSON'")
    before = file_digest(database)
    output = tmp_path / "failed"
    with pytest.raises((IntegrityError, ValidationError)):
        await rotation.rotate_archive(database, output, 3000, 1000)
    assert not (output / "catalog.json").exists() and file_digest(database) == before
    if output.exists():
        assert json.loads((output / "rotation-failed.json").read_text())["status"] == "FAILED"


async def test_final_typed_recheck_rejects_manifest_time_mutation(tmp_path, monkeypatch):
    database, _ = await source(tmp_path)
    original = rotation._write_manifest

    def mutate(archive, rows):
        with duckdb.connect() as connection:
            changed = archive.with_suffix(".changed.parquet")
            connection.execute(
                'COPY (SELECT * REPLACE (\'{"native":"changed"}\' AS payload_json) FROM read_parquet($source)) TO $destination (FORMAT PARQUET, COMPRESSION ZSTD)',
                {"source": str(archive), "destination": str(changed)},
            )
        archive.unlink()
        changed.rename(archive)
        return original(archive, rows)

    monkeypatch.setattr(rotation, "_write_manifest", mutate)
    output = tmp_path / "changed"
    with pytest.raises(IntegrityError, match="TYPED_RECORD"):
        await rotation.rotate_archive(database, output, 3000)
    assert not (output / "catalog.json").exists()
    assert (output / "rotation-failed.json").is_file()


async def test_publish_last_and_individual_chunk_restore_remains_compatible(tmp_path, monkeypatch):
    database, rows = await source(tmp_path)
    publish = rotation._publish

    def inspect(output, catalog, stop):
        assert not (output / "catalog.json").exists()
        assert not (output / "rotation-failed.json").exists()
        assert len(list(output.iterdir())) == 2 * len(catalog.parts)
        publish(output, catalog, stop)

    monkeypatch.setattr(rotation, "_publish", inspect)
    output = tmp_path / "sealed"
    await rotation.rotate_archive(database, output, 3000)
    result = await restore_archive(
        output / "part-000000.parquet", tmp_path / "restore", batch_size=1
    )
    assert result["rows"] == 2 and result["redis_delivery_pending"] == 2
    restored = await DuckStore.open(tmp_path / "restore/events.duckdb")
    assert (await restored.snapshot("BTC", 3000, "public_reference")).rows() == tuple(rows)
    await restored.close()
    before = file_digest(output / "catalog.json")
    with pytest.raises(FileExistsError):
        await rotation.rotate_archive(database, output, 3000)
    assert file_digest(output / "catalog.json") == before


@pytest.mark.parametrize(
    "case",
    [
        "part",
        "manifest",
        "unexpected",
        "duplicate_key",
        "part_traversal",
        "string_count",
        "range",
        "total",
        "version",
    ],
)
async def test_catalog_checksums_schema_and_names_are_fail_closed(tmp_path, case):
    database, _ = await source(tmp_path)
    output = tmp_path / "sealed"
    await rotation.rotate_archive(database, output, 3000)
    catalog_path = output / "catalog.json"
    values = json.loads(catalog_path.read_text())
    if case == "part":
        with (output / values["parts"][0]["file"]).open("ab") as stream:
            stream.write(b"corrupt")
    elif case == "manifest":
        with manifest_path(output / values["parts"][0]["file"]).open("a") as stream:
            stream.write(" ")
    elif case == "unexpected":
        (output / "rotation-failed.json").write_text("{}")
    elif case == "duplicate_key":
        catalog_path.write_text(
            catalog_path.read_text().replace(
                '"format_version": 1,', '"format_version": 1, "format_version": 1,'
            )
        )
    else:
        if case == "part_traversal":
            values["parts"][0]["file"] = "../part-000000.parquet"
        elif case == "string_count":
            values["parts"][0]["rows"] = "2"
        elif case == "range":
            values["parts"][0]["end_ms"] += 1
        elif case == "total":
            values["total_rows"] += 1
        else:
            values["format_version"] = 2
        catalog_path.write_text(json.dumps(values))
    with pytest.raises((IntegrityError, ValidationError)):
        rotation.validate_rotation_catalog(output)


async def test_symlink_and_windows_reparse_refusal_without_privilege(tmp_path, monkeypatch):
    database, _ = await source(tmp_path)
    from pathlib import Path

    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == database or original(self))
    with pytest.raises(ValueError, match="symlink"):
        await rotation.rotate_archive(database, tmp_path / "no", 3000)
    monkeypatch.setattr(Path, "is_symlink", original)
    stat = Path.lstat

    class Reparse:
        st_file_attributes = 0x400
        st_mode = 0o100644

    monkeypatch.setattr(Path, "lstat", lambda self: Reparse() if self == database else stat(self))
    with pytest.raises(ValueError, match="reparse"):
        await rotation.rotate_archive(database, tmp_path / "no", 3000)


async def test_repeated_cancellation_drains_closes_and_does_not_publish(tmp_path, monkeypatch):
    database, _ = await source(tmp_path)
    original = rotation._verify_part
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        try:
            return original(*args)
        finally:
            closed.set()

    monkeypatch.setattr(rotation, "_verify_part", blocked)
    before = file_digest(database)
    output = tmp_path / "interrupted"
    task = asyncio.create_task(rotation.rotate_archive(database, output, 3000, batch_size=1))
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set() and not (output / "catalog.json").exists()
    assert (
        json.loads((output / "rotation-failed.json").read_text())["failure_type"]
        == "InterruptedError"
    )
    assert file_digest(database) == before
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone() == (2,)


@pytest.mark.parametrize(
    "control,value",
    [
        ("format_version", True),
        ("format_version", 1.0),
        *[
            (name, value)
            for name in (
                "delivery_acknowledgements_copied",
                "source_history_modified",
                "history_complete",
                "ready_for_signals",
                "trading_enabled",
            )
            for value in (0, 0.0)
        ],
    ],
)
async def test_control_literal_equality_cannot_coerce_numeric_aliases(tmp_path, control, value):
    database, _ = await source(tmp_path)
    output = tmp_path / "strict-controls"
    await rotation.rotate_archive(database, output, 1)
    values = json.loads((output / "catalog.json").read_text())
    values[control] = value
    (output / "catalog.json").write_text(json.dumps(values))
    with pytest.raises(ValidationError):
        rotation.validate_rotation_catalog(output)


async def test_max_bigint_clock_and_half_open_partition_end_are_preserved(tmp_path):
    maximum = rotation.MAX_CLOCK
    database, rows = await source(tmp_path, [event(maximum, maximum)])
    output = tmp_path / "maximum-clock"
    report = await rotation.rotate_archive(database, output, maximum, partition_ms=86_400_000)
    part = report["parts"][0]
    assert part["start_ms"] <= maximum < part["end_ms"]
    assert rotation.validate_rotation_catalog(output).total_rows == 1
    with duckdb.connect() as connection:
        saved = connection.execute(
            "SELECT * FROM read_parquet(?)", [str(output / part["file"])]
        ).fetchone()
        assert saved == tuple(rows[0].model_dump().values())


async def test_publish_failure_leaves_inspectable_chunks_and_failed_journal(tmp_path, monkeypatch):
    database, _ = await source(tmp_path)
    before = file_digest(database)

    def unavailable(*args):
        raise OSError("exclusive catalog link unavailable")

    monkeypatch.setattr(rotation.os, "link", unavailable)
    output = tmp_path / "no-publish"
    with pytest.raises(OSError):
        await rotation.rotate_archive(database, output, 3000)
    assert not (output / "catalog.json").exists()
    assert (output / "part-000000.parquet").is_file()
    assert (output / ".catalog.pending.json").is_file()
    assert json.loads((output / "rotation-failed.json").read_text())["catalog_published"] is False
    assert file_digest(database) == before


async def test_source_change_after_reader_close_prevents_publication(tmp_path, monkeypatch):
    database, _ = await source(tmp_path)
    original = rotation.file_digest
    reads = 0

    def changed(path):
        nonlocal reads
        if path == database:
            reads += 1
            if reads == 2:
                # This is an explicit synthetic concurrent external mutation, not
                # an exporter write or real outage. Its bytes must not be accepted.
                with path.open("ab") as stream:
                    stream.write(b"external concurrent change")
        return original(path)

    monkeypatch.setattr(rotation, "file_digest", changed)
    output = tmp_path / "source-changed"
    with pytest.raises(IntegrityError, match="SOURCE_CHANGED"):
        await rotation.rotate_archive(database, output, 3000)
    assert not (output / "catalog.json").exists()
    assert (output / "rotation-failed.json").is_file()


@pytest.mark.parametrize("case", ["malformed_id", "nonfinite_payload"])
async def test_unrestorable_generic_envelope_cannot_be_sealed(tmp_path, case):
    database, _ = await source(tmp_path)
    with duckdb.connect(str(database)) as connection:
        if case == "malformed_id":
            connection.execute("UPDATE events SET event_id='malformed' WHERE event_time_ms=999")
        else:
            connection.execute("UPDATE events SET payload_json='{\"amount\":NaN}'")
    before = file_digest(database)
    output = tmp_path / "invalid-envelope"
    with pytest.raises((IntegrityError, ValueError)):
        await rotation.rotate_archive(database, output, 3000)
    assert file_digest(database) == before
    assert not (output / "catalog.json").exists()
    assert json.loads((output / "rotation-failed.json").read_text())["status"] == "FAILED"
