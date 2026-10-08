import json

import duckdb
import pytest
from pydantic import ValidationError

from selective_options.archive import manifest_path, restore_archive, seal_archive
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.store import DuckStore


async def create_archive(tmp_path):
    store = await DuckStore.open(tmp_path / "original.duckdb")
    rows = [
        make_event(
            source="kraken",
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USD",
            kind="spot",
            event_time_ms=1000 + i,
            observed_at_ms=2000 + i,
            payload={"price": 100 + i},
        )
        for i in range(3)
    ]
    await store.append(rows)
    path = tmp_path / "quote's archive.parquet"
    count = await store.export_parquet(path)
    await store.close()
    await seal_archive(path, count)
    return path, rows


async def test_restore_preserves_rows_pit_and_outbox(tmp_path):
    archive, rows = await create_archive(tmp_path)
    output = tmp_path / "restored"
    report = await restore_archive(archive, output, batch_size=1)
    assert report["status"] == "RESTORE_FINISHED" and report["rows"] == 3
    assert report["trading_enabled"] is False
    assert not (output / "restoring.duckdb").exists()
    store = await DuckStore.open(output / "events.duckdb")
    assert not (await store.snapshot("BTC", 1999, "public_reference")).rows()
    assert (await store.snapshot("BTC", 2001, "public_reference")).rows() == tuple(rows[:2])
    assert await store.pending_delivery_count() == 3
    await store.close()
    with pytest.raises(FileExistsError):
        await restore_archive(archive, output)
    with pytest.raises(FileExistsError):
        await seal_archive(archive, 3)


async def test_corrupt_archive_rejected_before_destination_created(tmp_path):
    archive, _ = await create_archive(tmp_path)
    with archive.open("ab") as stream:
        stream.write(b"tampered")
    output = tmp_path / "bad"
    with pytest.raises(IntegrityError, match="CHECKSUM"):
        await restore_archive(archive, output)
    assert not output.exists()


@pytest.mark.parametrize("case", ["extra_column", "duplicate_id", "invalid_event"])
async def test_correct_hash_is_not_enough_for_bad_schema_or_data(tmp_path, case):
    source, _ = await create_archive(tmp_path)
    invalid = tmp_path / f"{case}.parquet"
    with duckdb.connect() as db:
        if case == "extra_column":
            sql = "COPY (SELECT *, 1 AS surprise FROM read_parquet($source)) TO $destination (FORMAT PARQUET)"
            db.execute(sql, {"source": str(source), "destination": str(invalid)})
            count = 3
        elif case == "duplicate_id":
            db.execute(
                "COPY (SELECT * FROM read_parquet($source) UNION ALL SELECT * FROM read_parquet($source)) TO $destination (FORMAT PARQUET)",
                {"source": str(source), "destination": str(invalid)},
            )
            count = 6
        else:
            db.execute(
                "COPY (SELECT * REPLACE ('invalid' AS environment) FROM read_parquet($source)) TO $destination (FORMAT PARQUET)",
                {"source": str(source), "destination": str(invalid)},
            )
            count = 3
    await seal_archive(invalid, count)
    output = tmp_path / "restored"
    with pytest.raises((IntegrityError, ValidationError)):
        await restore_archive(invalid, output, batch_size=1)
    assert not (output / "events.duckdb").exists()
    if case == "invalid_event":
        assert json.loads((output / "restore-failed.json").read_text())["status"] == "FAILED"


async def test_missing_manifest_glob_and_invalid_batch_refused(tmp_path):
    archive, _ = await create_archive(tmp_path)
    with pytest.raises(ValueError):
        await restore_archive(archive, tmp_path / "bad", batch_size=0)
    with pytest.raises(ValueError):
        await restore_archive(tmp_path / "*.parquet", tmp_path / "bad")
    # Renaming a manifest cannot cause a fallback to an unchecked archive.
    manifest_path(archive).rename(tmp_path / "saved-manifest.json")
    with pytest.raises(FileNotFoundError):
        await restore_archive(archive, tmp_path / "bad")


async def test_archive_mutation_during_restore_does_not_publish(tmp_path, monkeypatch):
    archive, _ = await create_archive(tmp_path)
    append = DuckStore.append

    async def tamper(self, rows):
        await append(self, rows)
        # Change the manifest-independent bytes after the initial validation.
        with archive.open("ab") as stream:
            stream.write(b"changed")

    monkeypatch.setattr(DuckStore, "append", tamper)
    with pytest.raises(IntegrityError, match="CHANGED_DURING"):
        await restore_archive(archive, tmp_path / "bad")
    assert not (tmp_path / "bad/events.duckdb").exists()
