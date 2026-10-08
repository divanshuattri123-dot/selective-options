"""Checksummed event archives and fail-closed restore into a new database."""

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from .errors import IntegrityError
from .events import MarketEvent
from .store import DuckStore, acquire_thread_resource, complete_thread


class ArchiveManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    format_version: Literal[1] = 1
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: int = Field(ge=0)
    bytes: int = Field(ge=0)


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def manifest_path(archive: Path) -> Path:
    return archive.with_suffix(archive.suffix + ".manifest.json")


def _write_manifest(archive: Path, rows: int) -> ArchiveManifest:
    manifest = ArchiveManifest(sha256=file_digest(archive), rows=rows, bytes=archive.stat().st_size)
    with manifest_path(archive).open("x", encoding="utf-8") as stream:
        stream.write(manifest.model_dump_json(indent=2) + "\n")
    return manifest


async def seal_archive(archive: Path, rows: int) -> ArchiveManifest:
    return await complete_thread(_write_manifest, archive, rows)


def _verify_archive(archive: Path) -> ArchiveManifest:
    if any(character in str(archive) for character in "*?[") or not archive.is_file():
        raise ValueError("restore requires one existing local Parquet file, not a glob")
    manifest = ArchiveManifest.model_validate_json(
        manifest_path(archive).read_text(encoding="utf-8")
    )
    if archive.stat().st_size != manifest.bytes or file_digest(archive) != manifest.sha256:
        raise IntegrityError("ARCHIVE_CHECKSUM_MISMATCH")
    return manifest


def _open_reader(archive: Path, expected_rows: int) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0", [str(archive)]
        )
        columns = [column[0] for column in cursor.description]
        if set(columns) != set(MarketEvent.model_fields) or len(columns) != len(
            MarketEvent.model_fields
        ):
            raise IntegrityError("ARCHIVE_SCHEMA_MISMATCH")
        counts = connection.execute(
            "SELECT count(*), count(DISTINCT event_id) FROM read_parquet(?, hive_partitioning=false)",
            [str(archive)],
        ).fetchone()
        if counts != (expected_rows, expected_rows):
            raise IntegrityError("ARCHIVE_COUNT_OR_DUPLICATE_ID_MISMATCH")
        connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY observed_at_ms,event_id",
            [str(archive)],
        )
        return connection
    except BaseException:
        connection.close()
        raise


def _batch(connection: duckdb.DuckDBPyConnection, size: int) -> list[MarketEvent]:
    columns = [column[0] for column in connection.description]
    return [
        MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
        for row in connection.fetchmany(size)
    ]


async def restore_archive(
    archive: Path, output: Path, batch_size: int = 500, max_skew_ms: int = 5000
) -> dict[str, object]:
    if batch_size <= 0:
        raise ValueError("restore batch size must be positive")
    manifest = await complete_thread(_verify_archive, archive)
    reader = await acquire_thread_resource(
        _open_reader, lambda value: value.close(), archive, manifest.rows
    )
    store: DuckStore | None = None
    created = False
    staging = output / "restoring.duckdb"
    try:
        await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
        created = True
        store = await DuckStore.open(staging, max_skew_ms)
        restored = 0
        while rows := await complete_thread(_batch, reader, batch_size):
            await store.append(rows)
            restored += len(rows)
        if restored != manifest.rows:
            raise IntegrityError("RESTORE_ROW_COUNT_MISMATCH")
        if await complete_thread(file_digest, archive) != manifest.sha256:
            raise IntegrityError("ARCHIVE_CHANGED_DURING_RESTORE")
        await store.close()
        store = None
        await asyncio.to_thread(staging.rename, output / "events.duckdb")
        report: dict[str, object] = {
            "status": "RESTORE_FINISHED",
            "rows": restored,
            "archive_sha256": manifest.sha256,
            "observation_timestamps_preserved": True,
            "redis_delivery_pending": restored,
            "ready_for_signals": False,
            "trading_enabled": False,
            "limitation": "Event archive only; Redis acknowledgements are not archived. Replay is at-least-once and consumers deduplicate event_id.",
        }
        await asyncio.to_thread(
            (output / "restore.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        return report
    except BaseException as exc:
        if created:
            await asyncio.to_thread(
                (output / "restore-failed.json").write_text,
                json.dumps({"status": "FAILED", "failure_type": type(exc).__name__}) + "\n",
                encoding="utf-8",
            )
        raise
    finally:
        try:
            if store is not None:
                await store.close()
        finally:
            await complete_thread(reader.close)
