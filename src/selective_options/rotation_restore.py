"""Restore a complete verified rotation into a new, explicitly staged database."""

import asyncio
import json
import re
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

import duckdb

from .archive import file_digest
from .archive_rotation import RotationCatalog, RotationPart, validate_rotation_catalog
from .errors import IntegrityError
from .events import MarketEvent
from .store import DuckStore, acquire_thread_resource, complete_thread

_TYPES = {
    field: "BIGINT" if field in {"event_time_ms", "observed_at_ms"} else "VARCHAR"
    for field in MarketEvent.model_fields
}
_TYPES["schema_version"] = "INTEGER"


def _paths(catalog: Path, output: Path) -> None:
    for path in (catalog, output):
        if any(char in str(path) for char in "*?[") or ".." in path.parts:
            raise ValueError("rotation restore requires explicit local paths")
        for component in (path.absolute(), *path.absolute().parents):
            if component.is_symlink() or (
                component.exists() and getattr(component.lstat(), "st_file_attributes", 0) & 0x400
            ):
                raise ValueError("rotation restore refuses symlink or junction paths")
    if catalog.name != "catalog.json" or not catalog.is_file():
        raise ValueError("rotation restore requires an existing catalog.json")
    if output.exists() or not output.parent.is_dir():
        raise ValueError("restore requires a new output directory with an existing parent")
    if output.absolute().is_relative_to(catalog.parent.absolute()):
        raise ValueError("restore output must be outside the immutable rotation directory")


def _reader(path: Path, expected: int) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    try:
        schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        ).fetchall()
        if tuple((row[0], row[1]) for row in schema) != tuple(_TYPES.items()):
            raise IntegrityError("ROTATION_ARCHIVE_SCHEMA_MISMATCH")
        counts = connection.execute(
            "SELECT count(*), count(DISTINCT event_id) FROM read_parquet(?, hive_partitioning=false)",
            [str(path)],
        ).fetchone()
        if counts != (expected, expected):
            raise IntegrityError("ROTATION_PART_COUNT_OR_DUPLICATE_MISMATCH")
        connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY observed_at_ms,event_id",
            [str(path)],
        )
        return connection
    except BaseException:
        connection.close()
        raise


def _batch(reader: duckdb.DuckDBPyConnection, size: int) -> list[MarketEvent]:
    columns = [column[0] for column in reader.description]
    return [
        MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
        for row in reader.fetchmany(size)
    ]


def _validate_rows(rows: list[MarketEvent], part: RotationPart, cutoff: int) -> None:
    for row in rows:
        if not re.fullmatch(r"[0-9a-f]{64}", row.event_id):
            raise IntegrityError("ROTATION_INVALID_EVENT_ID")
        json.dumps(row.payload(), allow_nan=False)
        if row.event_time_ms > cutoff or row.observed_at_ms > cutoff:
            raise IntegrityError("ROTATION_EVENT_AFTER_CUTOFF")
        available = max(row.event_time_ms, row.observed_at_ms)
        if not part.start_ms <= available < part.end_ms:
            raise IntegrityError("ROTATION_EVENT_OUTSIDE_PARTITION")


def _counts(store: DuckStore, expected: int) -> None:
    counts = store._connection.execute(
        "SELECT count(*),count(DISTINCT event_id) FROM events"
    ).fetchone()
    outbox = store._connection.execute(
        "SELECT count(*),count(DISTINCT event_id),count(*) FILTER(WHERE delivered) FROM delivery_outbox"
    ).fetchone()
    membership = store._connection.execute(
        "SELECT count(*) FROM events e FULL OUTER JOIN delivery_outbox o USING(event_id) "
        "WHERE e.event_id IS NULL OR o.event_id IS NULL"
    ).fetchone()
    if counts != (expected, expected) or outbox != (expected, expected, 0) or membership != (0,):
        raise IntegrityError("ROTATION_GLOBAL_COUNT_OR_DUPLICATE_MISMATCH")


def _recheck(catalog: Path, initial: RotationCatalog, digest: str) -> None:
    _paths_for_existing_catalog(catalog)
    final = validate_rotation_catalog(catalog.parent)
    if final != initial or file_digest(catalog) != digest:
        raise IntegrityError("ROTATION_SOURCE_CHANGED_DURING_RESTORE")


def _paths_for_existing_catalog(catalog: Path) -> None:
    for component in (catalog.absolute(), *catalog.absolute().parents):
        if component.is_symlink() or (
            component.exists() and getattr(component.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise IntegrityError("ROTATION_SOURCE_PATH_CHANGED")


async def _cleanup(operation: Coroutine[Any, Any, None]) -> None:
    """Drain cleanup despite additional cancellation of the calling task."""
    task = asyncio.create_task(operation)
    while True:
        try:
            await asyncio.shield(task)
            return
        except asyncio.CancelledError:
            if task.done():
                task.result()
                return


def _publish(output: Path, report: dict[str, object]) -> None:
    with (output / "restore-prepared.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    (output / "restoring.duckdb").rename(output / "events.duckdb")
    (output / "restore-prepared.json").rename(output / "restore.json")


def _failed(output: Path, failure: str) -> None:
    # A cancellation delivered after the publication worker finishes still revokes
    # publication before propagating to the caller; the database remains staging.
    final = output / "events.duckdb"
    if final.exists():
        final.rename(output / "restoring.duckdb")
    success = output / "restore.json"
    if success.exists():
        success.rename(output / "restore-incomplete.json")
        incomplete = json.loads((output / "restore-incomplete.json").read_text(encoding="utf-8"))
        incomplete["status"] = "PUBLICATION_REVOKED"
        (output / "restore-incomplete.json").write_text(
            json.dumps(incomplete, indent=2) + "\n", encoding="utf-8"
        )
    with (output / "restore-failed.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps({"status": "FAILED", "failure_type": failure}) + "\n")


async def restore_rotation(
    catalog: Path, output: Path, batch_size: int = 500, max_skew_ms: int = 5000
) -> dict[str, object]:
    """Check every listed chunk, preserve clocks, and requeue every event on success.

    Batch size bounds Python materialization, not DuckDB execution cost. The
    catalog is a cutoff snapshot, not proof of complete history or trading readiness.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError("restore batch size must be an integer from 1 through 10000")
    if type(max_skew_ms) is not int or not 0 <= max_skew_ms <= 86400000:
        raise ValueError("restore skew must be an integer from 0 through 86400000")
    await complete_thread(_paths, catalog, output)
    created: list[bool] = []
    store: DuckStore | None = None
    reader: duckdb.DuckDBPyConnection | None = None

    def create() -> None:
        output.mkdir(exist_ok=False)
        created.append(True)

    async def failure_cleanup(exc: BaseException) -> None:
        try:
            if reader is not None:
                await complete_thread(reader.close)
        finally:
            try:
                if store is not None:
                    await store.close()
            finally:
                if created:
                    await complete_thread(_failed, output, type(exc).__name__)

    try:
        await complete_thread(create)
        initial = await complete_thread(validate_rotation_catalog, catalog.parent)
        digest = await complete_thread(file_digest, catalog)
        store = await DuckStore.open(output / "restoring.duckdb", max_skew_ms)
        restored = 0
        for part in initial.parts:
            reader = await acquire_thread_resource(
                _reader, lambda value: value.close(), catalog.parent / part.file, part.rows
            )
            part_rows = 0
            while rows := await complete_thread(_batch, reader, batch_size):
                _validate_rows(rows, part, initial.as_of_ms)
                await store.append(rows, exact=True)
                part_rows += len(rows)
                restored += len(rows)
            if part_rows != part.rows:
                raise IntegrityError("ROTATION_PART_RESTORE_COUNT_MISMATCH")
            await complete_thread(reader.close)
            reader = None
        if restored != initial.total_rows:
            raise IntegrityError("ROTATION_RESTORE_ROW_COUNT_MISMATCH")
        await complete_thread(_counts, store, restored)
        await complete_thread(_recheck, catalog, initial, digest)
        await store.close()
        store = None
        await complete_thread(_recheck, catalog, initial, digest)
        report: dict[str, object] = {
            "status": "ROTATION_RESTORE_FINISHED",
            "rows": restored,
            "parts": len(initial.parts),
            "catalog_sha256": digest,
            "source_sha256": initial.source_sha256,
            "as_of_ms": initial.as_of_ms,
            "partition_ms": initial.partition_ms,
            "observation_timestamps_preserved": True,
            "source_timestamps_preserved": True,
            "redis_delivery_pending": restored,
            "delivery": "AT_LEAST_ONCE",
            "consumer_deduplication_key": "event_id",
            "history_complete": False,
            "ready_for_signals": False,
            "trading_enabled": False,
            "limitation": "Delivery acknowledgements are not archived; all events are pending replay.",
        }
        await complete_thread(_publish, output, report)
        return report
    except BaseException as exc:
        await _cleanup(failure_cleanup(exc))
        raise
