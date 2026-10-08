"""Immutable availability partitions from one closed, read-only event snapshot.

The batch and partition budgets bound application materialization and file count,
not SQL scans, engine memory, database size, or execution time. No source records
or delivery acknowledgements are changed. A catalog is published only after every
partition passes byte, schema, identity, clock, and exact typed-record checks.
"""

import asyncio
import json
import os
import re
import threading
from pathlib import Path
from typing import Literal, Self, cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .archive import ArchiveManifest, _write_manifest, file_digest, manifest_path
from .errors import IntegrityError
from .events import MarketEvent

MAX_CLOCK = (1 << 63) - 1
SOURCE_SCOPE: Literal["all sources and environments in events; both clocks at or before cutoff"] = (
    "all sources and environments in events; both clocks at or before cutoff"
)
_SCHEMA = (
    ("event_id", "VARCHAR"),
    ("source", "VARCHAR"),
    ("environment", "VARCHAR"),
    ("asset", "VARCHAR"),
    ("symbol", "VARCHAR"),
    ("kind", "VARCHAR"),
    ("event_time_ms", "BIGINT"),
    ("observed_at_ms", "BIGINT"),
    ("timestamp_kind", "VARCHAR"),
    ("schema_version", "INTEGER"),
    ("payload_json", "VARCHAR"),
)
_ELIGIBLE = "event_time_ms<=? AND observed_at_ms<=?"


class RotationPart(BaseModel):
    """One exact nonempty half-open availability bucket; names never contain paths."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    index: int = Field(ge=0, le=999)
    file: str
    manifest: str
    start_ms: int = Field(ge=0, le=MAX_CLOCK)
    end_ms: int = Field(gt=0, le=MAX_CLOCK + 86_400_000)
    rows: int = Field(gt=0)
    bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def canonical_names(self) -> Self:
        if self.file != f"part-{self.index:06d}.parquet" or self.manifest != (
            self.file + ".manifest.json"
        ):
            raise ValueError("rotation files must have fixed indexed basenames")
        if self.end_ms <= self.start_ms:
            raise ValueError("rotation partition must have a positive half-open range")
        return self


class RotationCatalog(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    format_version: Literal[1] = 1
    status: Literal["ROTATION_VERIFIED"]
    as_of_ms: int = Field(gt=0, le=MAX_CLOCK)
    partition_ms: int = Field(ge=1, le=86_400_000)
    total_rows: int = Field(ge=0)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_scope: Literal["all sources and environments in events; both clocks at or before cutoff"]
    delivery_acknowledgements_copied: Literal[False]
    source_history_modified: Literal[False]
    history_complete: Literal[False]
    ready_for_signals: Literal[False]
    trading_enabled: Literal[False]
    parts: tuple[RotationPart, ...] = Field(max_length=1000)

    @model_validator(mode="before")
    @classmethod
    def strict_controls(cls, value: object) -> object:
        if isinstance(value, dict):
            if "format_version" in value and type(value["format_version"]) is not int:
                raise ValueError("rotation format_version must be integer 1")
            for name in (
                "delivery_acknowledgements_copied",
                "source_history_modified",
                "history_complete",
                "ready_for_signals",
                "trading_enabled",
            ):
                if name in value and type(value[name]) is not bool:
                    raise ValueError("rotation controls must be literal boolean false")
            # A before validator receives decoded JSON arrays as Python lists; retain
            # the immutable tuple representation without coercing individual fields.
            if isinstance(value.get("parts"), list):
                value = dict(value)
                value["parts"] = tuple(value["parts"])
        return value

    @model_validator(mode="after")
    def ordered_partitions(self) -> Self:
        previous = -1
        for index, part in enumerate(self.parts):
            if (
                part.index != index
                or part.start_ms % self.partition_ms
                or part.end_ms != part.start_ms + self.partition_ms
                or part.start_ms <= previous
                or part.start_ms > self.as_of_ms
            ):
                raise ValueError("rotation partitions must be aligned, ordered and eligible")
            previous = part.start_ms
        if sum(part.rows for part in self.parts) != self.total_rows:
            raise ValueError("rotation catalog row total does not equal partition totals")
        return self


def _safe_path(path: Path) -> Path:
    if any(character in str(path) for character in "*?[") or ".." in path.parts:
        raise ValueError("rotation requires an explicit local path without glob or traversal")
    absolute = path.absolute()
    for component in (absolute, *absolute.parents):
        if component.is_symlink():
            raise ValueError("rotation refuses symlink path components")
        if component.exists() and getattr(component.lstat(), "st_file_attributes", 0) & 0x400:
            raise ValueError("rotation refuses reparse/junction path components")
    return absolute


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError("ARCHIVE_ROTATION_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _json_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    json.loads(raw, object_pairs_hook=_unique_pairs)
    return raw


def _policy(as_of_ms: int, partition_ms: int, batch_size: int, max_partitions: int) -> None:
    for name, value, minimum, maximum in (
        ("as_of_ms", as_of_ms, 1, MAX_CLOCK),
        ("partition_ms", partition_ms, 1, 86_400_000),
        ("batch_size", batch_size, 1, 10_000),
        ("max_partitions", max_partitions, 1, 1000),
    ):
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"rotation {name} must be an integer in [{minimum}, {maximum}]")


def _check_parts(directory: Path, catalog: RotationCatalog) -> None:
    expected = {"catalog.json"}
    for part in catalog.parts:
        archive = _safe_path(directory / part.file)
        companion = _safe_path(directory / part.manifest)
        if not archive.is_file() or not companion.is_file():
            raise IntegrityError("ARCHIVE_ROTATION_MISSING_PART")
        if (
            archive.stat().st_size != part.bytes
            or file_digest(archive) != part.sha256
            or file_digest(companion) != part.manifest_sha256
        ):
            raise IntegrityError("ARCHIVE_ROTATION_CHECKSUM_MISMATCH")
        if companion.stat().st_size > 4096:
            raise IntegrityError("ARCHIVE_ROTATION_MANIFEST_OVERSIZE")
        manifest = ArchiveManifest.model_validate_json(_json_text(companion), strict=True)
        if (manifest.rows, manifest.bytes, manifest.sha256) != (
            part.rows,
            part.bytes,
            part.sha256,
        ):
            raise IntegrityError("ARCHIVE_ROTATION_MANIFEST_MISMATCH")
        expected.update((part.file, part.manifest))
    # A failed/in-progress journal or unexplained file cannot masquerade as a sealed rotation.
    if {entry.name for entry in directory.iterdir()} != expected:
        raise IntegrityError("ARCHIVE_ROTATION_UNEXPECTED_FILE")


def validate_rotation_catalog(directory: Path) -> RotationCatalog:
    """Validate fixed catalog names and every byte/manifest; content checks are separate.

    This helper performs synchronous local I/O. Async callers must run it in a drained
    worker. A valid catalog proves a finite saved cohort, not complete market history.
    """
    directory = _safe_path(directory)
    if not directory.is_dir():
        raise FileNotFoundError("rotation directory does not exist")
    catalog_path = _safe_path(directory / "catalog.json")
    if not catalog_path.is_file() or catalog_path.stat().st_size > 1_048_576:
        raise ValueError("rotation requires one bounded published catalog.json")
    catalog = RotationCatalog.model_validate_json(_json_text(catalog_path))
    _check_parts(directory, catalog)
    return catalog


def _schema(connection: duckdb.DuckDBPyConnection, query: str, params: list[object]) -> None:
    cursor = connection.execute(query, params)
    if tuple((entry[0], str(entry[1])) for entry in cursor.description) != _SCHEMA:
        raise IntegrityError("ARCHIVE_ROTATION_SCHEMA_MISMATCH")


def _stopped(stop: threading.Event) -> None:
    if stop.is_set():
        raise InterruptedError("archive rotation interrupted before publication")


def _verify_part(
    connection: duckdb.DuckDBPyConnection,
    archive: Path,
    start_ms: int,
    end_ms: int,
    count: int,
    as_of_ms: int,
    batch_size: int,
    stop: threading.Event,
) -> None:
    _schema(
        connection,
        "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0",
        [str(archive)],
    )
    counts = connection.execute(
        "SELECT count(*), count(DISTINCT event_id), "
        "count(*) FILTER (WHERE event_time_ms>? OR observed_at_ms>? "
        "OR greatest(event_time_ms,observed_at_ms)<? "
        "OR greatest(event_time_ms,observed_at_ms)>=?) "
        "FROM read_parquet(?, hive_partitioning=false)",
        [as_of_ms, as_of_ms, start_ms, end_ms, str(archive)],
    ).fetchone()
    if counts != (count, count, 0):
        raise IntegrityError("ARCHIVE_ROTATION_COUNT_ID_OR_CLOCK_MISMATCH")
    # Join against the same read-only transaction. Compare all 11 typed fields, not
    # just identities/hashes; fetchmany bounds application rows for the full audit.
    cursor = connection.execute(
        "SELECT e.*, p.* FROM events e "
        "JOIN read_parquet(?, hive_partitioning=false) p USING(event_id) "
        "WHERE e.event_time_ms<=? AND e.observed_at_ms<=? "
        "AND greatest(e.event_time_ms,e.observed_at_ms)>=? "
        "AND greatest(e.event_time_ms,e.observed_at_ms)<? "
        "ORDER BY e.event_time_ms,e.observed_at_ms,e.event_id",
        [str(archive), as_of_ms, as_of_ms, start_ms, end_ms],
    )
    checked = 0
    while rows := cursor.fetchmany(batch_size):
        _stopped(stop)
        for row in rows:
            native, saved = row[:11], row[11:]
            if native != saved:
                raise IntegrityError("ARCHIVE_ROTATION_TYPED_RECORD_MISMATCH")
            event = MarketEvent.model_validate(
                dict(zip(MarketEvent.model_fields, saved, strict=True)), strict=True
            )
            if not re.fullmatch(r"[0-9a-f]{64}", event.event_id):
                raise IntegrityError("ARCHIVE_ROTATION_INVALID_EVENT_ID")
            json.dumps(event.payload(), allow_nan=False)
            checked += 1
    if checked != count:
        raise IntegrityError("ARCHIVE_ROTATION_SOURCE_COHORT_MISMATCH")


def _publish(output: Path, catalog: RotationCatalog, stop: threading.Event) -> None:
    _stopped(stop)
    pending = output / ".catalog.pending.json"
    with pending.open("x", encoding="utf-8") as stream:
        stream.write(catalog.model_dump_json(indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    _stopped(stop)
    # Hard-link creation is exclusive and exposes the complete file atomically.
    # It cannot replace an existing catalog on either Windows or POSIX.
    os.link(pending, output / "catalog.json")
    pending.unlink()


def _rotate(
    database: Path,
    output: Path,
    as_of_ms: int,
    partition_ms: int,
    batch_size: int,
    max_partitions: int,
    stop: threading.Event,
) -> dict[str, object]:
    _policy(as_of_ms, partition_ms, batch_size, max_partitions)
    database, output = _safe_path(database), _safe_path(output)
    if not database.is_file():
        raise FileNotFoundError("rotation source must be one existing closed database")
    if database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("ARCHIVE_ROTATION_SOURCE_WAL_PRESENT")
    if output.exists():
        raise FileExistsError("refusing to overwrite an immutable rotation directory")
    if not output.parent.is_dir():
        raise FileNotFoundError("rotation output parent must already exist")
    source_sha = file_digest(database)
    created = False
    parts: list[RotationPart] = []
    try:
        # The sole connection is opened, used, rolled back and closed in this worker.
        # Incompatible active writers are rejected by DuckDB; none are interrupted.
        with duckdb.connect(str(database), read_only=True) as connection:
            connection.execute("BEGIN TRANSACTION")
            _schema(connection, "SELECT * FROM events LIMIT 0", [])
            ranges = connection.execute(
                "SELECT greatest(event_time_ms,observed_at_ms)//? AS bucket, count(*) "
                f"FROM events WHERE {_ELIGIBLE} GROUP BY bucket ORDER BY bucket LIMIT ?",
                [partition_ms, as_of_ms, as_of_ms, max_partitions + 1],
            ).fetchall()
            if len(ranges) > max_partitions:
                raise IntegrityError("ARCHIVE_ROTATION_PARTITION_BUDGET")
            counts = connection.execute(
                f"SELECT count(*),count(DISTINCT event_id) FROM events WHERE {_ELIGIBLE}",
                [as_of_ms, as_of_ms],
            ).fetchone()
            total = sum(count for _, count in ranges)
            if counts != (total, total):
                raise IntegrityError("ARCHIVE_ROTATION_GLOBAL_ID_MISMATCH")
            _stopped(stop)
            output.mkdir(exist_ok=False)
            created = True
            for index, (bucket, count) in enumerate(ranges):
                _stopped(stop)
                start_ms, end_ms = bucket * partition_ms, (bucket + 1) * partition_ms
                archive = output / f"part-{index:06d}.parquet"
                connection.execute(
                    "COPY (SELECT * FROM events WHERE event_time_ms<=$cutoff "
                    "AND observed_at_ms<=$cutoff "
                    "AND greatest(event_time_ms,observed_at_ms)>=$start "
                    "AND greatest(event_time_ms,observed_at_ms)<$end "
                    "ORDER BY event_time_ms,observed_at_ms,event_id) "
                    "TO $destination (FORMAT PARQUET, COMPRESSION ZSTD)",
                    {
                        "cutoff": as_of_ms,
                        "start": start_ms,
                        "end": end_ms,
                        "destination": str(archive),
                    },
                )
                _verify_part(
                    connection, archive, start_ms, end_ms, count, as_of_ms, batch_size, stop
                )
                manifest = _write_manifest(archive, count)
                parts.append(
                    RotationPart(
                        index=index,
                        file=archive.name,
                        manifest=manifest_path(archive).name,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        rows=count,
                        bytes=manifest.bytes,
                        sha256=manifest.sha256,
                        manifest_sha256=file_digest(manifest_path(archive)),
                    )
                )
            # Recheck every final sealed chunk against this exact transaction, after
            # manifests have been created and before publication/source close.
            for part in parts:
                _verify_part(
                    connection,
                    output / part.file,
                    part.start_ms,
                    part.end_ms,
                    part.rows,
                    as_of_ms,
                    batch_size,
                    stop,
                )
            connection.execute("ROLLBACK")
        _stopped(stop)
        if (
            database.with_suffix(database.suffix + ".wal").exists()
            or file_digest(database) != source_sha
        ):
            raise IntegrityError("ARCHIVE_ROTATION_SOURCE_CHANGED")
        catalog = RotationCatalog(
            status="ROTATION_VERIFIED",
            as_of_ms=as_of_ms,
            partition_ms=partition_ms,
            total_rows=sum(part.rows for part in parts),
            source_sha256=source_sha,
            source_scope=SOURCE_SCOPE,
            delivery_acknowledgements_copied=False,
            source_history_modified=False,
            history_complete=False,
            ready_for_signals=False,
            trading_enabled=False,
            parts=tuple(parts),
        )
        # Verify final bytes again before publication. No catalog or pending file exists yet.
        expected_names = {name for part in parts for name in (part.file, part.manifest)}
        if {entry.name for entry in output.iterdir()} != expected_names:
            raise IntegrityError("ARCHIVE_ROTATION_UNEXPECTED_FILE")
        for part in parts:
            if (
                file_digest(output / part.file) != part.sha256
                or file_digest(output / part.manifest) != part.manifest_sha256
            ):
                raise IntegrityError("ARCHIVE_ROTATION_CHANGED_BEFORE_PUBLICATION")
        _publish(output, catalog, stop)
        return cast(dict[str, object], catalog.model_dump(mode="json"))
    except BaseException as exc:
        if created and not (output / "catalog.json").exists():
            with (output / "rotation-failed.json").open("x", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(
                        {
                            "status": "FAILED",
                            "failure_type": type(exc).__name__,
                            "catalog_published": False,
                        }
                    )
                    + "\n"
                )
        raise


async def rotate_archive(
    database: Path,
    output: Path,
    as_of_ms: int,
    partition_ms: int = 86_400_000,
    batch_size: int = 500,
    max_partitions: int = 366,
) -> dict[str, object]:
    """Export one finite both-clock cohort. Cancellation drains before returning.

    A cancellation observed before publication prevents a final catalog. Interrupted
    files remain with a FAILED journal for inspection; nothing is recursively deleted.
    A catalog already published is immutable even if cancellation arrives afterwards.
    """
    _policy(as_of_ms, partition_ms, batch_size, max_partitions)
    stop = threading.Event()
    task = asyncio.create_task(
        asyncio.to_thread(
            _rotate, database, output, as_of_ms, partition_ms, batch_size, max_partitions, stop
        )
    )
    cancelled: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as exc:
            cancelled = cancelled or exc
            stop.set()
            if task.done():
                try:
                    result = task.result()
                except BaseException as failure:
                    raise cancelled from failure
                break
        except BaseException as failure:
            if cancelled is not None:
                raise cancelled from failure
            raise
    if cancelled is not None:
        raise cancelled
    return result
