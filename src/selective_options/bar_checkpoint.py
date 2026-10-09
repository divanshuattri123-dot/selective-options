"""Finite committed open-bar membership checkpoints for explicit stopped-capture resume.

The supplied consumed-input ledger is replayed exactly and bound to committed native
WebSocket rows. It does not establish completeness against arbitrary stored trade
history, an atomic sink boundary, periodic crash recovery, or current feed health.
Source delivery acknowledgements are never copied, and an active source is not hashed.
Budgets limit supplied records and artifact bytes, not SQL engine scans or lock duration.
Saved incomplete flags are conservative declared coverage metadata; this artifact
does not reconstruct historical gap lineage or promote an incomplete bucket.
"""

import asyncio
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Coroutine, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

from .bar_state import MAX_CLOCK, BarScope, decode_bar_state, encode_bar_state
from .bars import TradeBars
from .errors import IntegrityError
from .events import MarketEvent
from .spot_trades import stored_trade, trade_event
from .store import SCHEMA, DuckStore, complete_thread

DEFAULT_MAX_EVENTS = 10_000
DEFAULT_MAX_BOOKS = 32
DEFAULT_MAX_BYTES = 32 * 1024 * 1024


class CheckpointBook(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    state_blob: bytes
    input_events: tuple[MarketEvent, ...]
    tail_event: MarketEvent


class BarCheckpointResume(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    as_of_ms: StrictInt
    books: tuple[CheckpointBook, ...]
    events: tuple[MarketEvent, ...]


class CheckpointPart(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    state_file: str = Field(pattern=r"^state-[0-9]{6}\.json$")
    state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_bytes: StrictInt = Field(gt=0)
    scope: BarScope
    input_ids: tuple[str, ...]
    tail_id: str = Field(pattern=r"^[0-9a-f]{64}$")


class CheckpointManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    format_version: StrictInt = Field(default=1, ge=1, le=1)
    status: Literal["COMMITTED_BAR_CHECKPOINT"] = "COMMITTED_BAR_CHECKPOINT"
    as_of_ms: StrictInt = Field(gt=0, le=MAX_CLOCK)
    events_file: Literal["events.duckdb"] = "events.duckdb"
    events_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    events_bytes: StrictInt = Field(gt=0)
    event_ids: tuple[str, ...]
    books: tuple[CheckpointPart, ...]
    source_scope: Literal[
        "exact supplied consumed WS inputs and book tails committed at cutoff"
    ] = "exact supplied consumed WS inputs and book tails committed at cutoff"
    history_complete: bool = False
    source_frontier_complete: bool = False
    current_feed_health_verified: bool = False
    delivery_acknowledgements_copied: bool = False
    source_database_hash_claimed: bool = False
    periodic_crash_recovery: bool = False
    ready_for_signals: bool = False
    trading_enabled: bool = False

    @model_validator(mode="after")
    def controls(self) -> "CheckpointManifest":
        if any(
            getattr(self, name) is not False
            for name in (
                "history_complete",
                "source_frontier_complete",
                "current_feed_health_verified",
                "delivery_acknowledgements_copied",
                "source_database_hash_claimed",
                "periodic_crash_recovery",
                "ready_for_signals",
                "trading_enabled",
            )
        ):
            raise ValueError("checkpoint cannot promote history, health, ACK or live claims")
        return self


def _policy(as_of_ms: int, max_events: int, max_books: int, max_bytes: int) -> None:
    for name, value, maximum in (
        ("as_of_ms", as_of_ms, MAX_CLOCK),
        ("max_events", max_events, 100_000),
        ("max_books", max_books, 32),
        ("max_bytes", max_bytes, 64 * 1024 * 1024),
    ):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an explicit integer from 1 through {maximum}")


def _path(path: Path) -> Path:
    if not isinstance(path, Path) or any(part == ".." for part in path.parts):
        raise ValueError("checkpoint requires an explicit path without traversal")
    absolute = path.absolute()
    for component in (absolute, *absolute.parents):
        if any(char in component.name for char in "*?[]"):
            raise ValueError("checkpoint glob paths are refused")
        if component.is_symlink() or (
            component.exists() and getattr(component.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("checkpoint symlink/reparse/junction paths are refused")
    return absolute.resolve()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(path: Path, max_bytes: int) -> str:
    if path.stat().st_size > max_bytes:
        raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        while block := stream.read(min(1024 * 1024, max_bytes + 1 - total)):
            total += len(block)
            if total > max_bytes:
                raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
            digest.update(block)
    return digest.hexdigest()


def _read(path: Path, max_bytes: int) -> bytes:
    if path.stat().st_size > max_bytes:
        raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
    with path.open("rb") as stream:
        value = stream.read(max_bytes + 1)
    if len(value) > max_bytes:
        raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
    return value


@contextmanager
def _database_snapshot(
    database: Path, expected_sha256: str, expected_bytes: int, max_bytes: int
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Read SQL only from a private closed copy of the actual verified bytes.

    Source pre/post hashes alone cannot bind a DuckDB open to the bytes that were
    hashed when a source file is transiently replaced. The bounded bytes read
    here are immutable Python bytes, verified before the isolated temporary copy
    is created. Close the read-only connection before removing that private copy.
    """
    data = _read(database, max_bytes)
    if len(data) != expected_bytes or hashlib.sha256(data).hexdigest() != expected_sha256:
        raise IntegrityError("BAR_CHECKPOINT_DATABASE_CHANGED")
    with tempfile.TemporaryDirectory(prefix="selective-options-bar-read-") as directory:
        snapshot = Path(directory) / "events.duckdb"
        with snapshot.open("xb") as stream:
            stream.write(data)
        with duckdb.connect(str(snapshot), read_only=True) as connection:
            yield connection


def _event(event: MarketEvent, cutoff: int) -> MarketEvent:
    try:
        if type(event) is not MarketEvent or set(event.__dict__) != set(MarketEvent.model_fields):
            raise ValueError("invalid event object")
        if (
            type(event.schema_version) is not int
            or event.schema_version != 1
            or type(event.event_time_ms) is not int
            or type(event.observed_at_ms) is not int
            or not 0 < event.event_time_ms <= cutoff
            or not 0 < event.observed_at_ms <= cutoff
            or event.source != "kraken"
            or event.environment != "public_reference"
            or event.kind != "trade"
            or event.timestamp_kind != "source"
        ):
            raise ValueError("unavailable or non-native WS envelope")
        fresh = MarketEvent.model_validate(event.model_dump(), strict=True)
        payload = fresh.payload()
        if type(payload.get("trade_id")) is not int:
            raise ValueError("native trade ID must be an integer")
        trade = stored_trade(fresh)
        if not 0 <= trade.trade_id <= MAX_CLOCK:
            raise ValueError("native sequence ID exceeds BIGINT policy")
        if (
            trade.timestamp.tzinfo is None
            or trade_event(trade, fresh.asset, fresh.observed_at_ms) != fresh
        ):
            raise ValueError("native identity or payload mismatch")
        return fresh
    except (ValueError, TypeError, AttributeError, RecursionError) as exc:
        raise IntegrityError("BAR_CHECKPOINT_INVALID_NATIVE_EVENT") from exc


def _validate_books(
    books: tuple[CheckpointBook, ...], cutoff: int, max_events: int, max_books: int, max_bytes: int
) -> tuple[tuple[CheckpointBook, ...], tuple[MarketEvent, ...]]:
    if type(books) is not tuple or not 1 <= len(books) <= max_books:
        raise IntegrityError("BAR_CHECKPOINT_BOOK_BUDGET_OR_TYPE")
    output: list[CheckpointBook] = []
    events: dict[str, MarketEvent] = {}
    scopes: set[tuple[str, str, str, str]] = set()
    symbols: set[str] = set()
    config: tuple[tuple[int, ...], int] | None = None
    total = 0
    for book in books:
        if type(book) is not CheckpointBook or set(book.__dict__) != set(
            CheckpointBook.model_fields
        ):
            raise IntegrityError("BAR_CHECKPOINT_INVALID_BOOK")
        if type(book.state_blob) is not bytes or type(book.input_events) is not tuple:
            raise IntegrityError("BAR_CHECKPOINT_INVALID_BOOK")
        if len(book.input_events) > max_events:
            raise IntegrityError("BAR_CHECKPOINT_EVENT_BUDGET")
        total += len(book.state_blob)
        if total > max_bytes:
            raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
        state = decode_bar_state(book.state_blob, max_buckets=100_000, max_bytes=max_bytes)
        if state.as_of_ms != cutoff or state.scope is None:
            raise IntegrityError("BAR_CHECKPOINT_STATE_CUTOFF_OR_SCOPE")
        scope = state.scope.identity()
        if scope in scopes:
            raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_BOOK")
        scopes.add(scope)
        if scope[3] in symbols:
            raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_BOOK_SYMBOL")
        symbols.add(scope[3])
        current_config = state.timeframes_ms, state.lateness_ms
        if config is not None and config != current_config:
            raise IntegrityError("BAR_CHECKPOINT_CONFIGURATION_MISMATCH")
        config = current_config
        rows = tuple(_event(row, cutoff) for row in book.input_events)
        tail = _event(book.tail_event, cutoff)
        keys = [(row.source, row.environment, row.asset, row.symbol) for row in (*rows, tail)]
        if any(key != scope for key in keys):
            raise IntegrityError("BAR_CHECKPOINT_MIXED_SCOPE")
        identifiers = [stored_trade(row).trade_id for row in rows]
        if len(set(identifiers)) != len(identifiers):
            raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_INPUT")
        tail_native_id = stored_trade(tail).trade_id
        if any(
            identifier == tail_native_id and row != tail
            for identifier, row in zip(identifiers, rows, strict=True)
        ):
            raise IntegrityError("BAR_CHECKPOINT_CONFLICTING_TAIL_NATIVE_ID")
        if identifiers != sorted(identifiers):
            raise IntegrityError("BAR_CHECKPOINT_INPUT_ORDER")
        if any(identifier > tail_native_id for identifier in identifiers):
            raise IntegrityError("BAR_CHECKPOINT_TAIL_BEHIND_INPUT")
        retained = {(bucket.interval_ms, bucket.opening_ms) for bucket in state.buckets}
        if any(
            not any(
                (interval, row.event_time_ms // interval * interval) in retained
                for interval in state.timeframes_ms
            )
            for row in rows
        ):
            raise IntegrityError("BAR_CHECKPOINT_EXTRA_INPUT")
        replay = TradeBars(
            tuple(interval // 1000 for interval in state.timeframes_ms),
            state.coverage_start_ms,
            state.lateness_ms,
        )
        replay.identity = scope
        for row in rows:
            replay.add(row)
        replay.watermark_ms = state.watermark_ms
        replay.buckets = {
            key: value
            for key, value in replay.buckets.items()
            if value.opening_ms + value.interval_ms > state.watermark_ms
        }
        for bucket in state.buckets:
            key = bucket.interval_ms, bucket.opening_ms
            if not bucket.complete and key in replay.buckets:
                replay.buckets[key].complete = False
        rebuilt = decode_bar_state(
            encode_bar_state(replay, cutoff, max_buckets=100_000, max_bytes=max_bytes),
            max_buckets=100_000,
            max_bytes=max_bytes,
        )
        if rebuilt != state:
            raise IntegrityError("BAR_CHECKPOINT_REPLAY_MISMATCH")
        clean = CheckpointBook(
            state_blob=bytes(book.state_blob), input_events=rows, tail_event=tail
        )
        output.append(clean)
        for row in (*rows, tail):
            previous = events.get(row.event_id)
            if previous is not None and previous != row:
                raise IntegrityError("BAR_CHECKPOINT_EVENT_ID_CONFLICT")
            if previous is None:
                events[row.event_id] = row
                total += len(_canonical(row.model_dump(mode="json")))
        if len(events) > max_events:
            raise IntegrityError("BAR_CHECKPOINT_EVENT_BUDGET")
        if total > max_bytes:
            raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
    return tuple(output), tuple(events[key] for key in sorted(events))


def _committed(
    store: DuckStore,
    events: tuple[MarketEvent, ...],
    books: tuple[CheckpointBook, ...],
    cutoff: int,
) -> None:
    if store._closed:
        raise IntegrityError("BAR_CHECKPOINT_STORE_CLOSED")
    connection = store._connection
    first = connection.execute("SELECT current_transaction_id()").fetchone()
    second = connection.execute("SELECT current_transaction_id()").fetchone()
    if first == second:
        raise IntegrityError("BAR_CHECKPOINT_SOURCE_TRANSACTION_OPEN")
    connection.execute("BEGIN TRANSACTION")
    try:
        _schema(connection)
        expected = {row.event_id: row for row in events}
        values = store._select(
            "SELECT e.* FROM events e SEMI JOIN json_each(?) ids ON e.event_id=(ids.value ->> '$') "
            "WHERE event_time_ms<=? AND observed_at_ms<=?",
            [json.dumps(list(expected)), cutoff, cutoff],
        )
        rows = tuple(
            _event(MarketEvent.model_validate(value, strict=True), cutoff) for value in values
        )
        if len(rows) != len(events) or {row.event_id: row for row in rows} != expected:
            raise IntegrityError("BAR_CHECKPOINT_COMMITTED_MEMBERSHIP_MISMATCH")
        for book in books:
            tail = book.tail_event
            native_ids = sorted({stored_trade(row).trade_id for row in (*book.input_events, tail)})
            identities = store._select(
                "SELECT e.* FROM events e SEMI JOIN json_each(?) ids ON "
                "TRY_CAST(json_extract_string(e.payload_json,'$.trade_id') AS BIGINT)=CAST(ids.value AS BIGINT) "
                "WHERE source='kraken' AND environment='public_reference' AND kind='trade' "
                "AND asset=? AND symbol=? AND event_time_ms<=? AND observed_at_ms<=? LIMIT ?",
                [
                    json.dumps(native_ids),
                    tail.asset,
                    tail.symbol,
                    cutoff,
                    cutoff,
                    len(native_ids) + 1,
                ],
            )
            if len(identities) != len(native_ids):
                raise IntegrityError("BAR_CHECKPOINT_CONFLICTING_COMMITTED_NATIVE_ID")
            latest = store._select(
                "SELECT * FROM events WHERE source='kraken' AND environment='public_reference' "
                "AND kind='trade' AND asset=? AND symbol=? AND event_time_ms<=? AND observed_at_ms<=? "
                "QUALIFY dense_rank() OVER (ORDER BY TRY_CAST(json_extract_string(payload_json,'$.trade_id') AS BIGINT) DESC)=1 LIMIT 2",
                [tail.asset, tail.symbol, cutoff, cutoff],
            )
            if (
                len(latest) != 1
                or _event(MarketEvent.model_validate(latest[0], strict=True), cutoff) != tail
            ):
                raise IntegrityError("BAR_CHECKPOINT_TAIL_NOT_UNIQUE_COMMITTED_FRONTIER")
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def _stage(
    store: DuckStore,
    output: Path,
    books: tuple[CheckpointBook, ...],
    events: tuple[MarketEvent, ...],
    cutoff: int,
    max_bytes: int,
    created: list[bool],
) -> CheckpointManifest:
    _committed(store, events, books, cutoff)
    if output.exists():
        raise FileExistsError("bar checkpoint requires a new output directory")
    if not output.parent.is_dir():
        raise FileNotFoundError("bar checkpoint parent must already exist")
    output.mkdir(exist_ok=False)
    created.append(True)
    database = output / "events.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(SCHEMA)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.executemany(
                "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [tuple(row.model_dump().values()) for row in events],
            )
            connection.execute("INSERT INTO delivery_outbox SELECT event_id,FALSE FROM events")
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
    parts = []
    for index, book in enumerate(books):
        filename = f"state-{index:06d}.json"
        with (output / filename).open("xb") as stream:
            stream.write(book.state_blob)
            stream.flush()
            os.fsync(stream.fileno())
        state = decode_bar_state(book.state_blob, max_buckets=100_000, max_bytes=max_bytes)
        assert state.scope is not None
        parts.append(
            CheckpointPart(
                state_file=filename,
                state_sha256=hashlib.sha256(book.state_blob).hexdigest(),
                state_bytes=len(book.state_blob),
                scope=state.scope,
                input_ids=tuple(row.event_id for row in book.input_events),
                tail_id=book.tail_event.event_id,
            )
        )
    manifest = CheckpointManifest(
        as_of_ms=cutoff,
        events_sha256=_digest(database, max_bytes),
        events_bytes=database.stat().st_size,
        event_ids=tuple(row.event_id for row in events),
        books=tuple(parts),
    )
    return manifest


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in pairs:
        if key in output:
            raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_JSON_KEY")
        output[key] = value
    return output


def _schema(connection: duckdb.DuckDBPyConnection) -> None:
    cursor = connection.execute("SELECT * FROM events LIMIT 0")
    expected = tuple(
        (
            name,
            "BIGINT"
            if name in {"event_time_ms", "observed_at_ms"}
            else "INTEGER"
            if name == "schema_version"
            else "VARCHAR",
        )
        for name in MarketEvent.model_fields
    )
    if tuple((item[0], str(item[1])) for item in cursor.description) != expected:
        raise IntegrityError("BAR_CHECKPOINT_SQL_SCHEMA_MISMATCH")
    cursor = connection.execute("SELECT * FROM delivery_outbox LIMIT 0")
    if tuple((item[0], str(item[1])) for item in cursor.description) != (
        ("event_id", "VARCHAR"),
        ("delivered", "BOOLEAN"),
    ):
        raise IntegrityError("BAR_CHECKPOINT_SQL_SCHEMA_MISMATCH")


def _verify(
    output: Path,
    manifest: CheckpointManifest,
    now_ms: int,
    assets: tuple[str, ...],
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    max_events: int,
    max_books: int,
    max_bytes: int,
    *,
    published: bool,
) -> BarCheckpointResume:
    if now_ms < manifest.as_of_ms:
        raise IntegrityError("BAR_CHECKPOINT_RESTART_BEFORE_CUTOFF")
    if not 1 <= len(manifest.books) <= max_books or not 1 <= len(manifest.event_ids) <= max_events:
        raise IntegrityError("BAR_CHECKPOINT_MEMBERSHIP_BUDGET")
    if any(len(part.input_ids) > max_events for part in manifest.books):
        raise IntegrityError("BAR_CHECKPOINT_MEMBERSHIP_BUDGET")
    if tuple(sorted(set(manifest.event_ids))) != manifest.event_ids:
        raise IntegrityError("BAR_CHECKPOINT_DUPLICATE_OR_UNORDERED_IDS")
    filenames = [f"state-{index:06d}.json" for index in range(len(manifest.books))]
    if [part.state_file for part in manifest.books] != filenames:
        raise IntegrityError("BAR_CHECKPOINT_STATE_FILENAMES")
    expected_names = set(filenames) | {"events.duckdb"}
    if published:
        expected_names.add("checkpoint.json")
    if {path.name for path in output.iterdir()} != expected_names:
        raise IntegrityError("BAR_CHECKPOINT_UNEXPLAINED_FILES")
    total_bytes = manifest.events_bytes + sum(part.state_bytes for part in manifest.books)
    if published:
        total_bytes += (output / "checkpoint.json").stat().st_size
    if total_bytes > max_bytes:
        raise IntegrityError("BAR_CHECKPOINT_BYTE_BUDGET")
    database = _path(output / "events.duckdb")
    if (
        not database.is_file()
        or database.stat().st_size != manifest.events_bytes
        or _digest(database, max_bytes) != manifest.events_sha256
    ):
        raise IntegrityError("BAR_CHECKPOINT_DATABASE_CHANGED")
    with _database_snapshot(
        database, manifest.events_sha256, manifest.events_bytes, max_bytes
    ) as connection:
        _schema(connection)
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [max_events + 1]
        )
        columns = [entry[0] for entry in cursor.description]
        values = cursor.fetchall()
        if len(values) != len(manifest.event_ids) or len(values) > max_events:
            raise IntegrityError("BAR_CHECKPOINT_DATABASE_MEMBERSHIP")
        rows = tuple(
            _event(
                MarketEvent.model_validate(dict(zip(columns, value, strict=True)), strict=True),
                manifest.as_of_ms,
            )
            for value in values
        )
        pending = connection.execute(
            "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id LIMIT ?",
            [max_events + 1],
        ).fetchall()
        if pending != [(row.event_id, False) for row in rows]:
            raise IntegrityError("BAR_CHECKPOINT_OUTBOX_NOT_PENDING")
    if tuple(row.event_id for row in rows) != manifest.event_ids:
        raise IntegrityError("BAR_CHECKPOINT_DATABASE_MEMBERSHIP")
    by_id = {row.event_id: row for row in rows}
    books = []
    for part in manifest.books:
        path = _path(output / part.state_file)
        if (
            not path.is_file()
            or path.stat().st_size != part.state_bytes
            or _digest(path, max_bytes) != part.state_sha256
        ):
            raise IntegrityError("BAR_CHECKPOINT_STATE_CHANGED")
        if part.scope.asset not in assets:
            raise IntegrityError("BAR_CHECKPOINT_ASSET_MISMATCH")
        blob = _read(path, max_bytes)
        # Bind the bytes actually decoded, not only path hashes before/after
        # the read. A transient replacement can otherwise restore the path
        # while returning a different, internally valid coverage declaration.
        if len(blob) != part.state_bytes or hashlib.sha256(blob).hexdigest() != part.state_sha256:
            raise IntegrityError("BAR_CHECKPOINT_STATE_CHANGED")
        state = decode_bar_state(blob, max_buckets=100_000, max_bytes=max_bytes)
        if (
            state.scope != part.scope
            or state.timeframes_ms != tuple(sorted(t * 1000 for t in timeframes_s))
            or state.lateness_ms != lateness_ms
        ):
            raise IntegrityError("BAR_CHECKPOINT_CONFIGURATION_MISMATCH")
        try:
            books.append(
                CheckpointBook(
                    state_blob=blob,
                    input_events=tuple(by_id[key] for key in part.input_ids),
                    tail_event=by_id[part.tail_id],
                )
            )
        except KeyError as exc:
            raise IntegrityError("BAR_CHECKPOINT_MISSING_REFERENCE") from exc
    clean, union = _validate_books(
        tuple(books), manifest.as_of_ms, max_events, max_books, max_bytes
    )
    if union != rows:
        raise IntegrityError("BAR_CHECKPOINT_EXTRA_DATABASE_EVENTS")
    # Close the reader before final hashes; a checkpoint is a closed local artifact.
    if _digest(database, max_bytes) != manifest.events_sha256:
        raise IntegrityError("BAR_CHECKPOINT_DATABASE_CHANGED")
    for part in manifest.books:
        if _digest(_path(output / part.state_file), max_bytes) != part.state_sha256:
            raise IntegrityError("BAR_CHECKPOINT_STATE_CHANGED")
    return BarCheckpointResume(as_of_ms=manifest.as_of_ms, books=clean, events=rows)


def _publish(output: Path, manifest: CheckpointManifest, max_bytes: int) -> None:
    if _digest(_path(output / "events.duckdb"), max_bytes) != manifest.events_sha256:
        raise IntegrityError("BAR_CHECKPOINT_DATABASE_CHANGED_BEFORE_PUBLICATION")
    for part in manifest.books:
        if _digest(_path(output / part.state_file), max_bytes) != part.state_sha256:
            raise IntegrityError("BAR_CHECKPOINT_STATE_CHANGED_BEFORE_PUBLICATION")
    data = _canonical(manifest.model_dump(mode="json")) + b"\n"
    if (
        len(data) + manifest.events_bytes + sum(part.state_bytes for part in manifest.books)
        > max_bytes
    ):
        raise IntegrityError("BAR_CHECKPOINT_MANIFEST_BYTE_BUDGET")
    pending = output / ".checkpoint.pending.json"
    with pending.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, output / "checkpoint.json")
    pending.unlink()


def _failed(output: Path, failure: str) -> None:
    output = _path(output)
    success = output / "checkpoint.json"
    if success.exists():
        success.rename(output / "checkpoint-incomplete.json")
    with (output / "failure.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps({"status": "FAILED", "failure_type": failure}) + "\n")


async def _cleanup(operation: Coroutine[Any, Any, None]) -> None:
    task = asyncio.create_task(operation)
    while True:
        try:
            await asyncio.shield(task)
            return
        except asyncio.CancelledError:
            if task.done():
                task.result()
                return


async def create_bar_checkpoint(
    store: DuckStore,
    output: Path,
    books: tuple[CheckpointBook, ...],
    as_of_ms: int,
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_books: int = DEFAULT_MAX_BOOKS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict[str, object]:
    """Verify a caller's exact stopped-capture consumed ledger and committed WS tails."""
    _policy(as_of_ms, max_events, max_books, max_bytes)
    if type(store) is not DuckStore:
        raise ValueError("bar checkpoint requires an exact DuckStore")
    output = _path(output)
    if output.exists():
        raise FileExistsError("bar checkpoint requires a new output directory")
    if not output.parent.is_dir():
        raise FileNotFoundError("bar checkpoint parent must already exist")
    clean, events = _validate_books(books, as_of_ms, max_events, max_books, max_bytes)
    state = decode_bar_state(clean[0].state_blob, max_buckets=100_000, max_bytes=max_bytes)
    created: list[bool] = []
    try:
        async with store._lock:
            manifest = await complete_thread(
                _stage, store, output, clean, events, as_of_ms, max_bytes, created
            )
        await complete_thread(
            _verify,
            output,
            manifest,
            as_of_ms,
            tuple(sorted({row.asset for row in events})),
            tuple(interval // 1000 for interval in state.timeframes_ms),
            state.lateness_ms,
            max_events,
            max_books,
            max_bytes,
            published=False,
        )
        await complete_thread(_publish, output, manifest, max_bytes)
        return manifest.model_dump(mode="json")
    except BaseException as exc:
        if created:
            await _cleanup(complete_thread(_failed, output, type(exc).__name__))
        raise


def _load(
    output: Path,
    now_ms: int,
    assets: tuple[str, ...],
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    max_events: int,
    max_books: int,
    max_bytes: int,
    expected_manifest_sha256: str | None = None,
) -> BarCheckpointResume:
    path = _path(output / "checkpoint.json")
    if not path.is_file():
        raise FileNotFoundError("committed bar checkpoint marker is required")
    before = _digest(path, max_bytes)
    if expected_manifest_sha256 is not None and before != expected_manifest_sha256:
        raise IntegrityError("BAR_CHECKPOINT_MANIFEST_DIGEST_MISMATCH")
    data = _read(path, max_bytes)
    actual = hashlib.sha256(data).hexdigest()
    if actual != before or (
        expected_manifest_sha256 is not None and actual != expected_manifest_sha256
    ):
        raise IntegrityError("BAR_CHECKPOINT_MANIFEST_CHANGED")
    json.loads(data, object_pairs_hook=_pairs)
    manifest = CheckpointManifest.model_validate_json(data, strict=True)
    if len(data) > max_bytes:
        raise IntegrityError("BAR_CHECKPOINT_MANIFEST_BYTE_BUDGET")
    result = _verify(
        output,
        manifest,
        now_ms,
        assets,
        timeframes_s,
        lateness_ms,
        max_events,
        max_books,
        max_bytes,
        published=True,
    )
    if _digest(path, max_bytes) != before:
        raise IntegrityError("BAR_CHECKPOINT_MANIFEST_CHANGED")
    return result


async def load_bar_checkpoint(
    output: Path,
    now_ms: int,
    assets: tuple[str, ...],
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_books: int = DEFAULT_MAX_BOOKS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    expected_manifest_sha256: str | None = None,
) -> BarCheckpointResume:
    """Validate actual read bytes, optionally pinned to an explicit manifest digest.

    Read-only validation precedes producer startup; restored bars must be incomplete.
    """
    _policy(now_ms, max_events, max_books, max_bytes)
    if expected_manifest_sha256 is not None and (
        type(expected_manifest_sha256) is not str
        or not re.fullmatch(r"[0-9a-f]{64}", expected_manifest_sha256)
    ):
        raise ValueError("manifest selection requires an explicit lowercase SHA256 digest")
    if (
        type(assets) is not tuple
        or not assets
        or len(set(assets)) != len(assets)
        or any(type(asset) is not str or not 1 <= len(asset) <= 256 for asset in assets)
        or type(timeframes_s) is not tuple
        or not 1 <= len(timeframes_s) <= 32
        or any(type(value) is not int or not 1 <= value <= 86400 for value in timeframes_s)
        or len(set(timeframes_s)) != len(timeframes_s)
        or type(lateness_ms) is not int
        or not 0 <= lateness_ms <= 86400000
    ):
        raise ValueError("explicit bar checkpoint restart scope/config is required")
    output = _path(output)
    if not output.is_dir():
        raise FileNotFoundError("bar checkpoint directory does not exist")
    return await complete_thread(
        _load,
        output,
        now_ms,
        assets,
        timeframes_s,
        lateness_ms,
        max_events,
        max_books,
        max_bytes,
        expected_manifest_sha256,
    )


def validate_bar_resume(
    resume: BarCheckpointResume,
    now_ms: int,
    assets: tuple[str, ...],
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_books: int = DEFAULT_MAX_BOOKS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> BarCheckpointResume:
    """Pure strict reconstruction; does not establish a new durable source commit."""
    _policy(now_ms, max_events, max_books, max_bytes)
    if (
        type(assets) is not tuple
        or not assets
        or len(set(assets)) != len(assets)
        or any(type(asset) is not str or not 1 <= len(asset) <= 256 for asset in assets)
        or type(timeframes_s) is not tuple
        or not 1 <= len(timeframes_s) <= 32
        or any(type(value) is not int or not 1 <= value <= 86400 for value in timeframes_s)
        or len(set(timeframes_s)) != len(timeframes_s)
        or type(lateness_ms) is not int
        or not 0 <= lateness_ms <= 86400000
    ):
        raise ValueError("explicit bar checkpoint restart scope/config is required")
    if type(resume) is not BarCheckpointResume or set(resume.__dict__) != set(
        BarCheckpointResume.model_fields
    ):
        raise IntegrityError("BAR_CHECKPOINT_INVALID_RESUME")
    _policy(resume.as_of_ms, max_events, max_books, max_bytes)
    if now_ms < resume.as_of_ms or type(resume.events) is not tuple:
        raise IntegrityError("BAR_CHECKPOINT_INVALID_RESUME")
    books, expected = _validate_books(
        resume.books, resume.as_of_ms, max_events, max_books, max_bytes
    )
    actual = tuple(_event(row, resume.as_of_ms) for row in resume.events)
    if actual != expected:
        raise IntegrityError("BAR_CHECKPOINT_RESUME_MEMBERSHIP")
    for book in books:
        state = decode_bar_state(book.state_blob, max_buckets=100_000, max_bytes=max_bytes)
        if (
            state.scope is None
            or state.scope.asset not in assets
            or state.timeframes_ms != tuple(sorted(t * 1000 for t in timeframes_s))
            or state.lateness_ms != lateness_ms
        ):
            raise IntegrityError("BAR_CHECKPOINT_CONFIGURATION_MISMATCH")
    return BarCheckpointResume(as_of_ms=resume.as_of_ms, books=books, events=actual)


def _verify_seed(
    database: Path,
    resume: BarCheckpointResume,
    now_ms: int,
    max_events: int,
    max_books: int,
    max_bytes: int,
) -> dict[str, object]:
    if type(resume) is not BarCheckpointResume or set(resume.__dict__) != set(
        BarCheckpointResume.model_fields
    ):
        raise IntegrityError("BAR_CHECKPOINT_INVALID_RESUME")
    _policy(resume.as_of_ms, max_events, max_books, max_bytes)
    if now_ms < resume.as_of_ms or type(resume.events) is not tuple:
        raise IntegrityError("BAR_CHECKPOINT_INVALID_RESUME")
    clean, expected = _validate_books(
        resume.books, resume.as_of_ms, max_events, max_books, max_bytes
    )
    actual = tuple(_event(row, resume.as_of_ms) for row in resume.events)
    if actual != expected:
        raise IntegrityError("BAR_CHECKPOINT_RESUME_MEMBERSHIP")
    if not database.is_file() or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("BAR_CHECKPOINT_SEED_MUST_BE_CLOSED")
    before = _digest(database, max_bytes)
    with _database_snapshot(database, before, database.stat().st_size, max_bytes) as connection:
        _schema(connection)
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [max_events + 1]
        )
        columns = [item[0] for item in cursor.description]
        values = cursor.fetchall()
        if len(values) != len(expected):
            raise IntegrityError("BAR_CHECKPOINT_SEED_MEMBERSHIP")
        rows = tuple(
            _event(
                MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True),
                resume.as_of_ms,
            )
            for row in values
        )
        pending = connection.execute(
            "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id LIMIT ?",
            [max_events + 1],
        ).fetchall()
        if rows != expected or pending != [(row.event_id, False) for row in expected]:
            raise IntegrityError("BAR_CHECKPOINT_SEED_MEMBERSHIP_OR_ACK")
    if (
        database.with_suffix(database.suffix + ".wal").exists()
        or _digest(database, max_bytes) != before
    ):
        raise IntegrityError("BAR_CHECKPOINT_SEED_CHANGED")
    return {
        "status": "BAR_CHECKPOINT_SEED_VERIFIED",
        "events": len(expected),
        "books": len(clean),
        "database_sha256": before,
        "source_mutated": False,
        "delivery_acknowledgements_copied": False,
        "history_complete": False,
        "trading_enabled": False,
    }


async def verify_bar_checkpoint_seed(
    database: Path,
    resume: BarCheckpointResume,
    now_ms: int,
    *,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_books: int = DEFAULT_MAX_BOOKS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict[str, object]:
    """Verify the otherwise empty, closed destination seed before other producers/seeds."""
    _policy(now_ms, max_events, max_books, max_bytes)
    database = _path(database)
    return await complete_thread(
        _verify_seed, database, resume, now_ms, max_events, max_books, max_bytes
    )
