"""Bounded, read-only CoinDesk RSS checkpoint selection for explicit capture resume.

The loader returns the exact retained native headline rows and the smallest set of
canonical health receipts (including the unique latest healthy receipt) that binds
every retained headline.  It does not claim complete feed history, atomic source
commits, transport delivery, or publisher publication-time truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from pydantic import ValidationError

from .archive import ArchiveManifest, _verify_archive
from .crypto_news import (
    SOURCE,
    SYMBOL,
    NewsPollHealth,
    _health_valid,
    _news_valid,
)
from .errors import IntegrityError
from .events import MarketEvent
from .store import complete_thread

DEFAULT_MAX_POLL_AGE_MS = 600_000
MAX_ROWS = 100_000
MAX_BYTES = 64_000_000
MAX_AGE_MS = 365 * 86_400_000
MAX_SET_COVER_STATES = 100_000
MAX_SET_COVER_OPERATIONS = 2_000_000
MAX_SET_COVER_DEPTH = 512
MAX_BINDING_EDGES = 1_000_000

_SCOPE = (
    "source='coindesk_rss' AND environment='public_reference' "
    "AND asset='GLOBAL' AND symbol='CRYPTO_NEWS'"
)
_COLUMNS = (
    "event_id",
    "source",
    "environment",
    "asset",
    "symbol",
    "kind",
    "event_time_ms",
    "observed_at_ms",
    "timestamp_kind",
    "schema_version",
    "payload_json",
)
_EXPECTED_EVENTS_SCHEMA = (
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


@dataclass(frozen=True, slots=True)
class CryptoNewsCheckpoint:
    """Immutable, fully bound evidence to seed an opt-in resumed capture."""

    as_of_ms: int
    metadata: tuple[MarketEvent, ...]
    health: tuple[MarketEvent, ...]
    latest_health_event_id: str
    latest_health_received_at_ms: int
    max_poll_age_ms: int
    source_delivery: tuple[tuple[str, bool], ...]

    @property
    def events(self) -> tuple[MarketEvent, ...]:
        """Exact copy rows, ordered deterministically with each row's native clocks."""
        return tuple(
            sorted(
                (*self.metadata, *self.health),
                key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id),
            )
        )


def _validate_policy(
    database: Path,
    as_of_ms: int,
    max_poll_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> None:
    if (
        not isinstance(database, Path)
        or any(
            type(value) is not int or not 0 < value < 2**63
            for value in (
                as_of_ms,
                max_poll_age_ms,
                max_rows,
                max_bytes,
            )
        )
        or max_poll_age_ms > MAX_AGE_MS
        or not 1 <= max_rows <= MAX_ROWS
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        raise ValueError("CRYPTO_NEWS_CHECKPOINT_INVALID_POLICY")


def _check_schema(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        events = connection.execute("PRAGMA table_info('events')").fetchall()
        outbox = connection.execute("PRAGMA table_info('delivery_outbox')").fetchall()
    except duckdb.Error as exc:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA") from exc
    actual = tuple((row[1], row[2].upper()) for row in events)
    if (
        actual != _EXPECTED_EVENTS_SCHEMA
        or not events
        or events[0][5] is not True
        or tuple((row[1], row[2].upper()) for row in outbox)
        != (("event_id", "VARCHAR"), ("delivered", "BOOLEAN"))
        or len(outbox) != 2
        or outbox[0][5] is not True
        or outbox[1][3] is not True
    ):
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")


def _minimum_health_cover(
    metadata: tuple[MarketEvent, ...],
    health_status: tuple[tuple[MarketEvent, NewsPollHealth], ...],
    latest: MarketEvent,
) -> tuple[MarketEvent, ...]:
    """Find an exact minimum-cardinality health cover, pinning the latest receipt.

    Repeated polls can bind the same item. A bounded branch-and-bound set-cover
    search finds the smallest exact native subset; its state budget is independent
    of the database row/byte budget and fails closed if exceeded.
    """
    if not metadata:
        return (latest,)

    indexes = {row.event_id: bit for bit, row in enumerate(metadata)}
    full = (1 << len(metadata)) - 1
    latest_status = next(body for row, body in health_status if row.event_id == latest.event_id)
    latest_mask = 0
    for identity in latest_status.accepted_event_ids:
        latest_mask |= 1 << indexes[identity]
    target = full & ~latest_mask
    if target == 0:
        return (latest,)

    # Identical coverage is interchangeable for binding. Keep the earliest native
    # receipt deterministically so search stays bounded by distinct coverage masks.
    by_mask: dict[int, MarketEvent] = {}
    for row, body in health_status:
        if row.event_id == latest.event_id:
            continue
        mask = 0
        for identity in body.accepted_event_ids:
            mask |= 1 << indexes[identity]
        mask &= target
        if mask and (
            mask not in by_mask
            or (row.observed_at_ms, row.event_id)
            < (by_mask[mask].observed_at_ms, by_mask[mask].event_id)
        ):
            by_mask[mask] = row

    candidates = list(by_mask.items())
    if not candidates:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_UNBOUND_METADATA")

    visited: dict[int, int] = {}
    states = 0
    operations = 0

    def charge(count: int = 1) -> None:
        nonlocal operations
        operations += count
        if operations > MAX_SET_COVER_OPERATIONS:
            raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_COVER_OVER_BUDGET")

    def greedy_cover() -> list[MarketEvent]:
        remaining = target
        chosen: list[MarketEvent] = []
        while remaining:
            choices = []
            for mask, row in candidates:
                charge()
                gain = mask & remaining
                if gain:
                    choices.append((gain, row))
            if not choices:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_UNBOUND_METADATA")
            bits, row = min(
                choices,
                key=lambda item: (
                    -item[0].bit_count(),
                    item[1].observed_at_ms,
                    item[1].event_id,
                ),
            )
            chosen.append(row)
            remaining &= ~bits
        return chosen

    best = greedy_cover()
    best_count = len(best)

    candidate_by_bit: list[list[int]] = [[] for _ in metadata]
    for candidate_index, (mask, _row) in enumerate(candidates):
        remainder = mask
        while remainder:
            lowest = remainder & -remainder
            candidate_by_bit[lowest.bit_length() - 1].append(candidate_index)
            remainder ^= lowest

    def search(covered: int, chosen: list[MarketEvent]) -> None:
        nonlocal best, best_count, states
        states += 1
        if states > MAX_SET_COVER_STATES:
            raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_COVER_OVER_BUDGET")
        if len(chosen) > MAX_SET_COVER_DEPTH:
            raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_COVER_OVER_BUDGET")
        if covered == target:
            if len(chosen) < best_count:
                best, best_count = list(chosen), len(chosen)
            return
        if len(chosen) >= best_count:
            return
        prior = visited.get(covered)
        if prior is not None and prior <= len(chosen):
            return
        visited[covered] = len(chosen)
        remaining = target & ~covered
        relevant = []
        for mask, row in candidates:
            charge()
            gain = mask & remaining
            if gain:
                relevant.append((gain, row))
        if not relevant:
            return

        # A cheap admissible lower bound prunes branches that cannot improve the
        # currently valid greedy solution.
        max_gain = max(mask.bit_count() for mask, _ in relevant)
        lower_bound = (remaining.bit_count() + max_gain - 1) // max_gain
        if len(chosen) + lower_bound >= best_count:
            return

        # Branch on the least flexible missing headline to reduce search width.
        pivot_options: list[tuple[int, int, list[int]]] = []
        remainder = remaining
        while remainder:
            lowest = remainder & -remainder
            bit = lowest.bit_length() - 1
            choices: list[int] = []
            for candidate_index in candidate_by_bit[bit]:
                charge()
                if candidates[candidate_index][0] & remaining:
                    choices.append(candidate_index)
            pivot_options.append((len(choices), bit, choices))
            remainder ^= lowest
        count, pivot, branch_indexes = min(pivot_options, key=lambda item: item[0])
        if count == 0:
            return
        branches = sorted(
            (candidates[index] for index in branch_indexes),
            key=lambda item: (
                -item[0].bit_count(),
                item[1].observed_at_ms,
                item[1].event_id,
            ),
        )
        for mask, row in branches:
            search(covered | mask, [*chosen, row])

    search(0, [])
    selected = [latest, *best]
    return tuple(
        sorted(selected, key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id))
    )


def _validate_history(
    rows: tuple[MarketEvent, ...],
    deliveries: tuple[tuple[str, bool], ...],
    as_of_ms: int,
    max_poll_age_ms: int,
) -> CryptoNewsCheckpoint:
    expected_scope = (SOURCE, "public_reference", "GLOBAL", SYMBOL)
    metadata: list[MarketEvent] = []
    health_status: list[tuple[MarketEvent, NewsPollHealth]] = []
    by_id: dict[str, MarketEvent] = {}
    delivery_by_id = dict(deliveries)

    try:
        for row in rows:
            if (row.source, row.environment, row.asset, row.symbol) != expected_scope:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_SCOPE_MISMATCH")
            if row.event_time_ms > as_of_ms or row.observed_at_ms > as_of_ms:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_FUTURE_CLOCK")
            if row.event_id in by_id:
                if by_id[row.event_id] != row:
                    raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_EVENT_ID_CONFLICT")
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_DUPLICATE_EVENT")
            by_id[row.event_id] = row
            if row.kind == "news":
                _news_valid(row)
                metadata.append(row)
            elif row.kind == "feed_health":
                health_status.append((row, _health_valid(row)))
            else:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_SCOPE_MISMATCH")
    except IntegrityError:
        raise
    except (ValidationError, ValueError, TypeError, KeyError, OverflowError) as exc:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_EVIDENCE") from exc

    if not health_status:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_HEALTH_MISSING")
    metadata_tuple = tuple(
        sorted(metadata, key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id))
    )
    data = {row.event_id: row for row in metadata_tuple}
    bound: set[str] = set()
    binding_edges = 0
    for health_row, status in health_status:
        binding_edges += len(status.accepted_event_ids)
        if binding_edges > MAX_BINDING_EDGES:
            raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_OVER_BUDGET")
        for identity in status.accepted_event_ids:
            headline = data.get(identity)
            if headline is None:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_MISSING_BOUND_METADATA")
            if headline.payload()["feed_url"] != status.feed_url:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PROVENANCE_MISMATCH")
            if (
                status.response_received_at_ms is None
                or headline.observed_at_ms > status.response_received_at_ms
                or headline.observed_at_ms > health_row.observed_at_ms
            ):
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_CLOCK_BINDING_MISMATCH")
            bound.add(identity)
    if set(data) != bound:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_UNBOUND_METADATA")

    latest_time = max(row.observed_at_ms for row, _ in health_status)
    latest_options = [
        (row, body) for row, body in health_status if row.observed_at_ms == latest_time
    ]
    if len(latest_options) != 1:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_LATEST_HEALTH_TIED")
    latest, status = latest_options[0]
    if not status.ok:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_LATEST_HEALTH_FAILED")
    response_ms = status.response_received_at_ms
    if response_ms is None or as_of_ms - min(latest_time, response_ms) > max_poll_age_ms:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_LATEST_HEALTH_STALE")

    if set(delivery_by_id) != set(by_id) or any(
        type(value) is not bool for value in delivery_by_id.values()
    ):
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")
    health_tuple = _minimum_health_cover(metadata_tuple, tuple(health_status), latest)
    return CryptoNewsCheckpoint(
        as_of_ms=as_of_ms,
        metadata=metadata_tuple,
        health=health_tuple,
        latest_health_event_id=latest.event_id,
        latest_health_received_at_ms=latest_time,
        max_poll_age_ms=max_poll_age_ms,
        source_delivery=tuple(sorted(delivery_by_id.items())),
    )


def _load_sync(
    database: Path,
    as_of_ms: int,
    max_poll_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> CryptoNewsCheckpoint:
    if not database.is_file():
        raise FileNotFoundError("CRYPTO_NEWS_CHECKPOINT_DATABASE_MISSING")
    try:
        with duckdb.connect(str(database), read_only=True) as connection:
            connection.execute("BEGIN TRANSACTION")
            _check_schema(connection)
            budget = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "strlen(event_id)+strlen(source)+strlen(environment)+strlen(asset)+"
                "strlen(symbol)+strlen(kind)+strlen(timestamp_kind)+strlen(payload_json)+"
                "length(CAST(event_time_ms AS VARCHAR))+"
                "length(CAST(observed_at_ms AS VARCHAR))+"
                "length(CAST(schema_version AS VARCHAR))"
                "),0), COUNT(*) FILTER (WHERE "
                + " OR ".join(f"{column} IS NULL" for column in _COLUMNS)
                + f") FROM events WHERE {_SCOPE}",
            ).fetchone()
            if budget is None:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_DATABASE")
            row_count, byte_count, null_count = budget
            if null_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_ROW")
            outbox_budget = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "strlen(o.event_id)+length(CAST(o.delivered AS VARCHAR))"
                "),0) FROM delivery_outbox o WHERE o.event_id IN ("
                f"SELECT event_id FROM events WHERE {_SCOPE})"
            ).fetchone()
            if outbox_budget is None:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_DATABASE")
            outbox_count, outbox_bytes = outbox_budget
            if row_count + outbox_count > max_rows or byte_count + outbox_bytes > max_bytes:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_OVER_BUDGET")
            if outbox_count != row_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")

            cursor = connection.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM events WHERE {_SCOPE} "
                "ORDER BY observed_at_ms,event_time_ms,event_id"
            )
            columns = [column[0] for column in cursor.description]
            try:
                rows = tuple(
                    MarketEvent.model_validate(dict(zip(columns, values, strict=True)), strict=True)
                    for values in cursor.fetchall()
                )
            except (ValidationError, TypeError, ValueError) as exc:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_ROW") from exc
            if len(rows) != row_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_READ_CHANGED")

            delivery_rows = connection.execute(
                "SELECT e.event_id,o.event_id,o.delivered FROM events e "
                "LEFT JOIN delivery_outbox o ON o.event_id=e.event_id "
                f"WHERE {_SCOPE} ORDER BY e.event_id"
            ).fetchall()
            if len(delivery_rows) != row_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")
            deliveries: list[tuple[str, bool]] = []
            for event_id, outbox_id, delivered in delivery_rows:
                if outbox_id != event_id or type(delivered) is not bool:
                    raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")
                deliveries.append((event_id, delivered))
            connection.execute("COMMIT")
    except IntegrityError:
        raise
    except duckdb.Error as exc:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_DATABASE") from exc
    return _validate_history(
        rows,
        tuple(deliveries),
        as_of_ms,
        max_poll_age_ms,
    )


def _verify_closed_archive_sync(
    database: Path,
    archive: Path,
    *,
    max_rows: int,
    max_bytes: int,
) -> ArchiveManifest:
    """Verify a closed whole-store SQL/Parquet/outbox checkpoint within bounds."""
    if (
        not isinstance(database, Path)
        or not isinstance(archive, Path)
        or any(character in str(archive) for character in "*?[")
        or not database.is_file()
        or not archive.is_file()
    ):
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_ARCHIVE_MISSING")
    wal = database.with_suffix(database.suffix + ".wal")
    if wal.exists():
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_SOURCE_NOT_CLOSED")
    if archive.stat().st_size > max_bytes or database.stat().st_size > max_bytes:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_ARCHIVE_OVER_BUDGET")
    try:
        manifest = _verify_archive(archive)
        with duckdb.connect(str(database), read_only=True) as connection:
            connection.execute("BEGIN TRANSACTION")
            _check_schema(connection)
            budget = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "strlen(event_id)+strlen(source)+strlen(environment)+strlen(asset)+"
                "strlen(symbol)+strlen(kind)+strlen(timestamp_kind)+strlen(payload_json)+"
                "length(CAST(event_time_ms AS VARCHAR))+"
                "length(CAST(observed_at_ms AS VARCHAR))+"
                "length(CAST(schema_version AS VARCHAR))"
                "),0), COUNT(*) FILTER (WHERE "
                + " OR ".join(f"{column} IS NULL" for column in _COLUMNS)
                + ") FROM events"
            ).fetchone()
            outbox_budget = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "strlen(event_id)+length(CAST(delivered AS VARCHAR))"
                "),0) FROM delivery_outbox"
            ).fetchone()
            if budget is None or outbox_budget is None:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_DATABASE")
            row_count, row_bytes, null_count = budget
            outbox_count, outbox_bytes = outbox_budget
            if null_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_INVALID_ROW")
            if outbox_count != row_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")
            if row_count + outbox_count > max_rows or row_bytes + outbox_bytes > max_bytes:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_OVER_BUDGET")
            if manifest.rows != row_count:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_ARCHIVE_MISMATCH")
            outbox = connection.execute(
                "SELECT e.event_id,o.event_id,o.delivered FROM events e "
                "LEFT JOIN delivery_outbox o ON o.event_id=e.event_id ORDER BY e.event_id"
            ).fetchall()
            if len(outbox) != row_count or any(
                event_id != outbox_id or type(delivered) is not bool
                for event_id, outbox_id, delivered in outbox
            ):
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_PARTIAL_SINK_METADATA")
            sql_rows = connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            parquet_rows = connection.execute(
                "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                [str(archive)],
            ).fetchall()
            if sql_rows != parquet_rows or len(sql_rows) != manifest.rows:
                raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_DATABASE_PARQUET_MISMATCH")
        if _verify_archive(archive) != manifest:
            raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_ARCHIVE_CHANGED")
        return manifest
    except IntegrityError:
        raise
    except (duckdb.Error, OSError, ValueError, TypeError, ValidationError) as exc:
        raise IntegrityError("CRYPTO_NEWS_CHECKPOINT_ARCHIVE_INVALID") from exc


async def load_crypto_news_checkpoint(
    database: Path,
    as_of_ms: int,
    *,
    max_poll_age_ms: int = DEFAULT_MAX_POLL_AGE_MS,
    max_rows: int = 10_000,
    max_bytes: int = 16_000_000,
    parquet_archive: Path | None = None,
) -> CryptoNewsCheckpoint:
    """Read an exact scoped CoinDesk checkpoint without mutating its source.

    The policy bounds scoped event and matching outbox rows, plus UTF-8 bytes of
    their stored string values and integer text digits before decoding. Both clocks,
    canonical schemas/provenance, each historical health-to-headline binding, unique
    latest-health freshness and one outbox row per scoped event are required. When
    ``parquet_archive`` is supplied, closed whole-store SQL/Parquet/archive/outbox
    equality is additionally checked within the row/value-byte and file-size bounds.
    """
    _validate_policy(
        database,
        as_of_ms,
        max_poll_age_ms,
        max_rows,
        max_bytes,
    )
    if parquet_archive is not None:
        if not isinstance(parquet_archive, Path):
            raise ValueError("CRYPTO_NEWS_CHECKPOINT_INVALID_POLICY")
        await complete_thread(
            _verify_closed_archive_sync,
            database,
            parquet_archive,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
    checkpoint = await complete_thread(
        _load_sync,
        database,
        as_of_ms,
        max_poll_age_ms,
        max_rows,
        max_bytes,
    )
    return checkpoint
