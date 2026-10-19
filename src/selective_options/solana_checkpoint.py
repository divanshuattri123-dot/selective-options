"""Read an exact, bounded SOL network restart seed without modifying its source.

All eligible native history must be bound, including prefixes left by an interrupted
sink publication. Only the latest validated performance/stake pair and its health
receipt are handed off; that seed does not establish complete history or transport
recovery.
"""

from collections import Counter
from pathlib import Path

import duckdb
from pydantic import StrictInt, TypeAdapter, ValidationError

from .errors import IntegrityError
from .events import MarketEvent
from .solana_network import network_events
from .solana_quality import Health, network_quality
from .store import complete_thread

DEFAULT_MAX_RECEIPT_AGE_MS = 600_000
DEFAULT_MAX_SAMPLE_AGE_MS = 780_000
MAX_ROWS = 100_000
MAX_BYTES = 64_000_000
MAX_AGE_MS = 365 * 86_400_000
_INTEGER = TypeAdapter(StrictInt)
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
_ENVELOPES = {
    ("on_chain", "SOL_PERFORMANCE"),
    ("on_chain", "SOL_VOTE_STAKE"),
    ("feed_health", "SOL_NETWORK"),
}


def _validate_policy(
    database: Path,
    as_of_ms: int,
    max_receipt_age_ms: int,
    max_sample_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> None:
    if (
        not isinstance(database, Path)
        or any(
            type(value) is not int or not 0 < value < 2**63
            for value in (as_of_ms, max_receipt_age_ms, max_sample_age_ms)
        )
        or max_receipt_age_ms > MAX_AGE_MS
        or max_sample_age_ms > MAX_AGE_MS
        or type(max_rows) is not int
        or not 1 <= max_rows <= MAX_ROWS
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        raise ValueError("SOL_CHECKPOINT_INVALID_POLICY")


def _historical_pair(
    data: dict[str, MarketEvent], health: MarketEvent, status: Health
) -> tuple[MarketEvent, MarketEvent]:
    try:
        referenced = tuple(data[identity] for identity in status.event_ids)
    except KeyError as exc:
        raise IntegrityError("SOL_CHECKPOINT_MISSING_BOUND_SNAPSHOT") from exc
    by_symbol = {row.symbol: row for row in referenced}
    if set(by_symbol) != {"SOL_PERFORMANCE", "SOL_VOTE_STAKE"}:
        raise IntegrityError("SOL_CHECKPOINT_BOUND_METRICS_MISMATCH")
    performance, stake = by_symbol["SOL_PERFORMANCE"], by_symbol["SOL_VOTE_STAKE"]
    perf, stakes = performance.payload(), stake.payload()
    receipts = tuple(
        _INTEGER.validate_python(value)
        for value in (
            perf["genesis_observed_at_ms"],
            perf["sample_response_received_at_ms"],
            perf["block_time_response_received_at_ms"],
            stakes["snapshot_received_at_ms"],
        )
    )
    canonical = network_events(
        [perf["sample"]],
        perf["block_time_seconds"],
        stakes["vote_accounts"],
        (receipts[0], receipts[1], receipts[2], receipts[3]),
        _INTEGER.validate_python(perf["max_sample_age_ms"]),
    )
    if canonical != (performance, stake) or any(
        receipt > health.observed_at_ms for receipt in receipts
    ):
        raise IntegrityError("SOL_CHECKPOINT_HISTORICAL_SNAPSHOT_INVALID")
    return performance, stake


def _validate_history(
    rows: tuple[MarketEvent, ...],
    as_of_ms: int,
    max_receipt_age_ms: int,
    max_sample_age_ms: int,
    max_rows: int,
) -> tuple[MarketEvent, ...]:
    if any((row.kind, row.symbol) not in _ENVELOPES for row in rows):
        raise IntegrityError("SOL_CHECKPOINT_SCOPE_MISMATCH")
    by_id = {row.event_id: row for row in rows}
    if len(by_id) != len(rows):
        raise IntegrityError("SOL_CHECKPOINT_DUPLICATE_EVENT")
    quality = network_quality(rows, as_of_ms, max_receipt_age_ms, max_sample_age_ms, max_rows)
    if quality["state"] != "NETWORK_SAMPLE_VERIFIED":
        raise IntegrityError(str(quality["blocker"] or "SOL_CHECKPOINT_INVALID_EVIDENCE"))

    data = {row.event_id: row for row in rows if row.kind == "on_chain"}
    bindings: Counter[str] = Counter()
    try:
        for row in rows:
            if row.kind != "feed_health":
                continue
            # network_quality has validated every health's exact canonical envelope,
            # including failed historical outcomes and reference cardinality.
            status = Health.model_validate(row.payload())
            if status.ok:
                for event in _historical_pair(data, row, status):
                    bindings[event.event_id] += 1
    except IntegrityError:
        raise
    except (ValidationError, KeyError, ValueError, TypeError, OverflowError) as exc:
        raise IntegrityError("SOL_CHECKPOINT_INVALID_HISTORICAL_EVIDENCE") from exc
    for identity in data:
        if bindings[identity] == 0:
            raise IntegrityError("SOL_CHECKPOINT_UNBOUND_SNAPSHOT")
        if bindings[identity] != 1:
            raise IntegrityError("SOL_CHECKPOINT_AMBIGUOUS_SNAPSHOT_BINDING")

    selected = by_id[str(quality["health_event_id"])]
    status = Health.model_validate(selected.payload())
    seed = (*_historical_pair(data, selected, status), selected)
    return tuple(
        sorted(seed, key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id))
    )


def _load_sync(
    database: Path,
    as_of_ms: int,
    max_receipt_age_ms: int,
    max_sample_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> tuple[MarketEvent, ...]:
    if not database.is_file():
        raise FileNotFoundError("SOL_CHECKPOINT_DATABASE_MISSING")
    where = (
        "source='solana_public' AND environment='public_reference' AND asset='SOL' "
        "AND event_time_ms<=? AND observed_at_ms<=?"
    )
    parameters = [as_of_ms, as_of_ms]
    try:
        with duckdb.connect(str(database), read_only=True) as connection:
            # Aggregate the stored values before JSON decoding or Python materialization.
            # A single read-only transaction binds this budget to the subsequent read.
            connection.execute("BEGIN TRANSACTION")
            budget = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM("
                "strlen(event_id)+strlen(source)+strlen(environment)+strlen(asset)+"
                "strlen(symbol)+strlen(kind)+strlen(timestamp_kind)+strlen(payload_json)+"
                "length(CAST(event_time_ms AS VARCHAR))+"
                "length(CAST(observed_at_ms AS VARCHAR))+"
                "length(CAST(schema_version AS VARCHAR))"
                "),0), COUNT(*) FILTER (WHERE "
                + " OR ".join(f"{column} IS NULL" for column in _COLUMNS)
                + f") FROM events WHERE {where}",
                parameters,
            ).fetchone()
            if budget is None:
                raise IntegrityError("SOL_CHECKPOINT_INVALID_DATABASE")
            row_count, byte_count, null_count = budget
            # A permissive/nonstandard schema must not hide a large payload by
            # making the sum's per-row length expression NULL.
            if null_count:
                raise IntegrityError("SOL_CHECKPOINT_INVALID_ROW")
            if row_count > max_rows or byte_count > max_bytes:
                raise IntegrityError("SOL_CHECKPOINT_OVER_BUDGET")
            cursor = connection.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM events WHERE {where} "
                "ORDER BY observed_at_ms, event_time_ms, event_id",
                parameters,
            )
            columns = [column[0] for column in cursor.description]
            try:
                rows = tuple(
                    MarketEvent.model_validate(dict(zip(columns, values, strict=True)), strict=True)
                    for values in cursor.fetchall()
                )
            except (ValidationError, TypeError, ValueError) as exc:
                raise IntegrityError("SOL_CHECKPOINT_INVALID_ROW") from exc
            connection.execute("COMMIT")
    except IntegrityError:
        raise
    except duckdb.Error as exc:
        raise IntegrityError("SOL_CHECKPOINT_INVALID_DATABASE") from exc
    if len(rows) != row_count:
        raise IntegrityError("SOL_CHECKPOINT_READ_CHANGED")
    return _validate_history(rows, as_of_ms, max_receipt_age_ms, max_sample_age_ms, max_rows)


async def load_solana_resume(
    database: Path,
    as_of_ms: int,
    *,
    max_receipt_age_ms: int = DEFAULT_MAX_RECEIPT_AGE_MS,
    max_sample_age_ms: int = DEFAULT_MAX_SAMPLE_AGE_MS,
    max_rows: int = 10_000,
    max_bytes: int = 16_000_000,
) -> tuple[MarketEvent, ...]:
    """Return the exact latest two native SOL snapshots and their successful health.

    The fixed public source scope uses both event and observation clocks. All eligible
    history is audited before selecting the minimal seed. Limits cover rows and UTF-8
    stored string bytes plus integer digits, before parsing; they do not bound DuckDB
    scan cost, database size or engine memory. No delivery acknowledgements are loaded.
    """
    _validate_policy(database, as_of_ms, max_receipt_age_ms, max_sample_age_ms, max_rows, max_bytes)
    return await complete_thread(
        _load_sync,
        database,
        as_of_ms,
        max_receipt_age_ms,
        max_sample_age_ms,
        max_rows,
        max_bytes,
    )
