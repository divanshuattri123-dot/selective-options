"""Read-only validation and loading of a bounded Coin Metrics restart seed.

This module selects only the fixed public BTC/ETH daily valuation scope. It preserves
native observations and poll-health receipts exactly as stored; it does not contact the
provider, infer first-publication times, or claim complete history.
"""

from collections import Counter
from pathlib import Path

import duckdb
from pydantic import ValidationError

from .coinmetrics import (
    DAY_MS,
    SOURCE,
    SYMBOL,
    DailySnapshot,
    PollHealth,
    canonical_coinmetrics,
    canonical_coinmetrics_health,
)
from .coinmetrics_quality import coinmetrics_quality
from .errors import IntegrityError
from .events import MarketEvent
from .store import complete_thread

MAX_ROWS = 100_000
MAX_BYTES = 64_000_000
MAX_AGE_MS = 365 * DAY_MS
_ASSETS = ("BTC", "ETH")
_KINDS = ("on_chain", "feed_health")


def _validate_policy(
    database: Path,
    as_of_ms: int,
    max_poll_age_ms: int,
    max_data_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> None:
    if not isinstance(database, Path) or any(
        type(value) is not int or not 0 < value < 2**63
        for value in (as_of_ms, max_poll_age_ms, max_data_age_ms)
    ):
        raise ValueError("COINMETRICS_CHECKPOINT_INVALID_POLICY")
    if (
        max_poll_age_ms > MAX_AGE_MS
        or max_data_age_ms > MAX_AGE_MS
        or type(max_rows) is not int
        or not 1 <= max_rows <= MAX_ROWS
        or type(max_bytes) is not int
        or not 1 <= max_bytes <= MAX_BYTES
    ):
        raise ValueError("COINMETRICS_CHECKPOINT_INVALID_POLICY")


def _scope_sql() -> str:
    return (
        "source=? AND environment='public_reference' AND asset IN ('BTC','ETH') "
        "AND symbol=? AND kind IN ('on_chain','feed_health') "
        "AND event_time_ms<=? AND observed_at_ms<=?"
    )


def _load_sync(
    database: Path,
    as_of_ms: int,
    max_poll_age_ms: int,
    max_data_age_ms: int,
    max_rows: int,
    max_bytes: int,
) -> tuple[MarketEvent, ...]:
    _validate_policy(database, as_of_ms, max_poll_age_ms, max_data_age_ms, max_rows, max_bytes)
    if not database.is_file():
        raise FileNotFoundError("COINMETRICS_CHECKPOINT_DATABASE_MISSING")

    where = _scope_sql()
    parameters = [SOURCE, SYMBOL, as_of_ms, as_of_ms]
    try:
        with duckdb.connect(str(database), read_only=True) as connection:
            # Keep the budget check and exact read on one consistent read-only snapshot.
            connection.execute("BEGIN TRANSACTION")
            budget = connection.execute(
                f"SELECT COUNT(*), COALESCE(SUM("
                "strlen(event_id)+strlen(source)+strlen(environment)+strlen(asset)+"
                "strlen(symbol)+strlen(kind)+strlen(timestamp_kind)+strlen(payload_json)+"
                "length(CAST(event_time_ms AS VARCHAR))+"
                "length(CAST(observed_at_ms AS VARCHAR))+"
                "length(CAST(schema_version AS VARCHAR))"
                f"),0) FROM events WHERE {where}",
                parameters,
            ).fetchone()
            if budget is None:
                raise IntegrityError("COINMETRICS_CHECKPOINT_INVALID_DATABASE")
            row_count, byte_count = budget
            if row_count > max_rows or byte_count > max_bytes:
                raise IntegrityError("COINMETRICS_CHECKPOINT_OVER_BUDGET")

            cursor = connection.execute(
                f"SELECT * FROM events WHERE {where} "
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
                raise IntegrityError("COINMETRICS_CHECKPOINT_INVALID_ROW") from exc
            connection.execute("COMMIT")
    except IntegrityError:
        raise
    except duckdb.Error as exc:
        raise IntegrityError("COINMETRICS_CHECKPOINT_INVALID_DATABASE") from exc

    if len(rows) != row_count:
        raise IntegrityError("COINMETRICS_CHECKPOINT_READ_CHANGED")
    _validate_history(rows, as_of_ms, max_poll_age_ms, max_data_age_ms, max_rows)
    return tuple(
        sorted(rows, key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id))
    )


def _validate_history(
    rows: tuple[MarketEvent, ...],
    as_of_ms: int,
    max_poll_age_ms: int,
    max_data_age_ms: int,
    max_rows: int,
) -> None:
    data: dict[str, tuple[MarketEvent, DailySnapshot]] = {}
    health: list[tuple[MarketEvent, PollHealth]] = []
    try:
        for row in rows:
            if row.kind == "on_chain":
                data[row.event_id] = (row, canonical_coinmetrics(row))
            elif row.kind == "feed_health":
                health.append((row, canonical_coinmetrics_health(row)))
            else:
                raise IntegrityError("COINMETRICS_CHECKPOINT_SCOPE_MISMATCH")
    except (IntegrityError, ValidationError, ValueError, TypeError, OverflowError) as exc:
        if isinstance(exc, IntegrityError) and str(exc).startswith("COINMETRICS_CHECKPOINT_"):
            raise
        raise IntegrityError("COINMETRICS_INVALID_EVIDENCE") from exc

    for asset in _ASSETS:
        result = coinmetrics_quality(
            rows,
            asset=asset,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_data_age_ms=max_data_age_ms,
            max_rows=max_rows,
        )
        if result["state"] != "DAILY_REGIME_OBSERVED":
            raise IntegrityError(
                str(result["blocker"] or "COINMETRICS_CHECKPOINT_INVALID_EVIDENCE")
            )

    bindings: Counter[str] = Counter()
    for health_row, poll in health:
        for reference in poll.accepted_observations:
            pair = data.get(reference.event_id)
            if pair is None:
                raise IntegrityError("COINMETRICS_CHECKPOINT_MISSING_BOUND_OBSERVATION")
            event, snapshot = pair
            if (
                event.asset != health_row.asset
                or event.observed_at_ms != reference.observed_at_ms
                or snapshot.period_label_ms != reference.period_label_ms
                or snapshot.poll_started_at_ms != poll.poll_started_at_ms
            ):
                raise IntegrityError("COINMETRICS_CHECKPOINT_BOUND_OBSERVATION_MISMATCH")
            bindings[event.event_id] += 1

    for event_id in data:
        if bindings[event_id] == 0:
            raise IntegrityError("COINMETRICS_CHECKPOINT_UNBOUND_OBSERVATION")
        if bindings[event_id] != 1:
            raise IntegrityError("COINMETRICS_CHECKPOINT_AMBIGUOUS_OBSERVATION_BINDING")


async def load_coinmetrics_resume(
    database: Path,
    as_of_ms: int,
    *,
    max_poll_age_ms: int,
    max_data_age_ms: int,
    max_rows: int = 10_000,
    max_bytes: int = 64_000_000,
) -> tuple[MarketEvent, ...]:
    """Return the exact bounded PIT Coin Metrics observations and bound health rows.

    The fixed BTC/ETH source scope is fetched under both clocks from a read-only DuckDB
    snapshot. Latest poll freshness and daily-data freshness are assessed independently,
    and every selected native observation must have exactly one matching poll reference.
    The byte limit covers the UTF-8 bytes of stored string fields plus integer digits; it
    bounds selected event values, not DuckDB scan cost or database/index size.
    """
    _validate_policy(database, as_of_ms, max_poll_age_ms, max_data_age_ms, max_rows, max_bytes)
    return await complete_thread(
        _load_sync,
        database,
        as_of_ms,
        max_poll_age_ms,
        max_data_age_ms,
        max_rows,
        max_bytes,
    )
