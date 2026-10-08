"""As-of health and exact referenced SOL snapshots; never trading approval."""

from collections.abc import Iterable
from typing import Literal

from pydantic import StrictBool, StrictInt, StrictStr, TypeAdapter, ValidationError

from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel
from .solana_network import network_events


class Health(ExternalModel):
    ok: StrictBool
    reason: StrictStr
    code: StrictInt
    error_type: StrictStr | None
    event_ids: tuple[StrictStr, ...]
    scope: Literal["performance and vote-account stake; sink accepted, no atomic commit claim"]


def network_quality(
    events: Iterable[MarketEvent],
    as_of_ms: int,
    max_receipt_age_ms: int = 600_000,
    max_sample_age_ms: int = 780_000,
    max_rows: int = 10_000,
) -> dict[str, object]:
    if (
        any(
            type(v) is not int or v <= 0
            for v in (as_of_ms, max_receipt_age_ms, max_sample_age_ms, max_rows)
        )
        or max_rows > 100_000
    ):
        raise ValueError("invalid bounded Solana quality policy")
    result: dict[str, object] = {
        "source": "solana_public",
        "asset": "SOL",
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "trading_approval": False,
    }
    try:
        eligible = []
        for row in events:
            if (
                row.source != "solana_public"
                or row.environment != "public_reference"
                or row.asset != "SOL"
                or (row.kind, row.symbol)
                not in {
                    ("on_chain", "SOL_PERFORMANCE"),
                    ("on_chain", "SOL_VOTE_STAKE"),
                    ("feed_health", "SOL_NETWORK"),
                }
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            eligible.append(row)
            if len(eligible) > max_rows:
                raise IntegrityError("SOL_QUALITY_OVER_BUDGET")
        health = []
        for row in eligible:
            if row.kind != "feed_health":
                continue
            body = Health.model_validate(row.payload())
            if len(body.event_ids) != (2 if body.ok else 0) or len(set(body.event_ids)) != len(
                body.event_ids
            ):
                raise IntegrityError("SOL_HEALTH_REFERENCES")
            canonical = make_event(
                source="solana_public",
                environment="public_reference",
                asset="SOL",
                symbol="SOL_NETWORK",
                kind="feed_health",
                timestamp_kind="receipt",
                event_time_ms=row.observed_at_ms,
                observed_at_ms=row.observed_at_ms,
                payload=body.model_dump(mode="json"),
            )
            if canonical != row:
                raise IntegrityError("SOL_HEALTH_CANONICAL")
            health.append(row)
        if not health:
            raise IntegrityError("SOL_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row in health)
        choices = {row for row in health if row.observed_at_ms == latest}
        if len(choices) != 1:
            raise IntegrityError("SOL_HEALTH_AMBIGUOUS")
        selected = choices.pop()
        status = Health.model_validate(selected.payload())
        result.update(
            health_event_id=selected.event_id,
            health_received_at_ms=latest,
            health_reason=status.reason,
        )
        if as_of_ms - latest > max_receipt_age_ms:
            raise IntegrityError("SOL_HEALTH_STALE")
        if not status.ok:
            raise IntegrityError("SOL_FEED_UNAVAILABLE")
        rows = []
        for identity in status.event_ids:
            matches = {
                row for row in eligible if row.kind == "on_chain" and row.event_id == identity
            }
            if len(matches) != 1:
                raise IntegrityError("SOL_REFERENCED_SNAPSHOT_MISSING_OR_CONFLICTING")
            rows.append(matches.pop())
        by_symbol = {row.symbol: row for row in rows}
        if set(by_symbol) != {"SOL_PERFORMANCE", "SOL_VOTE_STAKE"}:
            raise IntegrityError("SOL_REFERENCED_METRICS")
        performance, stake = by_symbol["SOL_PERFORMANCE"], by_symbol["SOL_VOTE_STAKE"]
        perf, stakes = performance.payload(), stake.payload()
        receipts = tuple(
            TypeAdapter(StrictInt).validate_python(v)
            for v in (
                perf["genesis_observed_at_ms"],
                perf["sample_response_received_at_ms"],
                perf["block_time_response_received_at_ms"],
                stakes["snapshot_received_at_ms"],
            )
        )
        canonical_pair = network_events(
            [perf["sample"]],
            perf["block_time_seconds"],
            stakes["vote_accounts"],
            (receipts[0], receipts[1], receipts[2], receipts[3]),
            TypeAdapter(StrictInt).validate_python(perf["max_sample_age_ms"]),
        )
        if canonical_pair != (performance, stake) or any(r > latest for r in receipts):
            raise IntegrityError("SOL_SNAPSHOT_CANONICAL_OR_HEALTH_ORDER")
        if any(as_of_ms - r > max_receipt_age_ms for r in receipts):
            raise IntegrityError("SOL_SNAPSHOT_RECEIPT_STALE")
        if as_of_ms - performance.event_time_ms > max_sample_age_ms:
            raise IntegrityError("SOL_PERFORMANCE_SOURCE_STALE")
        result.update(
            state="NETWORK_SAMPLE_VERIFIED",
            event_ids=list(status.event_ids),
            performance_time_ms=performance.event_time_ms,
            stake_received_at_ms=stake.observed_at_ms,
        )
    except (IntegrityError, ValidationError, KeyError, TypeError, ValueError) as exc:
        result["blocker"] = str(exc) if isinstance(exc, IntegrityError) else "SOL_INVALID_EVIDENCE"
    return result
