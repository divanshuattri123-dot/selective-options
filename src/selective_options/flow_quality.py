"""Scoped as-of flow sampling quality; never a continuous-history or trading gate."""

from collections.abc import Iterable

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from .binance_taker import PERIOD_MS
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .flow_view import SOURCES, FlowKind, flow_at


class FlowHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    reason: StrictStr = Field(min_length=1, max_length=256)
    code: StrictInt


def _health_valid(event: MarketEvent) -> None:
    try:
        body = FlowHealth.model_validate_json(event.payload_json)
        canonical = make_event(
            source=event.source,
            environment="public_reference",
            asset=event.asset,
            symbol="USDT_PERPETUAL",
            kind="feed_health",
            event_time_ms=event.observed_at_ms,
            observed_at_ms=event.observed_at_ms,
            timestamp_kind="receipt",
            payload=body.model_dump(mode="json"),
        )
        if canonical != event or (event.asset == "GLOBAL" and body.ok):
            raise IntegrityError("FLOW_HEALTH_IDENTITY_CLOCK_OR_GLOBAL_SUCCESS")
    except (ValueError, IntegrityError) as exc:
        raise IntegrityError("FLOW_HEALTH_INVALID") from exc


def flow_quality(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    symbol: str,
    kind: FlowKind,
    as_of_ms: int,
    max_period_age_ms: int = 600_000,
    max_observation_age_ms: int = 600_000,
    max_health_age_ms: int = 600_000,
    required_periods: int = 3,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Require a recent contiguous sampled window and health after its selected receipts."""
    if (
        kind not in SOURCES
        or not asset.strip()
        or not symbol.strip()
        or any(
            type(value) is not int or value <= 0
            for value in (as_of_ms, max_period_age_ms, max_observation_age_ms, max_health_age_ms)
        )
        or type(required_periods) is not int
        or not 1 <= required_periods <= 288
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
    ):
        raise ValueError(
            "positive integer clocks/ages, valid scope and bounded window/budget required"
        )
    blockers: list[str] = []
    flows: list[MarketEvent] = []
    health_rows: list[MarketEvent] = []
    selected: tuple[MarketEvent, ...] = ()
    health: MarketEvent | None = None
    missing: list[int] = []
    latest_end: int | None = None
    latest_receipt: int | None = None
    try:
        for row in events:
            if (
                row.source != SOURCES[kind]
                or row.environment != "public_reference"
                or max(row.event_time_ms, row.observed_at_ms) > as_of_ms
            ):
                continue
            if row.asset == asset and row.symbol == symbol and row.kind == kind:
                flows.append(row)
            elif (
                row.asset in (asset, "GLOBAL")
                and row.symbol == "USDT_PERPETUAL"
                and row.kind == "feed_health"
            ):
                health_rows.append(row)
            else:
                continue
            if len(flows) + len(health_rows) > max_rows:
                raise IntegrityError("FLOW_QUALITY_OVER_BUDGET")
        selected = flow_at(
            flows, asset=asset, symbol=symbol, kind=kind, as_of_ms=as_of_ms, max_rows=max_rows
        )
        for row in health_rows:
            _health_valid(row)
        if health_rows:
            latest = max(row.observed_at_ms for row in health_rows)
            candidates = {row.event_id: row for row in health_rows if row.observed_at_ms == latest}
            if len(candidates) != 1:
                raise IntegrityError("FLOW_HEALTH_AMBIGUOUS")
            health = next(iter(candidates.values()))
    except IntegrityError as exc:
        blockers.append(str(exc))
    if not selected:
        blockers.append("MISSING_FLOW")
    else:
        latest_end = selected[-1].event_time_ms
        if any(row.event_time_ms % PERIOD_MS for row in selected):
            blockers.append("FLOW_PERIOD_ALIGNMENT")
        expected = {latest_end - index * PERIOD_MS for index in range(required_periods)}
        missing = sorted(expected - {row.event_time_ms for row in selected})
        if missing:
            blockers.append("MISSING_RECENT_PERIODS")
        window = [row for row in selected if row.event_time_ms in expected]
        latest_receipt = max(row.observed_at_ms for row in window)
        if as_of_ms - latest_end > max_period_age_ms:
            blockers.append("STALE_FLOW_PERIOD")
        if any(as_of_ms - row.observed_at_ms > max_observation_age_ms for row in window):
            blockers.append("STALE_FLOW_OBSERVATION")
        if health is not None and health.observed_at_ms < latest_receipt:
            blockers.append("HEALTH_PRECEDES_FLOW")
    if health is None:
        blockers.append("MISSING_HEALTH")
    else:
        if as_of_ms - health.observed_at_ms > max_health_age_ms:
            blockers.append("STALE_HEALTH")
        if health.payload()["ok"] is not True:
            blockers.append("SOURCE_UNAVAILABLE")
    return {
        "source": SOURCES[kind],
        "asset": asset,
        "symbol": symbol,
        "kind": kind,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED" if blockers else "OBSERVED_WINDOW_AVAILABLE",
        "blockers": blockers,
        "required_periods": required_periods,
        "period_ms": PERIOD_MS,
        "selected_periods": len(selected),
        "missing_period_ends_ms": missing,
        "latest_period_end_ms": latest_end,
        "latest_window_receipt_ms": latest_receipt,
        "health_event_id": health.event_id if health else None,
        "health_scope_asset": health.asset if health else None,
        "max_period_age_ms": max_period_age_ms,
        "max_observation_age_ms": max_observation_age_ms,
        "max_health_age_ms": max_health_age_ms,
        "scope": "Recent sampled window only; global discovery failures apply to every asset. Older continuity, finality, publication time, field semantics and units remain unverified.",
        "continuous_history_verified": False,
        "trading_approval": False,
    }
