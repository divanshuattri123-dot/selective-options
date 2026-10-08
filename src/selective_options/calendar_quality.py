"""As-of calendar data quality, scoped to named series; not a trading/event-risk gate."""

from collections.abc import Iterable

from pydantic import Field, StrictInt, StrictStr, ValidationError

from .bea_calendar import calendar_at
from .errors import IntegrityError
from .events import MarketEvent
from .models import ExternalModel

REQUIRED_SERIES = ("Personal Income and Outlays", "Gross Domestic Product")


class CalendarRelease(ExternalModel):
    title: StrictStr = Field(min_length=1, max_length=500)
    scheduled_at_ms: StrictInt = Field(gt=0)


class CalendarPayload(ExternalModel):
    releases: tuple[CalendarRelease, ...] = Field(min_length=1, max_length=10_000)


def calendar_quality(
    events: Iterable[MarketEvent],
    as_of_ms: int,
    max_age_ms: int,
    required_series: tuple[str, ...] = REQUIRED_SERIES,
) -> dict[str, object]:
    if (
        as_of_ms <= 0
        or max_age_ms <= 0
        or not required_series
        or any(not title.strip() for title in required_series)
    ):
        raise ValueError("positive clock/age and named required series are required")
    rows = tuple(events)
    blockers: list[str] = []
    snapshot: MarketEvent | None = None
    try:
        snapshot = calendar_at(rows, as_of_ms)
    except IntegrityError:
        blockers.append("AMBIGUOUS_SNAPSHOT")
    if snapshot is None and not blockers:
        blockers.append("MISSING_SNAPSHOT")
    health_rows = [
        e
        for e in rows
        if e.source == "bea"
        and e.kind == "feed_health"
        and e.asset == "GLOBAL"
        and e.symbol == "RELEASE_SCHEDULE"
        and e.environment == "public_reference"
        and max(e.event_time_ms, e.observed_at_ms) <= as_of_ms
    ]
    health: MarketEvent | None = None
    if not health_rows:
        blockers.append("MISSING_HEALTH")
    else:
        latest = max(e.observed_at_ms for e in health_rows)
        candidates = {e.event_id: e for e in health_rows if e.observed_at_ms == latest}
        if len(candidates) != 1:
            blockers.append("AMBIGUOUS_HEALTH")
        else:
            health = next(iter(candidates.values()))
            if health.timestamp_kind != "receipt" or health.event_time_ms != health.observed_at_ms:
                blockers.append("INVALID_HEALTH_CLOCK")
            if as_of_ms - health.observed_at_ms > max_age_ms:
                blockers.append("STALE_HEALTH")
            try:
                if health.payload().get("ok") is not True:
                    blockers.append("SOURCE_UNAVAILABLE")
            except ValueError:
                blockers.append("INVALID_HEALTH_PAYLOAD")
    upcoming: dict[str, list[int]] = {title: [] for title in sorted(set(required_series))}
    if snapshot is not None:
        if (
            snapshot.timestamp_kind != "receipt"
            or snapshot.event_time_ms != snapshot.observed_at_ms
        ):
            blockers.append("INVALID_SNAPSHOT_CLOCK")
        if as_of_ms - snapshot.observed_at_ms > max_age_ms:
            blockers.append("STALE_SNAPSHOT")
        if health is not None and health.observed_at_ms < snapshot.observed_at_ms:
            blockers.append("HEALTH_PRECEDES_SNAPSHOT")
        try:
            parsed = CalendarPayload.model_validate_json(snapshot.payload_json)
            for release in parsed.releases:
                # At the exact release instant it no longer proves future schedule coverage.
                if release.title in upcoming and release.scheduled_at_ms > as_of_ms:
                    upcoming[release.title].append(release.scheduled_at_ms)
            if any(not times for times in upcoming.values()):
                blockers.append("REQUIRED_SERIES_WITHOUT_FUTURE_DATE")
        except ValidationError:
            blockers.append("INVALID_SNAPSHOT_PAYLOAD")
    return {
        "as_of_ms": as_of_ms,
        "max_age_ms": max_age_ms,
        "state": "BLOCKED" if blockers else "OBSERVED_SCOPE_AVAILABLE",
        "blockers": blockers,
        "snapshot_event_id": snapshot.event_id if snapshot else None,
        "health_event_id": health.event_id if health else None,
        "required_series": sorted(set(required_series)),
        "next_scheduled_at_ms": {
            title: min(times) if times else None for title, times in upcoming.items()
        },
        "scope": "Named BEA series only; publisher completeness and other calendars unverified",
        "trading_approval": False,
    }
