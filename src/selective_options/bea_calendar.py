"""Official BEA schedule snapshots; receipt availability, never future event timestamps."""

import asyncio
import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime

import aiohttp
from pydantic import Field, StrictStr, TypeAdapter, ValidationError

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

URL = "https://apps.bea.gov/API/signup/release_dates.json"
MAX_BYTES = 1_048_576


class ScheduleGroup(ExternalModel):
    release_dates: tuple[StrictStr, ...] = Field(max_length=500)


def unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError("BEA_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def parse_calendar(body: bytes, received: int) -> MarketEvent:
    if not body or len(body) > MAX_BYTES:
        raise IntegrityError("BEA_BODY_SIZE")
    raw = json.loads(body, object_pairs_hook=unique_keys)
    updated = raw.pop("file_last_updated", None) if isinstance(raw, dict) else None
    if updated is not None and (not isinstance(updated, str) or not 0 < len(updated) <= 100):
        raise IntegrityError("BEA_INVALID_UPDATE_LABEL")
    groups = TypeAdapter(dict[str, ScheduleGroup]).validate_python(raw)
    if not 0 < len(groups) <= 200:
        raise IntegrityError("BEA_GROUP_COUNT")
    releases: set[tuple[str, int]] = set()
    total = 0
    for title, group in groups.items():
        if not title.strip() or len(title) > 500:
            raise IntegrityError("BEA_INVALID_TITLE")
        for value in group.release_dates:
            # An explicit date/time and offset are required; no host timezone or DST guessing.
            if "T" not in value:
                raise IntegrityError("BEA_MISSING_TIMEZONE_OR_TIME")
            date = datetime.fromisoformat(value)
            if date.tzinfo is None or date.utcoffset() is None:
                raise IntegrityError("BEA_MISSING_TIMEZONE_OR_TIME")
            timestamp = int(date.astimezone(UTC).timestamp() * 1000)
            if timestamp <= 0 or date.microsecond:
                raise IntegrityError("BEA_INVALID_RELEASE_TIME")
            releases.add((title, timestamp))
            total += 1
    if not releases or total > 10_000:
        raise IntegrityError("BEA_RELEASE_COUNT")
    return make_event(
        source="bea",
        environment="public_reference",
        asset="GLOBAL",
        symbol="RELEASE_SCHEDULE",
        kind="calendar",
        event_time_ms=received,
        observed_at_ms=received,
        timestamp_kind="receipt",
        payload={
            "feed_url": URL,
            "body_sha256": hashlib.sha256(body).hexdigest(),
            "releases": [
                {"title": title, "scheduled_at_ms": ts}
                for title, ts in sorted(releases, key=lambda item: (item[1], item[0]))
            ],
            "duplicate_entries_removed": total - len(releases),
            "group_count": len(groups),
            "source_last_updated_raw": updated,
            "source_last_updated_timezone_verified": False,
            "timestamp_basis": "HTTP receipt; source publication and revision times unavailable",
            "revision_policy": "latest received whole snapshot; never union older schedules",
            "coverage": "dated entries returned by BEA; unscheduled/TBA entries may be absent",
            "actual_release_values_available": False,
            "consensus_available": False,
        },
    )


def calendar_at(events: Iterable[MarketEvent], as_of_ms: int) -> MarketEvent | None:
    """Select one whole observed schedule, including removals, without leaking later revisions."""
    eligible = [
        e
        for e in events
        if e.source == "bea"
        and e.kind == "calendar"
        and e.asset == "GLOBAL"
        and e.symbol == "RELEASE_SCHEDULE"
        and e.environment == "public_reference"
        and max(e.event_time_ms, e.observed_at_ms) <= as_of_ms
    ]
    if not eligible:
        return None
    latest = max(e.observed_at_ms for e in eligible)
    candidates = {e.event_id: e for e in eligible if e.observed_at_ms == latest}
    if len(candidates) != 1:
        raise IntegrityError("BEA_AMBIGUOUS_SNAPSHOT")
    return next(iter(candidates.values()))


async def fetch_calendar(session: aiohttp.ClientSession) -> tuple[bytes, int]:
    async with session.get(URL, allow_redirects=False) as response:
        if response.status in {401, 403, 451}:
            raise FeedUnavailable("ACCESS_DENIED", response.status)
        if response.status == 429:
            raise FeedUnavailable("RATE_LIMITED", response.status)
        if response.status != 200:
            raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            body.extend(chunk)
            if len(body) > MAX_BYTES:
                raise IntegrityError("BEA_BODY_SIZE")
        return bytes(body), now_ms()


class BEACalendarCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink

    async def health(self, ok: bool, reason: str, code: int = 0) -> None:
        received = now_ms()
        await self.sink(
            make_event(
                source="bea",
                environment="public_reference",
                asset="GLOBAL",
                symbol="RELEASE_SCHEDULE",
                kind="feed_health",
                event_time_ms=received,
                observed_at_ms=received,
                timestamp_kind="receipt",
                payload={"ok": ok, "reason": reason, "code": code},
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        try:
            body, received = await fetch_calendar(session)
            event = parse_calendar(body, received)
        except FeedUnavailable as exc:
            await self.health(False, exc.reason, exc.code)
            return False
        except (aiohttp.ClientError, TimeoutError) as exc:
            await self.health(False, type(exc).__name__)
            return False
        except (ValueError, IntegrityError, ValidationError):
            await self.health(False, "INVALID_DATA")
            return False
        # Keep consumer failures outside the source error boundary.
        await self.sink(event)
        await self.health(True, "SCHEDULE_SNAPSHOT_ACCEPTED")
        return True

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"User-Agent": "SelectiveOptionsResearch/0.3 public calendar metadata"},
        ) as session:
            while True:
                await self.collect(session)
                await asyncio.sleep(self.config.ingestion.bea_calendar_poll_interval_s)
