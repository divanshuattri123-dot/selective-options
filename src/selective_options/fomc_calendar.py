"""Official Federal Reserve FOMC meeting-date snapshots (date-only, not release times)."""

import asyncio
import hashlib
import re
from collections.abc import Iterable
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from typing import Literal

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
SOURCE = "federal_reserve_fomc"
SYMBOL = "FOMC_MEETINGS"
MAX_BYTES = 1_048_576
MAX_MEETINGS = 200
MAX_AGE_MS = 7_200_000
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


class ScheduleEntry(ExternalModel):
    year: StrictInt = Field(ge=2000, le=2200)
    month_label: StrictStr = Field(min_length=3, max_length=32)
    date_label: StrictStr = Field(min_length=1, max_length=80)
    start_date: StrictStr = Field(pattern=r"^20\d{2}-\d{2}-\d{2}$")
    end_date: StrictStr = Field(pattern=r"^20\d{2}-\d{2}-\d{2}$")
    associated_with_sep: StrictBool
    event_type: Literal["meeting", "notation_vote"]


class CalendarPayload(ExternalModel):
    feed_url: StrictStr
    body_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    entries: tuple[ScheduleEntry, ...] = Field(min_length=1, max_length=MAX_MEETINGS)
    timestamp_basis: StrictStr
    schedule_precision: StrictStr
    coverage: StrictStr
    exact_meeting_time_available: StrictBool
    release_values_available: StrictBool
    consensus_available: StrictBool


class Health(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    reason: StrictStr = Field(min_length=1, max_length=80)
    code: StrictInt = Field(ge=0, le=599)


def _date_range(year: int, month_label: str, date_label: str) -> tuple[str, str, bool]:
    months = re.findall(r"[A-Za-z]+", month_label)
    days = re.fullmatch(
        r"\s*(\d{1,2})(?:\s*[-\u2013\u2014]\s*(\d{1,2}))?\s*(\*)?\s*(?:\([^)]{0,80}\))?\s*",
        date_label,
    )
    if not months or len(months) > 2 or days is None:
        raise IntegrityError("FOMC_INVALID_MEETING_DATE")
    try:
        start_month = _MONTHS[months[0][:3].upper()]
        end_month = _MONTHS[months[-1][:3].upper()]
    except KeyError as exc:
        raise IntegrityError("FOMC_INVALID_MONTH") from exc
    start_day, end_day = int(days.group(1)), int(days.group(2) or days.group(1))
    start = date(year, start_month, start_day)
    end_year = year + int(end_month < start_month)
    end = date(end_year, end_month, end_day)
    if end < start:
        raise IntegrityError("FOMC_REVERSED_MEETING_DATE")
    return start.isoformat(), end.isoformat(), days.group(3) is not None


class _MeetingHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.year: int | None = None
        self.in_heading = False
        self.heading: list[str] = []
        self.row: dict[str, str | int] | None = None
        self.row_depth = 0
        self.capture: str | None = None
        self.capture_depth = 0
        self.capture_text: list[str] = []
        self.rows: list[dict[str, str | int]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "h4":
            self.in_heading = True
            self.heading = []
        if tag != "div":
            return
        self.depth += 1
        attributes = dict(attrs)
        classes = set((attributes.get("class") or "").split())
        if self.row is None and "fomc-meeting" in classes:
            if self.year is None:
                raise IntegrityError("FOMC_MEETING_WITHOUT_YEAR")
            self.row = {"year": self.year, "month_label": "", "date_label": ""}
            self.row_depth = self.depth
        if self.row is not None:
            fields = {
                "fomc-meeting__month": "month_label",
                "fomc-meeting__date": "date_label",
            }
            for class_name, field in fields.items():
                if class_name in classes:
                    if self.capture is not None or str(self.row[field]):
                        raise IntegrityError("FOMC_DUPLICATE_MEETING_FIELD")
                    self.capture = field
                    self.capture_depth = self.depth
                    self.capture_text = []

    def handle_data(self, data: str) -> None:
        if self.in_heading:
            self.heading.append(data)
        if self.capture is not None:
            self.capture_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "h4":
            self.in_heading = False
            match = re.search(r"\b(20\d{2})\s+FOMC Meetings\b", " ".join(self.heading))
            if match:
                self.year = int(match.group(1))
        if tag != "div":
            return
        if self.capture is not None and self.depth == self.capture_depth:
            assert self.row is not None
            self.row[self.capture] = " ".join(" ".join(self.capture_text).split())
            self.capture = None
            self.capture_text = []
        if self.row is not None and self.depth == self.row_depth:
            if not self.row["month_label"] or not self.row["date_label"]:
                raise IntegrityError("FOMC_INCOMPLETE_MEETING")
            self.rows.append(self.row)
            self.row = None
        self.depth = max(0, self.depth - 1)


def parse_calendar(body: bytes, received: int) -> MarketEvent:
    if type(received) is not int or received <= 0 or not body or len(body) > MAX_BYTES:
        raise IntegrityError("FOMC_BODY_SIZE_OR_CLOCK")
    parser = _MeetingHTMLParser()
    try:
        parser.feed(body.decode("utf-8-sig", errors="strict"))
        parser.close()
    except (UnicodeDecodeError, AssertionError) as exc:
        raise IntegrityError("FOMC_INVALID_HTML") from exc
    if parser.row is not None or parser.capture is not None:
        raise IntegrityError("FOMC_TRUNCATED_HTML")
    if not parser.rows or len(parser.rows) > MAX_MEETINGS:
        raise IntegrityError("FOMC_MEETING_COUNT")
    entries: list[ScheduleEntry] = []
    seen: set[tuple[int, str, str]] = set()
    for raw in parser.rows:
        year = raw["year"]
        month_label = raw["month_label"]
        date_label = raw["date_label"]
        assert type(year) is int and isinstance(month_label, str) and isinstance(date_label, str)
        start_date, end_date, sep = _date_range(year, month_label, date_label)
        key = (year, start_date, end_date)
        if key in seen:
            raise IntegrityError("FOMC_DUPLICATE_MEETING")
        seen.add(key)
        entries.append(
            ScheduleEntry(
                year=year,
                month_label=month_label,
                date_label=date_label,
                start_date=start_date,
                end_date=end_date,
                associated_with_sep=sep,
                event_type=(
                    "notation_vote" if "notation vote" in date_label.casefold() else "meeting"
                ),
            )
        )
    payload = CalendarPayload(
        feed_url=URL,
        body_sha256=hashlib.sha256(body).hexdigest(),
        entries=tuple(entries),
        timestamp_basis="HTTP receipt; the page does not provide a reliable first-publication time",
        schedule_precision="Meeting start/end calendar dates only; exact decision/press-conference times are unavailable",
        coverage="FOMC schedule entries listed on the official page; unscheduled meetings may be absent",
        exact_meeting_time_available=False,
        release_values_available=False,
        consensus_available=False,
    )
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="calendar",
        event_time_ms=received,
        observed_at_ms=received,
        timestamp_kind="receipt",
        native_id=f"{payload.body_sha256}:{received}",
        payload=payload.model_dump(mode="json"),
    )


def calendar_at(events: Iterable[MarketEvent], as_of_ms: int) -> MarketEvent | None:
    eligible = [
        event
        for event in events
        if event.source == SOURCE
        and event.environment == "public_reference"
        and event.asset == "GLOBAL"
        and event.symbol == SYMBOL
        and event.kind == "calendar"
        and max(event.event_time_ms, event.observed_at_ms) <= as_of_ms
    ]
    if not eligible:
        return None
    latest = max(event.observed_at_ms for event in eligible)
    candidates = {event.event_id: event for event in eligible if event.observed_at_ms == latest}
    if len(candidates) != 1:
        raise IntegrityError("FOMC_AMBIGUOUS_SNAPSHOT")
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
                raise IntegrityError("FOMC_BODY_SIZE")
        return bytes(body), now_ms()


class FOMCCalendarCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink

    async def health(self, ok: bool, reason: str, code: int = 0) -> None:
        received = now_ms()
        await self.sink(
            make_event(
                source=SOURCE,
                environment="public_reference",
                asset="GLOBAL",
                symbol=SYMBOL,
                kind="feed_health",
                event_time_ms=received,
                observed_at_ms=received,
                timestamp_kind="receipt",
                payload=Health(ok=ok, reason=reason, code=code).model_dump(mode="json"),
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
        except (ValueError, IntegrityError):
            await self.health(False, "INVALID_DATA")
            return False
        await self.sink(event)
        await self.health(True, "SCHEDULE_SNAPSHOT_ACCEPTED")
        return True

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        headers = {"User-Agent": "SelectiveOptionsResearch/0.3 official FOMC calendar metadata"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            while True:
                await self.collect(session)
                await asyncio.sleep(self.config.ingestion.fomc_calendar_poll_interval_s)


def _upcoming(entries: Iterable[ScheduleEntry], as_of_ms: int) -> list[ScheduleEntry]:
    as_of_date = datetime.fromtimestamp(as_of_ms / 1000, UTC).date()
    return [entry for entry in entries if date.fromisoformat(entry.start_date) > as_of_date]


def calendar_quality(
    events: Iterable[MarketEvent], as_of_ms: int, max_age_ms: int = MAX_AGE_MS
) -> dict[str, object]:
    if type(as_of_ms) is not int or as_of_ms <= 0 or type(max_age_ms) is not int or max_age_ms <= 0:
        raise ValueError("invalid FOMC quality clock or age")
    rows = list(events)
    snapshot = calendar_at(rows, as_of_ms)
    health_rows = [
        event
        for event in rows
        if event.source == SOURCE
        and event.environment == "public_reference"
        and event.asset == "GLOBAL"
        and event.symbol == SYMBOL
        and event.kind == "feed_health"
        and max(event.event_time_ms, event.observed_at_ms) <= as_of_ms
    ]
    latest_health: MarketEvent | None = None
    if health_rows:
        latest_receipt = max(event.observed_at_ms for event in health_rows)
        matches = {
            event.event_id: event for event in health_rows if event.observed_at_ms == latest_receipt
        }
        if len(matches) > 1:
            raise IntegrityError("FOMC_AMBIGUOUS_HEALTH")
        latest_health = next(iter(matches.values()))
    blockers: list[str] = []
    if snapshot is None:
        blockers.append("MISSING_SNAPSHOT")
    if latest_health is None:
        blockers.append("MISSING_HEALTH")
    upcoming: list[ScheduleEntry] = []
    entry_count = 0
    if snapshot is not None:
        if (
            snapshot.timestamp_kind != "receipt"
            or snapshot.event_time_ms != snapshot.observed_at_ms
        ):
            blockers.append("INVALID_SNAPSHOT_CLOCK")
        elif as_of_ms - snapshot.observed_at_ms > max_age_ms:
            blockers.append("STALE_SNAPSHOT")
        try:
            payload = CalendarPayload.model_validate_json(snapshot.payload_json)
            entry_count = len(payload.entries)
            for entry in payload.entries:
                if date.fromisoformat(entry.end_date) < date.fromisoformat(entry.start_date):
                    raise IntegrityError("FOMC_REVERSED_MEETING_DATE")
            upcoming = _upcoming(payload.entries, as_of_ms)
            if not upcoming:
                blockers.append("NO_FUTURE_MEETING")
        except (ValueError, IntegrityError):
            blockers.append("INVALID_SNAPSHOT_PAYLOAD")
    if latest_health is not None:
        try:
            health = Health.model_validate_json(latest_health.payload_json)
            if (
                latest_health.timestamp_kind != "receipt"
                or latest_health.event_time_ms != latest_health.observed_at_ms
            ):
                blockers.append("INVALID_HEALTH_CLOCK")
            elif as_of_ms - latest_health.observed_at_ms > max_age_ms:
                blockers.append("STALE_HEALTH")
            elif not health.ok:
                blockers.append("SOURCE_UNAVAILABLE")
            elif snapshot is not None and latest_health.observed_at_ms < snapshot.observed_at_ms:
                blockers.append("HEALTH_PRECEDES_SNAPSHOT")
        except ValueError:
            blockers.append("INVALID_HEALTH_PAYLOAD")
    return {
        "as_of_ms": as_of_ms,
        "max_age_ms": max_age_ms,
        "state": "BLOCKED" if blockers else "OBSERVED_SCOPE_AVAILABLE",
        "blockers": blockers,
        "snapshot_event_id": snapshot.event_id if snapshot else None,
        "health_event_id": latest_health.event_id if latest_health else None,
        "schedule_entry_count": entry_count,
        "upcoming_events": [entry.model_dump(mode="json") for entry in upcoming],
        "scope": "Official listed FOMC schedule entries only; date-only, no exact decision time or unscheduled meeting coverage",
        "trading_approval": False,
    }
