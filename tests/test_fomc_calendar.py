import asyncio
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.fomc_calendar import (
    MAX_BYTES,
    SOURCE,
    SYMBOL,
    URL,
    FOMCCalendarCollector,
    calendar_at,
    calendar_quality,
    fetch_calendar,
    parse_calendar,
)
from selective_options.store import DuckStore

RECEIPT = int(datetime(2026, 10, 8, 12, 0, tzinfo=UTC).timestamp() * 1000)


def html(*rows: tuple[int, str, str]) -> bytes:
    parts = ["<html><body>"]
    for year, month, days in rows:
        parts.extend(
            [
                f"<h4><a>{year} FOMC Meetings</a></h4>",
                '<div class="row fomc-meeting">',
                f'<div class="fomc-meeting__month"><strong>{month}</strong></div>',
                f'<div class="fomc-meeting__date">{days}</div>',
                "</div>",
            ]
        )
    return "".join(parts).encode()


def health(received: int, *, ok: bool = True, reason: str | None = None):
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="feed_health",
        event_time_ms=received,
        observed_at_ms=received,
        timestamp_kind="receipt",
        payload={
            "ok": ok,
            "reason": reason or ("SCHEDULE_SNAPSHOT_ACCEPTED" if ok else "HTTP_UNAVAILABLE"),
            "code": 0 if ok else 503,
        },
    )


def test_parse_date_only_ranges_and_september_projections():
    event = parse_calendar(
        html((2026, "March", "17-18*"), (2026, "October/November", "31-1")),
        RECEIPT,
    )
    payload = event.payload()
    assert event.event_time_ms == event.observed_at_ms == RECEIPT
    assert event.timestamp_kind == "receipt"
    meetings = payload["entries"]
    assert meetings[0]["start_date"] == "2026-03-17"
    assert meetings[0]["end_date"] == "2026-03-18"
    assert meetings[0]["associated_with_sep"] is True
    assert meetings[1]["start_date"] == "2026-10-31"
    assert meetings[1]["end_date"] == "2026-11-01"
    assert payload["exact_meeting_time_available"] is False
    assert payload["release_values_available"] is False


def test_year_rollover_and_single_day_meeting():
    event = parse_calendar(html((2026, "December/January", "31-1")), RECEIPT)
    meeting = event.payload()["entries"][0]
    assert meeting["start_date"] == "2026-12-31"
    assert meeting["end_date"] == "2027-01-01"
    assert (
        parse_calendar(html((2026, "October", "27")), RECEIPT).payload()["entries"][0]["end_date"]
        == "2026-10-27"
    )


def test_notation_vote_is_not_misclassified_as_a_regular_meeting():
    entry = parse_calendar(html((2025, "August", "22 (notation vote)")), RECEIPT).payload()[
        "entries"
    ][0]
    assert entry["start_date"] == entry["end_date"] == "2025-08-22"
    assert entry["event_type"] == "notation_vote"


def test_saved_public_federal_reserve_page_parses_all_listed_years():
    response = (
        Path(__file__).resolve().parents[1]
        / "evidence"
        / "fomc-calendar-public-01"
        / "response.html"
    )
    event = parse_calendar(response.read_bytes(), RECEIPT)
    payload = event.payload()
    meetings = payload["entries"]
    assert len(meetings) == 57
    assert sorted({meeting["year"] for meeting in meetings}) == list(range(2021, 2028))
    assert any(meeting["start_date"] == "2026-10-27" for meeting in meetings)
    assert all(meeting["end_date"] >= meeting["start_date"] for meeting in meetings)
    assert sum(meeting["event_type"] == "meeting" for meeting in meetings) == 56
    assert sum(meeting["event_type"] == "notation_vote" for meeting in meetings) == 1


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b" " * (MAX_BYTES + 1), id="oversized"),
        pytest.param(b"<html><h4>2026 FOMC Meetings</h4></html>", id="no-meetings"),
        pytest.param(
            b"<html><h4>2026 FOMC Meetings</h4><div class='row fomc-meeting'></div></html>",
            id="incomplete-row",
        ),
        pytest.param(html((2026, "NotAMonth", "27-28")), id="unknown-month"),
        pytest.param(html((2026, "October", "32-33")), id="invalid-day"),
        pytest.param(html((2026, "December", "2-1")), id="reversed-range"),
        pytest.param(html((2026, "October", "27 (x) 28")), id="ambiguous-range"),
        pytest.param(html((2026, "October", "27"), (2026, "October", "27")), id="duplicate-row"),
        pytest.param(
            b"<h4>2026 FOMC Meetings</h4><div class='row fomc-meeting'><div class='fomc-meeting__month'>October",
            id="truncated-html",
        ),
    ],
)
def test_invalid_or_ambiguous_meeting_schedule_refuses(source):
    with pytest.raises((ValueError, IntegrityError)):
        parse_calendar(source, RECEIPT)


def test_calendar_at_uses_receipt_for_whole_snapshot_revisions():
    first = parse_calendar(html((2026, "October", "27-28")), RECEIPT)
    second = parse_calendar(html((2026, "October", "28-29")), RECEIPT + 1000)
    assert calendar_at([first, second], RECEIPT - 1) is None
    assert calendar_at([first, second], RECEIPT) == first
    assert calendar_at([first, second], RECEIPT + 1000) == second
    assert calendar_at([first, first], RECEIPT) == first
    conflicting = parse_calendar(html((2026, "October", "28-30")), RECEIPT)
    with pytest.raises(IntegrityError, match="AMBIGUOUS"):
        calendar_at([first, conflicting], RECEIPT)


def test_calendar_quality_requires_fresh_schedule_and_post_snapshot_health():
    snapshot = parse_calendar(html((2026, "October", "27-28")), RECEIPT)
    assert calendar_quality([], RECEIPT)["blockers"] == ["MISSING_SNAPSHOT", "MISSING_HEALTH"]
    assert calendar_quality([snapshot], RECEIPT)["blockers"] == ["MISSING_HEALTH"]
    accepted = health(RECEIPT + 1)
    report = calendar_quality([snapshot, accepted], RECEIPT + 1)
    assert report["state"] == "OBSERVED_SCOPE_AVAILABLE"
    assert report["upcoming_events"][0]["start_date"] == "2026-10-27"
    assert report["trading_approval"] is False
    assert "exact decision time" in report["scope"]


def test_calendar_quality_respects_receipt_cutoff_failures_and_expiry():
    snapshot = parse_calendar(html((2026, "October", "27-28")), RECEIPT)
    accepted = health(RECEIPT + 1)
    failure = health(RECEIPT + 2, ok=False)
    before_failure = calendar_quality([snapshot, accepted, failure], RECEIPT + 1)
    assert before_failure["state"] == "OBSERVED_SCOPE_AVAILABLE"
    at_failure = calendar_quality([snapshot, accepted, failure], RECEIPT + 2)
    assert "SOURCE_UNAVAILABLE" in at_failure["blockers"]
    stale = calendar_quality([snapshot, accepted], RECEIPT + 10_001, max_age_ms=9_999)
    assert "STALE_SNAPSHOT" in stale["blockers"]
    assert "STALE_HEALTH" in stale["blockers"]


def test_calendar_quality_blocks_schedule_without_future_meeting():
    snapshot = parse_calendar(html((2026, "January", "27-28")), RECEIPT)
    report = calendar_quality([snapshot, health(RECEIPT + 1)], RECEIPT + 1)
    assert "NO_FUTURE_MEETING" in report["blockers"]


def test_calendar_quality_rejects_ambiguous_latest_health():
    snapshot = parse_calendar(html((2026, "October", "27-28")), RECEIPT)
    good = health(RECEIPT + 1)
    bad = health(RECEIPT + 1, ok=False)
    with pytest.raises(IntegrityError, match="AMBIGUOUS"):
        calendar_quality([snapshot, good, bad], RECEIPT + 1)


async def test_snapshot_roundtrip_and_exact_pit_visibility(tmp_path):
    event = parse_calendar(html((2026, "October", "27-28")), RECEIPT)
    store = await DuckStore.open(tmp_path / "fomc.duckdb")
    try:
        await store.append([event])
        await store.append([event])
        before = await store.snapshot("GLOBAL", RECEIPT - 1, "public_reference")
        after = await store.snapshot("GLOBAL", RECEIPT, "public_reference")
        assert calendar_at(before.rows(), RECEIPT - 1) is None
        assert calendar_at(after.rows(), RECEIPT) == event
        assert await store.pending_delivery_count() == 1
    finally:
        await store.close()


class Response:
    def __init__(self, status=200, data=None):
        self.status, self.data, self.content = (
            status,
            data or html((2026, "October", "27-28")),
            self,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        yield self.data


class Session:
    def __init__(self, response):
        self.response = response

    def get(self, url, **kwargs):
        assert url == URL and kwargs == {"allow_redirects": False}
        return self.response


@pytest.mark.parametrize(
    "status,reason",
    [
        (401, "ACCESS_DENIED"),
        (403, "ACCESS_DENIED"),
        (451, "ACCESS_DENIED"),
        (429, "RATE_LIMITED"),
        (302, "HTTP_UNAVAILABLE"),
        (503, "HTTP_UNAVAILABLE"),
    ],
)
async def test_fetch_statuses_fail_closed(status, reason):
    with pytest.raises(FeedUnavailable) as exc:
        await fetch_calendar(Session(Response(status)))
    assert exc.value.code == status and exc.value.reason == reason


async def test_fetch_is_bounded_and_preserves_receipt():
    data, received = await fetch_calendar(Session(Response()))
    assert data == html((2026, "October", "27-28")) and received > 0
    with pytest.raises(IntegrityError):
        await fetch_calendar(Session(Response(data=b" " * (MAX_BYTES + 1))))


async def test_collector_emits_snapshot_then_health_and_keeps_sink_failures(monkeypatch):
    sink = AsyncMock()
    collector = FOMCCalendarCollector(AppConfig(), sink)
    assert await collector.collect(Session(Response()))
    assert [call.args[0].kind for call in sink.await_args_list] == ["calendar", "feed_health"]
    assert sink.await_args_list[-1].args[0].payload()["ok"] is True
    sink.side_effect = IntegrityError("store failure")
    with pytest.raises(IntegrityError, match="store failure"):
        await collector.collect(Session(Response()))


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (TimeoutError(), "TimeoutError"),
        (IntegrityError("bad html"), "INVALID_DATA"),
    ],
)
async def test_collector_records_failures_without_publishing_snapshot(monkeypatch, error, reason):
    monkeypatch.setattr(
        "selective_options.fomc_calendar.fetch_calendar", AsyncMock(side_effect=error)
    )
    sink = AsyncMock()
    assert not await FOMCCalendarCollector(AppConfig(), sink).collect(None)
    sink.assert_awaited_once()
    assert sink.await_args.args[0].payload()["reason"] == reason


async def test_run_waits_between_polls_and_propagates_cancellation(monkeypatch):
    collector = FOMCCalendarCollector(AppConfig(), AsyncMock())
    collector.collect = AsyncMock(return_value=False)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.fomc_calendar.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(3600)


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_fomc_is_opt_in(monkeypatch, tmp_path, enabled):
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    calls = []

    async def run(self):
        calls.append(True)
        await self.health(False, "ACCESS_DENIED", 403)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(FOMCCalendarCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "fomc_calendar_enabled": enabled,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert bool(calls) == enabled and report["rows"] == int(enabled)
    assert report["fomc_calendar_quality"]["state"] == ("BLOCKED" if enabled else "NOT_CONFIGURED")
    if enabled:
        assert "MISSING_SNAPSHOT" in report["fomc_calendar_quality"]["blockers"]
