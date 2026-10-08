import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.bea_calendar import (
    MAX_BYTES,
    URL,
    BEACalendarCollector,
    calendar_at,
    fetch_calendar,
    parse_calendar,
)
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore


def body(*dates, title="Personal Income and Outlays"):
    return json.dumps({title: {"release_dates": list(dates)}}).encode()


DATE = "2026-10-29T12:30:00+00:00"
RECEIPT = int(datetime.fromisoformat("2026-09-28T14:00:00+00:00").timestamp() * 1000)


def test_future_dates_are_payload_not_event_availability():
    event = parse_calendar(body(DATE, "2026-10-29T08:30:00-04:00"), RECEIPT)
    assert event.event_time_ms == event.observed_at_ms == RECEIPT
    assert event.timestamp_kind == "receipt"
    assert event.payload()["duplicate_entries_removed"] == 1
    release = event.payload()["releases"][0]
    assert release["scheduled_at_ms"] > RECEIPT
    assert calendar_at([event], RECEIPT - 1) is None
    assert calendar_at([event], RECEIPT) == event


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b" " * (MAX_BYTES + 1),
        b"{}",
        b"[]",
        b"<html>error</html>",
        b'{"A":{},"A":{}}',
        b'{"A":{"release_dates":[],"release_dates":[]}}',
        body(DATE, title=" "),
        body(DATE, title="X" * 501),
        body(),
        body("2026-10-29"),
        body("2026-10-29T12:30:00"),
        body("invalidTdate"),
        body("1970-01-01T00:00:00+00:00"),
        body("2026-10-29T12:30:00.001+00:00"),
        body(1793277000000),
        body(*([DATE] * 501)),
        json.dumps({str(i): {"release_dates": [DATE]} for i in range(201)}).encode(),
        json.dumps({str(i): {"release_dates": [DATE] * 500} for i in range(21)}).encode(),
    ],
    ids=[
        "empty",
        "oversize",
        "empty-object",
        "array",
        "html",
        "duplicate-title",
        "duplicate-field",
        "blank-title",
        "long-title",
        "no-dates",
        "date-only",
        "naive-time",
        "invalid-time",
        "epoch",
        "subsecond",
        "numeric-time",
        "group-limit",
        "title-limit",
        "total-limit",
    ],
)
def test_invalid_or_unbounded_schedule(raw):
    with pytest.raises((ValueError, IntegrityError, ValidationError)):
        parse_calendar(raw, RECEIPT)


async def test_revisions_removals_and_repeated_observations(tmp_path):
    first = parse_calendar(body(DATE, "2026-11-25T13:30:00+00:00"), RECEIPT)
    revised = parse_calendar(body("2026-11-26T13:30:00+00:00"), RECEIPT + 1000)
    repeated = parse_calendar(body("2026-11-26T13:30:00+00:00"), RECEIPT + 2000)
    store = await DuckStore.open(tmp_path / "calendar.duckdb")
    try:
        await store.append([first, revised, repeated])
        before = await store.snapshot("GLOBAL", RECEIPT - 1, "public_reference")
        assert calendar_at(before.rows(), RECEIPT - 1) is None
        earlier = await store.snapshot("GLOBAL", RECEIPT + 999, "public_reference")
        assert calendar_at(earlier.rows(), RECEIPT + 999) == first
        later = await store.snapshot("GLOBAL", RECEIPT + 1000, "public_reference")
        selected = calendar_at(later.rows(), RECEIPT + 1000)
        assert selected == revised and len(selected.payload()["releases"]) == 1
        assert calendar_at([first, revised, repeated], RECEIPT + 2000) == repeated
    finally:
        await store.close()


def test_equal_receipt_conflicting_snapshots_fail():
    event = parse_calendar(body(DATE), RECEIPT)
    assert calendar_at([event, event], RECEIPT) == event
    with pytest.raises(IntegrityError, match="AMBIGUOUS"):
        calendar_at([event, parse_calendar(body("2026-11-25T13:30:00+00:00"), RECEIPT)], RECEIPT)


def test_file_update_label_never_backdates_availability():
    raw = json.loads(body(DATE))
    raw["file_last_updated"] = "2026-07-13T08:00:42.402013"
    event = parse_calendar(json.dumps(raw).encode(), RECEIPT)
    assert event.event_time_ms == RECEIPT
    assert event.payload()["source_last_updated_raw"] == raw["file_last_updated"]
    assert event.payload()["source_last_updated_timezone_verified"] is False
    raw["file_last_updated"] = 123
    with pytest.raises(IntegrityError):
        parse_calendar(json.dumps(raw).encode(), RECEIPT)


class Response:
    def __init__(self, status=200, data=None):
        self.status, self.data, self.content = (
            status,
            data if data is not None else body(DATE),
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
        (500, "HTTP_UNAVAILABLE"),
    ],
)
async def test_fetch_denials(status, reason):
    with pytest.raises(FeedUnavailable) as exc:
        await fetch_calendar(Session(Response(status)))
    assert exc.value.code == status and exc.value.reason == reason


async def test_bounded_fetch():
    data, received = await fetch_calendar(Session(Response()))
    assert data == body(DATE) and received > 0
    with pytest.raises(IntegrityError):
        await fetch_calendar(Session(Response(data=b" " * (MAX_BYTES + 1))))


async def test_collect_success_and_sink_failure(monkeypatch):
    sink = AsyncMock()
    collector = BEACalendarCollector(AppConfig(), sink)
    assert await collector.collect(Session(Response()))
    assert [c.args[0].kind for c in sink.await_args_list] == ["calendar", "feed_health"]
    sink.side_effect = IntegrityError("store failure")
    with pytest.raises(IntegrityError, match="store failure"):
        await collector.collect(Session(Response()))


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (TimeoutError(), "TimeoutError"),
        (ValueError(), "INVALID_DATA"),
    ],
)
async def test_collect_failure_no_schedule(monkeypatch, error, reason):
    monkeypatch.setattr(
        "selective_options.bea_calendar.fetch_calendar", AsyncMock(side_effect=error)
    )
    sink = AsyncMock()
    assert not await BEACalendarCollector(AppConfig(), sink).collect(None)
    sink.assert_awaited_once()
    assert sink.await_args.args[0].payload()["reason"] == reason


async def test_cancel_and_poll_interval(monkeypatch):
    collector = BEACalendarCollector(AppConfig(), AsyncMock())
    collector.collect = AsyncMock(return_value=False)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.bea_calendar.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(3600)
    monkeypatch.setattr(
        "selective_options.bea_calendar.fetch_calendar",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )
    with pytest.raises(asyncio.CancelledError):
        await BEACalendarCollector(AppConfig(), AsyncMock()).collect(None)


@pytest.mark.parametrize("enabled", [True, False])
async def test_capture_opt_in(monkeypatch, tmp_path, enabled):
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
    monkeypatch.setattr(BEACalendarCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "bea_calendar_enabled": enabled,
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
    assert report["calendar_quality"]["state"] == ("BLOCKED" if enabled else "NOT_CONFIGURED")
    if enabled:
        assert "SOURCE_UNAVAILABLE" in report["calendar_quality"]["blockers"]
