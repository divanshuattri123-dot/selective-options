import json
from pathlib import Path

import pytest

from selective_options.bea_calendar import parse_calendar
from selective_options.calendar_quality import REQUIRED_SERIES, calendar_quality
from selective_options.events import make_event


def event(kind="calendar", received=1000, payload=None, **changes):
    return make_event(
        **{
            "source": "bea",
            "environment": "public_reference",
            "asset": "GLOBAL",
            "symbol": "RELEASE_SCHEDULE",
            "kind": kind,
            "observed_at_ms": received,
            "event_time_ms": received,
            "timestamp_kind": "receipt",
            "payload": payload
            if payload is not None
            else {
                "releases": [{"title": title, "scheduled_at_ms": 5000} for title in REQUIRED_SERIES]
            },
            **changes,
        }
    )


def health(received=1001, ok=True, **changes):
    return event("feed_health", received, {"ok": ok}, **changes)


def test_available_scope_and_boundary():
    rows = [event(), health()]
    result = calendar_quality(rows, 2000, 1000)
    assert result["state"] == "OBSERVED_SCOPE_AVAILABLE" and result["blockers"] == []
    assert result["trading_approval"] is False
    assert result["next_scheduled_at_ms"] == dict.fromkeys(REQUIRED_SERIES, 5000)
    assert "STALE_SNAPSHOT" in calendar_quality(rows, 2001, 1000)["blockers"]
    assert calendar_quality(rows, 1000, 1000)["blockers"] == ["MISSING_HEALTH"]


@pytest.mark.parametrize(
    "rows,asof,age,blocker",
    [
        ([], 2000, 1000, "MISSING_SNAPSHOT"),
        ([event()], 2000, 1000, "MISSING_HEALTH"),
        ([event(), health(ok=False)], 2000, 1000, "SOURCE_UNAVAILABLE"),
        ([event(), health()], 3000, 1000, "STALE_HEALTH"),
        ([event(), health(999)], 1000, 1000, "HEALTH_PRECEDES_SNAPSHOT"),
        ([event(), health(), health(ok=False)], 2000, 1000, "AMBIGUOUS_HEALTH"),
        ([event(), event(payload={"releases": []}), health()], 2000, 1000, "AMBIGUOUS_SNAPSHOT"),
        ([event(timestamp_kind="source"), health()], 2000, 1000, "INVALID_SNAPSHOT_CLOCK"),
        ([event(event_time_ms=900), health()], 2000, 1000, "INVALID_SNAPSHOT_CLOCK"),
        ([event(), health(timestamp_kind="source")], 2000, 1000, "INVALID_HEALTH_CLOCK"),
        ([event(), health(event_time_ms=900)], 2000, 1000, "INVALID_HEALTH_CLOCK"),
        ([event(payload={"releases": []}), health()], 2000, 1000, "INVALID_SNAPSHOT_PAYLOAD"),
        (
            [event(payload={"releases": [{"title": "Other", "scheduled_at_ms": 5000}]}), health()],
            2000,
            1000,
            "REQUIRED_SERIES_WITHOUT_FUTURE_DATE",
        ),
        ([event(), health()], 5000, 10000, "REQUIRED_SERIES_WITHOUT_FUTURE_DATE"),
    ],
)
def test_blocking_conditions(rows, asof, age, blocker):
    result = calendar_quality(rows, asof, age)
    assert result["state"] == "BLOCKED" and blocker in result["blockers"]


@pytest.mark.parametrize(
    "asof,age,series",
    [
        (0, 1000, REQUIRED_SERIES),
        (1000, 0, REQUIRED_SERIES),
        (1000, 1000, ()),
        (1000, 1000, (" ",)),
    ],
)
def test_invalid_policy(asof, age, series):
    with pytest.raises(ValueError):
        calendar_quality([], asof, age, series)


def test_future_failure_and_other_scopes_cannot_contaminate_earlier_read():
    original = event()
    rows = [original, health(), health(3000, False)]
    for changes in [
        {"asset": "BTC"},
        {"symbol": "OTHER"},
        {"environment": "testnet"},
        {"source": "other"},
    ]:
        rows += [
            event(received=1500, payload={"releases": []}, **changes),
            health(1501, False, **changes),
        ]
    assert calendar_quality(rows, 2000, 10000)["state"] == "OBSERVED_SCOPE_AVAILABLE"
    assert calendar_quality(rows, 3000, 10000)["blockers"] == ["SOURCE_UNAVAILABLE"]


def test_old_dates_not_reused_after_removal_and_recovery_requires_new_snapshot():
    old, ok = event(), health()
    missing = event(
        received=2000, payload={"releases": [{"title": "Other", "scheduled_at_ms": 9000}]}
    )
    rows = [old, ok, missing, health(2001)]
    assert calendar_quality(rows, 1999, 10000)["state"] == "OBSERVED_SCOPE_AVAILABLE"
    assert calendar_quality(rows, 2001, 10000)["blockers"] == [
        "REQUIRED_SERIES_WITHOUT_FUTURE_DATE"
    ]
    rows += [event(received=3000), health(3001)]
    assert calendar_quality(rows, 3001, 10000)["state"] == "OBSERVED_SCOPE_AVAILABLE"


def test_malformed_payloads_and_duplicate_identical_health():
    bad_health = health().model_copy(update={"payload_json": "["})
    assert (
        "INVALID_HEALTH_PAYLOAD" in calendar_quality([event(), bad_health], 2000, 1000)["blockers"]
    )
    bad_snapshot = event().model_copy(update={"payload_json": "["})
    assert (
        "INVALID_SNAPSHOT_PAYLOAD"
        in calendar_quality([bad_snapshot, health()], 2000, 1000)["blockers"]
    )
    assert (
        calendar_quality([event(), health(), health()], 2000, 1000)["state"]
        == "OBSERVED_SCOPE_AVAILABLE"
    )


def test_saved_public_calendar_requires_health_and_expires():
    root = Path(__file__).resolve().parents[1] / "evidence" / "bea-calendar-public-02"
    received = json.loads((root / "report.json").read_text())["observed_at_ms"]
    calendar = parse_calendar((root / "response.json").read_bytes(), received)
    assert calendar_quality([calendar], received, 7200000)["blockers"] == ["MISSING_HEALTH"]
    simulated_health = health(received)
    result = calendar_quality([calendar, simulated_health], received, 7200000)
    assert result["state"] == "OBSERVED_SCOPE_AVAILABLE"
    assert result["next_scheduled_at_ms"]["Personal Income and Outlays"] == 1790771400000
    assert calendar_quality([calendar, simulated_health], received + 7200001, 7200000)[
        "blockers"
    ] == ["STALE_HEALTH", "STALE_SNAPSHOT"]
