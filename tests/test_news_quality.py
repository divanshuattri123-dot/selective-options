import json

import duckdb
import pytest

from selective_options.events import make_event
from selective_options.news import FEEDS, news_poll_health, parse_feed
from selective_options.news_quality import policy_news_quality
from selective_options.store import DuckStore

PUBLISHED = 1704110400000
RECEIVED = PUBLISHED + 1000
POLLED = RECEIVED + 50


def headline(received=RECEIVED, source="federal_reserve", title="Policy statement"):
    body = f"<rss><channel><item><title>{title}</title><link>https://www.federalreserve.gov/example.htm</link><guid>stable-story-id</guid><pubDate>Mon, 01 Jan 2024 12:00:00 GMT</pubDate></item></channel></rss>".encode()
    return parse_feed(body, source, received)[0][0]


def successful(row=None, polled=POLLED, response=RECEIVED, source="federal_reserve"):
    return news_poll_health(
        source,
        polled,
        accepted_event_ids=(row.event_id,) if row else (),
        response_received_at_ms=response,
    )


def quality(rows, as_of=POLLED, **kwargs):
    return policy_news_quality(rows, source="federal_reserve", as_of_ms=as_of, **kwargs)


def mutate_payload(row, **updates):
    return row.model_copy(update={"payload_json": json.dumps({**row.payload(), **updates})})


def test_exact_boundaries_poll_receipt_publication_and_repeat_first_observation():
    row = headline()
    poll = successful(row)
    assert quality([row, poll], RECEIVED - 1)["blocker"] == "NEWS_HEALTH_MISSING"
    before_poll = quality([row, poll], POLLED - 1)
    assert before_poll["historical_metadata_items"] == 1
    assert before_poll["blocker"] == "NEWS_HEALTH_MISSING"
    result = quality([row, poll])
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["headline_state"] == "PUBLICATION_RECENT"
    assert result["latest_publication_time_ms"] == PUBLISHED
    assert result["first_observed_at_ms"] == [RECEIVED]
    repeated = successful(row, POLLED + 1000, RECEIVED + 1000)
    assert quality([row, poll, repeated], POLLED + 1000)["first_observed_at_ms"] == [RECEIVED]
    assert (
        quality([row, poll], POLLED + 100, max_poll_age_ms=150)["state"] == "POLL_OBSERVED_VERIFIED"
    )
    assert quality([row, poll], POLLED + 101, max_poll_age_ms=150)["blocker"] == "NEWS_POLL_STALE"
    assert (
        quality([row, poll], max_publication_age_ms=1050)["headline_state"] == "PUBLICATION_RECENT"
    )
    assert (
        quality([row, poll], max_publication_age_ms=1049)["headline_state"] == "PUBLICATION_STALE"
    )
    assert result["complete_feed_available"] is False
    assert result["all_news_complete"] is False
    assert result["trading_approval"] is False
    assert result["first_publication_independently_verified"] is False


def test_latest_failure_blocks_old_metadata_then_exact_recovery_boundary():
    row = headline()
    poll = successful(row)
    failure = news_poll_health("federal_reserve", POLLED + 100, error_type="FixtureOutage")
    recovery = successful(row, POLLED + 200, POLLED + 150)
    rows = [row, poll, failure, recovery]
    assert quality(rows, POLLED + 99)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality(rows, POLLED + 100)["blocker"] == "NEWS_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 199)["blocker"] == "NEWS_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 200)["health_event_id"] == recovery.event_id
    assert quality(rows, POLLED + 200)["state"] == "POLL_OBSERVED_VERIFIED"


def test_empty_success_has_no_headline_coverage_and_old_headlines_are_not_poll_evidence():
    row = headline()
    result = quality([row, successful()])
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["headline_state"] == "EMPTY_POLL"
    assert result["accepted_items"] == 0
    assert result["accepted_event_ids"] == []
    assert result["latest_publication_time_ms"] is None
    assert result["historical_metadata_items"] == 1
    assert quality([row])["blocker"] == "NEWS_HEALTH_MISSING"


def test_legacy_health_is_unbound_and_legacy_failure_still_blocks():
    row = headline()
    legacy = make_event(
        source="federal_reserve",
        environment="public_reference",
        asset="GLOBAL",
        symbol="POLICY_NEWS",
        kind="feed_health",
        event_time_ms=POLLED,
        observed_at_ms=POLLED,
        timestamp_kind="receipt",
        payload={"ok": True, "accepted_items": 1, "rejected_items": 0},
    )
    result = quality([row, legacy])
    assert result["blocker"] == "NEWS_HEALTH_UNBOUND"
    assert result["legacy_health_rows"] == 1
    failed = make_event(
        source="federal_reserve",
        environment="public_reference",
        asset="GLOBAL",
        symbol="POLICY_NEWS",
        kind="feed_health",
        event_time_ms=POLLED + 1,
        observed_at_ms=POLLED + 1,
        timestamp_kind="receipt",
        payload={"ok": False, "error_type": "FixtureFailure"},
    )
    assert quality([row, legacy, failed], POLLED + 1)["blocker"] == "NEWS_FEED_UNAVAILABLE"


@pytest.mark.parametrize(
    "change",
    [
        {"accepted_items": 2},
        {"ok": 1},
        {"ok": False},
        {"feed_url": FEEDS["ecb"]},
        {"scope": "complete news"},
        {"extra": 1},
        {"response_received_at_ms": True},
        {"response_received_at_ms": POLLED + 1},
        {"accepted_event_ids": ["bad"]},
        {"accepted_event_ids": []},
        {"error_type": ""},
    ],
)
def test_forged_health_fails_closed(change):
    row = headline()
    assert quality([row, mutate_payload(successful(row), **change)])["state"] == "BLOCKED"


@pytest.mark.parametrize(
    "change",
    [
        {"classification": "bullish"},
        {"feed_url": FEEDS["ecb"]},
        {"first_publication_independently_verified": 0},
        {"url": "https://u:p@example.com"},
        {"published_at": "2024-01-01T12:00:00"},
        {"published_at": "2024-01-01T12:00:01+00:00"},
        {"title": "Replaced"},
        {"extra": 1},
    ],
)
def test_forged_metadata_fails_closed(change):
    row = headline()
    assert quality([mutate_payload(row, **change), successful(row)])["state"] == "BLOCKED"


def test_missing_references_partial_poll_and_metadata_after_response():
    row = headline()
    assert quality([successful(row)])["blocker"] == "NEWS_REFERENCED_METADATA_MISSING"
    partial = news_poll_health(
        "federal_reserve",
        POLLED,
        accepted_event_ids=(row.event_id,),
        rejected_items=1,
        response_received_at_ms=RECEIVED,
    )
    assert quality([row, partial])["blocker"] == "NEWS_FEED_UNAVAILABLE"
    late_row = headline(RECEIVED + 1)
    assert (
        quality([late_row, successful(late_row)])["blocker"] == "NEWS_METADATA_AFTER_POLL_RESPONSE"
    )


def test_ties_conflicts_duplicate_identity_and_clock_integrity():
    row = headline()
    poll = successful(row)
    failure = news_poll_health("federal_reserve", POLLED, error_type="FixtureFailure")
    assert quality([row, poll, failure])["blocker"] == "NEWS_HEALTH_AMBIGUOUS"
    assert quality([row, poll, poll])["state"] == "POLL_OBSERVED_VERIFIED"
    assert (
        quality([row, poll, poll.model_copy(update={"observed_at_ms": POLLED - 1})])["blocker"]
        == "NEWS_EVENT_ID_CONFLICT"
    )
    assert (
        quality([row, poll.model_copy(update={"event_time_ms": POLLED - 1})])["blocker"]
        == "NEWS_HEALTH_CANONICAL"
    )
    assert (
        quality([row.model_copy(update={"timestamp_kind": "receipt"}), poll])["blocker"]
        == "NEWS_METADATA_CANONICAL"
    )
    assert (
        quality([row, row.model_copy(update={"observed_at_ms": RECEIVED + 1}), poll])["blocker"]
        == "NEWS_EVENT_ID_CONFLICT"
    )


def test_both_clocks_future_health_and_news_other_scopes_cannot_satisfy():
    row = headline()
    poll = successful(row)
    for field in ("event_time_ms", "observed_at_ms"):
        assert (
            quality([row, poll.model_copy(update={field: POLLED + 1})])["blocker"]
            == "NEWS_HEALTH_MISSING"
        )
        assert (
            quality([row.model_copy(update={field: POLLED + 1}), poll])["blocker"]
            == "NEWS_REFERENCED_METADATA_MISSING"
        )
    for field, value in (
        ("source", "ecb"),
        ("asset", "BTC"),
        ("symbol", "OTHER"),
        ("environment", "testnet"),
    ):
        assert (
            quality([row, poll.model_copy(update={field: value})])["blocker"]
            == "NEWS_HEALTH_MISSING"
        )
    ecb_row = headline(source="ecb")
    ecb_poll = successful(ecb_row, source="ecb")
    assert (
        policy_news_quality([ecb_row, ecb_poll], source="ecb", as_of_ms=POLLED)["state"]
        == "POLL_OBSERVED_VERIFIED"
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_poll_age_ms": 0},
        {"max_publication_age_ms": 1.5},
        {"max_rows": True},
        {"max_rows": 100001},
        {"source": "other"},
    ],
)
def test_policy_arguments_strict(kwargs):
    with pytest.raises(ValueError):
        policy_news_quality([], **{"source": "federal_reserve", "as_of_ms": POLLED, **kwargs})


def test_row_budget_counts_duplicates_and_stops_bounded_eligible_reads():
    row = headline()
    assert quality([row, row], max_rows=1)["blocker"] == "NEWS_QUALITY_OVER_BUDGET"
    assert quality([row, successful(row)], max_rows=2)["state"] == "POLL_OBSERVED_VERIFIED"


async def test_actual_store_dedup_reopen_outbox_and_parquet_exact(tmp_path):
    row = headline()
    poll = successful(row)
    repeated = headline(RECEIVED + 100)
    repeat_poll = successful(repeated, POLLED + 100, RECEIVED + 100)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([row, poll])
        await store.append([repeated, repeat_poll])
        assert await store.pending_delivery_count() == 3
        before = await store.snapshot("GLOBAL", POLLED - 1, "public_reference")
        assert quality(before.rows(), POLLED - 1)["blocker"] == "NEWS_HEALTH_MISSING"
        at = await store.snapshot("GLOBAL", POLLED, "public_reference")
        assert quality(at.rows())["state"] == "POLL_OBSERVED_VERIFIED"
        await store.export_parquet(tmp_path / "events.parquet")
    finally:
        await store.close()
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        snapshot = await store.snapshot("GLOBAL", POLLED + 100, "public_reference")
        result = quality(snapshot.rows(), POLLED + 100)
        assert result["first_observed_at_ms"] == [RECEIVED]
        assert result["health_event_id"] == repeat_poll.event_id
        assert await store.pending_delivery_count() == 3
        connection = duckdb.connect()
        try:
            values = connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(tmp_path / "events.parquet")],
            ).fetchall()
        finally:
            connection.close()
        assert values == [
            tuple(event.model_dump().values())
            for event in sorted(snapshot.rows(), key=lambda event: event.event_id)
        ]
    finally:
        await store.close()
