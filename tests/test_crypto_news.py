import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import aiohttp
import pytest
from defusedxml.common import DefusedXmlException

from selective_options.config import AppConfig
from selective_options.crypto_news import (
    FEED_URL,
    LEGACY_FEED_URL,
    MAX_BYTES,
    MAX_ITEMS,
    CryptoNewsCollector,
    crypto_news_quality,
    news_poll_health,
    parse_feed,
)
from selective_options.errors import IntegrityError

RECEIVED = int(datetime(2026, 10, 4, 12, tzinfo=UTC).timestamp() * 1000)
POLLED = RECEIVED + 100
ITEM = "<item><title>  BTC &amp; ETH news  </title><link>https://www.coindesk.com/markets/story</link><guid>  native-id  </guid><pubDate>Sun, 04 Oct 2026 10:30:00 GMT</pubDate></item>"


def feed(items=ITEM):
    return f'<?xml version="1.0"?><rss version="2.0"><channel>{items}</channel></rss>'.encode()


def row():
    return parse_feed(feed(), RECEIVED)[0][0]


def healthy(news=None, received=RECEIVED, polled=POLLED):
    news = [row()] if news is None else news
    return news_poll_health(
        polled,
        accepted_event_ids=tuple(item.event_id for item in news),
        response_received_at_ms=received,
        http_status=200,
    )


def quality(events, **kwargs):
    return crypto_news_quality(events, **{"as_of_ms": POLLED, **kwargs})


def mutate(event, **payload):
    return event.model_copy(update={"payload_json": json.dumps({**event.payload(), **payload})})


def test_native_text_claims_and_revision_identity_are_preserved():
    item = row()
    body = item.payload()
    assert body["native_title"] == "  BTC & ETH news  "
    assert body["title"] == "BTC & ETH news"
    assert body["native_guid"] == "  native-id  "
    assert body["publisher_item_id"] == "native-id"
    assert body["published_at"] == "2026-10-04T10:30:00+00:00"
    assert body["publication_time_claim"] == "publisher_RSS_pubDate"
    assert body["first_publication_independently_verified"] is False
    assert body["classification"] == "NOT_CLASSIFIED"
    assert item.timestamp_kind == "source" and item.observed_at_ms == RECEIVED
    later = parse_feed(feed(), RECEIVED + 200)[0][0]
    assert later.event_id == item.event_id and later.observed_at_ms != item.observed_at_ms
    revision = parse_feed(feed(ITEM.replace("BTC &amp; ETH news", "Revised story")), RECEIVED)[0][0]
    assert revision.event_id != item.event_id
    assert quality([item, healthy()])["state"] == "POLL_OBSERVED_VERIFIED"


def test_empty_duplicate_guid_fallback_and_unknown_fields():
    assert parse_feed(feed(""), RECEIVED) == ([], 0)
    assert len(parse_feed(feed(ITEM * 2), RECEIVED)[0]) == 1
    missing_guid = ITEM.replace("<guid>  native-id  </guid>", "<description>ignored</description>")
    result = parse_feed(feed(missing_guid), RECEIVED)[0][0].payload()
    assert result["native_guid"] == "" and result["publisher_item_id"] == result["url"]
    empty = quality([healthy([])])
    assert empty["state"] == "POLL_OBSERVED_VERIFIED" and empty["headline_state"] == "EMPTY_POLL"
    assert empty["trading_approval"] is False and empty["all_news_complete"] is False


@pytest.mark.parametrize(
    "altered",
    [
        ITEM.replace("https://www.coindesk.com", "http://www.coindesk.com"),
        ITEM.replace("https://www.coindesk.com", "https://evil.test"),
        ITEM.replace("https://www.coindesk.com", "https://coindesk.com.evil.test"),
        ITEM.replace("https://www.coindesk.com", "https://user@www.coindesk.com"),
        ITEM.replace("https://www.coindesk.com", "https://www.coindesk.com:123"),
        ITEM.replace("Sun, 04 Oct 2026 10:30:00 GMT", "Sun, 04 Oct 2026 13:30:00 GMT"),
        ITEM.replace("Sun, 04 Oct 2026 10:30:00 GMT", "Sun, 04 Oct 2026 10:30:00"),
        ITEM.replace("Sun, 04 Oct 2026 10:30:00 GMT", "unparseable"),
        ITEM.replace("  BTC &amp; ETH news  ", " "),
        ITEM.replace("</title>", "</title><title>duplicate</title>"),
        ITEM.replace("</link>", "</link><link>https://coindesk.com/other</link>"),
        ITEM.replace("  BTC &amp; ETH news  ", "a" * 2001),
    ],
)
def test_malformed_metadata_rejected_and_partial_poll_blocks(altered):
    rows, rejected = parse_feed(feed(ITEM + altered), RECEIVED)
    assert len(rows) == 1 and rejected == 1
    poll = news_poll_health(
        POLLED,
        accepted_event_ids=(rows[0].event_id,),
        rejected_items=1,
        response_received_at_ms=RECEIVED,
        http_status=200,
    )
    assert quality([*rows, poll])["blocker"] == "CRYPTO_NEWS_FEED_UNAVAILABLE"


@pytest.mark.parametrize(
    "raw",
    [b"<html/>", b"<rss/>", b"<rss><channel/><channel/></rss>", b"x" * (MAX_BYTES + 1)],
    ids=["html", "no-channel", "two-channels", "oversize"],
)
def test_whole_entity_shape_and_byte_limits(raw):
    with pytest.raises(IntegrityError):
        parse_feed(raw, RECEIVED)


def test_entity_dtd_and_item_budget_are_refused():
    with pytest.raises(DefusedXmlException):
        parse_feed(
            b'<!DOCTYPE rss [<!ENTITY x "hello">]><rss><channel>&x;</channel></rss>', RECEIVED
        )
    with pytest.raises(IntegrityError, match="ITEM_BUDGET"):
        parse_feed(feed(ITEM * (MAX_ITEMS + 1)), RECEIVED)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"observed_at_ms": True},
        {"observed_at_ms": 0},
        {"response_received_at_ms": POLLED + 1},
        {"http_status": 302},
        {"accepted_event_ids": ("a",)},
        {"accepted_event_ids": ("a" * 64, "a" * 64)},
        {"response_received_at_ms": True},
        {"rejected_items": True},
        {"error_type": ""},
    ],
)
def test_health_strict_policy(kwargs):
    values = dict(observed_at_ms=POLLED, response_received_at_ms=RECEIVED, http_status=200)
    with pytest.raises((IntegrityError, ValueError)):
        news_poll_health(**{**values, **kwargs})


def test_failure_recovery_receipt_boundary_stale_and_publication_age():
    item, poll = row(), healthy()
    failed = news_poll_health(POLLED + 1, error_type="TimeoutError")
    recovered = healthy(received=POLLED + 2, polled=POLLED + 3)
    rows = [item, poll, failed, recovered]
    assert quality(rows, as_of_ms=RECEIVED - 1)["historical_metadata_items"] == 0
    assert quality(rows, as_of_ms=RECEIVED)["blocker"] == "CRYPTO_NEWS_HEALTH_MISSING"
    assert quality(rows)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality(rows, as_of_ms=POLLED + 1)["blocker"] == "CRYPTO_NEWS_FEED_UNAVAILABLE"
    assert quality(rows, as_of_ms=POLLED + 3)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([item, poll], as_of_ms=RECEIVED + 600001)["blocker"] == "CRYPTO_NEWS_POLL_STALE"
    assert quality([item, poll], max_publication_age_ms=1)["headline_state"] == "PUBLICATION_STALE"


def test_identity_binding_canonical_conflict_and_latest_tie_fail_closed():
    item, poll = row(), healthy()
    assert quality([poll])["blocker"] == "CRYPTO_NEWS_REFERENCED_METADATA_MISSING"
    assert (
        quality([item, poll.model_copy(update={"event_id": "b" * 64})])["blocker"]
        == "CRYPTO_NEWS_HEALTH_CANONICAL"
    )
    assert (
        quality([item.model_copy(update={"timestamp_kind": "receipt"}), poll])["blocker"]
        == "CRYPTO_NEWS_METADATA_CANONICAL"
    )
    assert (
        quality([mutate(item, title="changed"), poll])["blocker"]
        == "CRYPTO_NEWS_METADATA_CANONICAL"
    )
    assert (
        quality([mutate(item, first_publication_independently_verified=0), poll])["state"]
        == "BLOCKED"
    )
    assert (
        quality([mutate(poll, accepted_items=0), item])["blocker"] == "CRYPTO_NEWS_HEALTH_CANONICAL"
    )
    assert (
        quality([item, item.model_copy(update={"observed_at_ms": RECEIVED - 1}), poll])["blocker"]
        == "CRYPTO_NEWS_EVENT_ID_CONFLICT"
    )
    tie = news_poll_health(POLLED, error_type="TimeoutError")
    assert quality([item, poll, tie])["blocker"] == "CRYPTO_NEWS_HEALTH_AMBIGUOUS"
    assert quality([item, item, poll, poll])["state"] == "POLL_OBSERVED_VERIFIED"
    delayed = item.model_copy(update={"observed_at_ms": POLLED})
    assert quality([delayed, poll])["blocker"] == "CRYPTO_NEWS_METADATA_AFTER_POLL_RESPONSE"


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "other"),
        ("asset", "BTC"),
        ("symbol", "POLICY_NEWS"),
        ("environment", "testnet"),
        ("kind", "index"),
    ],
)
def test_exact_scope_and_budget(field, value):
    item, poll = row(), healthy()
    noise = poll.model_copy(update={field: value})
    assert quality([noise] * 1000 + [item, poll], max_rows=2)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([item, poll], max_rows=1)["blocker"] == "CRYPTO_NEWS_QUALITY_OVER_BUDGET"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_poll_age_ms": 0},
        {"max_publication_age_ms": True},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
def test_quality_invalid_policy_preflights(kwargs):
    def never_read():
        raise AssertionError("invalid policy read iterator")
        yield

    with pytest.raises(ValueError):
        quality(never_read(), **kwargs)


class Content:
    def __init__(self, body):
        self.body = body

    async def iter_chunked(self, size):
        assert size == 65536
        yield self.body


class Response:
    def __init__(self, body, status):
        self.content, self.status = Content(body), status
        self.headers = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Session:
    def __init__(self, body=None, status=200, error=None):
        self.body, self.status, self.error = feed() if body is None else body, status, error
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return Response(self.body, self.status)


async def test_collector_finite_fixed_request_binds_after_sink_and_preserves_entity_hook(
    monkeypatch,
):
    times = iter([RECEIVED, POLLED])
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: next(times))
    sink = AsyncMock()
    collector = CryptoNewsCollector(AppConfig(), sink)
    collector.response_observed = AsyncMock()
    session = Session()
    await collector.poll(session)
    rows = [call.args[0] for call in sink.await_args_list]
    assert [row.kind for row in rows] == ["news", "feed_health"]
    assert rows[1].payload()["accepted_event_ids"] == [rows[0].event_id]
    assert quality(rows)["state"] == "POLL_OBSERVED_VERIFIED"
    collector.response_observed.assert_awaited_once_with(
        feed(), RECEIVED, 200, location_header=None
    )
    assert session.calls[0][0] == FEED_URL
    assert session.calls[0][1]["allow_redirects"] is False
    assert session.calls[0][1]["timeout"].total <= 30
    assert "ssl" not in session.calls[0][1]


@pytest.mark.parametrize(
    "session",
    [
        Session(status=302),
        Session(status=308),
        Session(status=403),
        Session(body=b"<html/>"),
        Session(body=b"x" * (MAX_BYTES + 1)),
        Session(error=aiohttp.ClientConnectionError("fixture")),
        Session(error=TimeoutError()),
    ],
)
async def test_http_transport_shape_limit_failures_record_failed_health(monkeypatch, session):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    sink = AsyncMock()
    await CryptoNewsCollector(AppConfig(), sink).poll(session)
    rows = [call.args[0] for call in sink.await_args_list]
    assert len(rows) == 1 and rows[0].kind == "feed_health"
    assert rows[0].payload()["ok"] is False
    assert quality(rows, as_of_ms=RECEIVED)["blocker"] == "CRYPTO_NEWS_FEED_UNAVAILABLE"


async def test_sink_failure_and_cancellation_cannot_claim_healthy_poll(monkeypatch):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    sink = AsyncMock(side_effect=OSError("fixture sink error"))
    with pytest.raises(OSError):
        await CryptoNewsCollector(AppConfig(), sink).poll(Session())
    assert sink.await_count == 1 and sink.await_args.args[0].kind == "news"
    sink = AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await CryptoNewsCollector(AppConfig(), sink).poll(Session(error=asyncio.CancelledError()))
    assert sink.await_count == 0


@pytest.mark.parametrize(
    "location", ["/arc/outboundfeeds/rss?outputType=xml", "https://evil.test/"]
)
async def test_redirect_header_hook_never_follows_target_or_parses_body(monkeypatch, location):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    sink = AsyncMock()
    collector = CryptoNewsCollector(AppConfig(), sink)
    collector.response_observed = AsyncMock()
    response = Response(feed(), 308)
    response.headers = {"Location": location}
    session = Session()
    session.get = lambda url, **kwargs: response
    parser = AsyncMock(side_effect=AssertionError("redirect body must not be parsed"))
    monkeypatch.setattr("selective_options.crypto_news.parse_feed", parser)
    await collector.poll(session)
    collector.response_observed.assert_awaited_once_with(
        feed(), RECEIVED, 308, location_header=location
    )
    parser.assert_not_called()
    assert sink.await_count == 1
    assert sink.await_args.args[0].payload()["http_status"] == 308
    assert quality([sink.await_args.args[0]], as_of_ms=RECEIVED)["state"] == "BLOCKED"


def test_fixed_canonical_endpoint_and_historical_redirect_health_survive_migration():
    from selective_options.events import make_event

    assert FEED_URL == "https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml"
    assert LEGACY_FEED_URL == "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml"
    old = news_poll_health(
        RECEIVED,
        response_received_at_ms=RECEIVED,
        http_status=308,
        error_type="IntegrityError",
        feed_url=LEGACY_FEED_URL,
    )
    saved_payload = old.payload()
    independent = make_event(
        source="coindesk_rss",
        environment="public_reference",
        asset="GLOBAL",
        symbol="CRYPTO_NEWS",
        kind="feed_health",
        event_time_ms=RECEIVED,
        observed_at_ms=RECEIVED,
        timestamp_kind="receipt",
        payload=saved_payload,
    )
    assert old == independent
    assert quality([old], as_of_ms=RECEIVED)["blocker"] == "CRYPTO_NEWS_FEED_UNAVAILABLE"
    new = row()
    assert new.payload()["feed_url"] == FEED_URL
    assert quality([old, new, healthy()])["state"] == "POLL_OBSERVED_VERIFIED"
    changed = mutate(new, feed_url=LEGACY_FEED_URL)
    assert quality([changed, healthy()])["blocker"] == "CRYPTO_NEWS_METADATA_CANONICAL"


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.test/rss",
        "http://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml",
        "https://www.coindesk.com/arc/outboundfeeds/rss?outputType=json",
        "https://www.coindesk.com:443/arc/outboundfeeds/rss?outputType=xml",
        "https://user@www.coindesk.com/arc/outboundfeeds/rss?outputType=xml",
    ],
)
def test_canonical_health_provenance_refuses_nonfixed_feed_urls(url):
    with pytest.raises(ValueError):
        news_poll_health(
            RECEIVED,
            response_received_at_ms=RECEIVED,
            http_status=308,
            error_type="IntegrityError",
            feed_url=url,
        )


async def test_invalid_location_header_is_failed_before_response_hook(monkeypatch):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    collector = CryptoNewsCollector(AppConfig(), AsyncMock())
    collector.response_observed = AsyncMock()
    response = Response(feed(), 200)
    response.headers = {"Location": 123}
    session = Session()
    session.get = lambda url, **kwargs: response
    await collector.poll(session)
    collector.response_observed.assert_not_called()
    assert collector.sink.await_args.args[0].payload()["ok"] is False


@pytest.mark.parametrize(
    "link",
    [
        "https://@www.coindesk.com/markets/story",
        "https://:@www.coindesk.com/markets/story",
        "https://www.coin\ndesk.com/markets/story",
        "https://www.coindesk.com/markets/sto\try",
        "https://www.coindesk.com/markets/story\x7f",
        "https://www.coindesk.com:invalid/markets/story",
    ],
)
def test_native_link_empty_auth_and_control_characters_are_refused(link):
    rows, rejected = parse_feed(
        feed(ITEM.replace("https://www.coindesk.com/markets/story", link)), RECEIVED
    )
    assert rows == [] and rejected == 1


@pytest.mark.parametrize("metadata_url", [LEGACY_FEED_URL, FEED_URL])
@pytest.mark.parametrize("poll_url", [LEGACY_FEED_URL, FEED_URL])
def test_health_binding_requires_exact_metadata_feed_provenance(metadata_url, poll_url):
    from selective_options.events import make_event

    current = row()
    payload = {**current.payload(), "feed_url": metadata_url}
    canonical = make_event(
        source=current.source,
        environment=current.environment,
        asset=current.asset,
        symbol=current.symbol,
        kind=current.kind,
        event_time_ms=current.event_time_ms,
        observed_at_ms=current.observed_at_ms,
        timestamp_kind=current.timestamp_kind,
        native_id=payload["publisher_item_id"],
        payload=payload,
    )
    poll = news_poll_health(
        POLLED,
        accepted_event_ids=(canonical.event_id,),
        response_received_at_ms=RECEIVED,
        http_status=200,
        feed_url=poll_url,
    )
    result = quality([canonical, poll])
    if metadata_url == poll_url:
        assert result["state"] == "POLL_OBSERVED_VERIFIED"
    else:
        assert result["blocker"] == "CRYPTO_NEWS_POLL_PROVENANCE_MISMATCH"
