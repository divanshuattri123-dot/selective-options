import asyncio
from unittest.mock import AsyncMock
from xml.etree.ElementTree import ParseError

import aiohttp
import pytest
from defusedxml.common import DTDForbidden

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.news import NewsPollHealth, PolicyNewsCollector, parse_feed
from selective_options.news_quality import policy_news_quality
from selective_options.quality import FeedRequirement, FreshnessMonitor
from selective_options.store import DuckStore

OBSERVED = 1735689600000


def feed(
    title="Policy statement",
    date="Mon, 01 Jan 2024 12:00:00 GMT",
    url="https://www.federalreserve.gov/example.htm",
):
    return f'<rss version="2.0"><channel><item><title>{title}</title><link>{url}</link><guid>stable-story-id</guid><pubDate>{date}</pubDate></item></channel></rss>'.encode()


async def test_news_first_observation_dedup_and_revision_pit(tmp_path):
    rows, rejected = parse_feed(feed(), "federal_reserve", OBSERVED)
    assert len(rows) == 1 and rejected == 0
    assert rows[0].payload()["classification"] == "NOT_CLASSIFIED"
    assert rows[0].payload()["first_publication_independently_verified"] is False
    store = await DuckStore.open(tmp_path / "news.duckdb")
    await store.append(rows)
    repeated, _ = parse_feed(feed(), "federal_reserve", OBSERVED + 1000)
    await store.append(repeated)
    assert not (await store.snapshot("GLOBAL", OBSERVED - 1, "public_reference")).rows()
    assert len((await store.snapshot("GLOBAL", OBSERVED, "public_reference")).rows()) == 1
    revised, _ = parse_feed(
        feed(title="Corrected policy statement"), "federal_reserve", OBSERVED + 2000
    )
    await store.append(revised)
    assert len((await store.snapshot("GLOBAL", OBSERVED + 1000, "public_reference")).rows()) == 1
    assert len((await store.snapshot("GLOBAL", OBSERVED + 2000, "public_reference")).rows()) == 2
    await store.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"date": ""},
        {"date": "Mon, 01 Jan 2024 12:00:00"},
        {"date": "Thu, 01 Jan 2099 12:00:00 GMT"},
        {"url": "http://example.com"},
        {"url": "https://name:secret@example.com"},
        {"title": ""},
    ],
)
def test_invalid_publication_metadata_not_fabricated(kwargs):
    rows, rejected = parse_feed(feed(**kwargs), "federal_reserve", OBSERVED)
    assert rows == [] and rejected == 1


def test_feed_security_size_shape_and_duplicate_items():
    with pytest.raises(DTDForbidden):
        parse_feed(
            b'<!DOCTYPE rss [<!ENTITY x "unsafe">]><rss>&x;</rss>', "federal_reserve", OBSERVED
        )
    with pytest.raises(IntegrityError, match="TOO_LARGE"):
        parse_feed(b" " * 1048577, "ecb", OBSERVED)
    with pytest.raises(IntegrityError, match="RSS"):
        parse_feed(b"<html/>", "ecb", OBSERVED)
    body = feed().replace(
        b"</channel>", feed().split(b"<channel>")[1].split(b"</channel>")[0] + b"</channel>"
    )
    assert len(parse_feed(body, "federal_reserve", OBSERVED)[0]) == 1


async def test_poll_streamed_size_bound_and_health(monkeypatch):
    class Content:
        async def iter_chunked(self, size):
            yield feed()

    class Response:
        content = Content()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def raise_for_status(self):
            pass

    class Session:
        def get(self, url):
            return Response()

    monkeypatch.setattr("selective_options.news.now_ms", lambda: OBSERVED)
    sink = AsyncMock()
    collector = PolicyNewsCollector(AppConfig(), sink, "federal_reserve")
    await collector.poll(Session())
    rows = [c.args[0] for c in sink.await_args_list]
    assert [r.kind for r in rows] == ["news", "feed_health"]
    health = NewsPollHealth.model_validate(rows[-1].payload())
    assert health.accepted_event_ids == (rows[0].event_id,)
    assert health.accepted_items == 1 and health.response_received_at_ms == OBSERVED
    assert (
        policy_news_quality(rows, source="federal_reserve", as_of_ms=OBSERVED)["state"]
        == "POLL_OBSERVED_VERIFIED"
    )
    monitor = FreshnessMonitor(5000)
    for row in rows:
        monitor.observe(row)
    requirement = [FeedRequirement("federal_reserve", "GLOBAL", "feed_health", 600000)]
    assert monitor.check(requirement, OBSERVED)[0]["state"] == "FRESH"
    await collector._health(False, error_type="network")
    monitor.observe(sink.await_args.args[0].model_copy(update={"observed_at_ms": OBSERVED + 1}))
    assert monitor.check(requirement, OBSERVED + 1)[0]["state"] == "UNAVAILABLE_OR_DEGRADED"


async def test_unavailable_source_retries_without_disabling_other_collectors(monkeypatch):
    attempted = asyncio.Event()

    async def fail(session):
        attempted.set()
        raise aiohttp.ClientConnectionError("fixture")

    sink = AsyncMock()
    collector = PolicyNewsCollector(AppConfig(), sink, "ecb")
    monkeypatch.setattr(collector, "poll", fail)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(attempted.wait(), 2)
        await asyncio.sleep(0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert sink.await_args.args[0].payload()["ok"] is False


@pytest.mark.parametrize(
    "body", [b"<rss/>", b"<rss><channel/><channel/></rss>", b"<html><channel/></html>"]
)
def test_invalid_feed_shape_cannot_be_empty_success(body):
    with pytest.raises(IntegrityError, match="RSS"):
        parse_feed(body, "federal_reserve", OBSERVED)


def test_item_budget_and_empty_real_channel():
    with pytest.raises(IntegrityError, match="ITEM_BUDGET"):
        parse_feed(b"<rss><channel>" + b"<item/>" * 10001 + b"</channel></rss>", "ecb", OBSERVED)
    assert parse_feed(b"<rss><channel/></rss>", "ecb", OBSERVED) == ([], 0)


@pytest.mark.parametrize(
    "failure",
    [
        IntegrityError("FixtureIntegrity"),
        ValueError("FixtureParse"),
        ParseError("FixtureXML"),
        DTDForbidden("rss", None, None),
    ],
)
async def test_invalid_poll_records_failed_health_and_retries(monkeypatch, failure):
    attempted = asyncio.Event()

    async def fail(session):
        attempted.set()
        raise failure

    sink = AsyncMock()
    collector = PolicyNewsCollector(AppConfig(), sink, "federal_reserve")
    monkeypatch.setattr(collector, "poll", fail)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(attempted.wait(), 2)
        await asyncio.sleep(0)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    status = NewsPollHealth.model_validate(sink.await_args.args[0].payload())
    assert status.ok is False and status.error_type == type(failure).__name__
    assert status.accepted_event_ids == () and status.response_received_at_ms is None
