"""Public policy RSS metadata only; no sentiment or trading decisions inferred."""

import asyncio
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal
from urllib.parse import urlsplit
from xml.etree.ElementTree import Element, ParseError

import aiohttp
from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

NewsSource = Literal["federal_reserve", "ecb"]
FEEDS: dict[NewsSource, str] = {
    "federal_reserve": "https://www.federalreserve.gov/feeds/press_monetary.xml",
    "ecb": "https://www.ecb.europa.eu/rss/press.html",
}


class NewsMetadata(ExternalModel):
    title: str = Field(min_length=1, max_length=2000)
    url: str = Field(min_length=1, max_length=4000)
    publisher_item_id: str = Field(min_length=1, max_length=4000)
    published_at: datetime


class NewsPollHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    accepted_items: StrictInt = Field(ge=0, le=10_000)
    rejected_items: StrictInt = Field(ge=0, le=10_000)
    accepted_event_ids: tuple[StrictStr, ...] = Field(max_length=10_000)
    response_received_at_ms: StrictInt | None
    error_type: StrictStr | None = Field(max_length=256)
    feed_url: StrictStr
    scope: Literal["policy RSS metadata; sink accepted, no atomic commit claim"]


def news_poll_health(
    source: NewsSource,
    observed_at_ms: int,
    *,
    accepted_event_ids: tuple[str, ...] = (),
    rejected_items: int = 0,
    response_received_at_ms: int | None = None,
    error_type: str | None = None,
) -> MarketEvent:
    """Bind one poll outcome to exact accepted metadata identities, including repeats."""
    body = NewsPollHealth(
        ok=error_type is None and rejected_items == 0,
        accepted_items=len(accepted_event_ids),
        rejected_items=rejected_items,
        accepted_event_ids=accepted_event_ids,
        response_received_at_ms=response_received_at_ms,
        error_type=error_type,
        feed_url=FEEDS[source],
        scope="policy RSS metadata; sink accepted, no atomic commit claim",
    )
    if (
        type(observed_at_ms) is not int
        or observed_at_ms <= 0
        or len(set(accepted_event_ids)) != len(accepted_event_ids)
        or any(
            len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity)
            for identity in accepted_event_ids
        )
        or (
            response_received_at_ms is not None
            and not 0 < response_received_at_ms <= observed_at_ms
        )
        or (body.ok and response_received_at_ms is None)
        or (response_received_at_ms is None and (accepted_event_ids or rejected_items))
        or (error_type is not None and not error_type)
    ):
        raise IntegrityError("NEWS_POLL_HEALTH_INVALID")
    return make_event(
        source=source,
        environment="public_reference",
        asset="GLOBAL",
        symbol="POLICY_NEWS",
        kind="feed_health",
        event_time_ms=observed_at_ms,
        observed_at_ms=observed_at_ms,
        timestamp_kind="receipt",
        payload=body.model_dump(mode="json"),
    )


def child_text(element: Element, name: str) -> str:
    for child in element:
        if child.tag.rsplit("}", 1)[-1] == name:
            return "".join(child.itertext()).strip()
    return ""


def parse_feed(
    body: bytes, source: NewsSource, observed_at_ms: int
) -> tuple[list[MarketEvent], int]:
    if len(body) > 1_048_576:
        raise IntegrityError("NEWS_FEED_TOO_LARGE")
    root = fromstring(body, forbid_dtd=True, forbid_entities=True, forbid_external=True)
    channels = [child for child in root if child.tag.rsplit("}", 1)[-1] == "channel"]
    if root.tag.rsplit("}", 1)[-1] != "rss" or len(channels) != 1:
        raise IntegrityError("expected an RSS feed with exactly one channel")
    events: list[MarketEvent] = []
    rejected = 0
    seen: set[str] = set()
    item_count = 0
    for item in channels[0]:
        if item.tag.rsplit("}", 1)[-1] != "item":
            continue
        item_count += 1
        if item_count > 10_000:
            raise IntegrityError("NEWS_FEED_ITEM_BUDGET")
        title, url, date = (
            child_text(item, "title"),
            child_text(item, "link"),
            child_text(item, "pubDate"),
        )
        try:
            published = parsedate_to_datetime(date)
            if published.tzinfo is None or published.utcoffset() is None:
                raise ValueError("missing timezone")
            parsed = NewsMetadata(
                title=title,
                url=url,
                publisher_item_id=child_text(item, "guid") or url,
                published_at=published.astimezone(UTC),
            )
            parts = urlsplit(parsed.url)
            if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
                raise ValueError("invalid article URL")
            timestamp = int(parsed.published_at.timestamp() * 1000)
            if not 0 < timestamp <= observed_at_ms:
                raise ValueError("future or invalid publication timestamp")
        except (ValueError, TypeError):
            rejected += 1
            continue
        event = make_event(
            source=source,
            environment="public_reference",
            asset="GLOBAL",
            symbol="POLICY_NEWS",
            kind="news",
            event_time_ms=timestamp,
            observed_at_ms=observed_at_ms,
            native_id=parsed.publisher_item_id,
            payload={
                "title": parsed.title,
                "url": parsed.url,
                "publisher_item_id": parsed.publisher_item_id,
                "published_at": parsed.published_at.isoformat(),
                "publication_time_claim": "publisher_RSS_pubDate",
                "first_publication_independently_verified": False,
                "classification": "NOT_CLASSIFIED",
                "source_category": "official_monetary_policy",
                "feed_url": FEEDS[source],
            },
        )
        if event.event_id not in seen:
            events.append(event)
            seen.add(event.event_id)
    return events, rejected


class PolicyNewsCollector:
    def __init__(self, config: AppConfig, sink: Sink, source: NewsSource) -> None:
        self.config, self.sink, self.source = config, sink, source

    async def _health(self, ok: bool, *, error_type: str) -> None:
        if ok:
            raise IntegrityError("NEWS_SUCCESS_REQUIRES_BOUND_POLL")
        await self.sink(news_poll_health(self.source, now_ms(), error_type=error_type))

    async def poll(self, session: aiohttp.ClientSession) -> None:
        async with session.get(FEEDS[self.source]) as response:
            response.raise_for_status()
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > 1_048_576:
                    raise IntegrityError("NEWS_FEED_TOO_LARGE")
        received = now_ms()
        rows, rejected = parse_feed(bytes(body), self.source, received)
        for event in rows:
            await self.sink(event)
        await self.sink(
            news_poll_health(
                self.source,
                now_ms(),
                accepted_event_ids=tuple(row.event_id for row in rows),
                rejected_items=rejected,
                response_received_at_ms=received,
            )
        )

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.venue.request_timeout_s),
            headers={"User-Agent": "SelectiveOptionsResearch/0.3 (public RSS metadata)"},
        ) as session:
            while True:
                try:
                    await self.poll(session)
                except (
                    aiohttp.ClientError,
                    TimeoutError,
                    IntegrityError,
                    ParseError,
                    DefusedXmlException,
                    ValueError,
                ) as exc:
                    await self._health(False, error_type=type(exc).__name__)
                await asyncio.sleep(self.config.ingestion.news_poll_interval_s)
