"""One public CoinDesk RSS metadata poll; no sentiment, attribution or trading claim."""

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Final, Literal
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

SOURCE = "coindesk_rss"
SYMBOL = "CRYPTO_NEWS"
LEGACY_FEED_URL: Final = "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml"
FEED_URL: Final = "https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml"
FeedURL = Literal[
    "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=xml",
    "https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml",
]
MAX_BYTES = 2_097_152
MAX_ITEMS = 100
MAX_RETAINED_IDENTITIES = 100_000
REFERENCES = ("https://www.coindesk.com/coindesk-news/2021/09/17/coindesk-rss",)
SCOPE = "CoinDesk RSS metadata; sink accepted, no atomic commit claim"


class NewsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    title: StrictStr = Field(min_length=1, max_length=2000)
    url: StrictStr = Field(min_length=1, max_length=4000)
    publisher_item_id: StrictStr = Field(min_length=1, max_length=4000)
    published_at: StrictStr
    native_title: StrictStr = Field(max_length=4000)
    native_link: StrictStr = Field(max_length=8000)
    native_guid: StrictStr = Field(max_length=8000)
    native_pub_date: StrictStr = Field(max_length=256)
    publication_time_claim: Literal["publisher_RSS_pubDate"] = "publisher_RSS_pubDate"
    first_publication_independently_verified: Literal[False] = False
    classification: Literal["NOT_CLASSIFIED"] = "NOT_CLASSIFIED"
    source_category: Literal["crypto_news_publisher"] = "crypto_news_publisher"
    feed_url: FeedURL = FEED_URL


class NewsPollHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    accepted_items: StrictInt = Field(ge=0, le=MAX_ITEMS)
    rejected_items: StrictInt = Field(ge=0, le=MAX_ITEMS)
    accepted_event_ids: tuple[StrictStr, ...] = Field(max_length=MAX_ITEMS)
    response_received_at_ms: StrictInt | None
    http_status: StrictInt | None = Field(ge=100, le=599)
    error_type: StrictStr | None = Field(min_length=1, max_length=256)
    feed_url: FeedURL
    scope: StrictStr


def _event(
    kind: Literal["news", "feed_health"],
    source_ms: int,
    received_ms: int,
    payload: NewsPayload | NewsPollHealth,
    native_id: str | None = None,
) -> MarketEvent:
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind=kind,
        event_time_ms=source_ms,
        observed_at_ms=received_ms,
        timestamp_kind="source" if kind == "news" else "receipt",
        payload=payload.model_dump(mode="json"),
        native_id=native_id,
    )


def news_poll_health(
    observed_at_ms: int,
    *,
    accepted_event_ids: tuple[str, ...] = (),
    rejected_items: int = 0,
    response_received_at_ms: int | None = None,
    http_status: int | None = None,
    error_type: str | None = None,
    feed_url: FeedURL = FEED_URL,
) -> MarketEvent:
    body = NewsPollHealth(
        ok=error_type is None and rejected_items == 0,
        accepted_items=len(accepted_event_ids),
        rejected_items=rejected_items,
        accepted_event_ids=accepted_event_ids,
        response_received_at_ms=response_received_at_ms,
        http_status=http_status,
        error_type=error_type,
        feed_url=feed_url,
        scope=SCOPE,
    )
    if (
        type(observed_at_ms) is not int
        or observed_at_ms <= 0
        or len(set(accepted_event_ids)) != len(accepted_event_ids)
        or any(
            len(identity) != 64 or any(c not in "0123456789abcdef" for c in identity)
            for identity in accepted_event_ids
        )
        or body.accepted_items + body.rejected_items > MAX_ITEMS
        or (
            response_received_at_ms is not None
            and not 0 < response_received_at_ms <= observed_at_ms
        )
        or (body.ok and (response_received_at_ms is None or http_status != 200))
        or (response_received_at_ms is None and (accepted_event_ids or rejected_items))
        or ((accepted_event_ids or rejected_items) and http_status != 200)
    ):
        raise IntegrityError("CRYPTO_NEWS_HEALTH_INVALID")
    return _event("feed_health", observed_at_ms, observed_at_ms, body)


def _text(item: Element, name: str) -> str:
    children = [child for child in item if child.tag.rsplit("}", 1)[-1] == name]
    if len(children) > 1:
        raise IntegrityError("CRYPTO_NEWS_DUPLICATE_FIELD")
    return "".join(children[0].itertext()) if children else ""


def _metadata(
    title: str, link: str, guid: str, date: str, received_ms: int, *, feed_url: FeedURL = FEED_URL
) -> NewsPayload:
    published = parsedate_to_datetime(date.strip())
    parts = urlsplit(link.strip())
    if (
        published.tzinfo is None
        or published.utcoffset() is None
        or parts.scheme != "https"
        or parts.hostname not in ("coindesk.com", "www.coindesk.com")
        or parts.port not in (None, 443)
        or parts.username is not None
        or parts.password is not None
        or any(ord(character) < 32 or ord(character) == 127 for character in link.strip())
        or not 0 < int(published.timestamp() * 1000) <= received_ms
    ):
        raise IntegrityError("CRYPTO_NEWS_METADATA_CLOCK_OR_URL")
    return NewsPayload(
        title=title.strip(),
        url=link.strip(),
        publisher_item_id=guid.strip() or link.strip(),
        published_at=published.astimezone(UTC).isoformat(),
        native_title=title,
        native_link=link,
        native_guid=guid,
        native_pub_date=date,
        feed_url=feed_url,
    )


def parse_feed(body: bytes, received_ms: int) -> tuple[list[MarketEvent], int]:
    if type(received_ms) is not int or received_ms <= 0:
        raise ValueError("positive integer receipt required")
    if type(body) is not bytes or len(body) > MAX_BYTES:
        raise IntegrityError("CRYPTO_NEWS_FEED_TOO_LARGE")
    root = fromstring(body, forbid_dtd=True, forbid_entities=True, forbid_external=True)
    channels = [child for child in root if child.tag.rsplit("}", 1)[-1] == "channel"]
    if root.tag.rsplit("}", 1)[-1] != "rss" or len(channels) != 1:
        raise IntegrityError("CRYPTO_NEWS_RSS_SHAPE")
    items = [item for item in channels[0] if item.tag.rsplit("}", 1)[-1] == "item"]
    if len(items) > MAX_ITEMS:
        raise IntegrityError("CRYPTO_NEWS_ITEM_BUDGET")
    events: dict[str, MarketEvent] = {}
    rejected = 0
    for item in items:
        try:
            metadata = _metadata(
                _text(item, "title"),
                _text(item, "link"),
                _text(item, "guid"),
                _text(item, "pubDate"),
                received_ms,
            )
            event = _event(
                "news",
                int(datetime.fromisoformat(metadata.published_at).timestamp() * 1000),
                received_ms,
                metadata,
                metadata.publisher_item_id,
            )
            events[event.event_id] = event
        except (ValueError, TypeError, IntegrityError, OverflowError):
            rejected += 1
    return list(events.values()), rejected


def _news_valid(row: MarketEvent) -> None:
    body = NewsPayload.model_validate_json(row.payload_json)
    expected = _metadata(
        body.native_title,
        body.native_link,
        body.native_guid,
        body.native_pub_date,
        row.observed_at_ms,
        feed_url=body.feed_url,
    )
    source_ms = int(datetime.fromisoformat(expected.published_at).timestamp() * 1000)
    if (
        body != expected
        or type(row.payload()["first_publication_independently_verified"]) is not bool
        or _event("news", source_ms, row.observed_at_ms, expected, expected.publisher_item_id)
        != row
    ):
        raise IntegrityError("CRYPTO_NEWS_METADATA_CANONICAL")


def _health_valid(row: MarketEvent) -> NewsPollHealth:
    body = NewsPollHealth.model_validate_json(row.payload_json)
    expected = news_poll_health(
        row.observed_at_ms,
        accepted_event_ids=body.accepted_event_ids,
        rejected_items=body.rejected_items,
        response_received_at_ms=body.response_received_at_ms,
        http_status=body.http_status,
        error_type=body.error_type,
        feed_url=body.feed_url,
    )
    if expected != row:
        raise IntegrityError("CRYPTO_NEWS_HEALTH_CANONICAL")
    return body


def crypto_news_quality(
    events: Iterable[MarketEvent],
    *,
    as_of_ms: int,
    max_poll_age_ms: int = 600_000,
    max_publication_age_ms: int = 2_592_000_000,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Bound scoped eligible materialization; scanning/database resources remain unbounded."""
    if (
        any(
            type(value) is not int or value <= 0
            for value in (as_of_ms, max_poll_age_ms, max_publication_age_ms)
        )
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
    ):
        raise ValueError("invalid bounded crypto RSS quality policy")
    result: dict[str, object] = dict(
        source=SOURCE,
        asset="GLOBAL",
        symbol=SYMBOL,
        as_of_ms=as_of_ms,
        state="BLOCKED",
        blocker=None,
        health_event_id=None,
        health_received_at_ms=None,
        response_received_at_ms=None,
        poll_ok=None,
        error_type=None,
        http_status=None,
        accepted_event_ids=[],
        accepted_items=None,
        rejected_items=None,
        historical_metadata_items=0,
        headline_state="UNVERIFIED",
        latest_publication_time_ms=None,
        recent_publication_items=0,
        first_observed_at_ms=[],
        max_poll_age_ms=max_poll_age_ms,
        max_publication_age_ms=max_publication_age_ms,
        scope="One CoinDesk RSS metadata poll; publisher publication claim and local receipt only. No sentiment, coin direction or full coverage. Sink acceptance is not atomic commit proof.",
        complete_feed_available=False,
        all_news_complete=False,
        first_publication_independently_verified=False,
        trading_approval=False,
    )
    try:
        eligible: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                row.source != SOURCE
                or row.environment != "public_reference"
                or row.asset != "GLOBAL"
                or row.symbol != SYMBOL
                or row.kind not in ("news", "feed_health")
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("CRYPTO_NEWS_QUALITY_OVER_BUDGET")
            if row.event_id in eligible and eligible[row.event_id] != row:
                raise IntegrityError("CRYPTO_NEWS_EVENT_ID_CONFLICT")
            eligible[row.event_id] = row
        news: dict[str, MarketEvent] = {}
        health: list[tuple[MarketEvent, NewsPollHealth]] = []
        for row in eligible.values():
            if row.kind == "news":
                _news_valid(row)
                news[row.event_id] = row
            else:
                health.append((row, _health_valid(row)))
        result["historical_metadata_items"] = len(news)
        if not health:
            raise IntegrityError("CRYPTO_NEWS_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        choices = [(row, body) for row, body in health if row.observed_at_ms == latest]
        if len(choices) != 1:
            raise IntegrityError("CRYPTO_NEWS_HEALTH_AMBIGUOUS")
        selected, status = choices[0]
        result.update(
            health_event_id=selected.event_id,
            health_received_at_ms=latest,
            poll_ok=status.ok,
            error_type=status.error_type,
            http_status=status.http_status,
            response_received_at_ms=status.response_received_at_ms,
            accepted_items=status.accepted_items,
            rejected_items=status.rejected_items,
        )
        if not status.ok:
            raise IntegrityError("CRYPTO_NEWS_FEED_UNAVAILABLE")
        assert status.response_received_at_ms is not None
        if as_of_ms - min(latest, status.response_received_at_ms) > max_poll_age_ms:
            raise IntegrityError("CRYPTO_NEWS_POLL_STALE")
        referenced = []
        for identity in status.accepted_event_ids:
            if identity not in news:
                raise IntegrityError("CRYPTO_NEWS_REFERENCED_METADATA_MISSING")
            row = news[identity]
            if row.payload()["feed_url"] != status.feed_url:
                raise IntegrityError("CRYPTO_NEWS_POLL_PROVENANCE_MISMATCH")
            if row.observed_at_ms > status.response_received_at_ms:
                raise IntegrityError("CRYPTO_NEWS_METADATA_AFTER_POLL_RESPONSE")
            referenced.append(row)
        latest_publication = max((row.event_time_ms for row in referenced), default=None)
        recent = sum(as_of_ms - row.event_time_ms <= max_publication_age_ms for row in referenced)
        result.update(
            state="POLL_OBSERVED_VERIFIED",
            accepted_event_ids=list(status.accepted_event_ids),
            headline_state="EMPTY_POLL"
            if not referenced
            else "PUBLICATION_RECENT"
            if recent
            else "PUBLICATION_STALE",
            latest_publication_time_ms=latest_publication,
            recent_publication_items=recent,
            first_observed_at_ms=[row.observed_at_ms for row in referenced],
        )
    except (IntegrityError, ValueError, TypeError, KeyError, OverflowError) as exc:
        result["blocker"] = (
            str(exc) if isinstance(exc, IntegrityError) else "CRYPTO_NEWS_INVALID_EVIDENCE"
        )
    return result


class CryptoNewsCollector:
    def __init__(
        self,
        config: AppConfig,
        sink: Sink,
        *,
        initial_metadata: tuple[MarketEvent, ...] = (),
    ) -> None:
        self.config, self.sink = config, sink
        if type(initial_metadata) is not tuple or len(initial_metadata) > MAX_RETAINED_IDENTITIES:
            raise IntegrityError("CRYPTO_NEWS_INITIAL_METADATA_INVALID")
        self._metadata_by_event_id: dict[str, MarketEvent] = {}
        for row in initial_metadata:
            if (
                row.kind != "news"
                or row.source != SOURCE
                or row.environment != "public_reference"
                or row.asset != "GLOBAL"
                or row.symbol != SYMBOL
            ):
                raise IntegrityError("CRYPTO_NEWS_INITIAL_METADATA_SCOPE")
            _news_valid(row)
            prior = self._metadata_by_event_id.get(row.event_id)
            if prior is not None:
                raise IntegrityError("CRYPTO_NEWS_INITIAL_METADATA_DUPLICATE")
            self._metadata_by_event_id[row.event_id] = row

    def request(self, session: aiohttp.ClientSession) -> aiohttp.client._RequestContextManager:
        return session.get(
            FEED_URL,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=min(self.config.venue.request_timeout_s, 30)),
        )

    async def response_observed(
        self, body: bytes, received_ms: int, http_status: int, *, location_header: str | None = None
    ) -> None:
        """Finite verification hook; the production collector stores metadata only."""

    async def poll(self, session: aiohttp.ClientSession) -> None:
        received: int | None = None
        status: int | None = None
        try:
            async with self.request(session) as response:
                status = response.status
                body = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body) > MAX_BYTES:
                        raise IntegrityError("CRYPTO_NEWS_FEED_TOO_LARGE")
                received = now_ms()
                location = response.headers.get("Location")
                if location is not None and not isinstance(location, str):
                    raise IntegrityError("CRYPTO_NEWS_LOCATION_HEADER_INVALID")
                await self.response_observed(
                    bytes(body), received, status, location_header=location
                )
                if status != 200:
                    raise IntegrityError("CRYPTO_NEWS_HTTP_STATUS")
            parsed_rows, rejected = parse_feed(bytes(body), received)
            current_by_native_id: dict[str, MarketEvent] = {}
            rows: list[MarketEvent] = []
            new_rows: list[MarketEvent] = []
            for row in parsed_rows:
                payload = NewsPayload.model_validate_json(row.payload_json)
                native_id = payload.publisher_item_id
                prior_in_response = current_by_native_id.get(native_id)
                if prior_in_response is not None and prior_in_response.event_id != row.event_id:
                    raise IntegrityError("CRYPTO_NEWS_DUPLICATE_NATIVE_ID_CONFLICT")
                retained = self._metadata_by_event_id.get(row.event_id)
                selected = retained or row
                current_by_native_id[native_id] = selected
                rows.append(selected)
                if retained is None:
                    new_rows.append(row)
            if len(self._metadata_by_event_id) + len(new_rows) > MAX_RETAINED_IDENTITIES:
                raise IntegrityError("CRYPTO_NEWS_RETAINED_IDENTITY_BUDGET")
        except (
            aiohttp.ClientError,
            TimeoutError,
            IntegrityError,
            ParseError,
            DefusedXmlException,
            ValueError,
            TypeError,
            OverflowError,
        ) as exc:
            await self.sink(
                news_poll_health(
                    now_ms(),
                    response_received_at_ms=received,
                    http_status=status,
                    error_type=type(exc).__name__,
                )
            )
            return
        # Sink errors escape: never announce a successful poll after failed storage.
        for row in new_rows:
            await self.sink(row)
            self._metadata_by_event_id[row.event_id] = row
        await self.sink(
            news_poll_health(
                now_ms(),
                accepted_event_ids=tuple(row.event_id for row in rows),
                rejected_items=rejected,
                response_received_at_ms=received,
                http_status=status,
            )
        )

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            headers={"User-Agent": "SelectiveOptionsResearch/0.3 (public RSS metadata)"}
        ) as session:
            while True:
                await self.poll(session)
                await asyncio.sleep(self.config.ingestion.crypto_news_poll_interval_s)
