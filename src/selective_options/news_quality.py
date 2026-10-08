"""Bounded as-of policy RSS poll evidence; no all-news or trading availability claim."""

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from .errors import IntegrityError
from .events import MarketEvent, make_event
from .news import FEEDS, NewsPollHealth, NewsSource, news_poll_health


class NewsPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    title: StrictStr = Field(min_length=1, max_length=2000)
    url: StrictStr = Field(min_length=1, max_length=4000)
    publisher_item_id: StrictStr = Field(min_length=1, max_length=4000)
    published_at: StrictStr
    publication_time_claim: Literal["publisher_RSS_pubDate"]
    first_publication_independently_verified: Literal[False]
    classification: Literal["NOT_CLASSIFIED"]
    source_category: Literal["official_monetary_policy"]
    feed_url: StrictStr


class LegacyHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    accepted_items: StrictInt = Field(default=0, ge=0, le=10_000)
    rejected_items: StrictInt = Field(default=0, ge=0, le=10_000)
    error_type: StrictStr | None = None


def _news_valid(row: MarketEvent, source: NewsSource) -> None:
    body = NewsPayload.model_validate_json(row.payload_json)
    date = datetime.fromisoformat(body.published_at)
    parts = urlsplit(body.url)
    if (
        date.tzinfo is None
        or date.utcoffset() is None
        or date.astimezone(UTC).isoformat() != body.published_at
        or int(date.timestamp() * 1000) != row.event_time_ms
        or row.event_time_ms > row.observed_at_ms
        or body.feed_url != FEEDS[source]
        or parts.scheme != "https"
        or not parts.hostname
        or parts.username
        or parts.password
        or type(row.payload()["first_publication_independently_verified"]) is not bool
    ):
        raise IntegrityError("NEWS_METADATA_CLOCK_OR_PROVENANCE")
    canonical = make_event(
        source=source,
        environment="public_reference",
        asset="GLOBAL",
        symbol="POLICY_NEWS",
        kind="news",
        timestamp_kind="source",
        native_id=body.publisher_item_id,
        event_time_ms=row.event_time_ms,
        observed_at_ms=row.observed_at_ms,
        payload=body.model_dump(mode="json"),
    )
    if canonical != row:
        raise IntegrityError("NEWS_METADATA_CANONICAL")


def _health_valid(row: MarketEvent, source: NewsSource) -> NewsPollHealth | None:
    payload = row.payload()
    if "accepted_event_ids" not in payload:
        # The historical shape has no identity binding. Validate it, preserve its
        # failures, but never manufacture references from nearby headline rows.
        body = LegacyHealth.model_validate(payload)
        canonical = make_event(
            source=source,
            environment="public_reference",
            asset="GLOBAL",
            symbol="POLICY_NEWS",
            kind="feed_health",
            timestamp_kind="receipt",
            event_time_ms=row.observed_at_ms,
            observed_at_ms=row.observed_at_ms,
            payload=payload,
        )
        if canonical != row or (body.ok and (body.rejected_items or body.error_type)):
            raise IntegrityError("NEWS_LEGACY_HEALTH_INVALID")
        return None
    status = NewsPollHealth.model_validate(payload)
    canonical = news_poll_health(
        source,
        row.observed_at_ms,
        accepted_event_ids=status.accepted_event_ids,
        rejected_items=status.rejected_items,
        response_received_at_ms=status.response_received_at_ms,
        error_type=status.error_type,
    )
    if canonical != row:
        raise IntegrityError("NEWS_HEALTH_CANONICAL")
    return status


def policy_news_quality(
    events: Iterable[MarketEvent],
    *,
    source: NewsSource,
    as_of_ms: int,
    max_poll_age_ms: int = 600_000,
    max_publication_age_ms: int = 2_592_000_000,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Latest eligible poll governs; publication age only describes its metadata.

    Only scoped rows passing both clocks count toward the application row budget.
    This does not bound iterable scanning, database execution or engine memory.
    """
    if (
        source not in FEEDS
        or any(
            type(value) is not int or value <= 0
            for value in (as_of_ms, max_poll_age_ms, max_publication_age_ms)
        )
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
    ):
        raise ValueError("invalid bounded policy RSS quality policy")
    result: dict[str, object] = {
        "source": source,
        "asset": "GLOBAL",
        "symbol": "POLICY_NEWS",
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "health_received_at_ms": None,
        "response_received_at_ms": None,
        "poll_ok": None,
        "error_type": None,
        "accepted_event_ids": [],
        "accepted_items": None,
        "rejected_items": None,
        "historical_metadata_items": 0,
        "legacy_health_rows": 0,
        "headline_state": "UNVERIFIED",
        "latest_publication_time_ms": None,
        "recent_publication_items": 0,
        "first_observed_at_ms": [],
        "max_poll_age_ms": max_poll_age_ms,
        "max_publication_age_ms": max_publication_age_ms,
        "scope": "One official policy RSS poll and its exact accepted metadata identities; publication is the publisher claim and observation is local receipt. Empty success proves poll observation only. Sink acceptance is not atomic commit proof.",
        "complete_feed_available": False,
        "all_news_complete": False,
        "first_publication_independently_verified": False,
        "trading_approval": False,
    }
    try:
        eligible: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                row.source != source
                or row.environment != "public_reference"
                or row.asset != "GLOBAL"
                or row.symbol != "POLICY_NEWS"
                or row.kind not in ("news", "feed_health")
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("NEWS_QUALITY_OVER_BUDGET")
            if row.event_id in eligible and eligible[row.event_id] != row:
                raise IntegrityError("NEWS_EVENT_ID_CONFLICT")
            eligible[row.event_id] = row
        news: dict[str, MarketEvent] = {}
        health: list[tuple[MarketEvent, NewsPollHealth | None]] = []
        for row in eligible.values():
            if row.kind == "news":
                _news_valid(row, source)
                news[row.event_id] = row
            else:
                health.append((row, _health_valid(row, source)))
        result["historical_metadata_items"] = len(news)
        result["legacy_health_rows"] = sum(status is None for _, status in health)
        if not health:
            raise IntegrityError("NEWS_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        choices = [(row, body) for row, body in health if row.observed_at_ms == latest]
        if len(choices) != 1:
            raise IntegrityError("NEWS_HEALTH_AMBIGUOUS")
        selected, status = choices[0]
        result.update(
            health_event_id=selected.event_id,
            health_received_at_ms=latest,
            poll_ok=selected.payload()["ok"],
            error_type=selected.payload().get("error_type"),
        )
        if status is not None:
            result.update(
                response_received_at_ms=status.response_received_at_ms,
                accepted_items=status.accepted_items,
                rejected_items=status.rejected_items,
            )
        if selected.payload()["ok"] is not True:
            raise IntegrityError("NEWS_FEED_UNAVAILABLE")
        if status is None:
            raise IntegrityError("NEWS_HEALTH_UNBOUND")
        # Canonical success always carries its actual HTTP response receipt.
        assert status.response_received_at_ms is not None
        if as_of_ms - min(latest, status.response_received_at_ms) > max_poll_age_ms:
            raise IntegrityError("NEWS_POLL_STALE")
        referenced: list[MarketEvent] = []
        for identity in status.accepted_event_ids:
            if identity not in news:
                raise IntegrityError("NEWS_REFERENCED_METADATA_MISSING")
            row = news[identity]
            if row.observed_at_ms > status.response_received_at_ms:
                raise IntegrityError("NEWS_METADATA_AFTER_POLL_RESPONSE")
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
        result["blocker"] = str(exc) if isinstance(exc, IntegrityError) else "NEWS_INVALID_EVIDENCE"
    return result
