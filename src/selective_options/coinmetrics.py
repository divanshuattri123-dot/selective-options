"""Bounded Community daily regime observations; no publication or exchange-flow inference.

Every poll is a new receipt-bound observation, including unchanged provider values.
Validation stages the entire poll. Sink acceptance is not a transaction or durable
commit: ambiguous delivery poisons this collector and requires external recovery.
"""

import asyncio
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal, Self
from urllib.parse import parse_qsl, urlsplit

import aiohttp
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .errors import IntegrityError
from .events import MarketEvent, make_event

SOURCE = "coinmetrics_community"
SYMBOL = "COINMETRICS_DAILY_REGIME"
URL = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
DAY_MS = 86_400_000
METRICS = ("CapMrktCurUSD", "CapMVRVCur")
UNAVAILABLE_METRICS: tuple[Literal["CapRealUSD", "SOPR"], ...] = ("CapRealUSD", "SOPR")
UNITS = {"CapMrktCurUSD": "USD", "CapMVRVCur": "Dimensionless"}
REVISION: Literal["response observation; not first publication"] = (
    "response observation; not first publication"
)
Clock = Annotated[int, Field(strict=True, gt=0, lt=2**63)]
Amount = Annotated[
    str,
    Field(strict=True, min_length=1, max_length=128, pattern=r"^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"),
]
EventId = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class DailyRow(StrictModel):
    asset: Literal["btc", "eth"]
    time: StrictStr
    CapMrktCurUSD: Amount | None = None
    CapMVRVCur: Amount | None = None

    @model_validator(mode="after")
    def valid(self) -> Self:
        native_time_ms(self.time)
        for metric in METRICS:
            value = getattr(self, metric)
            if value is not None and Decimal(value) <= 0:
                raise ValueError("COINMETRICS_NONPOSITIVE_VALUE")
        return self


class DailySnapshot(StrictModel):
    frequency: Literal["1d"]
    native_row: DailyRow
    units: dict[str, StrictStr]
    period_label_ms: Clock
    conservative_complete_after_ms: Clock
    response_received_at_ms: Clock
    poll_started_at_ms: Clock
    publication_time_available: StrictBool
    revision_basis: Literal["response observation; not first publication"]
    unavailable_metrics: tuple[Literal["CapRealUSD", "SOPR"], ...]
    exchange_attribution_available: StrictBool

    @model_validator(mode="after")
    def valid(self) -> Self:
        if self.units != UNITS or self.unavailable_metrics != UNAVAILABLE_METRICS:
            raise ValueError("COINMETRICS_METRIC_SCOPE")
        if self.publication_time_available or self.exchange_attribution_available:
            raise ValueError("COINMETRICS_UNPROVEN_ATTRIBUTION")
        if self.period_label_ms != native_time_ms(self.native_row.time):
            raise ValueError("COINMETRICS_NATIVE_CLOCK")
        if self.conservative_complete_after_ms != self.period_label_ms + DAY_MS:
            raise ValueError("COINMETRICS_PERIOD_POLICY")
        if (
            not self.conservative_complete_after_ms
            <= self.poll_started_at_ms
            <= self.response_received_at_ms
        ):
            raise ValueError("COINMETRICS_INCOMPLETE_OR_FUTURE_PERIOD")
        return self


class AcceptedObservation(StrictModel):
    event_id: EventId
    observed_at_ms: Clock
    period_label_ms: Clock

    @model_validator(mode="after")
    def valid(self) -> Self:
        if self.period_label_ms % DAY_MS or self.period_label_ms + DAY_MS > self.observed_at_ms:
            raise ValueError("COINMETRICS_REFERENCE_PERIOD")
        return self


class PollHealth(StrictModel):
    frequency: Literal["1d"]
    window_start_ms: Clock
    window_end_ms: Clock
    ok: StrictBool
    reason: Literal[
        "DAILY_REGIME_OBSERVED",
        "EMPTY",
        "MISSING_METRICS",
        "STALE",
        "ACCESS_DENIED",
        "RATE_LIMITED",
        "HTTP_UNAVAILABLE",
        "TRANSPORT_UNAVAILABLE",
        "INVALID_DATA",
    ]
    code: StrictInt | None = None
    error_type: StrictStr | None = None
    poll_started_at_ms: Clock
    poll_finished_at_ms: Clock
    response_received_at_ms: Clock | None = None
    accepted_observations: tuple[AcceptedObservation, ...] = ()
    unavailable_metrics: tuple[Literal["CapRealUSD", "SOPR"], ...] = UNAVAILABLE_METRICS
    publication_time_available: StrictBool = False

    @model_validator(mode="after")
    def valid(self) -> Self:
        if (
            self.window_start_ms % DAY_MS
            or self.window_end_ms % DAY_MS
            or not self.window_start_ms <= self.window_end_ms
            or self.window_end_ms - self.window_start_ms >= 30 * DAY_MS
            or self.window_end_ms + DAY_MS > self.poll_started_at_ms
        ):
            raise ValueError("COINMETRICS_HEALTH_WINDOW")
        if (
            self.poll_started_at_ms > self.poll_finished_at_ms
            or self.publication_time_available
            or self.unavailable_metrics != UNAVAILABLE_METRICS
        ):
            raise ValueError("COINMETRICS_HEALTH_SCOPE")
        if self.ok != (self.reason == "DAILY_REGIME_OBSERVED"):
            raise ValueError("COINMETRICS_HEALTH_OUTCOME")
        if self.ok and not self.accepted_observations:
            raise ValueError("COINMETRICS_HEALTH_WITHOUT_OBSERVATION")
        observed_outcomes = ("DAILY_REGIME_OBSERVED", "MISSING_METRICS", "STALE")
        if self.reason in observed_outcomes:
            if (
                not self.accepted_observations
                or self.response_received_at_ms is None
                or self.code is not None
                or self.error_type is not None
            ):
                raise ValueError("COINMETRICS_HEALTH_OBSERVATION_BINDING")
        elif self.accepted_observations:
            raise ValueError("COINMETRICS_FAILED_POLL_REFERENCES")
        if self.reason == "EMPTY" and (
            self.response_received_at_ms is None
            or self.code is not None
            or self.error_type is not None
        ):
            raise ValueError("COINMETRICS_EMPTY_POLL_BINDING")
        if (
            self.response_received_at_ms is not None
            and not self.poll_started_at_ms
            <= self.response_received_at_ms
            <= self.poll_finished_at_ms
        ):
            raise ValueError("COINMETRICS_HEALTH_CLOCK")
        if len({x.event_id for x in self.accepted_observations}) != len(self.accepted_observations):
            raise ValueError("COINMETRICS_DUPLICATE_REFERENCE")
        for ref in self.accepted_observations:
            if (
                not self.poll_started_at_ms <= ref.observed_at_ms <= self.poll_finished_at_ms
                or ref.period_label_ms + DAY_MS > self.poll_started_at_ms
                or self.response_received_at_ms is None
                or ref.observed_at_ms > self.response_received_at_ms
                or not self.window_start_ms <= ref.period_label_ms <= self.window_end_ms
            ):
                raise ValueError("COINMETRICS_REFERENCE_CLOCK")
        return self


def native_time_ms(value: str) -> int:
    if type(value) is not str or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00\.000000000Z", value
    ):
        raise ValueError("COINMETRICS_DAILY_TIME")
    instant = datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=UTC)
    result = int(instant.timestamp()) * 1000
    if result <= 0:
        raise ValueError("COINMETRICS_DAILY_TIME")
    return result


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError("COINMETRICS_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _json(data: bytes | str) -> object:
    return json.loads(
        data,
        object_pairs_hook=_object_pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(
            IntegrityError("COINMETRICS_NONFINITE_JSON")
        ),
    )


def normalize_row(row: DailyRow, received_at_ms: int, poll_started_at_ms: int) -> MarketEvent:
    row = DailyRow.model_validate(row.model_dump(exclude_unset=True))
    snapshot = DailySnapshot(
        frequency="1d",
        native_row=row,
        units=UNITS.copy(),
        period_label_ms=native_time_ms(row.time),
        conservative_complete_after_ms=native_time_ms(row.time) + DAY_MS,
        response_received_at_ms=received_at_ms,
        poll_started_at_ms=poll_started_at_ms,
        publication_time_available=False,
        revision_basis=REVISION,
        unavailable_metrics=UNAVAILABLE_METRICS,
        exchange_attribution_available=False,
    )
    payload = snapshot.model_dump(mode="json")
    payload["native_row"] = row.model_dump(mode="json", exclude_unset=True)
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset=row.asset.upper(),
        symbol=SYMBOL,
        kind="on_chain",
        event_time_ms=snapshot.period_label_ms,
        observed_at_ms=received_at_ms,
        payload=payload,
        native_id=f"{row.asset}:{row.time}:{received_at_ms}",
    )


def canonical_coinmetrics(event: MarketEvent) -> DailySnapshot:
    clean = _strict_event(event)
    _json(clean.payload_json)
    result = DailySnapshot.model_validate_json(clean.payload_json)
    rebuilt = normalize_row(
        result.native_row, result.response_received_at_ms, result.poll_started_at_ms
    )
    if clean != rebuilt:
        raise IntegrityError("COINMETRICS_NONCANONICAL_EVENT")
    return result


def coinmetrics_poll_health(asset: str, health: PollHealth) -> MarketEvent:
    if asset not in ("BTC", "ETH"):
        raise IntegrityError("COINMETRICS_ASSET")
    health = PollHealth.model_validate(health.model_dump())
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset=asset,
        symbol=SYMBOL,
        kind="feed_health",
        event_time_ms=health.poll_finished_at_ms,
        observed_at_ms=health.poll_finished_at_ms,
        timestamp_kind="receipt",
        payload=health.model_dump(mode="json"),
    )


def canonical_coinmetrics_health(event: MarketEvent) -> PollHealth:
    clean = _strict_event(event)
    _json(clean.payload_json)
    result = PollHealth.model_validate_json(clean.payload_json)
    if clean != coinmetrics_poll_health(clean.asset, result):
        raise IntegrityError("COINMETRICS_NONCANONICAL_HEALTH")
    return result


def _strict_event(event: MarketEvent) -> MarketEvent:
    if any(
        type(getattr(event, field)) is not int
        for field in ("event_time_ms", "observed_at_ms", "schema_version")
    ):
        raise IntegrityError("COINMETRICS_ENVELOPE_TYPE")
    return MarketEvent.model_validate(event.model_dump(), strict=True)


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("COINMETRICS_INVALID_POLICY")


class CoinMetricsCollector:
    def __init__(
        self,
        sink: Sink,
        *,
        assets: tuple[str, ...] = ("BTC", "ETH"),
        poll_interval_s: int = 3600,
        lookback_days: int = 7,
        max_age_ms: int = 259200000,
        max_pages: int = 4,
        max_rows: int = 128,
        max_response_bytes: int = 2000000,
        timeout_s: int = 30,
    ) -> None:
        if (
            type(assets) is not tuple
            or not assets
            or len(set(assets)) != len(assets)
            or any(a not in ("BTC", "ETH") for a in assets)
        ):
            raise ValueError("COINMETRICS_ASSET_SCOPE")
        for value, low, high in (
            (poll_interval_s, 60, 86400),
            (lookback_days, 1, 30),
            (max_age_ms, DAY_MS, 30 * DAY_MS),
            (max_pages, 1, 8),
            (max_rows, 1, 1024),
            (max_response_bytes, 1024, 4000000),
            (timeout_s, 1, 120),
        ):
            _integer(value, low, high)
        self.sink, self.assets = sink, assets
        self.poll_interval_s, self.lookback_days, self.max_age_ms = (
            poll_interval_s,
            lookback_days,
            max_age_ms,
        )
        self.max_pages, self.max_rows, self.max_response_bytes, self.timeout_s = (
            max_pages,
            max_rows,
            max_response_bytes,
            timeout_s,
        )
        self._emission_failed = False
        self._last_response_received_at_ms: int | None = None
        self._lock = asyncio.Lock()

    async def response_received(
        self, parameters: dict[str, str], status: int, body: bytes, received_at_ms: int
    ) -> None:
        """Optional native evidence hook; base implementation asserts no persistence."""

    async def request(
        self, session: aiohttp.ClientSession, parameters: dict[str, str]
    ) -> tuple[object, int]:
        async with session.get(URL, params=parameters, allow_redirects=False) as response:
            raw = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                if len(raw) + len(chunk) > self.max_response_bytes:
                    raise IntegrityError("COINMETRICS_RESPONSE_BUDGET")
                raw.extend(chunk)
            received = now_ms()
            self._last_response_received_at_ms = received
            await self.response_received(parameters.copy(), response.status, bytes(raw), received)
            if response.status != 200:
                reason = (
                    "ACCESS_DENIED"
                    if response.status in (401, 403)
                    else "RATE_LIMITED"
                    if response.status == 429
                    else "HTTP_UNAVAILABLE"
                )
                raise FeedUnavailable(reason, response.status)
            return _json(bytes(raw)), received

    async def _stage(
        self, session: aiohttp.ClientSession, started: int
    ) -> tuple[list[MarketEvent], int]:
        day = started // DAY_MS * DAY_MS

        def date(clock: int) -> str:
            return datetime.fromtimestamp(clock // 1000, UTC).strftime("%Y-%m-%d")

        base = {
            "assets": ",".join(a.lower() for a in self.assets),
            "metrics": ",".join(METRICS),
            "frequency": "1d",
            "page_size": str(min(100, self.max_rows)),
            "start_time": date(day - self.lookback_days * DAY_MS),
            "end_time": date(day - DAY_MS),
            "paging_from": "start",
        }
        parameters = base.copy()
        rows: list[MarketEvent] = []
        identities: set[tuple[str, str]] = set()
        tokens: set[str] = set()
        received = started
        for _ in range(self.max_pages):
            document, current_receipt = await self.request(session, parameters)
            if type(current_receipt) is not int or current_receipt < received:
                raise IntegrityError("COINMETRICS_RECEIPT_REGRESSION")
            received = current_receipt
            self._last_response_received_at_ms = received
            if (
                not isinstance(document, dict)
                or set(document) - {"data", "next_page_token", "next_page_url"}
                or not isinstance(document.get("data"), list)
            ):
                raise IntegrityError("COINMETRICS_ENVELOPE")
            data = document["data"]
            if len(data) > int(base["page_size"]) or len(rows) + len(data) > self.max_rows:
                raise IntegrityError("COINMETRICS_ROW_BUDGET")
            for raw_row in data:
                row = DailyRow.model_validate(raw_row)
                identity = (row.asset, row.time)
                if row.asset.upper() not in self.assets or identity in identities:
                    raise IntegrityError("COINMETRICS_DUPLICATE_OR_WRONG_ASSET")
                label = native_time_ms(row.time)
                if not day - self.lookback_days * DAY_MS <= label <= day - DAY_MS:
                    raise IntegrityError("COINMETRICS_QUERY_RANGE")
                identities.add(identity)
                rows.append(normalize_row(row, received, started))
            token, next_url = document.get("next_page_token"), document.get("next_page_url")
            if token is None and next_url is None:
                return rows, received
            if (
                type(token) is not str
                or not 1 <= len(token) <= 2048
                or token in tokens
                or type(next_url) is not str
                or len(next_url) > 10000
                or not data
            ):
                raise IntegrityError("COINMETRICS_PAGINATION")
            split = urlsplit(next_url)
            pairs = parse_qsl(split.query, keep_blank_values=True, strict_parsing=True)
            expected = {**base, "next_page_token": token}
            if (
                split.scheme != "https"
                or split.netloc != "community-api.coinmetrics.io"
                or split.path != "/v4/timeseries/asset-metrics"
                or split.fragment
                or len(pairs) != len(expected)
                or dict(pairs) != expected
            ):
                raise IntegrityError("COINMETRICS_PAGINATION_ROUTE")
            tokens.add(token)
            parameters = expected
        raise IntegrityError("COINMETRICS_PAGE_BUDGET")

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        async with self._lock:
            if self._emission_failed:
                raise IntegrityError("COINMETRICS_EMISSION_OUTCOME_UNKNOWN")
            started = now_ms()
            self._last_response_received_at_ms = None
            received: int | None = None
            reason, code, error_type = "INVALID_DATA", None, None
            try:
                async with asyncio.timeout(self.timeout_s):
                    rows, received = await self._stage(session, started)
            except FeedUnavailable as exc:
                reason, code = exc.reason, exc.code
                rows, received = [], self._last_response_received_at_ms
            except (aiohttp.ClientError, OSError, TimeoutError) as exc:
                reason, error_type = "TRANSPORT_UNAVAILABLE", type(exc).__name__
                rows, received = [], self._last_response_received_at_ms
            except (ValueError, ValidationError, IntegrityError) as exc:
                error_type = type(exc).__name__
                rows, received = [], self._last_response_received_at_ms
            finished = now_ms()
            if finished < started or (received is not None and finished < received):
                raise IntegrityError("COINMETRICS_LOCAL_CLOCK_REGRESSION")
            health_events = []
            for asset in self.assets:
                selected = sorted(
                    (r for r in rows if r.asset == asset), key=lambda r: r.event_time_ms
                )
                asset_reason = reason
                if selected:
                    latest = canonical_coinmetrics(selected[-1])
                    asset_reason = (
                        "MISSING_METRICS"
                        if any(getattr(latest.native_row, m) is None for m in METRICS)
                        else "STALE"
                        if started - latest.period_label_ms > self.max_age_ms
                        else "DAILY_REGIME_OBSERVED"
                    )
                elif received is not None and reason == "INVALID_DATA" and error_type is None:
                    asset_reason = "EMPTY"
                health = PollHealth.model_validate(
                    {
                        "frequency": "1d",
                        "window_start_ms": started // DAY_MS * DAY_MS - self.lookback_days * DAY_MS,
                        "window_end_ms": started // DAY_MS * DAY_MS - DAY_MS,
                        "ok": asset_reason == "DAILY_REGIME_OBSERVED",
                        "reason": asset_reason,
                        "code": code,
                        "error_type": error_type,
                        "poll_started_at_ms": started,
                        "poll_finished_at_ms": finished,
                        "response_received_at_ms": received,
                        "accepted_observations": tuple(
                            AcceptedObservation(
                                event_id=r.event_id,
                                observed_at_ms=r.observed_at_ms,
                                period_label_ms=r.event_time_ms,
                            )
                            for r in selected
                        ),
                    }
                )
                health_events.append(coinmetrics_poll_health(asset, health))
            try:
                for event in (*rows, *health_events):
                    await self.sink(event)
            except asyncio.CancelledError:
                self._emission_failed = True
                raise
            except Exception as exc:
                self._emission_failed = True
                raise IntegrityError("COINMETRICS_EMISSION_FAILED") from exc
            return all(canonical_coinmetrics_health(h).ok for h in health_events)

    async def run(self) -> None:
        if self._emission_failed:
            raise IntegrityError("COINMETRICS_EMISSION_OUTCOME_UNKNOWN")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout_s)
        ) as session:
            while True:
                await self.collect(session)
                await asyncio.sleep(self.poll_interval_s)
