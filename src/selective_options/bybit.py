"""Public USDT linear perpetual snapshots; no authentication or execution endpoints."""

import asyncio
from typing import Literal

import aiohttp
from pydantic import Field, JsonValue, ValidationError

from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

BASE_URL = "https://api.bybit.com/v5/market/"
MAX_BYTES = 2_000_000
PublicEndpoint = Literal["instruments-info", "tickers", "funding/history", "open-interest"]


class Envelope(ExternalModel):
    retCode: int
    result: dict[str, JsonValue]
    time: int = Field(gt=0)


class Instrument(ExternalModel):
    symbol: str = Field(min_length=1, max_length=128)
    baseCoin: str
    quoteCoin: str
    settleCoin: str
    contractType: str
    status: str
    fundingInterval: int = Field(ge=0)
    isPreListing: bool


class Instruments(ExternalModel):
    category: Literal["linear"]
    list: tuple[Instrument, ...]
    nextPageCursor: str = ""


class Ticker(ExternalModel):
    symbol: str
    markPrice: float = Field(gt=0)
    indexPrice: float = Field(gt=0)
    bid1Price: float = Field(ge=0)
    ask1Price: float = Field(ge=0)
    openInterest: float = Field(ge=0)
    openInterestValue: float = Field(ge=0)
    fundingRate: float
    nextFundingTime: int = Field(gt=0)
    fundingIntervalHour: int | None = Field(default=None, gt=0)


class Tickers(ExternalModel):
    category: Literal["linear"]
    list: tuple[Ticker, ...]


class FeedUnavailable(Exception):
    def __init__(self, reason: str, code: int) -> None:
        self.reason, self.code = reason, code
        super().__init__(reason)


def snapshot_events(
    instrument: Instrument, discovery_received: int, response: Envelope, received: int
) -> list[MarketEvent]:
    rows = Tickers.model_validate(response.result).list
    if len(rows) != 1 or rows[0].symbol != instrument.symbol:
        raise IntegrityError("BYBIT_TICKER_IDENTITY")
    row = rows[0]
    if discovery_received > received or instrument.fundingInterval <= 0:
        raise IntegrityError("BYBIT_INSTRUMENT_TIMING")
    if row.bid1Price and row.ask1Price and row.bid1Price > row.ask1Price:
        raise IntegrityError("BYBIT_CROSSED_QUOTE")
    if row.nextFundingTime <= response.time:
        raise IntegrityError("BYBIT_FUNDING_SCHEDULE_EXPIRED")
    if (
        row.fundingIntervalHour is not None
        and row.fundingIntervalHour * 60 != instrument.fundingInterval
    ):
        raise IntegrityError("BYBIT_FUNDING_INTERVAL_CHANGED")
    common: dict[str, JsonValue] = {
        "instrument": instrument.model_dump(mode="json"),
        "instrument_observed_at_ms": discovery_received,
        "server_response_time_ms": response.time,
        "timestamp_basis": "local_receipt; venue does not provide per-field publication time",
        "publication_time_available": False,
    }
    payloads: list[tuple[Literal["perpetual", "funding"], dict[str, JsonValue]]] = [
        (
            "perpetual",
            {
                **common,
                "mark_price": row.markPrice,
                "index_price": row.indexPrice,
                "best_bid_price": row.bid1Price,
                "best_ask_price": row.ask1Price,
                "price_units": instrument.quoteCoin,
                "open_interest": row.openInterest,
                "open_interest_units": instrument.baseCoin,
                "open_interest_sides": "both",
                "open_interest_value": row.openInterestValue,
                "open_interest_value_units": instrument.quoteCoin,
            },
        ),
        (
            "funding",
            {
                **common,
                "funding_rate": row.fundingRate,
                "units": "fraction_per_native_interval",
                "funding_interval_minutes": instrument.fundingInterval,
                "next_funding_time_ms": row.nextFundingTime,
                "rate_kind": "current_snapshot_not_settled_history",
            },
        ),
    ]
    return [
        make_event(
            source="bybit",
            environment="public_reference",
            asset=instrument.baseCoin,
            symbol=instrument.symbol,
            kind=kind,
            event_time_ms=received,
            observed_at_ms=received,
            timestamp_kind="receipt",
            payload=payload,
        )
        for kind, payload in payloads
    ]


class BybitCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink

    async def request(
        self,
        session: aiohttp.ClientSession,
        endpoint: PublicEndpoint,
        params: dict[str, str],
    ) -> tuple[Envelope, int]:
        # Fixed host/path, verified TLS, no redirects, credentials or regional fallback.
        async with session.get(
            BASE_URL + endpoint, params=params, allow_redirects=False
        ) as response:
            if response.status in {401, 403, 451}:
                raise FeedUnavailable("ACCESS_DENIED", response.status)
            if response.status == 429:
                raise FeedUnavailable("RATE_LIMITED", response.status)
            if response.status != 200:
                raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > MAX_BYTES:
                    raise IntegrityError("BYBIT_RESPONSE_TOO_LARGE")
            received = now_ms()
        envelope = Envelope.model_validate_json(body)
        if envelope.retCode:
            raise FeedUnavailable("API_UNAVAILABLE", envelope.retCode)
        if not received - 60_000 <= envelope.time <= received + 2_000:
            raise IntegrityError("BYBIT_RESPONSE_CLOCK_OR_STALENESS")
        return envelope, received

    async def discover(
        self, session: aiohttp.ClientSession, asset: str
    ) -> tuple[Instrument | None, int]:
        selected: list[Instrument] = []
        symbols: set[str] = set()
        cursors: set[str] = set()
        params = {"category": "linear", "baseCoin": asset, "limit": "1000"}
        for _ in range(5):
            response, received = await self.request(session, "instruments-info", params)
            page = Instruments.model_validate(response.result)
            for row in page.list:
                if row.symbol in symbols or row.baseCoin != asset:
                    raise IntegrityError("BYBIT_DISCOVERY_IDENTITY")
                symbols.add(row.symbol)
                if (
                    row.contractType == "LinearPerpetual"
                    and row.status == "Trading"
                    and row.quoteCoin == row.settleCoin == "USDT"
                    and not row.isPreListing
                ):
                    selected.append(row)
            if not page.nextPageCursor:
                if len(selected) > 1:
                    raise IntegrityError("BYBIT_AMBIGUOUS_PERPETUAL")
                return selected[0] if selected else None, received
            if page.nextPageCursor in cursors:
                raise IntegrityError("BYBIT_REPEATED_CURSOR")
            cursors.add(page.nextPageCursor)
            params = {**params, "cursor": page.nextPageCursor}
            await asyncio.sleep(0.25)
        raise IntegrityError("BYBIT_DISCOVERY_TRUNCATED")

    async def health(self, asset: str, ok: bool, reason: str, code: int = 0) -> None:
        received = now_ms()
        await self.sink(
            make_event(
                source="bybit",
                environment="public_reference",
                asset=asset,
                symbol="USDT_LINEAR_PERPETUAL",
                kind="feed_health",
                event_time_ms=received,
                observed_at_ms=received,
                timestamp_kind="receipt",
                payload={"ok": ok, "reason": reason, "code": code},
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        healthy = True
        for asset in self.config.system.assets:
            try:
                instrument, discovered = await self.discover(session, asset)
                if instrument is None:
                    await self.health(asset, False, "PERPETUAL_UNAVAILABLE")
                    continue
                await asyncio.sleep(0.25)
                response, received = await self.request(
                    session, "tickers", {"category": "linear", "symbol": instrument.symbol}
                )
                events = snapshot_events(instrument, discovered, response, received)
            except FeedUnavailable as exc:
                await self.health(asset, False, exc.reason, exc.code)
                # Venue-wide denial/rate limit: do not probe every remaining asset.
                return False
            except (aiohttp.ClientError, OSError, TimeoutError):
                await self.health(asset, False, "TRANSPORT_UNAVAILABLE")
                return False
            except (ValidationError, IntegrityError):
                await self.health(asset, False, "INVALID_DATA")
                healthy = False
            else:
                # Storage/sink errors propagate; they are not optional-source failures.
                for event in events:
                    await self.sink(event)
                await self.health(asset, True, "SNAPSHOT_ACCEPTED")
            await asyncio.sleep(0.25)
        return healthy

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            while True:
                healthy = await self.collect(session)
                await asyncio.sleep(self.config.ingestion.bybit_poll_interval_s if healthy else 600)
