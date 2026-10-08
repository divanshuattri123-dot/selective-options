"""Public Binance USD-M snapshots, with separate response times and native semantics."""

import asyncio
from typing import Literal

import aiohttp
from pydantic import Field, JsonValue, TypeAdapter, ValidationError

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import EventKind, MarketEvent, make_event
from .models import ExternalModel

Endpoint = Literal[
    "exchangeInfo",
    "premiumIndex",
    "openInterest",
    "fundingInfo",
    "fundingRate",
    "takerlongshortRatio",
    "globalLongShortAccountRatio",
]


class FundingInfo(ExternalModel):
    symbol: str
    adjustedFundingRateCap: float
    adjustedFundingRateFloor: float
    fundingIntervalHours: int = Field(gt=0)


def funding_metadata(raw: JsonValue) -> dict[str, FundingInfo]:
    rows = TypeAdapter(list[FundingInfo]).validate_python(raw)
    result: dict[str, FundingInfo] = {}
    for row in rows:
        if row.symbol in result or row.adjustedFundingRateFloor > row.adjustedFundingRateCap:
            raise IntegrityError("BINANCE_INVALID_FUNDING_METADATA")
        result[row.symbol] = row
    return result


class Instrument(ExternalModel):
    symbol: str
    contractType: str
    status: str
    baseAsset: str
    quoteAsset: str
    marginAsset: str


class Discovery(ExternalModel):
    symbols: tuple[Instrument, ...]


class Mark(ExternalModel):
    symbol: str
    markPrice: float = Field(gt=0)
    indexPrice: float = Field(gt=0)
    lastFundingRate: float
    nextFundingTime: int = Field(gt=0)
    time: int = Field(gt=0)


class Interest(ExternalModel):
    symbol: str
    openInterest: float = Field(ge=0)
    time: int = Field(gt=0)


def snapshots(
    instrument: Instrument,
    mark_raw: JsonValue,
    mark_received: int,
    oi_raw: JsonValue,
    oi_received: int,
    funding_info: FundingInfo | None = None,
    funding_info_received: int | None = None,
) -> list[MarketEvent]:
    mark, oi = Mark.model_validate(mark_raw), Interest.model_validate(oi_raw)
    for row, received in ((mark, mark_received), (oi, oi_received)):
        if row.symbol != instrument.symbol:
            raise IntegrityError("BINANCE_SYMBOL_MISMATCH")
        if not received - 60_000 <= row.time <= received + 2_000:
            raise IntegrityError("BINANCE_STALE_OR_CLOCK_SKEW")
    if mark.nextFundingTime <= mark.time:
        raise IntegrityError("BINANCE_EXPIRED_FUNDING_SCHEDULE")
    if funding_info is not None and (
        funding_info.symbol != instrument.symbol
        or funding_info_received is None
        or not 0 < funding_info_received <= mark_received
    ):
        raise IntegrityError("BINANCE_FUNDING_METADATA_IDENTITY_OR_AVAILABILITY")
    values: list[tuple[EventKind, int, int, dict[str, JsonValue]]] = [
        (
            "perpetual",
            mark.time,
            mark_received,
            {
                "mark_price": mark.markPrice,
                "index_price": mark.indexPrice,
                "price_units": instrument.quoteAsset,
            },
        ),
        (
            "funding",
            mark.time,
            mark_received,
            {
                "last_funding_rate": mark.lastFundingRate,
                "rate_kind": "venue_lastFundingRate_snapshot",
                "units": "native_funding_fraction",
                "funding_interval_minutes": funding_info.fundingIntervalHours * 60
                if funding_info
                else None,
                "funding_interval_basis": "explicit_fundingInfo"
                if funding_info
                else "NOT_RETURNED_NO_DEFAULT_ASSUMED",
                "funding_metadata_observed_at_ms": funding_info_received if funding_info else None,
                "adjusted_rate_cap": funding_info.adjustedFundingRateCap if funding_info else None,
                "adjusted_rate_floor": funding_info.adjustedFundingRateFloor
                if funding_info
                else None,
                "next_funding_time_ms": mark.nextFundingTime,
            },
        ),
        (
            "open_interest",
            oi.time,
            oi_received,
            {
                "open_interest": oi.openInterest,
                "units": "venue_native_quantity",
                "side_convention": "NOT_VERIFIED",
                "normalization_status": "BLOCKED_PENDING_UNIT_AND_SIDE_VERIFICATION",
            },
        ),
    ]
    return [
        make_event(
            source="binance",
            environment="public_reference",
            asset=instrument.baseAsset,
            symbol=instrument.symbol,
            kind=kind,
            event_time_ms=ts,
            observed_at_ms=received,
            payload={
                **payload,
                "instrument": instrument.model_dump(mode="json"),
                "timestamp_basis": "venue snapshot time; first-publication time unavailable",
            },
        )
        for kind, ts, received, payload in values
    ]


class BinanceCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink

    async def request(
        self, session: aiohttp.ClientSession, endpoint: Endpoint, params: dict[str, str]
    ) -> tuple[JsonValue, int]:
        if endpoint not in {
            "exchangeInfo",
            "premiumIndex",
            "openInterest",
            "fundingInfo",
            "fundingRate",
            "takerlongshortRatio",
            "globalLongShortAccountRatio",
        }:
            raise ValueError("unsupported public endpoint")
        path = (
            "futures/data/"
            if endpoint in {"takerlongshortRatio", "globalLongShortAccountRatio"}
            else "fapi/v1/"
        )
        async with session.get(
            "https://fapi.binance.com/" + path + endpoint, params=params, allow_redirects=False
        ) as response:
            if response.status in {401, 403, 451}:
                raise FeedUnavailable("ACCESS_DENIED", response.status)
            if response.status in {418, 429}:
                raise FeedUnavailable("RATE_LIMITED", response.status)
            if response.status != 200:
                raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > 4_000_000:
                    raise IntegrityError("BINANCE_RESPONSE_TOO_LARGE")
            received = now_ms()
        raw: JsonValue = TypeAdapter(JsonValue).validate_json(body)
        if isinstance(raw, dict) and isinstance(raw.get("code"), int) and raw["code"] != 0:
            raise FeedUnavailable("API_UNAVAILABLE", int(str(raw["code"])))
        return raw, received

    async def health(self, asset: str, ok: bool, reason: str, code: int = 0) -> None:
        received = now_ms()
        await self.sink(
            make_event(
                source="binance",
                environment="public_reference",
                asset=asset,
                symbol="USDT_PERPETUAL",
                kind="feed_health",
                event_time_ms=received,
                observed_at_ms=received,
                timestamp_kind="receipt",
                payload={"ok": ok, "reason": reason, "code": code},
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        # Validate the entire discovery before emitting any market records.
        asset = "GLOBAL"
        try:
            raw, _ = await self.request(session, "exchangeInfo", {})
            discovery = Discovery.model_validate(raw)
            if len({r.symbol for r in discovery.symbols}) != len(discovery.symbols):
                raise IntegrityError("BINANCE_DUPLICATE_DISCOVERY")
            selected: dict[str, Instrument] = {}
            for row in discovery.symbols:
                if (
                    row.baseAsset in self.config.system.assets
                    and row.contractType == "PERPETUAL"
                    and row.status == "TRADING"
                    and row.quoteAsset == row.marginAsset == "USDT"
                ):
                    if row.baseAsset in selected:
                        raise IntegrityError("BINANCE_AMBIGUOUS_DISCOVERY")
                    selected[row.baseAsset] = row
            metadata: dict[str, FundingInfo] = {}
            metadata_received: int | None = None
            if selected:
                await asyncio.sleep(0.25)
                raw_metadata, metadata_received = await self.request(session, "fundingInfo", {})
                metadata = funding_metadata(raw_metadata)
            for asset in self.config.system.assets:
                instrument = selected.get(asset)
                if instrument is None:
                    # Sink failures must propagate, not be relabelled as source errors.
                    events = []
                else:
                    await asyncio.sleep(0.25)
                    mark, mark_received = await self.request(
                        session, "premiumIndex", {"symbol": instrument.symbol}
                    )
                    await asyncio.sleep(0.25)
                    oi, oi_received = await self.request(
                        session, "openInterest", {"symbol": instrument.symbol}
                    )
                    events = snapshots(
                        instrument,
                        mark,
                        mark_received,
                        oi,
                        oi_received,
                        metadata.get(instrument.symbol),
                        metadata_received,
                    )
                # Isolate publication from transport/data exception handling below.
                try:
                    for event in events:
                        await self.sink(event)
                    await self.health(
                        asset,
                        bool(events),
                        "SNAPSHOT_ACCEPTED" if events else "PERPETUAL_UNAVAILABLE",
                    )
                except Exception as exc:
                    raise PublicationFailure from exc
        except FeedUnavailable as exc:
            await self.health(asset, False, exc.reason, exc.code)
            return False
        except (aiohttp.ClientError, OSError, TimeoutError):
            await self.health(asset, False, "TRANSPORT_UNAVAILABLE")
            return False
        except (ValidationError, IntegrityError):
            await self.health(asset, False, "INVALID_DATA")
            return False
        return True

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            while True:
                ok = await self.collect(session)
                await asyncio.sleep(self.config.ingestion.binance_poll_interval_s if ok else 600)


class PublicationFailure(RuntimeError):
    """Consumer/storage failure; never retry it as an optional source outage."""
