"""Bounded public ascending settlement archives, with receipt-time observation identity."""

import asyncio
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Literal

import aiohttp
from pydantic import Field, JsonValue, StrictInt, StrictStr, TypeAdapter

from .binance import BinanceCollector, Discovery, Instrument
from .binance_taker import DecimalText
from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

SignedDecimalText = Annotated[
    str, Field(strict=True, pattern=r"^-?(0|[1-9][0-9]{0,29})(\.[0-9]{1,18})?$")
]


class Settlement(ExternalModel):
    symbol: StrictStr
    fundingRate: SignedDecimalText
    fundingTime: StrictInt = Field(gt=0)
    markPrice: DecimalText
    rateType: Literal["Regular", "Special"] | None = None


@dataclass(frozen=True)
class BinanceFundingArchive:
    events: tuple[MarketEvent, ...]
    pages: int
    start_ms: int
    end_ms: int
    termination: Literal["EMPTY_PAGE", "END_BOUND_REACHED"]


def parse_page(
    instrument: Instrument,
    raw: JsonValue,
    received: int,
    start_ms: int,
    end_ms: int,
    page_size: int,
) -> tuple[MarketEvent, ...]:
    if not (
        instrument.contractType == "PERPETUAL"
        and instrument.status == "TRADING"
        and instrument.quoteAsset == instrument.marginAsset == "USDT"
        and instrument.symbol == instrument.baseAsset + "USDT"
    ):
        raise IntegrityError("BINANCE_FUNDING_INSTRUMENT_SCOPE")
    if (
        any(type(v) is not int for v in (received, start_ms, end_ms, page_size))
        or received <= 0
        or not 0 < start_ms <= end_ms
        or not 1 <= page_size <= 1000
    ):
        raise IntegrityError("BINANCE_FUNDING_RECEIPT_CLOCK")
    rows = TypeAdapter(list[Settlement]).validate_python(raw)
    if len(rows) > page_size:
        raise IntegrityError("BINANCE_FUNDING_PAGE_SIZE")
    result = []
    prior = start_ms - 1
    for row in rows:
        if row.symbol != instrument.symbol:
            raise IntegrityError("BINANCE_FUNDING_SYMBOL_MISMATCH")
        if not start_ms <= row.fundingTime <= min(end_ms, received):
            raise IntegrityError("BINANCE_FUNDING_OUTSIDE_WINDOW")
        if row.fundingTime <= prior:
            raise IntegrityError("BINANCE_FUNDING_NOT_STRICTLY_ASCENDING")
        if Decimal(row.markPrice) <= 0:
            raise IntegrityError("BINANCE_FUNDING_NONPOSITIVE_MARK")
        prior = row.fundingTime
        result.append(
            make_event(
                source="binance_history",
                environment="public_reference",
                asset=instrument.baseAsset,
                symbol=instrument.symbol,
                kind="funding",
                event_time_ms=row.fundingTime,
                observed_at_ms=received,
                native_id=str(row.fundingTime),
                payload={
                    "funding_rate": row.fundingRate,
                    "mark_price_at_charge": row.markPrice,
                    "price_units": instrument.quoteAsset,
                    "units": "fraction_per_reported_charge",
                    "rate_kind": "settled_history",
                    "reported_rate_type": row.rateType,
                    "settlement_currency": instrument.marginAsset,
                    "funding_interval_minutes": None,
                    "interval_basis": "historical interval unavailable; current metadata not applied",
                    "observation_received_at_ms": received,
                    "instrument": instrument.model_dump(mode="json"),
                    "timestamp_basis": "venue funding timestamp and actual receipt; publication time unavailable",
                    "finality_verified": False,
                    "aggregation_policy": "poll observations are not independent settlements; select observed revisions before aggregation",
                },
            )
        )
    return tuple(result)


async def collect_binance_funding_history(
    collector: BinanceCollector,
    session: aiohttp.ClientSession,
    asset: str,
    *,
    start_ms: int,
    end_ms: int,
    page_size: int = 200,
    max_pages: int = 10,
    timeout_s: float = 90,
) -> BinanceFundingArchive:
    if (
        asset not in collector.config.system.assets
        or any(type(v) is not int for v in (start_ms, end_ms, page_size, max_pages))
        or not 0 < start_ms <= end_ms <= now_ms()
        or end_ms - start_ms > 31 * 86_400_000
        or not 1 <= page_size <= 1000
        or not 1 <= max_pages <= 20
        or type(timeout_s) not in (int, float)
        or not math.isfinite(timeout_s)
        or not 0 < timeout_s <= 300
    ):
        raise ValueError("invalid bounded Binance funding-history request")
    async with asyncio.timeout(timeout_s):
        raw, discovered = await collector.request(session, "exchangeInfo", {})
        discovery = Discovery.model_validate(raw)
        if type(discovered) is not int or discovered <= 0:
            raise IntegrityError("BINANCE_FUNDING_DISCOVERY_CLOCK")
        if len({r.symbol for r in discovery.symbols}) != len(discovery.symbols):
            raise IntegrityError("BINANCE_FUNDING_DUPLICATE_DISCOVERY")
        choices = [
            r
            for r in discovery.symbols
            if r.baseAsset == asset
            and r.contractType == "PERPETUAL"
            and r.status == "TRADING"
            and r.quoteAsset == r.marginAsset == "USDT"
        ]
        if len(choices) != 1 or choices[0].symbol != asset + "USDT":
            raise IntegrityError("BINANCE_FUNDING_DISCOVERY_SCOPE")
        instrument = choices[0]
        cursor, prior_receipt = start_ms, discovered
        events: list[MarketEvent] = []
        for page_number in range(1, max_pages + 1):
            await asyncio.sleep(0.25)
            raw, received = await collector.request(
                session,
                "fundingRate",
                {
                    "symbol": instrument.symbol,
                    "startTime": str(cursor),
                    "endTime": str(end_ms),
                    "limit": str(page_size),
                },
            )
            if type(received) is not int or received < prior_receipt:
                raise IntegrityError("BINANCE_FUNDING_RECEIPT_REGRESSION")
            rows = parse_page(instrument, raw, received, cursor, end_ms, page_size)
            events.extend(rows)
            if not rows or rows[-1].event_time_ms == end_ms:
                return BinanceFundingArchive(
                    tuple(events),
                    page_number,
                    start_ms,
                    end_ms,
                    "EMPTY_PAGE" if not rows else "END_BOUND_REACHED",
                )
            cursor = rows[-1].event_time_ms + 1
            prior_receipt = received
        raise IntegrityError("BINANCE_FUNDING_HISTORY_TRUNCATED")
