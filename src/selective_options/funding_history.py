"""Bounded settled-funding archives; historical event times never imply past availability."""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import aiohttp
from pydantic import Field

from .bybit import BybitCollector, Envelope, Instrument
from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel


class FundingRow(ExternalModel):
    symbol: str
    fundingRate: Decimal = Field(allow_inf_nan=False, max_digits=40, decimal_places=30)
    fundingRateTimestamp: int = Field(gt=0)


class FundingPage(ExternalModel):
    category: Literal["linear"]
    list: tuple[FundingRow, ...]


@dataclass(frozen=True)
class FundingArchive:
    events: tuple[MarketEvent, ...]
    pages: int
    start_ms: int
    end_ms: int
    # API exhaustion is not proof of a gap-free historical settlement schedule.
    termination: Literal["EMPTY_PAGE", "START_BOUND_REACHED"]


def parse_funding_page(
    instrument: Instrument,
    envelope: Envelope,
    received: int,
    start_ms: int,
    end_ms: int,
    page_size: int,
) -> tuple[MarketEvent, ...]:
    if not (
        instrument.contractType == "LinearPerpetual"
        and instrument.status == "Trading"
        and instrument.quoteCoin == instrument.settleCoin == "USDT"
        and not instrument.isPreListing
    ):
        raise IntegrityError("FUNDING_UNSUPPORTED_INSTRUMENT")
    rows = FundingPage.model_validate(envelope.result).list
    if len(rows) > page_size:
        raise IntegrityError("FUNDING_PAGE_SIZE")
    events: list[MarketEvent] = []
    previous = end_ms + 1
    for row in rows:
        ts = row.fundingRateTimestamp
        if row.symbol != instrument.symbol:
            raise IntegrityError("FUNDING_SYMBOL_MISMATCH")
        if not start_ms <= ts <= end_ms or ts > min(received, envelope.time):
            raise IntegrityError("FUNDING_OUTSIDE_TIME_WINDOW")
        if ts >= previous:
            raise IntegrityError("FUNDING_NOT_STRICTLY_DESCENDING")
        previous = ts
        rate = format(row.fundingRate, "f")
        # Canonical decimal string without context-dependent rounding or float conversion.
        if "." in rate:
            rate = rate.rstrip("0").rstrip(".")
        if row.fundingRate == 0:
            rate = "0"
        events.append(
            make_event(
                source="bybit_history",
                environment="public_reference",
                asset=instrument.baseCoin,
                symbol=instrument.symbol,
                kind="funding",
                event_time_ms=ts,
                observed_at_ms=received,
                native_id=str(ts),
                payload={
                    "funding_rate": rate,
                    "units": "fraction_per_settled_interval",
                    "rate_kind": "settled_history",
                    "settlement_currency": instrument.settleCoin,
                    "funding_interval_minutes": None,
                    "interval_basis": "historical interval unavailable; current metadata not applied",
                    "timestamp_basis": "venue settlement timestamp; publication time unavailable",
                },
            )
        )
    return tuple(events)


async def collect_funding_history(
    collector: BybitCollector,
    session: aiohttp.ClientSession,
    asset: str,
    *,
    start_ms: int,
    end_ms: int,
    page_size: int = 200,
    max_pages: int = 10,
    timeout_s: float = 90,
) -> FundingArchive:
    if (
        asset not in collector.config.system.assets
        or not 0 < start_ms <= end_ms <= now_ms()
        or not 1 <= page_size <= 200
        or not 1 <= max_pages <= 20
        or not 0 < timeout_s <= 300
    ):
        raise ValueError("invalid bounded funding-history request")
    async with asyncio.timeout(timeout_s):
        instrument, _ = await collector.discover(session, asset)
        if instrument is None:
            raise IntegrityError("FUNDING_INSTRUMENT_UNAVAILABLE")
        cursor = end_ms
        events: list[MarketEvent] = []
        for page_number in range(1, max_pages + 1):
            await asyncio.sleep(0.25)
            response, received = await collector.request(
                session,
                "funding/history",
                {
                    "category": "linear",
                    "symbol": instrument.symbol,
                    "startTime": str(start_ms),
                    "endTime": str(cursor),
                    "limit": str(page_size),
                },
            )
            rows = parse_funding_page(instrument, response, received, start_ms, cursor, page_size)
            events.extend(rows)
            if not rows or rows[-1].event_time_ms == start_ms:
                return FundingArchive(
                    tuple(events),
                    page_number,
                    start_ms,
                    end_ms,
                    "EMPTY_PAGE" if not rows else "START_BOUND_REACHED",
                )
            # Even a short page does not prove exhaustion. Ask once beyond its oldest row.
            cursor = rows[-1].event_time_ms - 1
        raise IntegrityError("FUNDING_HISTORY_TRUNCATED")
