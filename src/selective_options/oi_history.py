"""Bounded native-cursor open-interest history, with explicit units and receipt times."""

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

Interval = Literal["5min", "15min", "30min", "1h", "4h", "1d"]
INTERVALS: dict[str, int] = {
    "5min": 300,
    "15min": 900,
    "30min": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class OIRow(ExternalModel):
    openInterest: Decimal = Field(ge=0, allow_inf_nan=False, max_digits=40, decimal_places=20)
    singleOpenInterest: Decimal | None = Field(
        default=None, ge=0, allow_inf_nan=False, max_digits=40, decimal_places=20
    )
    timestamp: int = Field(gt=0)


class OIPage(ExternalModel):
    category: Literal["linear"]
    symbol: str
    list: tuple[OIRow, ...]
    nextPageCursor: str = Field(max_length=4096)


@dataclass(frozen=True)
class OIArchive:
    events: tuple[MarketEvent, ...]
    pages: int
    start_ms: int
    end_ms: int
    termination: Literal["CURSOR_EXHAUSTED", "START_BOUND_REACHED"]


def decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def parse_oi_page(
    instrument: Instrument,
    response: Envelope,
    received: int,
    start_ms: int,
    end_ms: int,
    page_size: int,
    interval: Interval,
) -> tuple[tuple[MarketEvent, ...], str]:
    if (
        instrument.contractType != "LinearPerpetual"
        or instrument.status != "Trading"
        or instrument.quoteCoin != "USDT"
        or instrument.settleCoin != "USDT"
        or instrument.isPreListing
    ):
        raise IntegrityError("OI_UNSUPPORTED_INSTRUMENT")
    page = OIPage.model_validate(response.result)
    if page.symbol != instrument.symbol:
        raise IntegrityError("OI_SYMBOL_MISMATCH")
    if len(page.list) > page_size or (not page.list and page.nextPageCursor):
        raise IntegrityError("OI_INVALID_PAGE_SIZE_OR_EMPTY_CONTINUATION")
    previous = end_ms + 1
    events: list[MarketEvent] = []
    for row in page.list:
        if not start_ms <= row.timestamp <= end_ms or row.timestamp > min(received, response.time):
            raise IntegrityError("OI_OUTSIDE_TIME_WINDOW")
        if row.timestamp >= previous:
            raise IntegrityError("OI_NOT_STRICTLY_DESCENDING")
        if row.singleOpenInterest is not None and row.singleOpenInterest > row.openInterest:
            raise IntegrityError("OI_INCONSISTENT_SIDE_VALUES")
        previous = row.timestamp
        events.append(
            make_event(
                source="bybit_history",
                environment="public_reference",
                asset=instrument.baseCoin,
                symbol=instrument.symbol,
                kind="open_interest",
                event_time_ms=row.timestamp,
                observed_at_ms=received,
                native_id=f"{interval}:{row.timestamp}",
                payload={
                    "open_interest": decimal_text(row.openInterest),
                    "open_interest_units": instrument.baseCoin,
                    "open_interest_sides": "both",
                    "single_open_interest": decimal_text(row.singleOpenInterest)
                    if row.singleOpenInterest is not None
                    else None,
                    "single_open_interest_sides": "single",
                    "sampling_interval_s": INTERVALS[interval],
                    "timestamp_basis": "venue OI sample timestamp; publication time unavailable",
                },
            )
        )
    return tuple(events), page.nextPageCursor


async def collect_oi_history(
    collector: BybitCollector,
    session: aiohttp.ClientSession,
    asset: str,
    *,
    start_ms: int,
    end_ms: int,
    interval: Interval = "5min",
    page_size: int = 200,
    max_pages: int = 10,
    timeout_s: float = 90,
) -> OIArchive:
    if (
        asset not in collector.config.system.assets
        or interval not in INTERVALS
        or not 0 < start_ms <= end_ms <= now_ms()
        or not 1 <= page_size <= 200
        or not 1 <= max_pages <= 20
        or not 0 < timeout_s <= 300
    ):
        raise ValueError("invalid bounded OI-history request")
    async with asyncio.timeout(timeout_s):
        instrument, _ = await collector.discover(session, asset)
        if instrument is None:
            raise IntegrityError("OI_INSTRUMENT_UNAVAILABLE")
        params = {
            "category": "linear",
            "symbol": instrument.symbol,
            "intervalTime": interval,
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": str(page_size),
        }
        seen_cursors: set[str] = set()
        events: list[MarketEvent] = []
        upper = end_ms
        for page_number in range(1, max_pages + 1):
            await asyncio.sleep(0.25)
            response, received = await collector.request(session, "open-interest", params)
            rows, cursor = parse_oi_page(
                instrument, response, received, start_ms, upper, page_size, interval
            )
            events.extend(rows)
            if not cursor or (rows and rows[-1].event_time_ms == start_ms):
                return OIArchive(
                    tuple(events),
                    page_number,
                    start_ms,
                    end_ms,
                    "CURSOR_EXHAUSTED" if not cursor else "START_BOUND_REACHED",
                )
            if cursor in seen_cursors:
                raise IntegrityError("OI_REPEATED_CURSOR")
            seen_cursors.add(cursor)
            # The opaque cursor travels unchanged; source time must still advance backwards.
            params = {**params, "cursor": cursor}
            upper = rows[-1].event_time_ms - 1
        raise IntegrityError("OI_HISTORY_TRUNCATED")
