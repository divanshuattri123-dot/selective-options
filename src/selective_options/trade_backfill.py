"""Bounded public REST trade archives; no implicit WebSocket/bar-state repair."""

import asyncio
import json
import math
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal

import aiohttp
from pydantic import Field

from .archive import seal_archive
from .checkpoints import load_trade_checkpoints
from .collectors import now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel
from .spot_trades import stored_trade
from .store import DuckStore

ENDPOINT = "https://api.kraken.com/0/public/Trades"
MAX_BYTES = 1_048_576
RestRow = tuple[
    str,
    str,
    Decimal,
    Literal["b", "s"],
    Literal["l", "m"],
    str,
    Annotated[int, Field(strict=True, ge=0)],
]


class Envelope(ExternalModel):
    error: tuple[str, ...]
    result: dict[str, str | tuple[RestRow, ...]] = {}


class RestTrade(ExternalModel):
    price: Decimal = Field(gt=0)
    qty: Decimal = Field(gt=0)
    timestamp: Decimal = Field(gt=0)
    side: Literal["buy", "sell"]
    trade_id: int = Field(ge=0)

    def event(self, anchor: MarketEvent, observed: int) -> MarketEvent:
        if not math.isfinite(float(self.price)) or not math.isfinite(float(self.qty)):
            raise IntegrityError("REST_TRADE_NUMERIC_OVERFLOW")
        return make_event(
            source="kraken_rest",
            environment="public_reference",
            asset=anchor.asset,
            symbol=anchor.symbol,
            kind="trade",
            native_id=str(self.trade_id),
            event_time_ms=int(self.timestamp * 1000),
            observed_at_ms=observed,
            payload={
                "trade_id": self.trade_id,
                "price": float(self.price),
                "qty": float(self.qty),
                "side": self.side,
                "quote_currency": "USD",
                "price_decimal": str(self.price),
                "qty_decimal": str(self.qty),
                "source_timestamp_seconds": str(self.timestamp),
                "transport": "REST",
                "timestamp_precision": "as_returned",
            },
        )


def parse_page(body: bytes, symbol: str, limit: int) -> tuple[tuple[RestTrade, ...], str]:
    if len(body) > MAX_BYTES:
        raise IntegrityError("TRADE_HISTORY_RESPONSE_TOO_LARGE")
    data = Envelope.model_validate(json.loads(body, parse_float=Decimal))
    if data.error:
        raise IntegrityError("TRADE_HISTORY_PROVIDER_ERROR")
    if set(data.result) != {symbol, "last"}:
        raise IntegrityError("TRADE_HISTORY_PAIR_MISMATCH")
    rows, cursor = data.result[symbol], data.result["last"]
    if (
        not isinstance(rows, tuple)
        or len(rows) > limit
        or not isinstance(cursor, str)
        or not cursor.isascii()
        or not cursor.isdigit()
    ):
        raise IntegrityError("TRADE_HISTORY_INVALID_PAGE")
    trades = tuple(
        RestTrade(
            price=row[0],
            qty=row[1],
            timestamp=row[2],
            side="buy" if row[3] == "b" else "sell",
            trade_id=row[6],
        )
        for row in rows
    )
    if any(b.trade_id < a.trade_id for a, b in zip(trades, trades[1:], strict=False)):
        raise IntegrityError("TRADE_HISTORY_UNORDERED_PAGE")
    return trades, cursor


async def fetch_page(
    session: aiohttp.ClientSession, symbol: str, cursor: str, limit: int
) -> tuple[bytes, int]:
    async with session.get(
        ENDPOINT,
        params={"pair": symbol, "assetVersion": "1", "since": cursor, "count": str(limit)},
        allow_redirects=False,
    ) as response:
        if response.status != 200:
            raise IntegrityError("TRADE_HISTORY_HTTP_FAILURE")
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            body.extend(chunk)
            if len(body) > MAX_BYTES:
                raise IntegrityError("TRADE_HISTORY_RESPONSE_TOO_LARGE")
        return bytes(body), now_ms()


async def collect_range(
    config: AppConfig, anchor: MarketEvent, through_id: int, output: Path
) -> tuple[list[MarketEvent], int]:
    # Import here because the reconciliation schema itself uses RestTrade.
    from .trade_reconciliation import TradeIdentity

    trade = TradeIdentity.read(anchor)
    timestamp = trade.timestamp
    # Include the known anchor even if REST has sub-microsecond precision or a shared timestamp.
    cursor = str(max(0, int((timestamp - 1) * 1_000_000_000)))
    if (
        not 0
        < through_id - trade.native_id
        <= (config.ingestion.kraken_backfill_max_pages * config.ingestion.kraken_backfill_page_size)
    ):
        raise ValueError("requested trade range exceeds the page budget or is not positive")
    expected = trade.native_id + 1
    anchored = False
    seen: dict[int, RestTrade] = {}
    events: list[MarketEvent] = []
    previous_observation = anchor.observed_at_ms
    settings = config.ingestion
    async with asyncio.timeout(settings.kraken_backfill_timeout_s):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=config.venue.request_timeout_s)
        ) as session:
            for page in range(1, settings.kraken_backfill_max_pages + 1):
                if page > 1:
                    await asyncio.sleep(settings.kraken_backfill_interval_s)
                body, observed = await fetch_page(
                    session, anchor.symbol, cursor, settings.kraken_backfill_page_size
                )
                if observed < previous_observation:
                    raise IntegrityError("TRADE_HISTORY_OBSERVATION_REGRESSION")
                previous_observation = observed
                await asyncio.to_thread((output / f"page-{page:03}.json").write_bytes, body)
                rows, next_cursor = parse_page(
                    body, anchor.symbol, settings.kraken_backfill_page_size
                )
                if not rows or int(next_cursor) <= int(cursor):
                    raise IntegrityError("TRADE_HISTORY_CURSOR_STALLED")
                for row in rows:
                    if row.timestamp * 1000 > observed + config.data.max_clock_skew_s * 1000:
                        raise IntegrityError("TRADE_HISTORY_CLOCK_SKEW")
                    if row.trade_id in seen:
                        if seen[row.trade_id] != row:
                            raise IntegrityError("TRADE_HISTORY_CONFLICTING_ID")
                        continue
                    seen[row.trade_id] = row
                    if row.trade_id < trade.native_id:
                        continue
                    if row.trade_id == trade.native_id:
                        if (
                            row.price != trade.price
                            or row.qty != trade.quantity
                            or row.side != trade.side
                            or abs(row.timestamp - timestamp)
                            > (Decimal("0.000001") if trade.transport == "WS" else Decimal(0))
                        ):
                            raise IntegrityError("TRADE_HISTORY_ANCHOR_CONFLICT")
                        anchored = True
                        continue
                    if not anchored:
                        raise IntegrityError("TRADE_HISTORY_ANCHOR_MISSING")
                    if row.trade_id != expected:
                        raise IntegrityError("TRADE_HISTORY_SEQUENCE_GAP")
                    events.append(row.event(anchor, observed))
                    expected += 1
                    if row.trade_id == through_id:
                        return events, page
                cursor = next_cursor
    raise IntegrityError("TRADE_HISTORY_PAGE_LIMIT")


async def backfill_trades(
    config: AppConfig, database: Path, output: Path, symbol: str, through_id: int
) -> dict[str, object]:
    checkpoints = await load_trade_checkpoints(database, config.system.assets)
    matches = [row for row in checkpoints if row.symbol == symbol]
    if not matches:
        raise IntegrityError("TRADE_HISTORY_NO_SYMBOL_CHECKPOINT")
    anchor = matches[-1]
    after_id = stored_trade(anchor).trade_id
    if (
        not 0
        < through_id - after_id
        <= config.ingestion.kraken_backfill_max_pages * config.ingestion.kraken_backfill_page_size
    ):
        raise ValueError(
            "requested trade range must be positive and within the configured page budget"
        )
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    report: dict[str, object] = {
        "status": "FAILED",
        "symbol": symbol,
        "after_trade_id": after_id,
        "through_trade_id": through_id,
        "checkpoint_event_id": anchor.event_id,
        "started_at_ms": now_ms(),
        "trading_enabled": False,
        "stream_repaired": False,
        "bars_rebuilt": False,
        "source": "kraken_rest",
        "observation_policy": "actual complete HTTP response receipt",
        "max_pages": config.ingestion.kraken_backfill_max_pages,
        "page_size": config.ingestion.kraken_backfill_page_size,
    }
    try:
        await asyncio.to_thread(
            (output / "anchor.json").write_text, anchor.model_dump_json(indent=2), encoding="utf-8"
        )
        events, pages = await collect_range(config, anchor, through_id, output)
        store = await DuckStore.open(
            output / "events.duckdb", int(config.data.max_clock_skew_s * 1000)
        )
        try:
            await store.append(events)
            count = await store.export_parquet(output / "events.parquet")
            manifest = await seal_archive(output / "events.parquet", count)
        finally:
            await store.close()
        report.update(
            status="RANGE_VERIFIED", rows=count, pages=pages, parquet_sha256=manifest.sha256
        )
    except BaseException as exc:
        report.update(
            failure_type=type(exc).__name__,
            failure_reason=str(exc)
            if isinstance(exc, IntegrityError)
            else "See failure type; external error text omitted",
        )
        raise
    finally:
        report["ended_at_ms"] = now_ms()
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
    return report
