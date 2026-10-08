"""Read-only perpetual/funding snapshots and closed volatility-index candles."""

import asyncio
from typing import Literal

from pydantic import Field, JsonValue, TypeAdapter

from .collectors import Sink, now_ms, status
from .config import AppConfig
from .errors import IntegrityError, TransportError
from .events import MarketEvent, make_event
from .models import ExternalModel
from .rate_limit import EndpointLimiter
from .rpc import RpcClient, WebSocketRpc


class FutureInstrument(ExternalModel):
    instrument_name: str
    base_currency: str
    quote_currency: str
    settlement_currency: str
    kind: Literal["future"]
    settlement_period: str
    is_active: bool
    contract_size: float = Field(gt=0)
    instrument_type: Literal["linear", "reversed"]
    price_index: str


class FutureTicker(ExternalModel):
    instrument_name: str
    timestamp: int = Field(gt=0)
    mark_price: float = Field(gt=0)
    index_price: float = Field(gt=0)
    open_interest: float = Field(ge=0)
    best_bid_price: float | None = Field(default=None, ge=0)
    best_ask_price: float | None = Field(default=None, ge=0)
    current_funding: float | None = None
    funding_8h: float | None = None


class VolatilityResult(ExternalModel):
    data: tuple[tuple[int, float, float, float, float], ...]
    continuation: int | None


def volatility_events(
    raw: JsonValue, asset: str, start_ms: int, cutoff_ms: int, observed_ms: int
) -> list[MarketEvent]:
    result = VolatilityResult.model_validate(raw)
    if result.continuation is not None:
        raise IntegrityError("VOLATILITY_WINDOW_TRUNCATED: reduce window or implement pagination")
    events: list[MarketEvent] = []
    seen: set[int] = set()
    for timestamp, opening, high, low, close in result.data:
        if timestamp % 60_000 or timestamp in seen:
            raise IntegrityError("invalid or duplicate volatility candle timestamp")
        seen.add(timestamp)
        if not 0 <= low <= min(opening, close) <= max(opening, close) <= high:
            raise IntegrityError("invalid volatility candle OHLC")
        end = timestamp + 60_000
        # Ignore out-of-window and still-forming candles returned by the venue.
        if timestamp < start_ms or end > cutoff_ms or end > observed_ms:
            continue
        events.append(
            make_event(
                source="deribit",
                environment="testnet",
                asset=asset,
                symbol=f"{asset}:VOLATILITY_INDEX",
                kind="volatility_index",
                event_time_ms=end,
                observed_at_ms=observed_ms,
                payload={
                    "open_time_ms": timestamp,
                    "close_time_ms": end,
                    "resolution_s": 60,
                    "open": opening,
                    "high": high,
                    "low": low,
                    "close": close,
                    "units": "venue_native_volatility_index",
                    "closed": True,
                },
            )
        )
    return events


class DerivativesCollector:
    def __init__(self, config: AppConfig, sink: Sink, limiter: EndpointLimiter) -> None:
        self.config, self.sink, self.limiter = config, sink, limiter

    async def collect(self, rpc: RpcClient) -> None:
        raw = await rpc.call(
            "public/get_instruments", {"currency": "any", "kind": "future", "expired": False}
        )
        instruments = TypeAdapter(list[FutureInstrument]).validate_python(raw)
        selected = [
            row
            for row in instruments
            if row.is_active
            and row.settlement_period == "perpetual"
            and row.base_currency in self.config.system.assets
        ]
        if len({row.instrument_name for row in selected}) != len(selected):
            raise IntegrityError("duplicate perpetual discovery")
        for asset in self.config.system.assets:
            if not any(row.base_currency == asset for row in selected):
                await status(self.sink, "deribit", "PERPETUAL_UNAVAILABLE", asset=asset)
        for instrument in selected:
            raw_ticker = await rpc.call(
                "public/ticker", {"instrument_name": instrument.instrument_name}
            )
            received = now_ms()
            ticker = FutureTicker.model_validate(raw_ticker)
            if ticker.instrument_name != instrument.instrument_name:
                raise IntegrityError("perpetual ticker identity mismatch")
            if (
                ticker.best_bid_price
                and ticker.best_ask_price
                and ticker.best_bid_price > ticker.best_ask_price
            ):
                raise IntegrityError("crossed perpetual quote")
            payload = TypeAdapter(dict[str, JsonValue]).validate_python(
                ticker.model_dump(mode="json")
            )
            payload["instrument"] = TypeAdapter(dict[str, JsonValue]).validate_python(
                instrument.model_dump(mode="json")
            )
            await self.sink(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset=instrument.base_currency,
                    symbol=instrument.instrument_name,
                    kind="perpetual",
                    event_time_ms=ticker.timestamp,
                    observed_at_ms=received,
                    payload=payload,
                )
            )
            if ticker.current_funding is None or ticker.funding_8h is None:
                await status(
                    self.sink,
                    "deribit",
                    "FUNDING_UNAVAILABLE",
                    asset=instrument.base_currency,
                    symbol=instrument.instrument_name,
                )
            else:
                await self.sink(
                    make_event(
                        source="deribit",
                        environment="testnet",
                        asset=instrument.base_currency,
                        symbol=instrument.instrument_name,
                        kind="funding",
                        event_time_ms=ticker.timestamp,
                        observed_at_ms=received,
                        payload={
                            "current_funding": ticker.current_funding,
                            "funding_8h": ticker.funding_8h,
                            "units": "venue_native_rate",
                            "quote_currency": instrument.quote_currency,
                            "settlement_currency": instrument.settlement_currency,
                        },
                    )
                )
        for asset in self.config.system.assets:
            # Availability is verified by returned data; no synthetic SOL/gold proxy.
            if asset not in {"BTC", "ETH"}:
                await status(self.sink, "deribit", "VOLATILITY_INDEX_NOT_CONFIGURED", asset=asset)
                continue
            cutoff = now_ms() // 60_000 * 60_000
            start = cutoff - self.config.ingestion.volatility_lookback_minutes * 60_000
            raw_volatility = await rpc.call(
                "public/get_volatility_index_data",
                {
                    "currency": asset,
                    "start_timestamp": start,
                    "end_timestamp": cutoff - 1,
                    "resolution": "60",
                },
            )
            rows = volatility_events(raw_volatility, asset, start, cutoff, now_ms())
            for event in rows:
                await self.sink(event)
            if not rows:
                await status(self.sink, "deribit", "VOLATILITY_INDEX_UNAVAILABLE", asset=asset)

    async def run(self) -> None:
        while True:
            try:
                async with WebSocketRpc(self.config.venue, self.limiter) as rpc:
                    await self.collect(rpc)
            except TransportError as exc:
                await status(
                    self.sink, "deribit", "DERIVATIVES_DISCONNECTED", error_type=type(exc).__name__
                )
            await asyncio.sleep(self.config.ingestion.derivatives_poll_interval_s)
