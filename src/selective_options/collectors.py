"""Read-only public collectors. Sources are never treated as interchangeable."""

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from time import perf_counter_ns
from typing import Literal

import aiohttp
from pydantic import Field, JsonValue, TypeAdapter
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .config import AppConfig
from .errors import IntegrityError, RpcError, TradingUnavailable, TransportError
from .events import EventKind, MarketEvent, make_event
from .models import ExternalModel, RpcEnvelope, Ticker
from .rate_limit import EndpointLimiter
from .rpc import WebSocketRpc
from .venue import DeribitAdapter

Sink = Callable[[MarketEvent], Awaitable[None]]
logger = logging.getLogger(__name__)
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])


def now_ms() -> int:
    return time.time_ns() // 1_000_000


async def status(sink: Sink, source: str, reason: str, **details: JsonValue) -> MarketEvent:
    timestamp = now_ms()
    logger.warning("%s source=%s", reason, source)
    event = make_event(
        source=source,
        environment="testnet" if source == "deribit" else "public_reference",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=timestamp,
        observed_at_ms=timestamp,
        timestamp_kind="receipt",
        payload={"reason": reason, **details},
    )
    await sink(event)
    return event


class BookPayload(ExternalModel):
    instrument_name: str
    timestamp: int = Field(gt=0)
    change_id: int = Field(ge=0)
    prev_change_id: int | None = None
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


class TradePayload(ExternalModel):
    instrument_name: str
    timestamp: int = Field(gt=0)
    trade_id: str
    trade_seq: int = Field(ge=0)
    price: float = Field(ge=0)
    amount: float = Field(gt=0)
    direction: Literal["buy", "sell"]


class IndexPayload(ExternalModel):
    timestamp: int = Field(gt=0)
    price: float = Field(gt=0)
    index_name: str


class NotificationParams(ExternalModel):
    channel: str
    data: JsonValue


class Notification(ExternalModel):
    jsonrpc: Literal["2.0"]
    method: Literal["subscription"]
    params: NotificationParams


class ChannelRegistry:
    def __init__(self) -> None:
        self.channels: dict[str, tuple[str, str, EventKind]] = {}
        self.book_changes: dict[str, int] = {}
        self.trade_sequences: dict[str, int] = {}
        self.universe_event_ids: list[str] = []

    def add_instrument(self, asset: str, symbol: str, index: str) -> None:
        self.channels[f"ticker.{symbol}.100ms"] = (asset, symbol, "ticker")
        self.channels[f"book.{symbol}.none.20.100ms"] = (asset, symbol, "book")
        self.channels[f"trades.{symbol}.100ms"] = (asset, symbol, "trade")
        self.channels[f"deribit_price_index.{index}"] = (asset, index, "index")

    def normalize(self, notification: Notification, observed_at_ms: int) -> list[MarketEvent]:
        channel, data = notification.params.channel, notification.params.data
        if channel not in self.channels:
            raise IntegrityError("unrequested subscription channel")
        asset, symbol, kind = self.channels[channel]
        events: list[MarketEvent] = []
        values = data if kind == "trade" and isinstance(data, list) else [data]
        for value in values:
            payload = _JSON_OBJECT.validate_python(value)
            native_id: str | None = None
            if kind == "ticker":
                parsed = Ticker.model_validate(payload)
                timestamp, actual_symbol = parsed.timestamp, parsed.instrument_name
            elif kind == "book":
                book = BookPayload.model_validate(payload)
                timestamp, actual_symbol = book.timestamp, book.instrument_name
                if any(price <= 0 or amount < 0 for price, amount in (*book.bids, *book.asks)):
                    raise IntegrityError("invalid book price/amount")
                if (
                    book.bids
                    and book.asks
                    and max(x[0] for x in book.bids) > min(x[0] for x in book.asks)
                ):
                    raise IntegrityError("crossed order book")
                previous = self.book_changes.get(symbol)
                # Grouped books are complete snapshots: skipped change IDs are valid.
                # A supplied incremental predecessor must still match exactly.
                if (
                    previous is not None
                    and book.prev_change_id is not None
                    and book.prev_change_id != previous
                ):
                    raise IntegrityError("SEQUENCE_GAP: book resynchronisation required")
                if previous is not None and book.change_id < previous:
                    raise IntegrityError("OUT_OF_ORDER_BOOK: resynchronisation required")
                self.book_changes[symbol] = book.change_id
                native_id = str(book.change_id)
            elif kind == "trade":
                trade = TradePayload.model_validate(payload)
                timestamp, actual_symbol = trade.timestamp, trade.instrument_name
                previous = self.trade_sequences.get(symbol)
                if previous is not None and trade.trade_seq > previous + 1:
                    raise IntegrityError("SEQUENCE_GAP: trade history requires backfill")
                if previous is not None and trade.trade_seq <= previous:
                    continue
                self.trade_sequences[symbol] = trade.trade_seq
                native_id = trade.trade_id
            else:
                index = IndexPayload.model_validate(payload)
                timestamp, actual_symbol = index.timestamp, index.index_name
            if actual_symbol != symbol:
                raise IntegrityError("subscription instrument identity mismatch")
            events.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset=asset,
                    symbol=symbol,
                    kind=kind,
                    event_time_ms=timestamp,
                    observed_at_ms=observed_at_ms,
                    payload=payload,
                    native_id=native_id,
                )
            )
        return events


async def discover_and_record(
    config: AppConfig, sink: Sink, limiter: EndpointLimiter
) -> ChannelRegistry:
    registry = ChannelRegistry()
    async with WebSocketRpc(config.venue, limiter) as rpc:
        adapter = DeribitAdapter(rpc)
        for asset in config.system.assets:
            chain = await adapter.option_chain(asset)
            metadata_ids: list[JsonValue] = []
            for row in chain.rows:
                metadata = make_event(
                    source="deribit",
                    environment="testnet",
                    asset=asset,
                    symbol=row.instrument.instrument_name,
                    kind="instrument",
                    event_time_ms=chain.observed_at_ms,
                    observed_at_ms=chain.observed_at_ms,
                    timestamp_kind="receipt",
                    payload=_JSON_OBJECT.validate_python(row.instrument.model_dump(mode="json")),
                )
                await sink(metadata)
                metadata_ids.append(metadata.event_id)
                if row.summary is not None:
                    await sink(
                        make_event(
                            source="deribit",
                            environment="testnet",
                            asset=asset,
                            symbol=row.instrument.instrument_name,
                            kind="chain_summary",
                            event_time_ms=row.summary.creation_timestamp,
                            observed_at_ms=chain.observed_at_ms,
                            payload=_JSON_OBJECT.validate_python(
                                row.summary.model_dump(mode="json")
                            ),
                        )
                    )
            ranked = sorted(
                chain.rows,
                key=lambda row: (
                    -(row.summary.open_interest if row.summary else 0),
                    -(row.summary.volume if row.summary else 0),
                    row.instrument.expiration_timestamp,
                    row.instrument.instrument_name,
                ),
            )
            cap = config.ingestion.max_stream_instruments_per_asset
            selected = ranked if cap is None else ranked[:cap]
            for row in selected:
                registry.add_instrument(
                    asset, row.instrument.instrument_name, row.instrument.price_index
                )
            manifest = await status(
                sink,
                "deribit",
                "UNIVERSE_DISCOVERED",
                asset=asset,
                available=len(ranked),
                selected=len(selected),
                coverage="FULL" if len(selected) == len(ranked) and ranked else "PARTIAL_OR_ABSENT",
                instruments=[row.instrument.instrument_name for row in chain.rows],
                selected_instruments=[row.instrument.instrument_name for row in selected],
                metadata_event_ids=metadata_ids,
            )
            registry.universe_event_ids.append(manifest.event_id)
    return registry


class DeribitCollector:
    def __init__(
        self, config: AppConfig, sink: Sink, limiter: EndpointLimiter | None = None
    ) -> None:
        if config.venue.ws_url != "wss://test.deribit.com/ws/api/v2":
            raise TradingUnavailable("collector only connects to testnet")
        self.config, self.sink = config, sink
        self.limiter = limiter or EndpointLimiter(config.rate_limits)
        self.connections = 0
        self.universe_refreshes = 0
        self.receive_phase = "IDLE"
        self.notification_frames = 0
        self.published_stream_events = 0
        self.receive_timing_ns = {"SOCKET_RECEIVE": 0, "NORMALIZE": 0, "SINK_PUBLISH": 0}

    @contextmanager
    def _measure_receive(
        self, phase: Literal["SOCKET_RECEIVE", "NORMALIZE", "SINK_PUBLISH"]
    ) -> Iterator[None]:
        self.receive_phase = phase
        started = perf_counter_ns()
        try:
            yield
        finally:
            self.receive_timing_ns[phase] += perf_counter_ns() - started

    def _timing_fields(
        self, before: dict[str, int], elapsed_ns: int | None
    ) -> dict[str, JsonValue]:
        cumulative: dict[str, JsonValue] = {
            key: value for key, value in self.receive_timing_ns.items()
        }
        response: dict[str, JsonValue] | None = (
            None
            if elapsed_ns is None
            else {key: value - before[key] for key, value in self.receive_timing_ns.items()}
        )
        return {
            "receive_timing_ns": cumulative,
            "response_timing_ns": response,
            "response_elapsed_ns": elapsed_ns,
        }

    async def receive_until_refresh(
        self, socket: ClientConnection, registry: ChannelRegistry
    ) -> None:
        """An independent monotonic deadline rotates even a continuously busy stream."""
        interval = self.config.ingestion.stream_universe_refresh_interval_s
        loop = asyncio.get_running_loop()
        deadline = None if interval is None else loop.time() + interval
        while True:
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return
            wait = self.config.system.heartbeat_timeout_s
            refresh_timer = remaining is not None and remaining <= wait
            timer = asyncio.timeout(wait if remaining is None else min(wait, remaining))
            try:
                async with timer:
                    await self._receive(socket, registry)
            except TimeoutError:
                # A timeout raised by the receive/sink itself is not a scheduled rotation.
                if timer.expired() and refresh_timer:
                    # Loop timers may fire one clock-resolution tick early on Windows.
                    # Recheck the monotonic deadline rather than report a false outage.
                    continue
                raise

    async def _receive(
        self, socket: ClientConnection, registry: ChannelRegistry, expected_id: int | None = None
    ) -> JsonValue:
        with self._measure_receive("SOCKET_RECEIVE"):
            raw = await socket.recv()
        received = now_ms()
        with self._measure_receive("NORMALIZE"):
            message = _JSON_OBJECT.validate_json(raw)
            if "id" in message:
                response = RpcEnvelope.model_validate(message)
                if expected_id is None or response.id != expected_id:
                    raise IntegrityError("unexpected stream RPC response")
                if response.error is not None:
                    raise RpcError("public/subscribe", response.error.code)
                self.receive_phase = "IDLE"
                return response.result
            notification = Notification.model_validate(message)
            self.notification_frames += 1
            events = registry.normalize(notification, received)
        for event in events:
            with self._measure_receive("SINK_PUBLISH"):
                await self.sink(event)
            self.published_stream_events += 1
        self.receive_phase = "IDLE"
        return None

    async def run(self) -> None:
        attempt = 0
        while True:
            phase = "DISCOVERY"
            request_id = None
            acknowledged_channels = 0
            acknowledged_batches = 0
            requested_channels = 0
            total_channels = 0
            timing_start = dict(self.receive_timing_ns)
            response_elapsed_ns: int | None = None
            self.receive_phase = "IDLE"
            try:
                registry = await discover_and_record(self.config, self.sink, self.limiter)
                if not registry.channels:
                    raise IntegrityError("NO_STREAM_CHANNELS")
                phase = "CONNECT"
                async with connect(
                    self.config.venue.ws_url,
                    open_timeout=self.config.venue.connect_timeout_s,
                    close_timeout=2,
                    max_size=self.config.venue.max_message_bytes,
                    ping_interval=20,
                    ping_timeout=20,
                ) as socket:
                    self.connections += 1
                    await status(self.sink, "deribit", "STREAM_RESYNC", connection=self.connections)
                    channels = list(registry.channels)
                    total_channels = len(channels)
                    batch_size = self.config.ingestion.subscription_batch_size
                    for start in range(0, len(channels), batch_size):
                        batch = channels[start : start + batch_size]
                        request_id = start + 1
                        requested_channels = len(batch)
                        timing_start = dict(self.receive_timing_ns)
                        response_elapsed_ns = None
                        phase = "SUBSCRIPTION_LIMITER"
                        await self.limiter.acquire("public/subscribe")
                        phase = "SUBSCRIPTION_SEND"
                        await socket.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "method": "public/subscribe",
                                    "params": {"channels": batch},
                                }
                            )
                        )
                        phase = "SUBSCRIPTION_RESPONSE"
                        response_started_ns = perf_counter_ns()
                        try:
                            async with asyncio.timeout(self.config.venue.request_timeout_s):
                                while True:
                                    result = await self._receive(socket, registry, request_id)
                                    if result is not None:
                                        if set(
                                            TypeAdapter(list[str]).validate_python(result)
                                        ) != set(batch):
                                            raise IntegrityError(
                                                "incomplete subscription acknowledgement"
                                            )
                                        break
                        finally:
                            response_elapsed_ns = perf_counter_ns() - response_started_ns
                        acknowledged_channels += len(batch)
                        acknowledged_batches += 1
                        phase = "SUBSCRIPTION_PROGRESS_PUBLISH"
                        await status(
                            self.sink,
                            "deribit",
                            "STREAM_SUBSCRIPTION_BATCH_ACK",
                            connection=self.connections,
                            request_id=request_id,
                            batch_channels=len(batch),
                            acknowledged_batches=acknowledged_batches,
                            acknowledged_channels=acknowledged_channels,
                            total_channels=total_channels,
                            notification_frames=self.notification_frames,
                            published_stream_events=self.published_stream_events,
                            **self._timing_fields(timing_start, response_elapsed_ns),
                        )
                    instruments: list[JsonValue] = [
                        {"asset": asset, "symbol": symbol}
                        for asset, symbol, kind in registry.channels.values()
                        if kind == "ticker"
                    ]
                    phase = "SUBSCRIPTION_COMPLETE_PUBLISH"
                    await status(
                        self.sink,
                        "deribit",
                        "STREAM_SUBSCRIBED",
                        connection=self.connections,
                        channels=len(channels),
                        instruments=instruments,
                        universe_event_ids=list(registry.universe_event_ids),
                    )
                    attempt = 0
                    phase = "ACTIVE_RECEIVE"
                    request_id = None
                    requested_channels = 0
                    timing_start = dict(self.receive_timing_ns)
                    response_elapsed_ns = None
                    await self.receive_until_refresh(socket, registry)
                    self.universe_refreshes += 1
                    await status(
                        self.sink,
                        "deribit",
                        "STREAM_REFRESH_DUE",
                        connection=self.connections,
                        refresh=self.universe_refreshes,
                        continuity="UNVERIFIED_GAP_REQUIRES_HISTORY_RECONCILIATION",
                    )
            except (OSError, TimeoutError, WebSocketException, TransportError) as exc:
                attempt += 1
                await status(
                    self.sink,
                    "deribit",
                    "STREAM_DISCONNECTED",
                    error_type=type(exc).__name__,
                    connection=self.connections,
                    phase=phase,
                    receive_phase=self.receive_phase,
                    request_id=request_id,
                    requested_channels=requested_channels,
                    acknowledged_channels=acknowledged_channels,
                    acknowledged_batches=acknowledged_batches,
                    total_channels=total_channels,
                    notification_frames=self.notification_frames,
                    published_stream_events=self.published_stream_events,
                    **self._timing_fields(timing_start, response_elapsed_ns),
                )
                await asyncio.sleep(
                    min(
                        self.config.ingestion.reconnect_delay_s * 2 ** min(attempt - 1, 6),
                        self.config.venue.retry_max_s,
                    )
                )


class CoinbaseTicker(ExternalModel):
    price: float = Field(gt=0)
    time: datetime
    bid: float = Field(ge=0)
    ask: float = Field(ge=0)


class CoinbaseProduct(ExternalModel):
    id: str
    base_currency: str
    quote_currency: str
    status: str


class KrakenPair(ExternalModel):
    altname: str
    wsname: str | None = None


async def get_json(session: aiohttp.ClientSession, url: str) -> JsonValue:
    async with session.get(url) as response:
        response.raise_for_status()
        body = await response.text()
        if len(body) > 20_000_000:
            raise IntegrityError("HTTP payload exceeds size bound")
        return TypeAdapter(JsonValue).validate_json(body)


class SpotCollector:
    def __init__(
        self, config: AppConfig, sink: Sink, source: Literal["coinbase", "kraken"]
    ) -> None:
        self.config, self.sink, self.source = config, sink, source

    async def discover(self, session: aiohttp.ClientSession) -> dict[str, str]:
        products: dict[str, str] = {}
        if self.source == "coinbase":
            raw = await get_json(session, "https://api.exchange.coinbase.com/products")
            for item in TypeAdapter(list[CoinbaseProduct]).validate_python(raw):
                if (
                    item.base_currency in self.config.system.assets
                    and item.quote_currency == "USD"
                    and item.status == "online"
                ):
                    products[item.base_currency] = item.id
        else:
            raw = _JSON_OBJECT.validate_python(
                await get_json(session, "https://api.kraken.com/0/public/AssetPairs")
            )
            if raw.get("error"):
                raise IntegrityError("Kraken discovery returned an error")
            for pair in (
                TypeAdapter(dict[str, KrakenPair]).validate_python(raw.get("result")).values()
            ):
                if pair.wsname and pair.wsname.endswith("/USD"):
                    base = pair.wsname.split("/")[0]
                    asset = "BTC" if base == "XBT" else base
                    if asset in self.config.system.assets:
                        products[asset] = pair.altname
        return products

    async def poll(self, session: aiohttp.ClientSession, asset: str, product: str) -> MarketEvent:
        if self.source == "coinbase":
            raw = await get_json(
                session, f"https://api.exchange.coinbase.com/products/{product}/ticker"
            )
            received = now_ms()
            ticker = CoinbaseTicker.model_validate(raw)
            if ticker.time.tzinfo is None:
                raise IntegrityError("Coinbase timestamp lacks timezone")
            return make_event(
                source=self.source,
                environment="public_reference",
                asset=asset,
                symbol=product,
                kind="spot",
                event_time_ms=int(ticker.time.timestamp() * 1000),
                observed_at_ms=received,
                payload={
                    "price": ticker.price,
                    "bid": ticker.bid,
                    "ask": ticker.ask,
                    "price_type": "last_trade",
                    "currency": "USD",
                },
            )
        raw = _JSON_OBJECT.validate_python(
            await get_json(session, f"https://api.kraken.com/0/public/Ticker?pair={product}")
        )
        received = now_ms()
        if raw.get("error"):
            raise IntegrityError("Kraken ticker returned an error")
        results = TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(raw.get("result"))
        if len(results) != 1:
            raise IntegrityError("unexpected Kraken ticker count")
        values = next(iter(results.values()))
        bid = TypeAdapter(list[float]).validate_python(values.get("b"))[0]
        ask = TypeAdapter(list[float]).validate_python(values.get("a"))[0]
        if not 0 < bid <= ask:
            raise IntegrityError("invalid Kraken spot quote")
        return make_event(
            source=self.source,
            environment="public_reference",
            asset=asset,
            symbol=product,
            kind="spot",
            event_time_ms=received,
            observed_at_ms=received,
            timestamp_kind="receipt",
            payload={
                "price": (bid + ask) / 2,
                "bid": bid,
                "ask": ask,
                "price_type": "mid",
                "currency": "USD",
                "source_timestamp_available": False,
            },
        )

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.venue.request_timeout_s)
        ) as session:
            while True:
                try:
                    products = await self.discover(session)
                    await status(
                        self.sink, self.source, "SPOT_PRODUCTS_DISCOVERED", assets=list(products)
                    )
                    while True:
                        for asset, product in products.items():
                            await self.sink(await self.poll(session, asset, product))
                        await asyncio.sleep(self.config.ingestion.spot_poll_interval_s)
                except (aiohttp.ClientError, TimeoutError) as exc:
                    await status(
                        self.sink, self.source, "SOURCE_UNAVAILABLE", error_type=type(exc).__name__
                    )
                    await asyncio.sleep(self.config.ingestion.spot_poll_interval_s)


async def poll_chains(config: AppConfig, sink: Sink, limiter: EndpointLimiter) -> None:
    while True:
        await asyncio.sleep(config.ingestion.chain_poll_interval_s)
        await discover_and_record(config, sink, limiter)
