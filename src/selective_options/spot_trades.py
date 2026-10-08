"""Kraken v2 trades and bars with full local validation before publication.

Sink calls are not an atomic persistence transaction or a durable commit barrier.
An interrupted publication can have delivered a prefix; this collector then stops
and retains its previous local frontier. Restart/recovery must use actual persisted
records instead of retrying the ambiguous batch on this collector instance.
"""

import asyncio
import json
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from copy import deepcopy
from datetime import datetime
from typing import Literal

from pydantic import Field, JsonValue, TypeAdapter
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .bars import TradeBars
from .collectors import Sink, now_ms, status
from .config import AppConfig
from .errors import IntegrityError, TradeGap
from .events import MarketEvent, make_event
from .models import ExternalModel

Frame = tuple[dict[str, JsonValue], int]
GapHandler = Callable[
    [ClientConnection, dict[str, JsonValue], int, TradeGap], Awaitable[list[Frame]]
]


class KrakenInstrument(ExternalModel):
    symbol: str
    base: str
    quote: str
    status: str


class KrakenInstruments(ExternalModel):
    pairs: tuple[KrakenInstrument, ...]


class KrakenTrade(ExternalModel):
    symbol: str
    side: Literal["buy", "sell"]
    price: float = Field(gt=0)
    qty: float = Field(gt=0)
    trade_id: int = Field(ge=0)
    timestamp: datetime


class KrakenTradeMessage(ExternalModel):
    channel: Literal["trade"]
    type: Literal["snapshot", "update"]
    data: tuple[KrakenTrade, ...]


class KrakenAck(ExternalModel):
    method: Literal["subscribe"]
    success: bool
    req_id: int
    result: dict[str, JsonValue] | None = None


def stored_trade(event: MarketEvent) -> KrakenTrade:
    """Reconstruct the exchange fields used by the sequence fingerprint."""
    return KrakenTrade.model_validate(
        event.payload()
        | {"symbol": event.symbol, "timestamp": event.payload().get("source_timestamp")}
    )


def trade_event(trade: KrakenTrade, asset: str, received: int) -> MarketEvent:
    return make_event(
        source="kraken",
        environment="public_reference",
        asset=asset,
        symbol=trade.symbol,
        kind="trade",
        event_time_ms=int(trade.timestamp.timestamp() * 1000),
        observed_at_ms=received,
        native_id=str(trade.trade_id),
        payload={
            "trade_id": trade.trade_id,
            "price": trade.price,
            "qty": trade.qty,
            "side": trade.side,
            "quote_currency": "USD",
            "source_timestamp": trade.timestamp.isoformat(),
        },
    )


class TradeSequence:
    def __init__(self, capacity: int = 2000) -> None:
        self.last: int | None = None
        self.seen: OrderedDict[int, str] = OrderedDict()
        self.capacity = capacity

    def accept(self, trade: KrakenTrade) -> bool:
        fingerprint = trade.model_dump_json()
        if trade.trade_id in self.seen:
            if self.seen[trade.trade_id] != fingerprint:
                raise IntegrityError("CONFLICTING_TRADE_ID")
            return False
        if self.last is not None and trade.trade_id != self.last + 1:
            raise TradeGap(trade.symbol, self.last + 1, trade.trade_id)
        self.last = trade.trade_id
        self.seen[trade.trade_id] = fingerprint
        if len(self.seen) > self.capacity:
            self.seen.popitem(last=False)
        return True


class SpotTradeCollector:
    def __init__(
        self,
        config: AppConfig,
        sink: Sink,
        checkpoints: tuple[MarketEvent, ...] = (),
        *,
        track_bar_inputs: bool = False,
        bar_input_limit: int = 10000,
    ) -> None:
        if type(track_bar_inputs) is not bool:
            raise ValueError("bar input tracking requires an explicit boolean")
        if type(bar_input_limit) is not int or not 1 <= bar_input_limit <= 100000:
            raise ValueError("bar input tracking limit must be in 1..100000")
        self.config, self.sink = config, sink
        self.symbols: dict[str, str] = {}
        self.sequences: dict[str, TradeSequence] = {}
        self.bars: dict[str, TradeBars] = {}
        self.track_bar_inputs = track_bar_inputs
        self.bar_input_limit = bar_input_limit
        self.bar_inputs: dict[str, tuple[MarketEvent, ...]] = {}
        self.connections = 0
        self.last_events: dict[str, MarketEvent] = {}
        self.repairing: set[str] = set()
        self.recovery_lock = asyncio.Lock()
        self.gap_handler: GapHandler | None = None
        self.pending_frames: deque[Frame] = deque()
        self.checkpoint_assets: dict[str, str] = {}
        self._emission_failed = False
        for event in checkpoints:
            self.sequences.setdefault(event.symbol, TradeSequence()).accept(stored_trade(event))
            self.checkpoint_assets[event.symbol] = event.asset
            self.last_events[event.symbol] = event
        self.awaiting_snapshot = set(self.sequences)

    async def _read(self, socket: ClientConnection) -> dict[str, JsonValue]:
        async with asyncio.timeout(self.config.system.heartbeat_timeout_s):
            return TypeAdapter(dict[str, JsonValue]).validate_json(await socket.recv())

    async def _discover(self, socket: ClientConnection) -> None:
        await socket.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "params": {"channel": "instrument", "snapshot": True},
                    "req_id": 1,
                }
            )
        )
        acknowledged = False
        instruments: KrakenInstruments | None = None
        async with asyncio.timeout(self.config.venue.request_timeout_s):
            while not acknowledged or instruments is None:
                message = await self._read(socket)
                if message.get("method") == "subscribe":
                    ack = KrakenAck.model_validate(message)
                    if (
                        not ack.success
                        or ack.req_id != 1
                        or not ack.result
                        or ack.result.get("channel") != "instrument"
                    ):
                        raise IntegrityError("instrument subscription rejected or mismatched")
                    acknowledged = True
                elif message.get("channel") == "instrument" and message.get("type") == "snapshot":
                    instruments = KrakenInstruments.model_validate(message.get("data"))
                elif message.get("channel") not in {"heartbeat", "status"}:
                    raise IntegrityError("unexpected instrument discovery response")
        self.symbols = {
            row.symbol: row.base
            for row in instruments.pairs
            if row.base in self.config.system.assets
            and row.quote == "USD"
            and row.status == "online"
        }
        if not self.symbols:
            raise IntegrityError("NO_SPOT_TRADE_SYMBOLS")
        if any(
            self.symbols.get(symbol) != asset for symbol, asset in self.checkpoint_assets.items()
        ):
            raise IntegrityError("CHECKPOINT_MARKET_UNAVAILABLE")
        await status(self.sink, "kraken", "TRADE_PRODUCTS_DISCOVERED", symbols=list(self.symbols))

    async def _trades(self, message: dict[str, JsonValue], received: int) -> None:
        if self._emission_failed:
            raise IntegrityError("SPOT_EMISSION_OUTCOME_UNKNOWN")
        parsed = KrakenTradeMessage.model_validate(message)
        # Sort snapshots by native sequence; live updates retain exchange order.
        trades = (
            sorted(parsed.data, key=lambda row: (row.symbol, row.trade_id))
            if parsed.type == "snapshot"
            else parsed.data
        )
        # Validate the entire batch on copies before accepting any part of a broken gap.
        candidate = deepcopy(self.sequences)
        last_events = dict(self.last_events)
        candidate_bars = dict(self.bars)
        candidate_inputs = {symbol: list(rows) for symbol, rows in self.bar_inputs.items()}
        input_ids = {event.event_id for rows in candidate_inputs.values() for event in rows}
        copied_builders: set[str] = set()
        staged: list[MarketEvent] = []

        async def stage_status(event: MarketEvent) -> None:
            staged.append(event)

        accepted: list[KrakenTrade] = []
        snapshot_symbols = (
            {trade.symbol for trade in trades} if parsed.type == "snapshot" else set()
        )
        recovering = {
            symbol
            for symbol in snapshot_symbols & self.awaiting_snapshot
            if symbol in self.sequences
        }
        for trade in trades:
            if trade.symbol in self.repairing:
                raise IntegrityError("SPOT_BOOK_RECOVERY_IN_PROGRESS")
            if trade.symbol not in self.symbols:
                raise IntegrityError("unrequested Kraken trade symbol")
            if trade.timestamp.tzinfo is None:
                raise IntegrityError("trade timestamp lacks timezone")
            timestamp = int(trade.timestamp.timestamp() * 1000)
            if timestamp > received + self.config.data.max_clock_skew_s * 1000:
                raise IntegrityError("CLOCK_SKEW: future spot trade")
            if parsed.type != "snapshot" and trade.symbol in self.awaiting_snapshot:
                raise IntegrityError("TRADE_UPDATE_BEFORE_RECOVERY_SNAPSHOT")
            sequence = candidate.setdefault(trade.symbol, TradeSequence())
            # A snapshot may predate the first trade ever recorded in this session.
            # Do not import or claim to validate that unobserved older prefix.
            if (
                parsed.type == "snapshot"
                and sequence.seen
                and trade.trade_id < next(iter(sequence.seen))
            ):
                continue
            if not sequence.accept(trade):
                continue
            accepted.append(trade)
        for symbol in snapshot_symbols:
            previous = self.sequences.get(symbol)
            if (
                previous
                and previous.last is not None
                and max(t.trade_id for t in trades if t.symbol == symbol) < previous.last
            ):
                raise IntegrityError("STALE_RECOVERY_SNAPSHOT")
        for trade in accepted:
            timestamp = int(trade.timestamp.timestamp() * 1000)
            event = trade_event(trade, self.symbols[trade.symbol], received)
            last_events[trade.symbol] = event
            if trade.symbol not in copied_builders:
                candidate_bars[trade.symbol] = (
                    deepcopy(self.bars[trade.symbol])
                    if trade.symbol in self.bars
                    else TradeBars(
                        self.config.ingestion.bar_timeframes_s,
                        received,
                        int(self.config.ingestion.bar_lateness_s * 1000),
                    )
                )
                copied_builders.add(trade.symbol)
            builder = candidate_bars[trade.symbol]
            # Old snapshots remain useful raw PIT records; never silently revise closed bars.
            if timestamp < builder.watermark_ms and parsed.type == "snapshot":
                if self.track_bar_inputs:
                    self._validate_staged_input_budget(input_ids, last_events, candidate_bars)
                staged.append(event)
                await status(
                    stage_status, "kraken", "HISTORICAL_TRADE_RAW_ONLY", symbol=trade.symbol
                )
                continue
            builder.add(event)
            if self.track_bar_inputs:
                input_ids.add(event.event_id)
                self._validate_staged_input_budget(input_ids, last_events, candidate_bars)
                candidate_inputs.setdefault(trade.symbol, []).append(event)
            staged.append(event)
        for symbol in snapshot_symbols & self.awaiting_snapshot:
            await status(
                stage_status,
                "kraken",
                "TRADE_SEQUENCE_RECOVERED"
                if symbol in recovering
                else "TRADE_SNAPSHOT_INITIALIZED",
                symbol=symbol,
                last_trade_id=candidate[symbol].last,
                recovered_trades=sum(t.symbol == symbol for t in accepted),
            )
        # Validate generated envelopes/payloads as a complete batch, including
        # statuses, before the first call to the actual sink. All builder.add calls
        # above affected copies; a later bar failure cannot alter the live frontier.
        frozen_inputs = {symbol: tuple(rows) for symbol, rows in candidate_inputs.items()}
        if self.track_bar_inputs:
            self._validate_input_budget(frozen_inputs, last_events, candidate_bars)
        self._validate_emissions(staged)
        await self._emit(staged)
        self.sequences = candidate
        self.last_events = last_events
        self.bars = candidate_bars
        self.bar_inputs = frozen_inputs
        self.awaiting_snapshot.difference_update(snapshot_symbols)

    async def _flush(self, received: int) -> None:
        if self._emission_failed:
            raise IntegrityError("SPOT_EMISSION_OUTCOME_UNKNOWN")
        candidate_bars = dict(self.bars)
        staged: list[MarketEvent] = []
        for symbol, builder in self.bars.items():
            if symbol in self.awaiting_snapshot or symbol in self.repairing:
                continue
            candidate = deepcopy(builder)
            staged.extend(candidate.close_ready(received))
            candidate_bars[symbol] = candidate
        self._validate_emissions(staged)
        candidate_inputs = (
            self._retained_inputs(self.bar_inputs, candidate_bars)
            if self.track_bar_inputs
            else self.bar_inputs
        )
        if self.track_bar_inputs:
            self._validate_input_budget(candidate_inputs, self.last_events, candidate_bars)
        await self._emit(staged)
        self.bars = candidate_bars
        self.bar_inputs = candidate_inputs

    @staticmethod
    def _retained_inputs(
        inputs: dict[str, tuple[MarketEvent, ...]], builders: dict[str, TradeBars]
    ) -> dict[str, tuple[MarketEvent, ...]]:
        """Union of open windows; long bars can retain inputs below the watermark."""
        return {
            symbol: tuple(
                event
                for event in inputs.get(symbol, ())
                if any(
                    opening <= event.event_time_ms < opening + interval
                    for interval, opening in builder.buckets
                )
            )
            for symbol, builder in builders.items()
        }

    def _validate_input_budget(
        self,
        inputs: dict[str, tuple[MarketEvent, ...]],
        tails: dict[str, MarketEvent],
        builders: dict[str, TradeBars],
    ) -> None:
        rows = {event.event_id for symbol in builders for event in inputs.get(symbol, ())} | {
            tails[symbol].event_id for symbol in builders if symbol in tails
        }
        if len(rows) > self.bar_input_limit:
            raise IntegrityError("BAR_CHECKPOINT_INPUT_BUDGET_EXCEEDED")

    def _validate_staged_input_budget(
        self,
        input_ids: set[str],
        tails: dict[str, MarketEvent],
        builders: dict[str, TradeBars],
    ) -> None:
        # Only the small per-book tail set is added on each iteration. Avoid
        # repeatedly copying the retained ledger before an oversized batch refuses.
        extra = {
            tails[symbol].event_id
            for symbol in builders
            if symbol in tails and tails[symbol].event_id not in input_ids
        }
        if len(input_ids) + len(extra) > self.bar_input_limit:
            raise IntegrityError("BAR_CHECKPOINT_INPUT_BUDGET_EXCEEDED")

    @staticmethod
    def _validate_emissions(events: list[MarketEvent]) -> None:
        for event in events:
            # Revalidate even a model constructed/copy-updated outside normal
            # make_event validation. Bar interval/OHLC/availability checks are the
            # canonical MarketEvent/ClosedBarPayload checks, not new bar semantics.
            validated = MarketEvent.model_validate(event.model_dump(), strict=True)
            json.dumps(validated.payload(), allow_nan=False)

    async def _emit(self, events: list[MarketEvent]) -> None:
        try:
            for event in events:
                await self.sink(event)
        except asyncio.CancelledError:
            self._emission_failed = True
            raise
        except Exception as exc:
            self._emission_failed = True
            # An OSError/TimeoutError from a sink must never be mistaken for a
            # reconnectable websocket transport failure after possible delivery.
            raise IntegrityError("SPOT_EMISSION_FAILED") from exc

    async def run(self) -> None:
        if self._emission_failed:
            raise IntegrityError("SPOT_EMISSION_OUTCOME_UNKNOWN")
        while True:
            try:
                async with connect(
                    "wss://ws.kraken.com/v2",
                    open_timeout=self.config.venue.connect_timeout_s,
                    close_timeout=2,
                    max_size=self.config.venue.max_message_bytes,
                    max_queue=16,
                    ping_interval=20,
                    ping_timeout=20,
                ) as socket:
                    self.connections += 1
                    await self._discover(socket)
                    self.awaiting_snapshot.update(self.symbols)
                    await socket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "params": {
                                    "channel": "trade",
                                    "symbol": list(self.symbols),
                                    "snapshot": True,
                                },
                                "req_id": 2,
                            }
                        )
                    )
                    pending = set(self.symbols)
                    deadline = (
                        asyncio.get_running_loop().time() + self.config.venue.request_timeout_s
                    )
                    while True:
                        if self.pending_frames:
                            message, received = self.pending_frames.popleft()
                        else:
                            message = await self._read(socket)
                            received = now_ms()
                        if message.get("method") == "subscribe":
                            ack = KrakenAck.model_validate(message)
                            if (
                                not ack.success
                                or ack.req_id != 2
                                or not ack.result
                                or ack.result.get("channel") != "trade"
                            ):
                                raise IntegrityError("trade subscription rejected or mismatched")
                            symbol = ack.result.get("symbol")
                            if not isinstance(symbol, str) or symbol not in pending:
                                raise IntegrityError(
                                    "unexpected trade subscription acknowledgement"
                                )
                            pending.remove(symbol)
                            if not pending:
                                await status(
                                    self.sink,
                                    "kraken",
                                    "SPOT_TRADES_SUBSCRIBED",
                                    symbols=list(self.symbols),
                                )
                        elif message.get("channel") == "trade":
                            try:
                                await self._trades(message, received)
                            except TradeGap as gap:
                                if (
                                    self.gap_handler is None
                                    or pending
                                    or gap.received_id <= gap.expected_id
                                ):
                                    raise
                                deferred = await self.gap_handler(socket, message, received, gap)
                                self.pending_frames.extendleft(reversed(deferred))
                        elif message.get("channel") == "instrument":
                            updates = KrakenInstruments.model_validate(message.get("data"))
                            if any(
                                row.symbol in self.symbols and row.status != "online"
                                for row in updates.pairs
                            ):
                                raise IntegrityError("subscribed spot market is no longer online")
                        elif message.get("channel") not in {"heartbeat", "status"}:
                            raise IntegrityError("unexpected Kraken trade stream message")
                        if (
                            pending or self.awaiting_snapshot
                        ) and asyncio.get_running_loop().time() > deadline:
                            raise IntegrityError(
                                "incomplete trade subscription acknowledgement or recovery snapshot"
                            )
                        await self._flush(received)
            except IntegrityError as exc:
                try:
                    await status(self.sink, "kraken", "SPOT_TRADE_HALTED", detail=str(exc))
                except Exception:
                    # The original fail-stop reason remains useful even if the
                    # already failing sink cannot accept the fatal status event.
                    raise exc from exc.__cause__
                raise
            except (OSError, TimeoutError, WebSocketException) as exc:
                self.pending_frames.clear()
                received = now_ms()
                for builder in self.bars.values():
                    builder.mark_gap(received)
                self.awaiting_snapshot.update(self.sequences)
                await status(
                    self.sink, "kraken", "SPOT_TRADE_DISCONNECTED", error_type=type(exc).__name__
                )
                await asyncio.sleep(self.config.ingestion.reconnect_delay_s)
