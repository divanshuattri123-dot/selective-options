"""Bounded single-reader socket routing during an opted-in public trade repair."""

import asyncio
import json
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from pydantic import JsonValue
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import WebSocketException

from .bus import EventBus
from .collectors import now_ms
from .errors import IntegrityError, TradeGap
from .events import MarketEvent
from .gap_recovery import recover_trade_gap
from .recovery_runtime import activate_gap_repair, begin_gap_repair
from .spot_trades import (
    Frame,
    GapHandler,
    KrakenInstruments,
    KrakenTradeMessage,
    SpotTradeCollector,
    trade_event,
)
from .store import DuckStore
from .trade_reconciliation import TradeIdentity


@dataclass
class SavedFrame:
    trades: KrakenTradeMessage
    received: int
    consumed: set[int]


class FrameRouter:
    def __init__(
        self,
        collector: SpotTradeCollector,
        anchor: MarketEvent,
        max_frames: int = 1000,
        max_bytes: int = 16_777_216,
        max_trades: int = 20000,
    ) -> None:
        self.collector, self.anchor = collector, anchor
        self.anchor_id = TradeIdentity.read(anchor).native_id
        self.previous = deepcopy(collector.sequences[anchor.symbol])
        self.frames: list[SavedFrame] = []
        self.ready: deque[tuple[MarketEvent, SavedFrame, int]] = deque()
        self.max_frames, self.max_bytes, self.max_trades = max_frames, max_bytes, max_trades
        self.frame_count = self.byte_count = self.trade_count = 0

    def add(self, message: dict[str, JsonValue], received: int) -> None:
        self.frame_count += 1
        self.byte_count += len(json.dumps(message, separators=(",", ":")).encode())
        if self.frame_count > self.max_frames or self.byte_count > self.max_bytes:
            raise IntegrityError("RECOVERY_SOCKET_BUFFER_OVERFLOW")
        channel = message.get("channel")
        if channel == "instrument":
            updates = KrakenInstruments.model_validate(message.get("data"))
            if any(
                row.symbol in self.collector.symbols and row.status != "online"
                for row in updates.pairs
            ):
                raise IntegrityError("subscribed spot market is no longer online")
            return
        if channel in {"heartbeat", "status"}:
            return
        if channel != "trade" or message.get("method") is not None:
            raise IntegrityError("unexpected Kraken recovery stream message")
        parsed = KrakenTradeMessage.model_validate(message)
        self.trade_count += len(parsed.data)
        if self.trade_count > self.max_trades:
            raise IntegrityError("RECOVERY_SOCKET_BUFFER_OVERFLOW")
        frame = SavedFrame(parsed, received, set())
        self.frames.append(frame)
        for index, trade in enumerate(parsed.data):
            if trade.symbol not in self.collector.symbols or trade.timestamp.tzinfo is None:
                raise IntegrityError("RECOVERY_SOCKET_INVALID_TRADE")
            if (
                trade.timestamp.timestamp() * 1000
                > received + self.collector.config.data.max_clock_skew_s * 1000
            ):
                raise IntegrityError("CLOCK_SKEW: future spot trade")
            if trade.symbol != self.anchor.symbol:
                continue
            if trade.trade_id <= self.anchor_id:
                if not (
                    parsed.type == "snapshot"
                    and self.previous.seen
                    and trade.trade_id < next(iter(self.previous.seen))
                ):
                    self.previous.accept(trade)
                frame.consumed.add(index)
                continue
            event = trade_event(trade, self.anchor.asset, received)
            self.ready.append((event, frame, index))

    def take(self) -> MarketEvent:
        event, frame, index = self.ready.popleft()
        frame.consumed.add(index)
        return event

    def deferred(self) -> list[Frame]:
        output: list[Frame] = []
        for frame in self.frames:
            rows: list[JsonValue] = [
                trade.model_dump(mode="json")
                for index, trade in enumerate(frame.trades.data)
                if index not in frame.consumed
            ]
            if rows:
                output.append(
                    ({"channel": "trade", "type": frame.trades.type, "data": rows}, frame.received)
                )
        return output


def socket_gap_handler(
    collector: SpotTradeCollector, bus: EventBus, store: DuckStore, output: Path
) -> GapHandler:
    attempt = 0

    async def repair(
        socket: ClientConnection, message: dict[str, JsonValue], received: int, gap: TradeGap
    ) -> list[Frame]:
        nonlocal attempt
        attempt += 1
        try:
            # During storage/delivery awaits, websockets' bounded frame queue
            # provides backpressure. During fetch, only the recovery pump reads.
            async with asyncio.timeout(
                collector.config.ingestion.kraken_backfill_timeout_s + 2 * bus.timeout_s + 10
            ):
                anchor = await begin_gap_repair(collector, bus, store, gap.symbol)
                router = FrameRouter(collector, anchor)
                router.add(message, received)
                initial = tuple(router.take() for _ in range(len(router.ready)))
                if not initial:
                    raise IntegrityError("RECOVERY_SOCKET_EMPTY_FORWARD_GAP")
                through = max(TradeIdentity.read(row).native_id for row in initial)

                async def receive() -> MarketEvent:
                    while not router.ready:
                        buffered = await collector.take_pending_frame()
                        if buffered is not None:
                            frame, observed = buffered
                        else:
                            frame = await collector._read(socket)
                            observed = now_ms()
                        router.add(frame, observed)
                    return router.take()

                result = await recover_trade_gap(
                    collector.config,
                    store,
                    anchor,
                    through,
                    receive,
                    output / f"repair-{attempt:04d}-{anchor.asset}",
                    initial,
                )
                await activate_gap_repair(collector, bus, store, anchor, result.marker.event_id)
                return router.deferred()
        except (OSError, TimeoutError, WebSocketException) as exc:
            # A partly attempted repair must not reconnect and silently clear its
            # suspension; durable evidence remains available for explicit restart.
            raise IntegrityError("RECOVERY_SOCKET_TRANSPORT_OR_DEADLINE_FAILURE") from exc

    return repair
