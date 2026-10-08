"""Bounded cross-transport recovery plans. Raw receipt times are never rewritten."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent
from .spot_trades import stored_trade, trade_event
from .trade_backfill import RestTrade


@dataclass(frozen=True)
class TradeIdentity:
    native_id: int
    price: Decimal
    quantity: Decimal
    side: str
    timestamp: Decimal
    transport: str

    @classmethod
    def read(cls, event: MarketEvent) -> "TradeIdentity":
        if event.kind != "trade" or event.environment != "public_reference":
            raise IntegrityError("RECOVERY_INVALID_EVENT_KIND_OR_ENVIRONMENT")
        if event.source == "kraken":
            trade = stored_trade(event)
            if trade.timestamp.tzinfo is None:
                raise IntegrityError("RECOVERY_TIMESTAMP_WITHOUT_TIMEZONE")
            delta = trade.timestamp - datetime(1970, 1, 1, tzinfo=UTC)
            timestamp = (
                Decimal(delta.days * 86400 + delta.seconds)
                + Decimal(delta.microseconds) / 1_000_000
            )
            expected = trade_event(trade, event.asset, event.observed_at_ms)
            identity = cls(
                trade.trade_id,
                Decimal(str(trade.price)),
                Decimal(str(trade.qty)),
                trade.side,
                timestamp,
                "WS",
            )
        elif event.source == "kraken_rest":
            payload = event.payload()
            rest = RestTrade.model_validate(
                {
                    "price": payload.get("price_decimal"),
                    "qty": payload.get("qty_decimal"),
                    "timestamp": payload.get("source_timestamp_seconds"),
                    "side": payload.get("side"),
                    "trade_id": payload.get("trade_id"),
                }
            )
            expected = rest.event(event, event.observed_at_ms)
            identity = cls(rest.trade_id, rest.price, rest.qty, rest.side, rest.timestamp, "REST")
        else:
            raise IntegrityError("RECOVERY_UNSUPPORTED_SOURCE")
        if expected != event:
            raise IntegrityError("RECOVERY_EVENT_IDENTITY_MISMATCH")
        return identity

    def matches(self, other: "TradeIdentity") -> bool:
        tolerance = Decimal(0) if self.transport == other.transport else Decimal("0.000001")
        return (
            self.native_id == other.native_id
            and self.price == other.price
            and self.quantity == other.quantity
            and self.side == other.side
            and abs(self.timestamp - other.timestamp) <= tolerance
        )


@dataclass(frozen=True)
class RecoveryPlan:
    anchor_id: int
    through_id: int
    verified_at_ms: int
    selected: tuple[MarketEvent, ...]
    raw: tuple[MarketEvent, ...]

    def ready_rows(self, as_of_ms: int) -> tuple[MarketEvent, ...]:
        if as_of_ms < self.verified_at_ms:
            raise IntegrityError("RECOVERY_NOT_YET_VERIFIED")
        if any(
            row.event_time_ms > as_of_ms or row.observed_at_ms > as_of_ms for row in self.selected
        ):
            raise IntegrityError("LOOKAHEAD: recovery rows are unavailable at this time")
        return self.selected


class RecoveryBuffer:
    """One book and one gap. Overflow or any validation failure poisons the attempt."""

    def __init__(
        self,
        anchor: MarketEvent,
        through_id: int,
        max_events: int = 20000,
        max_bytes: int = 16_777_216,
        max_skew_ms: int = 5000,
    ) -> None:
        self.anchor = anchor
        self.identity = TradeIdentity.read(anchor)
        if (
            through_id <= self.identity.native_id
            or max_events < 1
            or max_bytes < 1
            or max_skew_ms < 0
        ):
            raise ValueError("recovery range and budgets must be valid and positive")
        self.through_id = through_id
        self.max_events, self.max_bytes, self.max_skew_ms = max_events, max_bytes, max_skew_ms
        self.raw: list[MarketEvent] = []
        self.identities: dict[int, dict[str, TradeIdentity]] = {}
        self.selected: dict[int, MarketEvent] = {}
        self.bytes_used = 0
        self.failed = False
        self.closed = False

    def add(self, event: MarketEvent) -> None:
        if self.failed or self.closed:
            raise IntegrityError("RECOVERY_BUFFER_UNAVAILABLE")
        try:
            size = len(event.model_dump_json().encode("utf-8"))
            if len(self.raw) >= self.max_events or self.bytes_used + size > self.max_bytes:
                raise IntegrityError("RECOVERY_BUFFER_OVERFLOW")
            identity = TradeIdentity.read(event)
            if (event.asset, event.symbol) != (self.anchor.asset, self.anchor.symbol):
                raise IntegrityError("RECOVERY_BOOK_MISMATCH")
            if event.observed_at_ms < self.anchor.observed_at_ms:
                raise IntegrityError("RECOVERY_OBSERVATION_BEFORE_ANCHOR")
            if identity.timestamp * 1000 > event.observed_at_ms + self.max_skew_ms:
                raise IntegrityError("RECOVERY_SOURCE_CLOCK_SKEW")
            identifier = identity.native_id
            if identifier == self.identity.native_id and not identity.matches(self.identity):
                raise IntegrityError("RECOVERY_ANCHOR_CONFLICT")
            if identifier >= self.identity.native_id:
                known = self.identities.setdefault(identifier, {})
                # Retain both transport identities: tolerance must not chain through
                # an intervening REST row to disguise conflicting WS timestamps.
                if any(not identity.matches(previous) for previous in known.values()):
                    raise IntegrityError("RECOVERY_CONFLICTING_TRADE")
                known[identity.transport] = identity
                if identifier > self.identity.native_id:
                    previous = self.selected.get(identifier)
                    if previous is None or (event.observed_at_ms, event.source, event.event_id) < (
                        previous.observed_at_ms,
                        previous.source,
                        previous.event_id,
                    ):
                        self.selected[identifier] = event
            self.raw.append(event)
            self.bytes_used += size
        except BaseException:
            self.failed = True
            raise

    def finish(self, verified_at_ms: int) -> RecoveryPlan:
        if self.failed or self.closed:
            raise IntegrityError("RECOVERY_BUFFER_UNAVAILABLE")
        try:
            high = max(self.selected, default=self.identity.native_id)
            if high < self.through_id or len(self.selected) != high - self.identity.native_id:
                raise IntegrityError("RECOVERY_SEQUENCE_INCOMPLETE")
            if verified_at_ms < max(
                [self.anchor.observed_at_ms, *(row.observed_at_ms for row in self.raw)]
            ):
                raise IntegrityError("RECOVERY_VERIFICATION_BACKDATED")
            result = RecoveryPlan(
                self.identity.native_id,
                high,
                verified_at_ms,
                tuple(self.selected[i] for i in sorted(self.selected)),
                tuple(self.raw),
            )
            self.closed = True
            return result
        except BaseException:
            self.failed = True
            raise


async def reconcile_while_receiving(
    buffer: RecoveryBuffer,
    receive: Callable[[], Awaitable[MarketEvent]],
    recover: Callable[[], Awaitable[list[MarketEvent]]],
    timeout_s: float = 60,
    clock: Callable[[], int] = now_ms,
) -> RecoveryPlan:
    """Keep receiving during a bounded fetch; never leave receiver/fetch tasks orphaned."""
    import math

    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("recovery deadline must be finite and positive")

    async def pump() -> None:
        while True:
            buffer.add(await receive())
            await asyncio.sleep(0)  # A ready fake/local receiver must not starve the fetch.

    async def fetch() -> list[MarketEvent]:
        return await recover()

    reader = asyncio.create_task(pump())
    fetcher = asyncio.create_task(fetch())
    try:
        async with asyncio.timeout(timeout_s):
            done, _ = await asyncio.wait([reader, fetcher], return_when=asyncio.FIRST_COMPLETED)
            if reader in done:
                reader.result()
            fetched = fetcher.result()
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
            if not reader.cancelled():
                reader.result()
            for index, row in enumerate(fetched):
                buffer.add(row)
                if index % 256 == 0:
                    await asyncio.sleep(0)
            available_at = max(
                buffer.anchor.observed_at_ms,
                buffer.anchor.event_time_ms,
                *(max(row.event_time_ms, row.observed_at_ms) for row in buffer.raw),
            )
            # Allowed source-clock skew is not permission to publish future data.
            # Wait on the actual clock within the original recovery deadline.
            while (verified_at := clock()) < available_at:  # noqa: ASYNC110 - wall-clock convergence has no event source
                await asyncio.sleep(min(0.1, (available_at - verified_at) / 1000))
            return buffer.finish(verified_at)
    except BaseException:
        buffer.failed = True
        raise
    finally:
        reader.cancel()
        fetcher.cancel()
        await asyncio.gather(reader, fetcher, return_exceptions=True)
