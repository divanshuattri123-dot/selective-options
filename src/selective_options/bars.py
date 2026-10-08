"""Locally aggregate trades into closed UTC bars without inventing quiet intervals."""

from dataclasses import dataclass
from decimal import Decimal

from pydantic import Field

from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel


class BarTrade(ExternalModel):
    price: float = Field(gt=0)
    qty: float = Field(gt=0)
    trade_id: int = Field(ge=0)


@dataclass
class Accumulator:
    prototype: MarketEvent
    opening_ms: int
    interval_ms: int
    first: tuple[int, int]
    last: tuple[int, int]
    open: float
    close: float
    high: float
    low: float
    volume: Decimal
    count: int
    complete: bool


class TradeBars:
    """One symbol/source/session. Feed only new, sequence-validated trades."""

    def __init__(
        self, timeframes_s: tuple[int, ...], coverage_start_ms: int, lateness_ms: int
    ) -> None:
        if (
            not timeframes_s
            or any(t <= 0 for t in timeframes_s)
            or len(set(timeframes_s)) != len(timeframes_s)
        ):
            raise ValueError("bar timeframes must be positive and unique")
        if lateness_ms < 0:
            raise ValueError("lateness must not be negative")
        self.timeframes = tuple(sorted(t * 1000 for t in timeframes_s))
        self.coverage_start_ms, self.lateness_ms = coverage_start_ms, lateness_ms
        self.watermark_ms = 0
        self.identity: tuple[str, str, str, str] | None = None
        self.buckets: dict[tuple[int, int], Accumulator] = {}

    def mark_gap(self, observed_at_ms: int) -> None:
        self.coverage_start_ms = observed_at_ms
        for bar in self.buckets.values():
            bar.complete = False

    def add(self, event: MarketEvent) -> None:
        if event.kind != "trade":
            raise IntegrityError("bar input must be a trade")
        identity = (event.source, event.environment, event.asset, event.symbol)
        if self.identity is not None and identity != self.identity:
            raise IntegrityError("bar source/instrument mismatch")
        if event.event_time_ms < self.watermark_ms:
            raise IntegrityError("LATE_TRADE: a finalized bar could require revision")
        self.identity = identity
        trade = BarTrade.model_validate(event.payload())
        key = (event.event_time_ms, trade.trade_id)
        for interval in self.timeframes:
            opening = event.event_time_ms // interval * interval
            bucket_key = (interval, opening)
            bar = self.buckets.get(bucket_key)
            if bar is None:
                self.buckets[bucket_key] = Accumulator(
                    event,
                    opening,
                    interval,
                    key,
                    key,
                    trade.price,
                    trade.price,
                    trade.price,
                    trade.price,
                    Decimal(str(trade.qty)),
                    1,
                    opening >= self.coverage_start_ms,
                )
                continue
            if key < bar.first:
                bar.first, bar.open = key, trade.price
            if key > bar.last:
                bar.last, bar.close = key, trade.price
            bar.high, bar.low = max(bar.high, trade.price), min(bar.low, trade.price)
            bar.volume += Decimal(str(trade.qty))
            bar.count += 1

    def close_ready(self, observed_at_ms: int) -> list[MarketEvent]:
        watermark = observed_at_ms - self.lateness_ms
        if watermark < self.watermark_ms:
            raise IntegrityError("bar clock moved backwards")
        self.watermark_ms = watermark
        output: list[MarketEvent] = []
        for key, bar in sorted(list(self.buckets.items())):
            closing = bar.opening_ms + bar.interval_ms
            if closing > watermark:
                continue
            row = bar.prototype
            output.append(
                make_event(
                    source=row.source,
                    environment=row.environment,
                    asset=row.asset,
                    symbol=row.symbol,
                    kind="bar",
                    event_time_ms=closing,
                    observed_at_ms=observed_at_ms,
                    payload={
                        "open_time_ms": bar.opening_ms,
                        "close_time_ms": closing,
                        "timeframe_s": bar.interval_ms // 1000,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": float(bar.volume),
                        "volume_unit": "base_asset",
                        "trade_count": bar.count,
                        "closed": True,
                        "complete": bar.complete,
                        "aggregation_version": 1,
                        "lateness_ms": self.lateness_ms,
                        "coverage_start_ms": self.coverage_start_ms,
                    },
                )
            )
            del self.buckets[key]
        return output
