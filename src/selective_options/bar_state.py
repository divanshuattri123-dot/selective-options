"""Bounded deterministic local accumulator state, without checkpoint provenance.

Encoding is not a database commit, a delivery acknowledgement, or a complete
capture checkpoint. Callers must independently bind the blob to committed source
records. Restoration preserves native prototypes and watermark but treats every
retained open bucket as incomplete across the restart boundary.

Codec policy accepts 1..32 distinct integer-second timeframes up to one day and
lateness up to one day, unlike the more general TradeBars constructor. Payload
inspection rejects nonfinite values and nesting deeper than 64. Explicit native
millisecond clocks (apart from fixed duration fields) and aware ISO
source_timestamp fields must be available at the encoded cutoff.
"""

import json
import math
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .bars import Accumulator, BarTrade, TradeBars
from .errors import IntegrityError
from .events import MarketEvent

MAX_CLOCK = (1 << 63) - 1
DEFAULT_MAX_BUCKETS = 10_000
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
_DURATIONS = {"interval_ms", "lateness_ms", "timeframe_ms", "duration_ms", "latency_ms"}


class BarScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    source: str = Field(min_length=1, max_length=256)
    environment: Literal["testnet", "public_reference"]
    asset: str = Field(min_length=1, max_length=256)
    symbol: str = Field(min_length=1, max_length=256)

    def identity(self) -> tuple[str, str, str, str]:
        return self.source, self.environment, self.asset, self.symbol


class BarPrototype(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str = Field(min_length=1, max_length=256)
    environment: Literal["testnet", "public_reference"]
    asset: str = Field(min_length=1, max_length=256)
    symbol: str = Field(min_length=1, max_length=256)
    kind: Literal["trade"]
    event_time_ms: int = Field(gt=0, le=MAX_CLOCK)
    observed_at_ms: int = Field(gt=0, le=MAX_CLOCK)
    timestamp_kind: Literal["source", "receipt"]
    schema_version: int = Field(ge=1, le=1)
    payload_json: str

    def event(self) -> MarketEvent:
        return MarketEvent.model_validate(self.model_dump(), strict=True)


class BarBucket(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)
    prototype: BarPrototype
    opening_ms: int = Field(ge=0, le=MAX_CLOCK)
    interval_ms: int = Field(ge=1000, le=86_400_000)
    first: tuple[int, int]
    last: tuple[int, int]
    open: float = Field(gt=0)
    close: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    volume_decimal: str = Field(min_length=1, max_length=1024)
    count: int = Field(ge=1, le=1_000_000_000)
    complete: bool

    def volume(self) -> Decimal:
        if not _DECIMAL.fullmatch(self.volume_decimal):
            raise ValueError("bar volume must use explicit finite positive decimal text")
        try:
            value = Decimal(self.volume_decimal)
        except InvalidOperation as exc:
            raise ValueError("invalid decimal bar volume") from exc
        if not value.is_finite() or value <= 0 or not math.isfinite(float(value)):
            raise ValueError("bar volume must be positive and emit as a finite float")
        return value

    @model_validator(mode="after")
    def coherent_bucket(self) -> Self:
        if self.interval_ms % 1000 or self.opening_ms % self.interval_ms:
            raise ValueError("bar interval/opening is not aligned")
        end = self.opening_ms + self.interval_ms
        for key in (self.first, self.last):
            if (
                any(type(value) is not int for value in key)
                or not self.opening_ms <= key[0] < end
                or not 0 < key[0] <= MAX_CLOCK
                or not 0 <= key[1] <= MAX_CLOCK
            ):
                raise ValueError("bar trade key has invalid types, clock or interval")
        if not self.first <= self.last:
            raise ValueError("bar first key follows its last key")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("bar OHLC is inconsistent")
        payload = _object(self.prototype.payload_json)
        # Standard generic trade aggregation fields only; no exchange identity or
        # committed-provenance reconstruction can be inferred from this codec.
        if type(payload.get("trade_id")) is not int:
            raise ValueError("prototype trade ID must be an integer")
        trade = BarTrade.model_validate(payload, strict=True)
        key = (self.prototype.event_time_ms, trade.trade_id)
        if not self.first <= key <= self.last:
            raise ValueError("prototype trade key lies outside retained first/last keys")
        if not self.low <= trade.price <= self.high:
            raise ValueError("prototype price lies outside bar range")
        if key == self.first and self.open != trade.price:
            raise ValueError("bar opening price conflicts with its prototype")
        if key == self.last and self.close != trade.price:
            raise ValueError("bar closing price conflicts with its prototype")
        volume = self.volume()
        if volume < Decimal(str(trade.qty)):
            raise ValueError("bar volume is smaller than its prototype quantity")
        if self.count == 1:
            if (
                self.first != self.last
                or self.first != key
                or len({self.open, self.high, self.low, self.close, trade.price}) != 1
                or volume != Decimal(str(trade.qty))
            ):
                raise ValueError("single-trade bucket is inconsistent")
        elif self.first == self.last:
            raise ValueError("multiple trades cannot have one identical sequence key")
        return self


class BarState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    format_version: int = Field(ge=1, le=1)
    as_of_ms: int = Field(gt=0, le=MAX_CLOCK)
    timeframes_ms: tuple[int, ...] = Field(min_length=1, max_length=32)
    lateness_ms: int = Field(ge=0, le=86_400_000)
    coverage_start_ms: int = Field(ge=0, le=MAX_CLOCK)
    watermark_ms: int = Field(ge=0, le=MAX_CLOCK)
    scope: BarScope | None
    buckets: tuple[BarBucket, ...] = Field(max_length=100_000)

    @model_validator(mode="after")
    def consistent_state(self) -> Self:
        if (
            any(
                type(value) is not int or not 1000 <= value <= 86_400_000 or value % 1000
                for value in self.timeframes_ms
            )
            or tuple(sorted(set(self.timeframes_ms))) != self.timeframes_ms
            or self.coverage_start_ms > self.as_of_ms
            or self.watermark_ms > self.as_of_ms
            or (self.buckets and self.scope is None)
        ):
            raise ValueError("invalid bar state timeframe, scope or availability clocks")
        previous = (-1, -1)
        for bucket in self.buckets:
            key = (bucket.interval_ms, bucket.opening_ms)
            if key <= previous or bucket.interval_ms not in self.timeframes_ms:
                raise ValueError("duplicate, unordered or unconfigured bucket")
            previous = key
            prototype = bucket.prototype
            if (
                self.scope is None
                or (prototype.source, prototype.environment, prototype.asset, prototype.symbol)
                != self.scope.identity()
            ):
                raise ValueError("bar prototype scope conflicts with builder scope")
            _payload_clocks(_object(prototype.payload_json), self.as_of_ms)
            if (
                prototype.event_time_ms > self.as_of_ms
                or prototype.observed_at_ms > self.as_of_ms
                or bucket.first[0] > self.as_of_ms
                or bucket.last[0] > self.as_of_ms
                or bucket.opening_ms + bucket.interval_ms <= self.watermark_ms
                or (bucket.complete and bucket.opening_ms < self.coverage_start_ms)
            ):
                raise ValueError("finished, unavailable or inconsistent complete bucket")
        return self


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _object(text: str) -> dict[str, object]:
    value = json.loads(text, object_pairs_hook=_unique_pairs, parse_constant=_constant)
    if not isinstance(value, dict):
        raise ValueError("bar state requires a JSON object")
    return value


def _constant(value: str) -> object:
    raise ValueError("nonfinite JSON constant")


def _payload_clocks(value: object, as_of_ms: int, depth: int = 0) -> None:
    """Audit explicit millisecond clocks and aware ISO source times to depth 64."""
    if depth > 64:
        raise ValueError("prototype payload nesting exceeds codec policy")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("nonfinite value in prototype payload")
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str):
                raise ValueError("prototype payload object requires string field names")
            if key.endswith("_ms") and key not in _DURATIONS:
                if type(nested) is not int or not 0 <= nested <= as_of_ms:
                    raise ValueError("nested millisecond clock is invalid or unavailable")
            elif key == "source_timestamp":
                if not isinstance(nested, str):
                    raise ValueError("source_timestamp must be an aware ISO datetime")
                timestamp = datetime.fromisoformat(nested)
                if (
                    timestamp.tzinfo is None
                    or not 0 < int(timestamp.timestamp() * 1000) <= as_of_ms
                ):
                    raise ValueError("nested source timestamp is invalid or unavailable")
            _payload_clocks(nested, as_of_ms, depth + 1)
    elif isinstance(value, list):
        for nested in value:
            _payload_clocks(nested, as_of_ms, depth + 1)


def _budgets(max_buckets: int, max_bytes: int) -> None:
    for name, value, maximum in (
        ("max_buckets", max_buckets, 100_000),
        ("max_bytes", max_bytes, 64 * 1024 * 1024),
    ):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an integer from 1 through {maximum}")


def _clock(value: int, name: str) -> None:
    if type(value) is not int or not 0 < value <= MAX_CLOCK:
        raise ValueError(f"{name} must be an explicit positive integer clock")


def encode_bar_state(
    builder: TradeBars,
    as_of_ms: int,
    *,
    max_buckets: int = DEFAULT_MAX_BUCKETS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> bytes:
    """Encode one validated local builder; this function establishes no durable commit."""
    _budgets(max_buckets, max_bytes)
    _clock(as_of_ms, "as_of_ms")
    if len(builder.buckets) > max_buckets:
        raise IntegrityError("BAR_STATE_BUCKET_BUDGET")
    try:
        if builder.identity is not None and (
            type(builder.identity) is not tuple or len(builder.identity) != 4
        ):
            raise ValueError("builder scope must be one exact four-field tuple")
        buckets = []
        for key, bucket in sorted(builder.buckets.items()):
            if key != (bucket.interval_ms, bucket.opening_ms):
                raise ValueError("builder bucket key disagrees with accumulator")
            if not isinstance(bucket.volume, Decimal):
                raise ValueError("builder volume must retain an exact Decimal value")
            buckets.append(
                BarBucket(
                    prototype=BarPrototype.model_validate(
                        bucket.prototype.model_dump(), strict=True
                    ),
                    opening_ms=bucket.opening_ms,
                    interval_ms=bucket.interval_ms,
                    first=bucket.first,
                    last=bucket.last,
                    open=bucket.open,
                    close=bucket.close,
                    high=bucket.high,
                    low=bucket.low,
                    volume_decimal=str(bucket.volume),
                    count=bucket.count,
                    complete=bucket.complete,
                )
            )
        scope = (
            BarScope.model_validate(
                {
                    "source": builder.identity[0],
                    "environment": builder.identity[1],
                    "asset": builder.identity[2],
                    "symbol": builder.identity[3],
                },
                strict=True,
            )
            if builder.identity is not None
            else None
        )
        state = BarState(
            format_version=1,
            as_of_ms=as_of_ms,
            timeframes_ms=builder.timeframes,
            lateness_ms=builder.lateness_ms,
            coverage_start_ms=builder.coverage_start_ms,
            watermark_ms=builder.watermark_ms,
            scope=scope,
            buckets=tuple(buckets),
        )
        data = json.dumps(
            state.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (
        ValueError,
        TypeError,
        AttributeError,
        IndexError,
        InvalidOperation,
        RecursionError,
    ) as exc:
        raise IntegrityError("BAR_STATE_INVALID") from exc
    if len(data) > max_bytes:
        raise IntegrityError("BAR_STATE_BYTE_BUDGET")
    return data


def decode_bar_state(
    data: bytes,
    *,
    max_buckets: int = DEFAULT_MAX_BUCKETS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> BarState:
    """Inspect frozen validated state; external committed provenance remains required."""
    _budgets(max_buckets, max_bytes)
    if type(data) is not bytes:
        raise ValueError("bar state must be one explicit byte string")
    if len(data) > max_bytes:
        raise IntegrityError("BAR_STATE_BYTE_BUDGET")
    try:
        raw = _object(data.decode("utf-8"))
        buckets = raw.get("buckets")
        if not isinstance(buckets, list):
            raise ValueError("bar state buckets must be an array")
        if len(buckets) > max_buckets:
            raise IntegrityError("BAR_STATE_BUCKET_BUDGET")
        return BarState.model_validate_json(data, strict=True)
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise IntegrityError("BAR_STATE_INVALID") from exc


def restore_bar_state(
    data: bytes,
    now_ms: int,
    *,
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    max_buckets: int = DEFAULT_MAX_BUCKETS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> TradeBars:
    """Restore independent open bars as incomplete; preserve native prototypes/watermark.

    Explicit restart time must be at or after the encoded cutoff. Quiet intervals are
    never created. Closed buckets are refused rather than reopened or revised.
    """
    _budgets(max_buckets, max_bytes)
    _clock(now_ms, "now_ms")
    if (
        type(timeframes_s) is not tuple
        or not 1 <= len(timeframes_s) <= 32
        or any(type(value) is not int or not 1 <= value <= 86_400 for value in timeframes_s)
        or len(set(timeframes_s)) != len(timeframes_s)
        or type(lateness_ms) is not int
        or not 0 <= lateness_ms <= 86_400_000
    ):
        raise ValueError("explicit bar restart timeframe/lateness policy is invalid")
    state = decode_bar_state(data, max_buckets=max_buckets, max_bytes=max_bytes)
    if now_ms < state.as_of_ms:
        raise IntegrityError("BAR_STATE_RESTART_BEFORE_CUTOFF")
    if (
        state.timeframes_ms != tuple(sorted(value * 1000 for value in timeframes_s))
        or state.lateness_ms != lateness_ms
    ):
        raise IntegrityError("BAR_STATE_CONFIGURATION_MISMATCH")
    restored = TradeBars(timeframes_s, now_ms, lateness_ms)
    restored.watermark_ms = state.watermark_ms
    restored.identity = state.scope.identity() if state.scope is not None else None
    for bucket in state.buckets:
        restored.buckets[(bucket.interval_ms, bucket.opening_ms)] = Accumulator(
            prototype=bucket.prototype.event(),
            opening_ms=bucket.opening_ms,
            interval_ms=bucket.interval_ms,
            first=bucket.first,
            last=bucket.last,
            open=bucket.open,
            close=bucket.close,
            high=bucket.high,
            low=bucket.low,
            volume=bucket.volume(),
            count=bucket.count,
            complete=False,
        )
    return restored
