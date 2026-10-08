"""Immutable events with both source and observation timestamps; no implicit backdating."""

import hashlib
import json
from dataclasses import dataclass
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, model_validator

from .errors import IntegrityError
from .models import ClosedBarPayload

_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])

Environment = Literal["testnet", "public_reference"]
EventKind = Literal[
    "liquidation",
    "account_ratio",
    "taker_flow",
    "on_chain",
    "calendar",
    "news",
    "feed_health",
    "perpetual",
    "funding",
    "open_interest",
    "volatility_index",
    "instrument",
    "chain_summary",
    "ticker",
    "book",
    "trade",
    "index",
    "spot",
    "bar",
    "status",
]


class MarketEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    event_id: str
    source: str
    environment: Environment
    asset: str
    symbol: str
    kind: EventKind
    event_time_ms: int = Field(gt=0)
    observed_at_ms: int = Field(gt=0)
    timestamp_kind: Literal["source", "receipt"]
    schema_version: Literal[1] = 1
    payload_json: str

    @model_validator(mode="after")
    def closed_bar_available(self) -> Self:
        if self.kind == "bar":
            bar = ClosedBarPayload.model_validate_json(self.payload_json)
            if bar.close_time_ms != self.event_time_ms or bar.close_time_ms > self.observed_at_ms:
                raise ValueError(
                    "bar event must use its close time and cannot be observed before closing"
                )
        return self

    def payload(self) -> dict[str, JsonValue]:
        # Return a fresh decoded value; mutating it cannot mutate event/snapshot state.
        return _JSON_OBJECT.validate_json(self.payload_json)


def make_event(
    *,
    source: str,
    environment: Environment,
    asset: str,
    symbol: str,
    kind: EventKind,
    event_time_ms: int,
    observed_at_ms: int,
    payload: dict[str, JsonValue],
    timestamp_kind: Literal["source", "receipt"] = "source",
    native_id: str | None = None,
) -> MarketEvent:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    identity = json.dumps(
        [source, environment, asset, symbol, kind, native_id, event_time_ms, body],
        separators=(",", ":"),
    )
    return MarketEvent(
        event_id=hashlib.sha256(identity.encode()).hexdigest(),
        source=source,
        environment=environment,
        asset=asset,
        symbol=symbol,
        kind=kind,
        event_time_ms=event_time_ms,
        observed_at_ms=observed_at_ms,
        timestamp_kind=timestamp_kind,
        payload_json=body,
    )


@dataclass(frozen=True, slots=True)
class PointInTimeSnapshot:
    as_of_ms: int
    asset: str
    environment: Environment
    _events: tuple[MarketEvent, ...]

    def __post_init__(self) -> None:
        for event in self._events:
            if event.event_time_ms > self.as_of_ms or event.observed_at_ms > self.as_of_ms:
                raise IntegrityError(
                    "LOOKAHEAD: snapshot contains data unavailable at decision time"
                )
            if event.asset != self.asset or event.environment != self.environment:
                raise IntegrityError("snapshot asset/environment mismatch")

    def rows(
        self, source: str | None = None, kind: EventKind | None = None
    ) -> tuple[MarketEvent, ...]:
        return tuple(
            e
            for e in self._events
            if (source is None or e.source == source) and (kind is None or e.kind == kind)
        )
