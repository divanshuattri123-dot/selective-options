"""Public Binance forceOrder snapshots; lossy one-per-symbol/1000ms, never a ledger."""

import asyncio
import json
import uuid
from collections.abc import Iterable
from decimal import Decimal
from typing import Annotated, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictBool, StrictInt, StrictStr
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event

URL = "wss://fstream.binance.com/market/stream"
SOURCE = "binance_liquidations"
SYMBOLS = {asset: asset + "USDT" for asset in ("BTC", "ETH", "SOL", "PAXG")}
SCOPE = "lossy newest forceOrder snapshot per symbol/1000ms; not a complete liquidation ledger"
MAX_FRAME_BYTES = 65536
MAX_NATIVE_IDENTITIES = 10000
REQUEST_ID = "selective-liquidations-1"
Amount = Annotated[StrictStr, Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]+)?$", max_length=100)]
Timestamp = Annotated[StrictInt, Field(gt=0, lt=2**63)]
Session = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{32}$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class NativeOrder(StrictModel):
    s: StrictStr = Field(min_length=1, max_length=30)
    S: Literal["BUY", "SELL"]
    o: Literal[
        "LIMIT",
        "MARKET",
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    ]
    f: Literal["GTC", "IOC", "FOK", "GTX", "GTD", "RPI"]
    q: Amount
    p: Amount
    ap: Amount
    X: Literal[
        "NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"
    ]
    l: Amount  # noqa: E741 - exact native field name
    z: Amount
    T: Timestamp


class NativeSnapshot(StrictModel):
    e: Literal["forceOrder"]
    E: Timestamp
    o: NativeOrder
    st: StrictInt | None = None
    ps: StrictStr | None = None


class Observation(StrictModel):
    native: NativeSnapshot
    source_url: Literal["wss://fstream.binance.com/market/stream"]
    coverage: Literal[
        "lossy newest forceOrder snapshot per symbol/1000ms; not a complete liquidation ledger"
    ]
    side_convention: Literal[
        "native order side; liquidated position side and normalized flow not inferred"
    ]
    timestamp_basis: Literal[
        "native E event time; native o.T trade time retained; local receipt required"
    ]
    trading_approval: Literal[False]


class Health(StrictModel):
    state: Literal["CONNECTED", "DISCONNECTED", "INVALID"]
    ok: StrictBool
    reason: Literal[
        "SUBSCRIPTION_ACKNOWLEDGED",
        "FRAME_OBSERVED",
        "TRANSPORT_UNAVAILABLE",
        "INVALID_DATA",
        "CANCELLED",
        "CONNECTION_CLOSED",
    ]
    session_id: Session
    session_started_at_ms: Timestamp
    streams: tuple[StrictStr, ...] = Field(min_length=1, max_length=4)
    ignored_unrequested_frames: StrictInt = Field(ge=0)
    accepted_event_id: StrictStr | None = Field(pattern=r"^[0-9a-f]{64}$")
    accepted_observed_at_ms: Timestamp | None
    source_url: Literal["wss://fstream.binance.com/market/stream"]
    coverage: Literal[
        "lossy newest forceOrder snapshot per symbol/1000ms; not a complete liquidation ledger"
    ]


def streams_for(assets: Iterable[str]) -> tuple[str, ...]:
    values = tuple(assets)
    if not values or len(set(values)) != len(values) or any(v not in SYMBOLS for v in values):
        raise ValueError("invalid supported liquidation assets")
    return tuple(sorted(SYMBOLS[asset].lower() + "@forceOrder" for asset in values))


def native_event(raw: JsonValue, received: int, asset: str) -> MarketEvent:
    if asset not in SYMBOLS or type(received) is not int or not 0 < received < 2**63:
        raise ValueError("invalid liquidation observation scope/receipt")
    native = NativeSnapshot.model_validate(raw)
    order = native.o
    if (
        order.s != SYMBOLS[asset]
        or (native.st is not None and native.st != 1)
        or (native.ps is not None and native.ps != order.s)
    ):
        raise IntegrityError("LIQUIDATION_NATIVE_SCOPE")
    if not received - 60000 <= native.E <= received + 2000 or not 0 < order.T <= native.E:
        raise IntegrityError("LIQUIDATION_NATIVE_CLOCK")
    quantity, filled, last = Decimal(order.q), Decimal(order.z), Decimal(order.l)
    if quantity <= 0 or not 0 <= last <= filled <= quantity:
        raise IntegrityError("LIQUIDATION_NATIVE_QUANTITIES")
    if order.X == "FILLED" and filled != quantity:
        raise IntegrityError("LIQUIDATION_FILLED_QUANTITY")
    # Preserve the exact original native dictionary, including absent optional keys.
    assert isinstance(raw, dict)
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset=asset,
        symbol=order.s,
        kind="liquidation",
        event_time_ms=native.E,
        observed_at_ms=received,
        native_id=f"{order.s}:{native.E}:{order.T}",
        payload={
            "native": raw,
            "source_url": URL,
            "coverage": SCOPE,
            "side_convention": "native order side; liquidated position side and normalized flow not inferred",
            "timestamp_basis": "native E event time; native o.T trade time retained; local receipt required",
            "trading_approval": False,
        },
    )


def health_event(asset: str, received: int, **body: object) -> MarketEvent:
    if asset not in SYMBOLS or type(received) is not int or not 0 < received < 2**63:
        raise ValueError("invalid liquidation health scope/receipt")
    health = Health.model_validate({**body, "source_url": URL, "coverage": SCOPE})
    supported = set(streams_for(SYMBOLS))
    own_stream = SYMBOLS[asset].lower() + "@forceOrder"
    if (
        health.session_started_at_ms > received
        or len(set(health.streams)) != len(health.streams)
        or not set(health.streams) <= supported
        or own_stream not in health.streams
        or health.streams != tuple(sorted(health.streams))
        or health.ok != (health.state == "CONNECTED")
        or (health.state == "CONNECTED")
        != (health.reason in {"SUBSCRIPTION_ACKNOWLEDGED", "FRAME_OBSERVED"})
        or (health.state == "INVALID") != (health.reason == "INVALID_DATA")
        or (health.accepted_event_id is None) != (health.accepted_observed_at_ms is None)
        or (
            health.accepted_observed_at_ms is not None
            and not health.session_started_at_ms <= health.accepted_observed_at_ms <= received
        )
    ):
        raise IntegrityError("LIQUIDATION_HEALTH_SCOPE_OR_STATE")
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset=asset,
        symbol=SYMBOLS[asset],
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=received,
        observed_at_ms=received,
        payload=health.model_dump(mode="json"),
    )


def decode_frame(frame: str | bytes) -> dict[str, JsonValue]:
    encoded = frame.encode() if isinstance(frame, str) else frame
    if len(encoded) > MAX_FRAME_BYTES:
        raise IntegrityError("LIQUIDATION_FRAME_OVER_BUDGET")

    def unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for key, value in pairs:
            if key in result:
                raise IntegrityError("LIQUIDATION_DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    def invalid(value: str) -> JsonValue:
        raise IntegrityError("LIQUIDATION_NONFINITE_JSON")

    result = json.loads(encoded, object_pairs_hook=unique, parse_constant=invalid)
    if type(result) is not dict:
        raise IntegrityError("LIQUIDATION_FRAME_ROOT")
    return cast(dict[str, JsonValue], result)


class LiquidationCollector:
    """One bounded-memory public socket session; no reconnect or order methods."""

    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink
        self.assets = tuple(config.system.assets)
        self.streams = streams_for(self.assets)
        self.session_id = uuid.uuid4().hex
        self.session_started_at_ms = 0
        self.ignored_unrequested_frames = 0
        self.seen: dict[tuple[str, int, int], MarketEvent] = {}
        self.latest: dict[str, MarketEvent] = {}
        self.subscription_attempts = 0
        self.last_frame_received_at_ms = 0

    async def frame_received(self, frame: str | bytes, received: int) -> None:
        """Verifier hook before validation; transport receipt is never rewritten."""

    async def health(self, state: str, reason: str) -> None:
        received = now_ms()
        for asset in self.assets:
            latest = self.latest.get(asset)
            await self.sink(
                health_event(
                    asset,
                    received,
                    state=state,
                    ok=state == "CONNECTED",
                    reason=reason,
                    session_id=self.session_id,
                    session_started_at_ms=self.session_started_at_ms,
                    streams=self.streams,
                    ignored_unrequested_frames=self.ignored_unrequested_frames,
                    accepted_event_id=None if latest is None else latest.event_id,
                    accepted_observed_at_ms=None if latest is None else latest.observed_at_ms,
                )
            )

    async def accept(self, body: dict[str, JsonValue], received: int) -> None:
        if type(received) is not int or received < self.last_frame_received_at_ms:
            raise IntegrityError("LIQUIDATION_FRAME_RECEIPT_REGRESSION")
        self.last_frame_received_at_ms = received
        if (
            set(body) != {"stream", "data"}
            or type(body["stream"]) is not str
            or type(body["data"]) is not dict
        ):
            raise IntegrityError("LIQUIDATION_UNKNOWN_FRAME")
        data = body["data"]
        if data.get("e") != "forceOrder" or type(data.get("o")) is not dict:
            raise IntegrityError("LIQUIDATION_UNKNOWN_EVENT")
        order = cast(dict[str, JsonValue], data["o"])
        symbol = order.get("s")
        if type(symbol) is not str or body["stream"] != str(symbol).lower() + "@forceOrder":
            raise IntegrityError("LIQUIDATION_STREAM_SYMBOL_MISMATCH")
        if body["stream"] not in self.streams:
            self.ignored_unrequested_frames += 1
            return
        asset = next(asset for asset in self.assets if SYMBOLS[asset] == symbol)
        event = native_event(data, received, asset)
        parsed = NativeSnapshot.model_validate(data)
        key = (parsed.o.s, parsed.E, parsed.o.T)
        previous = self.seen.get(key)
        if previous is not None:
            if previous.payload_json != event.payload_json or received < previous.observed_at_ms:
                raise IntegrityError("LIQUIDATION_NATIVE_ID_CONFLICT_OR_RECEIPT_REGRESSION")
            # Stable snapshot identity and first receipt survive duplicate delivery.
            return
        if len(self.seen) >= MAX_NATIVE_IDENTITIES:
            raise IntegrityError("LIQUIDATION_NATIVE_ID_BUDGET")
        await self.sink(event)
        self.seen[key] = event
        latest = self.latest.get(asset)
        if latest is None or (event.event_time_ms, event.observed_at_ms) > (
            latest.event_time_ms,
            latest.observed_at_ms,
        ):
            self.latest[asset] = event

    async def session(self, socket: ClientConnection) -> None:
        self.subscription_attempts += 1
        await socket.send(
            json.dumps({"method": "SUBSCRIBE", "params": self.streams, "id": REQUEST_ID})
        )
        pending: list[tuple[dict[str, JsonValue], int]] = []
        async with asyncio.timeout(10):
            while True:
                frame = await socket.recv()
                received = now_ms()
                await self.frame_received(frame, received)
                body = decode_frame(frame)
                if set(body) == {"result", "id"}:
                    if body != {"result": None, "id": REQUEST_ID}:
                        raise IntegrityError("LIQUIDATION_SUBSCRIPTION_ACK")
                    break
                if len(pending) >= 32:
                    raise IntegrityError("LIQUIDATION_PRE_ACK_FRAME_BUDGET")
                # Validate structural discrimination before buffering full bounded frames.
                if set(body) != {"stream", "data"}:
                    raise IntegrityError("LIQUIDATION_UNKNOWN_CONTROL")
                pending.append((body, received))
        await self.health("CONNECTED", "SUBSCRIPTION_ACKNOWLEDGED")
        for body, received in pending:
            await self.accept(body, received)
        if pending:
            await self.health("CONNECTED", "FRAME_OBSERVED")
        while True:
            frame = await socket.recv()
            received = now_ms()
            await self.frame_received(frame, received)
            await self.accept(decode_frame(frame), received)
            await self.health("CONNECTED", "FRAME_OBSERVED")

    async def run(self) -> None:
        if self.session_started_at_ms:
            raise IntegrityError("LIQUIDATION_SESSION_ALREADY_STARTED")
        self.session_started_at_ms = now_ms()
        try:
            async with connect(
                URL,
                max_size=MAX_FRAME_BYTES,
                max_queue=16,
                open_timeout=10,
                close_timeout=2,
                ping_interval=20,
                ping_timeout=20,
            ) as socket:
                try:
                    await self.session(socket)
                except (ValueError, IntegrityError) as exc:
                    await self.health("INVALID", "INVALID_DATA")
                    raise IntegrityError("LIQUIDATION_INVALID_STREAM") from exc
        except asyncio.CancelledError:
            await self.health("DISCONNECTED", "CANCELLED")
            raise
        except (WebSocketException, OSError, TimeoutError):
            await self.health("DISCONNECTED", "TRANSPORT_UNAVAILABLE")
            raise


def liquidations_quality(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    as_of_ms: int,
    max_health_age_ms: int = 300000,
    max_rows: int = 10000,
) -> dict[str, object]:
    if (
        asset not in SYMBOLS
        or any(type(v) is not int or v <= 0 for v in (as_of_ms, max_health_age_ms, max_rows))
        or max_rows > 100000
    ):
        raise ValueError("invalid bounded liquidation quality policy")
    result: dict[str, object] = {
        "asset": asset,
        "symbol": SYMBOLS[asset],
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "session_id": None,
        "selected_session_observations": 0,
        "observation_state": "UNOBSERVED",
        "latest_observation_event_id": None,
        "source_age_ms": None,
        "receipt_age_ms": None,
        "coverage": SCOPE,
        "complete_feed_available": False,
        "true_liquidation_ledger": False,
        "normalized_flow_verified": False,
        "trading_approval": False,
    }
    try:
        rows: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                row.source != SOURCE
                or row.environment != "public_reference"
                or row.asset != asset
                or row.symbol != SYMBOLS[asset]
                or row.kind not in {"liquidation", "feed_health"}
                or max(row.event_time_ms, row.observed_at_ms) > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("LIQUIDATION_QUALITY_OVER_BUDGET")
            if row.event_id in rows and rows[row.event_id] != row:
                raise IntegrityError("LIQUIDATION_EVENT_ID_CONFLICT")
            rows[row.event_id] = row
        health: list[tuple[MarketEvent, Health]] = []
        data: list[MarketEvent] = []
        native_keys: dict[tuple[str, int, int], str] = {}
        for row in rows.values():
            if row.kind == "feed_health":
                status = Health.model_validate(row.payload())
                expected = health_event(asset, row.observed_at_ms, **status.model_dump())
                if row != expected:
                    raise IntegrityError("LIQUIDATION_HEALTH_CANONICAL")
                health.append((row, status))
            else:
                parsed = Observation.model_validate(row.payload())
                expected = native_event(row.payload()["native"], row.observed_at_ms, asset)
                if row != expected:
                    raise IntegrityError("LIQUIDATION_OBSERVATION_CANONICAL")
                key = (parsed.native.o.s, parsed.native.E, parsed.native.o.T)
                if key in native_keys and native_keys[key] != row.event_id:
                    raise IntegrityError("LIQUIDATION_NATIVE_SNAPSHOT_CONFLICT")
                native_keys[key] = row.event_id
                data.append(row)
        if not health:
            raise IntegrityError("LIQUIDATION_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        choices = [(row, status) for row, status in health if row.observed_at_ms == latest]
        if len(choices) != 1:
            raise IntegrityError("LIQUIDATION_HEALTH_AMBIGUOUS")
        selected, status = choices[0]
        result.update(health_event_id=selected.event_id, session_id=status.session_id)
        if status.state != "CONNECTED":
            raise IntegrityError("LIQUIDATION_STREAM_UNAVAILABLE")
        if as_of_ms - selected.observed_at_ms > max_health_age_ms:
            raise IntegrityError("LIQUIDATION_HEALTH_STALE")
        acknowledgments = [
            row
            for row, body in health
            if body.session_id == status.session_id
            and body.session_started_at_ms == status.session_started_at_ms
            and body.streams == status.streams
            and body.reason == "SUBSCRIPTION_ACKNOWLEDGED"
        ]
        if len(acknowledgments) != 1 or acknowledgments[0].observed_at_ms > selected.observed_at_ms:
            raise IntegrityError("LIQUIDATION_SESSION_ACK_MISSING_OR_AMBIGUOUS")
        session_health = [
            (row, body) for row, body in health if body.session_id == status.session_id
        ]
        selected_ids: set[str] = set()
        for _row, body in session_health:
            if (
                body.session_started_at_ms != status.session_started_at_ms
                or body.streams != status.streams
            ):
                raise IntegrityError("LIQUIDATION_SESSION_SCOPE_CONFLICT")
            if body.state != "CONNECTED":
                raise IntegrityError("LIQUIDATION_SESSION_INVALIDATED")
            if body.accepted_event_id is not None:
                observed = rows.get(body.accepted_event_id)
                if (
                    observed is None
                    or observed.kind != "liquidation"
                    or observed.observed_at_ms != body.accepted_observed_at_ms
                ):
                    raise IntegrityError(
                        "LIQUIDATION_REFERENCED_OBSERVATION_MISSING_OR_CONFLICTING"
                    )
                selected_ids.add(body.accepted_event_id)
        selected_data = [row for row in data if row.event_id in selected_ids]
        result.update(
            state="LOSSY_STREAM_SESSION_OBSERVED", selected_session_observations=len(selected_data)
        )
        if selected_data:
            pair = max((row.event_time_ms, row.observed_at_ms) for row in selected_data)
            choices_data = [
                row for row in selected_data if (row.event_time_ms, row.observed_at_ms) == pair
            ]
            if len(choices_data) != 1:
                raise IntegrityError("LIQUIDATION_LATEST_OBSERVATION_AMBIGUOUS")
            chosen = choices_data[0]
            result.update(
                observation_state="NATIVE_SNAPSHOT_OBSERVED",
                latest_observation_event_id=chosen.event_id,
                source_age_ms=as_of_ms - chosen.event_time_ms,
                receipt_age_ms=as_of_ms - chosen.observed_at_ms,
            )
    except (IntegrityError, ValueError, TypeError, KeyError) as exc:
        result["state"] = "BLOCKED"
        result["blocker"] = (
            str(exc) if isinstance(exc, IntegrityError) else "LIQUIDATION_INVALID_EVIDENCE"
        )
    return result
