"""Read-only paired Ethereum USDC/USDT totalSupply polls, retaining native words/receipts."""

import asyncio
from collections.abc import Iterable
from itertools import islice
from typing import Annotated, Literal

import aiohttp
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictBool,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
)

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

URL = "https://eth.drpc.org/"
SOURCE = "ethereum_drpc"
SYMBOL = "ETH_MAINNET_STABLECOIN_SUPPLY"
USDC_ADDRESS = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDT_ADDRESS = "0xdac17f958d2ee523a2206206994597c13d831ec7"
SUPPLY_SELECTOR = "0x18160ddd"
DECIMALS_SELECTOR = "0x313ce567"
MAX_RESPONSE_BYTES = 4_000_000
POLL_TIMEOUT_S = 90
Step = Literal[
    "chain",
    "finalized",
    "usdc_supply",
    "usdc_decimals",
    "usdt_supply",
    "usdt_decimals",
    "reference",
]
STEPS: tuple[Step, ...] = (
    "chain",
    "finalized",
    "usdc_supply",
    "usdc_decimals",
    "usdt_supply",
    "usdt_decimals",
    "reference",
)
Quantity = Annotated[str, Field(strict=True, pattern=r"^0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})$")]
Hash = Annotated[str, Field(strict=True, pattern=r"^0x[0-9a-fA-F]{64}$")]
Word = Annotated[str, Field(strict=True, pattern=r"^0x[0-9a-fA-F]{64}$")]
Amount = Annotated[str, Field(strict=True, pattern=r"^(0|[1-9][0-9]{0,77})$")]
EventId = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
Clock = Annotated[int, Field(strict=True, gt=0)]
COVERAGE = "Ethereum mainnet USDC and USDT contract totalSupply only; no all-chain supply, exchange balances or flow attribution"
REVISION = "one complete same-block poll; every response receipt retained; not first publication"
FINALITY = "RPC provider assertion; not independently verified"


class Header(ExternalModel):
    number: Quantity
    hash: Hash
    parentHash: Hash
    timestamp: Quantity


class TokenSupply(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    symbol: Literal["USDC", "USDT"]
    contract_address: StrictStr
    total_supply_selector: Literal["0x18160ddd"]
    decimals_selector: Literal["0x313ce567"]
    total_supply_abi_word: Word
    total_supply_base_units: Amount
    decimals_abi_word: Word
    decimals: StrictInt = Field(ge=0, le=255)
    units: Literal["token base units"]
    total_supply_received_at_ms: Clock
    decimals_received_at_ms: Clock


class SupplySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    chain_id: Literal[1]
    chain_id_quantity: Quantity
    block_number: StrictInt = Field(gt=0, lt=2**64)
    block_number_quantity: Quantity
    block_hash: Hash
    parent_hash: Hash
    block_timestamp_quantity: Quantity
    call_block_quantity: Quantity
    requested_block_tag: Literal["finalized"]
    chain_received_at_ms: Clock
    header_received_at_ms: Clock
    reference_received_at_ms: Clock
    usdc: TokenSupply
    usdt: TokenSupply
    poll_revision_basis: Literal[
        "one complete same-block poll; every response receipt retained; not first publication"
    ]
    finality_basis: Literal["RPC provider assertion; not independently verified"]
    coverage: Literal[
        "Ethereum mainnet USDC and USDT contract totalSupply only; no all-chain supply, exchange balances or flow attribution"
    ]
    execution_quote: Literal[False]


def rpc_request(step: Step, block_number: int | None = None) -> dict[str, JsonValue]:
    """Seven fixed read-only requests; callers cannot supply addresses, selectors or URLs."""
    if type(step) is not str or step not in STEPS:
        raise ValueError("unsupported read-only stablecoin RPC step")
    if step in {"chain", "finalized"}:
        if block_number is not None:
            raise ValueError("block argument is invalid for this fixed RPC step")
    elif type(block_number) is not int or not 0 < block_number < 2**64:
        raise ValueError("stablecoin calls require one explicit numeric block")
    rpc_id = STEPS.index(step) + 1
    if step == "chain":
        method, params = "eth_chainId", []
    elif step in {"finalized", "reference"}:
        method = "eth_getBlockByNumber"
        params = ["finalized" if step == "finalized" else hex(block_number), False]
    else:
        method = "eth_call"
        address = USDC_ADDRESS if step.startswith("usdc_") else USDT_ADDRESS
        selector = SUPPLY_SELECTOR if step.endswith("_supply") else DECIMALS_SELECTOR
        params = [{"to": address, "data": selector}, hex(block_number)]
    return {"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}


def _snapshot_valid(body: SupplySnapshot, event_time_ms: int, observed_at_ms: int) -> None:
    if (
        int(body.chain_id_quantity, 16) != 1
        or int(body.block_number_quantity, 16) != body.block_number
        or body.call_block_quantity != hex(body.block_number)
        or int(body.block_timestamp_quantity, 16) * 1000 != event_time_ms
        or event_time_ms > observed_at_ms + 2000
        or body.reference_received_at_ms != observed_at_ms
    ):
        raise IntegrityError("STABLECOIN_SNAPSHOT_CLOCK_OR_BLOCK")
    receipts = [
        body.chain_received_at_ms,
        body.header_received_at_ms,
        body.usdc.total_supply_received_at_ms,
        body.usdc.decimals_received_at_ms,
        body.usdt.total_supply_received_at_ms,
        body.usdt.decimals_received_at_ms,
        body.reference_received_at_ms,
    ]
    if receipts != sorted(receipts):
        raise IntegrityError("STABLECOIN_RESPONSE_CLOCK_REGRESSION")
    for token, symbol, address in [
        (body.usdc, "USDC", USDC_ADDRESS),
        (body.usdt, "USDT", USDT_ADDRESS),
    ]:
        if (
            token.symbol != symbol
            or token.contract_address != address
            or int(token.total_supply_abi_word, 16) != int(token.total_supply_base_units)
            or int(token.decimals_abi_word, 16) != token.decimals
            or token.decimals != 6
        ):
            raise IntegrityError("STABLECOIN_CONTRACT_UNITS_OR_AMOUNT")


def _event(body: SupplySnapshot, event_time_ms: int, observed_at_ms: int) -> MarketEvent:
    _snapshot_valid(body, event_time_ms, observed_at_ms)
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="on_chain",
        event_time_ms=event_time_ms,
        observed_at_ms=observed_at_ms,
        native_id=f"{body.block_hash.lower()}:{observed_at_ms}",
        payload=body.model_dump(mode="json"),
    )


def supply_event(results: Iterable[tuple[JsonValue, int]], max_block_age_ms: int) -> MarketEvent:
    """Build one complete poll revision only; never emit a partial token pair."""
    if type(max_block_age_ms) is not int or max_block_age_ms <= 0:
        raise ValueError("invalid stablecoin block-age policy")
    values = list(islice(results, len(STEPS) + 1))
    if len(values) != len(STEPS) or any(
        type(receipt) is not int or receipt <= 0 for _, receipt in values
    ):
        raise IntegrityError("STABLECOIN_INCOMPLETE_POLL_OR_CLOCK")
    chain = TypeAdapter(Quantity).validate_python(values[0][0])
    header = Header.model_validate(values[1][0])
    reference = Header.model_validate(values[6][0])
    if int(chain, 16) != 1:
        raise IntegrityError("STABLECOIN_WRONG_CHAIN")
    if header != reference:
        raise IntegrityError("STABLECOIN_REFERENCED_HEADER_MISMATCH")
    received = values[-1][1]
    timestamp = int(header.timestamp, 16) * 1000
    if not 0 < timestamp or not received - max_block_age_ms <= timestamp <= received + 2000:
        raise IntegrityError("STABLECOIN_BLOCK_STALE_OR_CLOCK_SKEW")

    def token(symbol: Literal["USDC", "USDT"], address: str, index: int) -> TokenSupply:
        supply = TypeAdapter(Word).validate_python(values[index][0])
        decimals = TypeAdapter(Word).validate_python(values[index + 1][0])
        return TokenSupply(
            symbol=symbol,
            contract_address=address,
            total_supply_selector=SUPPLY_SELECTOR,
            decimals_selector=DECIMALS_SELECTOR,
            total_supply_abi_word=supply,
            total_supply_base_units=str(int(supply, 16)),
            decimals_abi_word=decimals,
            decimals=int(decimals, 16),
            units="token base units",
            total_supply_received_at_ms=values[index][1],
            decimals_received_at_ms=values[index + 1][1],
        )

    body = SupplySnapshot(
        chain_id=1,
        chain_id_quantity=chain,
        block_number=int(header.number, 16),
        block_number_quantity=header.number,
        block_hash=header.hash,
        parent_hash=header.parentHash,
        block_timestamp_quantity=header.timestamp,
        call_block_quantity=hex(int(header.number, 16)),
        requested_block_tag="finalized",
        chain_received_at_ms=values[0][1],
        header_received_at_ms=values[1][1],
        reference_received_at_ms=received,
        usdc=token("USDC", USDC_ADDRESS, 2),
        usdt=token("USDT", USDT_ADDRESS, 4),
        poll_revision_basis=REVISION,
        finality_basis=FINALITY,
        coverage=COVERAGE,
        execution_quote=False,
    )
    return _event(body, timestamp, received)


def validate_supply_event(row: MarketEvent, as_of_ms: int) -> SupplySnapshot:
    if any(
        type(clock) is not int or clock <= 0
        for clock in (as_of_ms, row.event_time_ms, row.observed_at_ms)
    ):
        raise IntegrityError("STABLECOIN_EVIDENCE_CLOCK_INVALID")
    if (row.source, row.environment, row.asset, row.symbol, row.kind, row.timestamp_kind) != (
        SOURCE,
        "public_reference",
        "GLOBAL",
        SYMBOL,
        "on_chain",
        "source",
    ):
        raise IntegrityError("STABLECOIN_SNAPSHOT_SCOPE")
    if max(row.event_time_ms, row.observed_at_ms) > as_of_ms:
        raise IntegrityError("STABLECOIN_SNAPSHOT_UNAVAILABLE")
    body = SupplySnapshot.model_validate_json(row.payload_json)
    if _event(body, row.event_time_ms, row.observed_at_ms) != row:
        raise IntegrityError("STABLECOIN_SNAPSHOT_CANONICAL")
    return body


def check_supply_progress(previous: MarketEvent | None, current: MarketEvent) -> None:
    if previous is None:
        return
    old = SupplySnapshot.model_validate_json(previous.payload_json)
    new = SupplySnapshot.model_validate_json(current.payload_json)
    if (
        current.observed_at_ms < previous.observed_at_ms
        or current.event_time_ms < previous.event_time_ms
        or new.block_number < old.block_number
    ):
        raise IntegrityError("STABLECOIN_BLOCK_OR_RECEIPT_REGRESSION")
    if new.block_number == old.block_number:
        old_tokens = [
            (old.usdc.total_supply_base_units, old.usdc.decimals),
            (old.usdt.total_supply_base_units, old.usdt.decimals),
        ]
        new_tokens = [
            (new.usdc.total_supply_base_units, new.usdc.decimals),
            (new.usdt.total_supply_base_units, new.usdt.decimals),
        ]
        if (
            old.block_hash.lower(),
            old.parent_hash.lower(),
            previous.event_time_ms,
            old_tokens,
        ) != (new.block_hash.lower(), new.parent_hash.lower(), current.event_time_ms, new_tokens):
            raise IntegrityError("STABLECOIN_SAME_BLOCK_CONFLICT")
    elif (
        current.event_time_ms <= previous.event_time_ms
        or new.block_hash.lower() == old.block_hash.lower()
        or (
            new.block_number == old.block_number + 1
            and new.parent_hash.lower() != old.block_hash.lower()
        )
    ):
        raise IntegrityError("STABLECOIN_BLOCK_SEQUENCE_CONFLICT")


class PollHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    reason: Literal[
        "SUPPLY_SNAPSHOT_OBSERVED",
        "ACCESS_DENIED",
        "RATE_LIMITED",
        "HTTP_UNAVAILABLE",
        "RPC_UNAVAILABLE",
        "TRANSPORT_UNAVAILABLE",
        "INVALID_DATA",
    ]
    code: StrictInt = Field(ge=0, le=599)
    error_type: StrictStr | None
    accepted_event_id: EventId | None
    accepted_observed_at_ms: Clock | None
    response_received_at_ms: Clock | None


def stablecoin_poll_health(
    observed_at_ms: int,
    *,
    ok: bool,
    reason: str,
    code: int = 0,
    error_type: str | None = None,
    accepted_event_id: str | None = None,
    accepted_observed_at_ms: int | None = None,
    response_received_at_ms: int | None = None,
) -> MarketEvent:
    if type(observed_at_ms) is not int or observed_at_ms <= 0:
        raise ValueError("invalid stablecoin poll health clock")
    body = PollHealth.model_validate(
        {
            "ok": ok,
            "reason": reason,
            "code": code,
            "error_type": error_type,
            "accepted_event_id": accepted_event_id,
            "accepted_observed_at_ms": accepted_observed_at_ms,
            "response_received_at_ms": response_received_at_ms,
        }
    )
    if (
        response_received_at_ms is not None
        and response_received_at_ms > observed_at_ms
        or error_type is not None
        and (not error_type or len(error_type) > 128)
        or body.ok
        and (
            reason != "SUPPLY_SNAPSHOT_OBSERVED"
            or code != 0
            or error_type is not None
            or accepted_event_id is None
            or accepted_observed_at_ms is None
            or response_received_at_ms != accepted_observed_at_ms
        )
        or not body.ok
        and (
            reason == "SUPPLY_SNAPSHOT_OBSERVED"
            or accepted_event_id is not None
            or accepted_observed_at_ms is not None
            or reason == "ACCESS_DENIED"
            and code not in {401, 403, 451}
            or reason == "RATE_LIMITED"
            and code != 429
            or reason == "HTTP_UNAVAILABLE"
            and (code < 100 or code in {200, 401, 403, 429, 451})
            or reason in {"RPC_UNAVAILABLE", "TRANSPORT_UNAVAILABLE", "INVALID_DATA"}
            and code != 0
            or reason == "TRANSPORT_UNAVAILABLE"
            and error_type is None
            or reason != "TRANSPORT_UNAVAILABLE"
            and error_type is not None
        )
    ):
        raise IntegrityError("STABLECOIN_POLL_HEALTH_INVALID")
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=observed_at_ms,
        observed_at_ms=observed_at_ms,
        payload=body.model_dump(mode="json"),
    )


class StablecoinSupplyCollector:
    def __init__(
        self, config: AppConfig, sink: Sink, *, initial_snapshot: MarketEvent | None = None
    ) -> None:
        self.config, self.sink = config, sink
        if initial_snapshot is not None:
            # A seed preserves native progress evidence; it does not establish a
            # current healthy poll, freshness, persistence or resumed transport.
            validate_supply_event(initial_snapshot, now_ms())
        self.previous = initial_snapshot

    async def response_received(
        self, request: dict[str, JsonValue], status: int, body: bytes, received: int
    ) -> None:
        """Optional bounded native-response evidence hook; default makes no persistence claim."""

    async def request(
        self, session: aiohttp.ClientSession, step: Step, block_number: int | None = None
    ) -> tuple[JsonValue, int]:
        request = rpc_request(step, block_number)
        async with session.post(URL, json=request, allow_redirects=False) as response:
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > MAX_RESPONSE_BYTES:
                    raise IntegrityError("STABLECOIN_RESPONSE_TOO_LARGE")
            received = now_ms()
            await self.response_received(request, response.status, bytes(body), received)
            if response.status in {401, 403, 451}:
                raise FeedUnavailable("ACCESS_DENIED", response.status)
            if response.status == 429:
                raise FeedUnavailable("RATE_LIMITED", response.status)
            if response.status != 200:
                raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
        raw = TypeAdapter(dict[str, JsonValue]).validate_json(body)
        if (
            raw.get("jsonrpc") != "2.0"
            or type(raw.get("id")) is not int
            or raw["id"] != request["id"]
        ):
            raise IntegrityError("STABLECOIN_RPC_IDENTITY")
        if "error" in raw:
            raise FeedUnavailable("RPC_UNAVAILABLE", 0)
        if raw.get("result") is None:
            raise IntegrityError("STABLECOIN_MISSING_RESULT")
        return raw["result"], received

    async def _collect(self, session: aiohttp.ClientSession) -> MarketEvent:
        values = [await self.request(session, "chain")]
        if int(TypeAdapter(Quantity).validate_python(values[0][0]), 16) != 1:
            raise IntegrityError("STABLECOIN_WRONG_CHAIN")
        values.append(await self.request(session, "finalized"))
        header = Header.model_validate(values[-1][0])
        number = int(header.number, 16)
        timestamp = int(header.timestamp, 16) * 1000
        age = int(self.config.ingestion.stablecoin_supply_max_block_age_s * 1000)
        if (
            not 0 < number < 2**64
            or not 0 < timestamp
            or not values[-1][1] - age <= timestamp <= values[-1][1] + 2000
        ):
            raise IntegrityError("STABLECOIN_INVALID_OR_STALE_HEADER")
        for step in STEPS[2:]:
            values.append(await self.request(session, step, number))
        return supply_event(
            values, int(self.config.ingestion.stablecoin_supply_max_block_age_s * 1000)
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        try:
            async with asyncio.timeout(POLL_TIMEOUT_S):
                event = await self._collect(session)
            check_supply_progress(self.previous, event)
        except FeedUnavailable as exc:
            health = stablecoin_poll_health(now_ms(), ok=False, reason=exc.reason, code=exc.code)
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            health = stablecoin_poll_health(
                now_ms(), ok=False, reason="TRANSPORT_UNAVAILABLE", error_type=type(exc).__name__
            )
        except (ValidationError, IntegrityError):
            health = stablecoin_poll_health(now_ms(), ok=False, reason="INVALID_DATA")
        else:
            await self.sink(event)
            self.previous = event
            await self.sink(
                stablecoin_poll_health(
                    now_ms(),
                    ok=True,
                    reason="SUPPLY_SNAPSHOT_OBSERVED",
                    accepted_event_id=event.event_id,
                    accepted_observed_at_ms=event.observed_at_ms,
                    response_received_at_ms=event.observed_at_ms,
                )
            )
            return True
        await self.sink(health)
        return False

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            while True:
                await self.collect(session)
                await asyncio.sleep(self.config.ingestion.stablecoin_supply_poll_interval_s)


def _health_valid(row: MarketEvent) -> PollHealth | None:
    payload = row.payload()
    fields = {"accepted_event_id", "accepted_observed_at_ms", "response_received_at_ms"}
    legacy = not fields.intersection(payload)
    body = PollHealth.model_validate({**payload, **dict.fromkeys(fields)} if legacy else payload)
    if legacy and body.ok:
        if (
            body.reason != "SUPPLY_SNAPSHOT_OBSERVED"
            or body.code != 0
            or body.error_type is not None
        ):
            raise IntegrityError("STABLECOIN_LEGACY_HEALTH_INVALID")
    else:
        stablecoin_poll_health(row.observed_at_ms, **body.model_dump())
    canonical = make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=row.observed_at_ms,
        observed_at_ms=row.observed_at_ms,
        payload=payload,
    )
    if row != canonical:
        raise IntegrityError("STABLECOIN_HEALTH_CANONICAL")
    return None if legacy else body


def stablecoin_supply_quality(
    events: Iterable[MarketEvent],
    *,
    as_of_ms: int,
    max_poll_age_ms: int = 1_200_000,
    max_block_age_ms: int = 1_800_000,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Bound scoped eligible rows, not iterable scanning or storage-engine execution/memory."""
    if (
        any(
            type(value) is not int or value <= 0
            for value in (as_of_ms, max_poll_age_ms, max_block_age_ms)
        )
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
    ):
        raise ValueError("invalid bounded stablecoin supply quality policy")
    result: dict[str, object] = {
        "source": SOURCE,
        "asset": "GLOBAL",
        "symbol": SYMBOL,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "health_received_at_ms": None,
        "response_received_at_ms": None,
        "accepted_event_id": None,
        "accepted_observed_at_ms": None,
        "poll_ok": None,
        "reason": None,
        "error_type": None,
        "code": None,
        "block_number": None,
        "block_hash": None,
        "block_time_ms": None,
        "poll_age_ms": None,
        "response_age_ms": None,
        "block_age_ms": None,
        "historical_poll_revisions": 0,
        "legacy_health_rows": 0,
        "max_poll_age_ms": max_poll_age_ms,
        "max_block_age_ms": max_block_age_ms,
        "scope": COVERAGE,
        "poll_revision_basis": REVISION,
        "binding_basis": "sink accepted; not atomic commit proof",
        "first_publication_independently_verified": False,
        "finality_independently_verified": False,
        "all_chain_supply_complete": False,
        "continuous_history_complete": False,
        "exchange_balance_coverage": False,
        "trading_approval": False,
    }
    try:
        eligible: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                row.source != SOURCE
                or row.environment != "public_reference"
                or row.asset != "GLOBAL"
                or row.symbol != SYMBOL
                or row.kind not in {"on_chain", "feed_health"}
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("STABLECOIN_QUALITY_OVER_BUDGET")
            if any(
                type(clock) is not int or clock <= 0
                for clock in (row.event_time_ms, row.observed_at_ms)
            ):
                raise IntegrityError("STABLECOIN_EVIDENCE_CLOCK_INVALID")
            if row.event_id in eligible and eligible[row.event_id] != row:
                raise IntegrityError("STABLECOIN_EVENT_ID_CONFLICT")
            eligible[row.event_id] = row
        snapshots: dict[str, MarketEvent] = {}
        health: list[tuple[MarketEvent, PollHealth | None]] = []
        for row in eligible.values():
            if row.kind == "on_chain":
                validate_supply_event(row, as_of_ms)
                snapshots[row.event_id] = row
            else:
                health.append((row, _health_valid(row)))
        previous = None
        for row in sorted(
            snapshots.values(),
            key=lambda item: (item.observed_at_ms, item.event_time_ms, item.event_id),
        ):
            check_supply_progress(previous, row)
            previous = row
        result["historical_poll_revisions"] = len(snapshots)
        result["legacy_health_rows"] = sum(body is None for _, body in health)
        if not health:
            raise IntegrityError("STABLECOIN_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        candidates = [(row, body) for row, body in health if row.observed_at_ms == latest]
        if len(candidates) != 1:
            raise IntegrityError("STABLECOIN_HEALTH_AMBIGUOUS")
        selected, body = candidates[0]
        payload = selected.payload()
        result.update(
            health_event_id=selected.event_id,
            health_received_at_ms=latest,
            poll_age_ms=as_of_ms - latest,
            poll_ok=payload["ok"],
            reason=payload["reason"],
            code=payload["code"],
            error_type=payload["error_type"],
        )
        if body is not None:
            result.update(
                accepted_event_id=body.accepted_event_id,
                accepted_observed_at_ms=body.accepted_observed_at_ms,
                response_received_at_ms=body.response_received_at_ms,
            )
            if body.response_received_at_ms is not None:
                result["response_age_ms"] = as_of_ms - body.response_received_at_ms
        if payload["ok"] is not True:
            raise IntegrityError("STABLECOIN_FEED_UNAVAILABLE")
        if body is None:
            raise IntegrityError("STABLECOIN_HEALTH_UNBOUND")
        assert body.accepted_event_id is not None and body.response_received_at_ms is not None
        if as_of_ms - min(latest, body.response_received_at_ms) > max_poll_age_ms:
            raise IntegrityError("STABLECOIN_POLL_STALE")
        if body.accepted_event_id not in snapshots:
            raise IntegrityError("STABLECOIN_REFERENCED_SNAPSHOT_MISSING")
        referenced = snapshots[body.accepted_event_id]
        if (
            referenced.observed_at_ms != body.accepted_observed_at_ms
            or referenced.observed_at_ms != body.response_received_at_ms
        ):
            raise IntegrityError("STABLECOIN_REFERENCED_RECEIPT_MISMATCH")
        payload = referenced.payload()
        result.update(
            block_number=payload["block_number"],
            block_hash=payload["block_hash"],
            block_time_ms=referenced.event_time_ms,
            block_age_ms=as_of_ms - referenced.event_time_ms,
        )
        if as_of_ms - referenced.event_time_ms > max_block_age_ms:
            raise IntegrityError("STABLECOIN_BLOCK_STALE")
        result["state"] = "POLL_OBSERVED_VERIFIED"
    except (IntegrityError, ValueError, TypeError, KeyError, OverflowError) as exc:
        result["blocker"] = (
            str(exc) if isinstance(exc, IntegrityError) else "STABLECOIN_INVALID_EVIDENCE"
        )
    return result
