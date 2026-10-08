"""Read-only Ethereum mainnet finalized-block gas observations, with native units."""

import asyncio
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

URL = "https://ethereum-rpc.publicnode.com"
Provider = Literal["publicnode", "cloudflare", "drpc"]
PUBLIC_PROVIDERS: dict[Provider, tuple[str, str]] = {
    "publicnode": (URL, "ethereum_publicnode"),
    "cloudflare": ("https://cloudflare-eth.com/v1/mainnet", "ethereum_cloudflare"),
    "drpc": ("https://eth.drpc.org/", "ethereum_drpc"),
}
Method = Literal["eth_chainId", "eth_getBlockByNumber"]
Quantity = Annotated[str, Field(strict=True, pattern=r"^0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})$")]
Hash = Annotated[str, Field(strict=True, pattern=r"^0x[0-9a-fA-F]{64}$")]
EventId = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
HealthReason = Literal[
    "FINALIZED_GAS_OBSERVED",
    "ACCESS_DENIED",
    "RATE_LIMITED",
    "HTTP_UNAVAILABLE",
    "RPC_UNAVAILABLE",
    "TRANSPORT_UNAVAILABLE",
    "INVALID_DATA",
]


class GasPollHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    reason: HealthReason
    code: StrictInt = Field(ge=0, le=599)
    error_type: StrictStr | None
    accepted_event_id: EventId | None
    accepted_observed_at_ms: StrictInt | None
    response_received_at_ms: StrictInt | None


def eth_gas_poll_health(
    provider: Provider,
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
    """Bind poll completion to sink acceptance, without asserting an atomic commit."""
    if (
        type(provider) is not str
        or provider not in PUBLIC_PROVIDERS
        or type(observed_at_ms) is not int
        or observed_at_ms <= 0
    ):
        raise ValueError("invalid ETH poll health clock or provider")
    body = GasPollHealth.model_validate(
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
        and not 0 < response_received_at_ms <= observed_at_ms
        or error_type is not None
        and (not error_type or len(error_type) > 128)
        or body.ok
        and (
            reason != "FINALIZED_GAS_OBSERVED"
            or code != 0
            or error_type is not None
            or accepted_event_id is None
            or accepted_observed_at_ms is None
            or response_received_at_ms is None
            or not 0 < accepted_observed_at_ms <= response_received_at_ms
        )
        or not body.ok
        and (
            reason == "FINALIZED_GAS_OBSERVED"
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
        raise IntegrityError("ETH_POLL_HEALTH_INVALID")
    return make_event(
        source=PUBLIC_PROVIDERS[provider][1],
        environment="public_reference",
        asset="ETH",
        symbol="ETH_MAINNET_GAS",
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=observed_at_ms,
        observed_at_ms=observed_at_ms,
        payload=body.model_dump(mode="json"),
    )


class Block(ExternalModel):
    number: Quantity
    hash: Hash
    parentHash: Hash
    timestamp: Quantity
    gasLimit: Quantity
    gasUsed: Quantity
    baseFeePerGas: Quantity
    transactions: tuple[Hash, ...] = Field(max_length=100_000)


def gas_event(
    raw: JsonValue, received: int, max_age_ms: int, *, provider: Provider = "publicnode"
) -> MarketEvent:
    if provider not in PUBLIC_PROVIDERS:
        raise ValueError("unsupported public Ethereum provider")
    block = Block.model_validate(raw)
    ts, number = int(block.timestamp, 16) * 1000, int(block.number, 16)
    gas_limit, gas_used = int(block.gasLimit, 16), int(block.gasUsed, 16)
    if max_age_ms <= 0 or not 0 < ts or not received - max_age_ms <= ts <= received + 2000:
        raise IntegrityError("ETH_BLOCK_STALE_OR_CLOCK_SKEW")
    if number >= 2**64 or not 0 < gas_limit or gas_used > gas_limit:
        raise IntegrityError("ETH_INVALID_BLOCK_QUANTITIES")
    if len(set(h.lower() for h in block.transactions)) != len(block.transactions):
        raise IntegrityError("ETH_DUPLICATE_TRANSACTION_HASH")
    return make_event(
        source=PUBLIC_PROVIDERS[provider][1],
        environment="public_reference",
        asset="ETH",
        symbol="ETH_MAINNET_GAS",
        kind="on_chain",
        event_time_ms=ts,
        observed_at_ms=received,
        native_id=block.hash.lower(),
        payload={
            "chain_id": 1,
            "block_number": number,
            "block_hash": block.hash.lower(),
            "parent_hash": block.parentHash.lower(),
            "gas_limit": str(gas_limit),
            "gas_used": str(gas_used),
            "gas_units": "gas",
            "base_fee_per_gas_wei": str(int(block.baseFeePerGas, 16)),
            "transaction_count": len(block.transactions),
            "requested_block_tag": "finalized",
            "finality_basis": "RPC provider assertion; not independently verified",
            "timestamp_basis": "block header time; first publication unavailable; receipt also required",
            "coverage": "sampled finalized blocks; no continuous history or exchange-flow attribution",
            "execution_quote": False,
        },
    )


def check_progress(previous: MarketEvent | None, current: MarketEvent) -> None:
    if previous is None:
        return
    if current.observed_at_ms < previous.observed_at_ms:
        raise IntegrityError("ETH_RECEIPT_CLOCK_REGRESSION")
    old, new = previous.payload(), current.payload()
    old_number, new_number = int(str(old["block_number"])), int(str(new["block_number"]))
    if new_number < old_number or (
        new_number == old_number and previous.event_id != current.event_id
    ):
        raise IntegrityError("ETH_FINALIZED_BLOCK_REGRESSION_OR_CONFLICT")
    if new_number > old_number:
        if (
            current.event_time_ms <= previous.event_time_ms
            or new["block_hash"] == old["block_hash"]
        ):
            raise IntegrityError("ETH_BLOCK_SEQUENCE_CONFLICT")
        if new_number == old_number + 1 and new["parent_hash"] != old["block_hash"]:
            raise IntegrityError("ETH_PARENT_HASH_MISMATCH")


class EthereumGasCollector:
    def __init__(self, config: AppConfig, sink: Sink, *, provider: Provider = "publicnode") -> None:
        if provider not in PUBLIC_PROVIDERS:
            raise ValueError("unsupported public Ethereum provider")
        self.config, self.sink = config, sink
        self.provider = provider
        self.url, self.source = PUBLIC_PROVIDERS[provider]
        self.previous: MarketEvent | None = None

    async def response_received(
        self, method: Method, status: int, body: bytes, received: int
    ) -> None:
        """Optional evidence hook called before response validation, without rewriting receipts."""

    async def request(
        self, session: aiohttp.ClientSession, method: Method
    ) -> tuple[JsonValue, int]:
        if method not in {"eth_chainId", "eth_getBlockByNumber"}:
            raise ValueError("unsupported read-only Ethereum method")
        rpc_id = 1 if method == "eth_chainId" else 2
        params: list[JsonValue] = [] if method == "eth_chainId" else ["finalized", False]
        async with session.post(
            self.url,
            json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params},
            allow_redirects=False,
        ) as response:
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > 4_000_000:
                    raise IntegrityError("ETH_RESPONSE_TOO_LARGE")
            received = now_ms()
            await self.response_received(method, response.status, bytes(body), received)
            if response.status in {401, 403, 451}:
                raise FeedUnavailable("ACCESS_DENIED", response.status)
            if response.status == 429:
                raise FeedUnavailable("RATE_LIMITED", response.status)
            if response.status != 200:
                raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
        raw = TypeAdapter(dict[str, JsonValue]).validate_json(body)
        if raw.get("jsonrpc") != "2.0" or type(raw.get("id")) is not int or raw["id"] != rpc_id:
            raise IntegrityError("ETH_RPC_IDENTITY")
        if "error" in raw:
            raise FeedUnavailable("RPC_UNAVAILABLE", 0)
        if raw.get("result") is None:
            raise IntegrityError("ETH_MISSING_RESULT")
        return raw["result"], received

    async def health(
        self,
        ok: bool,
        reason: str,
        code: int = 0,
        error_type: str | None = None,
        *,
        accepted_event: MarketEvent | None = None,
        response_received_at_ms: int | None = None,
    ) -> None:
        await self.sink(
            eth_gas_poll_health(
                self.provider,
                now_ms(),
                ok=ok,
                reason=reason,
                code=code,
                error_type=error_type,
                accepted_event_id=accepted_event.event_id if accepted_event else None,
                accepted_observed_at_ms=accepted_event.observed_at_ms if accepted_event else None,
                response_received_at_ms=response_received_at_ms,
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        response_received: int | None = None
        try:
            chain, chain_received = await self.request(session, "eth_chainId")
            if int(TypeAdapter(Quantity).validate_python(chain), 16) != 1:
                raise IntegrityError("ETH_WRONG_CHAIN")
            raw, received = await self.request(session, "eth_getBlockByNumber")
            response_received = received
            if chain_received > received:
                raise IntegrityError("ETH_RECEIPT_CLOCK_REGRESSION")
            event = gas_event(
                raw,
                received,
                int(self.config.ingestion.eth_gas_max_block_age_s * 1000),
                provider=self.provider,
            )
            check_progress(self.previous, event)
            retained = (
                self.previous
                if self.previous and self.previous.event_id == event.event_id
                else event
            )
            if retained.observed_at_ms > received:
                raise IntegrityError("ETH_RECEIPT_CLOCK_REGRESSION")
        except FeedUnavailable as exc:
            await self.health(
                False, exc.reason, exc.code, response_received_at_ms=response_received
            )
            return False
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            await self.health(
                False,
                "TRANSPORT_UNAVAILABLE",
                error_type=type(exc).__name__,
                response_received_at_ms=response_received,
            )
            return False
        except (ValidationError, IntegrityError):
            await self.health(False, "INVALID_DATA", response_received_at_ms=response_received)
            return False
        await self.sink(retained)
        self.previous = retained
        await self.health(
            True,
            "FINALIZED_GAS_OBSERVED",
            accepted_event=retained,
            response_received_at_ms=received,
        )
        return True

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            while True:
                ok = await self.collect(session)
                await asyncio.sleep(self.config.ingestion.eth_gas_poll_interval_s if ok else 600)
