"""Bounded read-only SOL performance and vote-account stake observations."""

import asyncio
from typing import Annotated, Literal

import aiohttp
from pydantic import Field, JsonValue, StrictBool, StrictInt, TypeAdapter, ValidationError

from .bybit import FeedUnavailable
from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import ExternalModel

URL = "https://api.mainnet.solana.com"
GENESIS = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
Method = Literal["getGenesisHash", "getRecentPerformanceSamples", "getBlockTime", "getVoteAccounts"]
U64 = Annotated[StrictInt, Field(ge=0, le=2**64 - 1)]
Pubkey = Annotated[str, Field(strict=True, pattern=r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")]


class Performance(ExternalModel):
    slot: U64 = Field(gt=0)
    numTransactions: U64
    numSlots: U64 = Field(gt=0)
    samplePeriodSecs: StrictInt = Field(gt=0, le=65535)
    numNonVoteTransactions: U64 | None = None


class Vote(ExternalModel):
    votePubkey: Pubkey
    nodePubkey: Pubkey
    activatedStake: U64
    epochVoteAccount: StrictBool
    lastVote: U64
    rootSlot: U64
    commission: StrictInt = Field(ge=0, le=100)


class Votes(ExternalModel):
    current: tuple[Vote, ...] = Field(max_length=10_000)
    delinquent: tuple[Vote, ...] = Field(max_length=10_000)


def network_events(
    performance: JsonValue,
    block_time: JsonValue,
    votes: JsonValue,
    receipts: tuple[int, int, int, int],
    max_age_ms: int,
) -> tuple[MarketEvent, MarketEvent]:
    if (
        len(receipts) != 4
        or any(type(r) is not int or r <= 0 for r in receipts)
        or tuple(sorted(receipts)) != receipts
        or type(max_age_ms) is not int
        or max_age_ms <= 0
    ):
        raise IntegrityError("SOL_RECEIPT_POLICY")
    samples = TypeAdapter(list[Performance]).validate_python(performance)
    if len(samples) != 1:
        raise IntegrityError("SOL_PERFORMANCE_SAMPLE_COUNT")
    sample = samples[0]
    if (
        sample.numSlots > sample.slot + 1
        or sample.numNonVoteTransactions is not None
        and sample.numNonVoteTransactions > sample.numTransactions
    ):
        raise IntegrityError("SOL_PERFORMANCE_COUNTS")
    seconds = TypeAdapter(StrictInt).validate_python(block_time)
    ts = seconds * 1000
    if seconds <= 0 or not receipts[1] - max_age_ms <= ts <= receipts[1] + 2000:
        raise IntegrityError("SOL_SAMPLE_TIME_UNAVAILABLE_OR_STALE")
    parsed = Votes.model_validate(votes)
    all_votes = (*parsed.current, *parsed.delinquent)
    if (
        not all_votes
        or len(all_votes) > 10_000
        or len({v.votePubkey for v in all_votes}) != len(all_votes)
    ):
        raise IntegrityError("SOL_VOTE_ACCOUNT_COUNT_OR_DUPLICATES")
    current_stake = sum(v.activatedStake for v in parsed.current)
    delinquent_stake = sum(v.activatedStake for v in parsed.delinquent)
    if current_stake + delinquent_stake > 2**64 - 1:
        raise IntegrityError("SOL_AGGREGATE_STAKE_RANGE")
    common: dict[str, JsonValue] = {
        "genesis_hash": GENESIS,
        "genesis_observed_at_ms": receipts[0],
        "source_url": URL,
        "publication_time_available": False,
        "trading_approval": False,
    }
    perf = make_event(
        source="solana_public",
        environment="public_reference",
        asset="SOL",
        symbol="SOL_PERFORMANCE",
        kind="on_chain",
        event_time_ms=ts,
        observed_at_ms=receipts[2],
        native_id=str(sample.slot),
        payload={
            **common,
            "metric": "performance",
            "sample": sample.model_dump(mode="json"),
            "sample_response_received_at_ms": receipts[1],
            "block_time_seconds": seconds,
            "block_time_response_received_at_ms": receipts[2],
            "transactions_per_second_numerator": str(sample.numTransactions),
            "transactions_per_second_denominator": sample.samplePeriodSecs,
            "non_vote_transactions_per_second_numerator": str(sample.numNonVoteTransactions)
            if sample.numNonVoteTransactions is not None
            else None,
            "timestamp_basis": "estimated production time of reported sample-end slot; exact sample start and first publication unavailable",
            "finality_verified": False,
            "max_sample_age_ms": max_age_ms,
            "coverage": "one recent node performance sample; no active fee-payer or DEX-volume inference",
        },
    )
    stake = make_event(
        source="solana_public",
        environment="public_reference",
        asset="SOL",
        symbol="SOL_VOTE_STAKE",
        kind="on_chain",
        event_time_ms=receipts[3],
        observed_at_ms=receipts[3],
        timestamp_kind="receipt",
        payload={
            **common,
            "metric": "vote_stake",
            "vote_accounts": parsed.model_dump(mode="json"),
            "current_activated_stake_lamports": str(current_stake),
            "delinquent_activated_stake_lamports": str(delinquent_stake),
            "total_visible_activated_stake_lamports": str(current_stake + delinquent_stake),
            "current_vote_accounts": len(parsed.current),
            "delinquent_vote_accounts": len(parsed.delinquent),
            "units": "lamports",
            "lamports_per_sol": 1_000_000_000,
            "requested_commitment": "finalized",
            "keep_unstaked_delinquents": True,
            "snapshot_received_at_ms": receipts[3],
            "timestamp_basis": "vote-account snapshot has no context slot/time; receipt only, not the performance sample time",
            "coverage": "activated stake visible in returned current/delinquent vote accounts, not all stake accounts or staking flows",
            "finality_basis": "RPC commitment request; not independently verified",
        },
    )
    return perf, stake


class SolanaNetworkCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink

    async def request(
        self, session: aiohttp.ClientSession, method: Method, slot: int | None = None
    ) -> tuple[JsonValue, int]:
        methods = (
            "getGenesisHash",
            "getRecentPerformanceSamples",
            "getBlockTime",
            "getVoteAccounts",
        )
        if method not in methods:
            raise ValueError("unsupported read-only Solana method")
        if method == "getBlockTime":
            if type(slot) is not int or not 0 < slot < 2**64:
                raise ValueError("block time requires a positive bounded slot")
            params: list[JsonValue] = [slot]
        elif slot is not None:
            raise ValueError("unexpected slot argument")
        elif method == "getRecentPerformanceSamples":
            params = [1]
        elif method == "getVoteAccounts":
            params = [{"commitment": "finalized", "keepUnstakedDelinquents": True}]
        else:
            params = []
        rpc_id = methods.index(method) + 1
        async with session.post(
            URL,
            json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params},
            allow_redirects=False,
        ) as response:
            if response.status in {401, 403, 451}:
                raise FeedUnavailable("ACCESS_DENIED", response.status)
            if response.status == 429:
                raise FeedUnavailable("RATE_LIMITED", response.status)
            if response.status != 200:
                raise FeedUnavailable("HTTP_UNAVAILABLE", response.status)
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > 4_000_000:
                    raise IntegrityError("SOL_RESPONSE_TOO_LARGE")
            received = now_ms()
        raw = TypeAdapter(dict[str, JsonValue]).validate_json(body)
        if raw.get("jsonrpc") != "2.0" or type(raw.get("id")) is not int or raw["id"] != rpc_id:
            raise IntegrityError("SOL_RPC_IDENTITY")
        if "error" in raw and "result" in raw:
            raise IntegrityError("SOL_RPC_AMBIGUOUS_ENVELOPE")
        if "error" in raw:
            raise FeedUnavailable("RPC_UNAVAILABLE", 0)
        if raw.get("result") is None:
            raise IntegrityError("SOL_MISSING_RESULT")
        return raw["result"], received

    async def health(
        self,
        ok: bool,
        reason: str,
        code: int = 0,
        error_type: str | None = None,
        events: tuple[MarketEvent, ...] = (),
    ) -> None:
        received = now_ms()
        if any(e.observed_at_ms > received for e in events):
            raise IntegrityError("SOL_HEALTH_CLOCK_REGRESSION")
        await self.sink(
            make_event(
                source="solana_public",
                environment="public_reference",
                asset="SOL",
                symbol="SOL_NETWORK",
                kind="feed_health",
                timestamp_kind="receipt",
                event_time_ms=received,
                observed_at_ms=received,
                payload={
                    "ok": ok,
                    "reason": reason,
                    "code": code,
                    "error_type": error_type,
                    "event_ids": [e.event_id for e in events],
                    "scope": "performance and vote-account stake; sink accepted, no atomic commit claim",
                },
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        try:
            async with asyncio.timeout(self.config.ingestion.solana_network_timeout_s):
                genesis, g = await self.request(session, "getGenesisHash")
                if genesis != GENESIS:
                    raise IntegrityError("SOL_WRONG_GENESIS")
                await asyncio.sleep(0.25)
                raw, p = await self.request(session, "getRecentPerformanceSamples")
                samples = TypeAdapter(list[Performance]).validate_python(raw)
                if len(samples) != 1:
                    raise IntegrityError("SOL_PERFORMANCE_SAMPLE_COUNT")
                await asyncio.sleep(0.25)
                time, t = await self.request(session, "getBlockTime", samples[0].slot)
                await asyncio.sleep(0.25)
                votes, v = await self.request(session, "getVoteAccounts")
                events = network_events(
                    raw,
                    time,
                    votes,
                    (g, p, t, v),
                    int(self.config.ingestion.solana_network_max_sample_age_s * 1000),
                )
        except FeedUnavailable as exc:
            await self.health(False, exc.reason, exc.code)
            return False
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            await self.health(False, "TRANSPORT_UNAVAILABLE", error_type=type(exc).__name__)
            return False
        except (ValidationError, IntegrityError):
            await self.health(False, "INVALID_DATA")
            return False
        for event in events:
            await self.sink(event)
        await self.health(True, "NETWORK_SAMPLES_ACCEPTED", events=events)
        return True

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            while True:
                ok = await self.collect(session)
                await asyncio.sleep(
                    self.config.ingestion.solana_network_poll_interval_s if ok else 600
                )
