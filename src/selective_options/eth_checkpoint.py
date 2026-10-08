"""Explicit ETH startup anchors from validated committed observations; no network recovery."""

from pathlib import Path
from typing import Annotated, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, StrictInt

from .errors import IntegrityError
from .eth_gas import PUBLIC_PROVIDERS, Provider, check_progress
from .events import MarketEvent, make_event
from .store import complete_thread

Hash = Annotated[str, Field(strict=True, pattern=r"^0x[0-9a-f]{64}$")]
Amount = Annotated[str, Field(strict=True, pattern=r"^(0|[1-9][0-9]{0,77})$")]


class GasCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    chain_id: Literal[1]
    block_number: StrictInt = Field(ge=0, lt=2**64)
    block_hash: Hash
    parent_hash: Hash
    gas_limit: Amount
    gas_used: Amount
    gas_units: Literal["gas"]
    base_fee_per_gas_wei: Amount
    transaction_count: StrictInt = Field(ge=0, le=100_000)
    requested_block_tag: Literal["finalized"]
    finality_basis: Literal["RPC provider assertion; not independently verified"]
    timestamp_basis: Literal[
        "block header time; first publication unavailable; receipt also required"
    ]
    coverage: Literal[
        "sampled finalized blocks; no continuous history or exchange-flow attribution"
    ]
    execution_quote: Literal[False]


def validate_checkpoint(event: MarketEvent, now: int, *, provider: Provider = "publicnode") -> None:
    if (
        type(now) is not int
        or now <= 0
        or type(provider) is not str
        or provider not in PUBLIC_PROVIDERS
    ):
        raise ValueError("invalid ETH checkpoint clock or provider policy")
    if (
        event.source,
        event.environment,
        event.asset,
        event.symbol,
        event.kind,
        event.timestamp_kind,
    ) != (
        PUBLIC_PROVIDERS[provider][1],
        "public_reference",
        "ETH",
        "ETH_MAINNET_GAS",
        "on_chain",
        "source",
    ):
        raise IntegrityError("ETH_CHECKPOINT_SCOPE")
    payload = GasCheckpoint.model_validate_json(event.payload_json)
    if (
        event.event_time_ms % 1000
        or max(event.observed_at_ms, event.event_time_ms) > now
        or event.event_time_ms > event.observed_at_ms + 2000
    ):
        raise IntegrityError("ETH_CHECKPOINT_CLOCK")
    limit, used, fee = (
        int(payload.gas_limit),
        int(payload.gas_used),
        int(payload.base_fee_per_gas_wei),
    )
    if not 0 < limit < 2**256 or used > limit or fee >= 2**256:
        raise IntegrityError("ETH_CHECKPOINT_QUANTITIES")
    canonical = make_event(
        source=event.source,
        environment=event.environment,
        asset=event.asset,
        symbol=event.symbol,
        kind=event.kind,
        event_time_ms=event.event_time_ms,
        observed_at_ms=event.observed_at_ms,
        native_id=payload.block_hash,
        payload=payload.model_dump(mode="json"),
    )
    if canonical != event:
        raise IntegrityError("ETH_CHECKPOINT_IDENTITY")


def _load(path: Path, now: int, max_rows: int, provider: Provider) -> MarketEvent:
    if (
        type(now) is not int
        or now <= 0
        or type(max_rows) is not int
        or not 0 < max_rows <= 100_000
        or type(provider) is not str
        or provider not in PUBLIC_PROVIDERS
    ):
        raise ValueError("invalid bounded ETH checkpoint clock or provider policy")
    if not path.is_file():
        raise FileNotFoundError("ETH checkpoint database does not exist")
    with duckdb.connect(str(path), read_only=True) as db:
        cursor = db.execute(
            "SELECT * FROM events WHERE source=? AND environment='public_reference' "
            "AND kind='on_chain' ORDER BY observed_at_ms, event_time_ms, event_id LIMIT ?",
            [PUBLIC_PROVIDERS[provider][1], max_rows + 1],
        )
        columns = [c[0] for c in cursor.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        ]
    if not rows or len(rows) > max_rows:
        raise IntegrityError("ETH_CHECKPOINT_EMPTY_OR_OVER_BUDGET")
    previous: MarketEvent | None = None
    for event in rows:
        validate_checkpoint(event, now, provider=provider)
        check_progress(previous, event)
        previous = event
    return rows[-1]


async def load_eth_checkpoint(
    path: Path, now: int, max_rows: int = 10_000, *, provider: Provider = "publicnode"
) -> MarketEvent:
    """Read-only, bounded complete validation; never silently truncate conflicting history."""
    return await complete_thread(_load, path, now, max_rows, provider)
