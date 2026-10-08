"""Bounded provider-scoped finalized gas poll quality; no consensus or trading approval."""

from collections.abc import Iterable

from .errors import IntegrityError
from .eth_gas import PUBLIC_PROVIDERS, GasPollHealth, Provider, check_progress, eth_gas_poll_health
from .events import MarketEvent, make_event

_BINDING_FIELDS = {"accepted_event_id", "accepted_observed_at_ms", "response_received_at_ms"}


def _health_valid(row: MarketEvent, provider: Provider) -> GasPollHealth | None:
    payload = row.payload()
    legacy = not _BINDING_FIELDS.intersection(payload)
    if legacy:
        status = GasPollHealth.model_validate({**payload, **dict.fromkeys(_BINDING_FIELDS)})
        if status.ok:
            if (
                status.reason != "FINALIZED_GAS_OBSERVED"
                or status.code
                or status.error_type is not None
            ):
                raise IntegrityError("ETH_LEGACY_HEALTH_INVALID")
        else:
            eth_gas_poll_health(provider, row.observed_at_ms, **status.model_dump())
        canonical = make_event(
            source=PUBLIC_PROVIDERS[provider][1],
            environment="public_reference",
            asset="ETH",
            symbol="ETH_MAINNET_GAS",
            kind="feed_health",
            timestamp_kind="receipt",
            event_time_ms=row.observed_at_ms,
            observed_at_ms=row.observed_at_ms,
            payload=payload,
        )
    else:
        status = GasPollHealth.model_validate(payload)
        canonical = eth_gas_poll_health(provider, row.observed_at_ms, **status.model_dump())
    if canonical != row:
        raise IntegrityError("ETH_HEALTH_CANONICAL")
    return None if legacy else status


def eth_gas_quality(
    events: Iterable[MarketEvent],
    *,
    provider: Provider,
    as_of_ms: int,
    max_poll_age_ms: int,
    max_block_age_ms: int,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Latest eligible poll binds one exact observation, using both availability clocks.

    Duplicate eligible rows count toward the application budget. The budget does
    not bound iterable scanning, database execution or storage engine memory.
    """
    if (
        type(provider) is not str
        or provider not in PUBLIC_PROVIDERS
        or any(
            type(value) is not int or value <= 0
            for value in (as_of_ms, max_poll_age_ms, max_block_age_ms)
        )
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
    ):
        raise ValueError("invalid bounded ETH gas quality policy")
    source = PUBLIC_PROVIDERS[provider][1]
    result: dict[str, object] = {
        "provider": provider,
        "source": source,
        "asset": "ETH",
        "symbol": "ETH_MAINNET_GAS",
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "health_received_at_ms": None,
        "response_received_at_ms": None,
        "poll_ok": None,
        "reason": None,
        "error_type": None,
        "code": None,
        "accepted_event_id": None,
        "accepted_observed_at_ms": None,
        "block_time_ms": None,
        "block_number": None,
        "block_hash": None,
        "poll_age_ms": None,
        "response_age_ms": None,
        "observation_age_ms": None,
        "block_age_ms": None,
        "historical_gas_observations": 0,
        "legacy_health_rows": 0,
        "max_poll_age_ms": max_poll_age_ms,
        "max_block_age_ms": max_block_age_ms,
        "scope": "Latest provider-scoped finalized gas poll and exact retained accepted observation. Sink acceptance is not atomic commit proof. Header time is provider data, distinct from local response and observation receipts.",
        "continuous_history_complete": False,
        "complete_feed_available": False,
        "finality_independently_verified": False,
        "trading_approval": False,
    }
    try:
        eligible: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                row.source != source
                or row.environment != "public_reference"
                or row.asset != "ETH"
                or row.symbol != "ETH_MAINNET_GAS"
                or row.kind not in {"on_chain", "feed_health"}
                or row.event_time_ms > as_of_ms
                or row.observed_at_ms > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("ETH_QUALITY_OVER_BUDGET")
            if any(
                type(clock) is not int or clock <= 0
                for clock in (row.event_time_ms, row.observed_at_ms)
            ):
                raise IntegrityError("ETH_EVIDENCE_CLOCK_INVALID")
            if row.event_id in eligible and eligible[row.event_id] != row:
                raise IntegrityError("ETH_EVENT_ID_CONFLICT")
            eligible[row.event_id] = row
        # Avoid a store -> quality -> checkpoint -> store import cycle.
        from .eth_checkpoint import validate_checkpoint

        gas: dict[str, MarketEvent] = {}
        health: list[tuple[MarketEvent, GasPollHealth | None]] = []
        for row in eligible.values():
            if row.kind == "on_chain":
                validate_checkpoint(row, as_of_ms, provider=provider)
                gas[row.event_id] = row
            else:
                health.append((row, _health_valid(row, provider)))
        previous = None
        for row in sorted(
            gas.values(),
            key=lambda event: (event.observed_at_ms, event.event_time_ms, event.event_id),
        ):
            check_progress(previous, row)
            previous = row
        result["historical_gas_observations"] = len(gas)
        result["legacy_health_rows"] = sum(status is None for _, status in health)
        if not health:
            raise IntegrityError("ETH_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        choices = [(row, status) for row, status in health if row.observed_at_ms == latest]
        if len(choices) != 1:
            raise IntegrityError("ETH_HEALTH_AMBIGUOUS")
        selected, status = choices[0]
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
        if status is not None:
            result.update(
                response_received_at_ms=status.response_received_at_ms,
                accepted_event_id=status.accepted_event_id,
                accepted_observed_at_ms=status.accepted_observed_at_ms,
            )
            if status.response_received_at_ms is not None:
                result["response_age_ms"] = as_of_ms - status.response_received_at_ms
        if payload["ok"] is not True:
            raise IntegrityError("ETH_FEED_UNAVAILABLE")
        if status is None:
            raise IntegrityError("ETH_HEALTH_UNBOUND")
        assert status.response_received_at_ms is not None and status.accepted_event_id is not None
        if as_of_ms - min(latest, status.response_received_at_ms) > max_poll_age_ms:
            raise IntegrityError("ETH_POLL_STALE")
        if status.accepted_event_id not in gas:
            raise IntegrityError("ETH_REFERENCED_GAS_MISSING")
        referenced = gas[status.accepted_event_id]
        if referenced.observed_at_ms != status.accepted_observed_at_ms:
            raise IntegrityError("ETH_REFERENCED_GAS_RECEIPT_MISMATCH")
        if referenced.observed_at_ms > status.response_received_at_ms:
            raise IntegrityError("ETH_GAS_AFTER_POLL_RESPONSE")
        body = referenced.payload()
        result.update(
            block_time_ms=referenced.event_time_ms,
            block_number=body["block_number"],
            block_hash=body["block_hash"],
            observation_age_ms=as_of_ms - referenced.observed_at_ms,
            block_age_ms=as_of_ms - referenced.event_time_ms,
        )
        if as_of_ms - referenced.event_time_ms > max_block_age_ms:
            raise IntegrityError("ETH_BLOCK_STALE")
        result["state"] = "POLL_OBSERVED_VERIFIED"
    except (IntegrityError, ValueError, TypeError, KeyError, OverflowError) as exc:
        result["blocker"] = str(exc) if isinstance(exc, IntegrityError) else "ETH_INVALID_EVIDENCE"
    return result
