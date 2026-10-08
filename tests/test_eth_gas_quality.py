import json

import pytest

from selective_options.errors import IntegrityError
from selective_options.eth_gas import eth_gas_poll_health, gas_event
from selective_options.eth_gas_quality import eth_gas_quality
from selective_options.events import make_event

BLOCK = 1_000_000
RECEIVED = 1_100_000
POLLED = RECEIVED + 50


def gas(received=RECEIVED, provider="publicnode", **changes):
    return gas_event(
        {
            "number": "0x64",
            "hash": "0x" + "11" * 32,
            "parentHash": "0x" + "22" * 32,
            "timestamp": hex(BLOCK // 1000),
            "gasLimit": "0x100",
            "gasUsed": "0x10",
            "baseFeePerGas": "0x20",
            "transactions": [],
            **changes,
        },
        received,
        1_800_000,
        provider=provider,
    )


def success(row, polled=POLLED, response=RECEIVED, provider="publicnode"):
    return eth_gas_poll_health(
        provider,
        polled,
        ok=True,
        reason="FINALIZED_GAS_OBSERVED",
        accepted_event_id=row.event_id,
        accepted_observed_at_ms=row.observed_at_ms,
        response_received_at_ms=response,
    )


def quality(rows, as_of=POLLED, **kwargs):
    return eth_gas_quality(
        rows,
        provider=kwargs.pop("provider", "publicnode"),
        as_of_ms=as_of,
        max_poll_age_ms=kwargs.pop("max_poll_age_ms", 1000),
        max_block_age_ms=kwargs.pop("max_block_age_ms", 200000),
        **kwargs,
    )


def changed(row, **values):
    return row.model_copy(update={"payload_json": json.dumps({**row.payload(), **values})})


def legacy(ok=True, polled=POLLED):
    return make_event(
        source="ethereum_publicnode",
        environment="public_reference",
        asset="ETH",
        symbol="ETH_MAINNET_GAS",
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=polled,
        observed_at_ms=polled,
        payload={
            "ok": ok,
            "reason": "FINALIZED_GAS_OBSERVED" if ok else "RPC_UNAVAILABLE",
            "code": 0,
            "error_type": None,
        },
    )


def test_both_clocks_poll_and_block_age_boundaries():
    row = gas()
    poll = success(row)
    assert quality([row, poll], RECEIVED - 1)["blocker"] == "ETH_HEALTH_MISSING"
    assert quality([row, poll], POLLED - 1)["blocker"] == "ETH_HEALTH_MISSING"
    result = quality([row, poll])
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["accepted_event_id"] == row.event_id
    assert result["accepted_observed_at_ms"] == RECEIVED
    assert result["poll_age_ms"] == 0 and result["response_age_ms"] == 50
    assert result["observation_age_ms"] == 50 and result["block_age_ms"] == 100050
    assert quality([row, poll], RECEIVED + 1000)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, poll], RECEIVED + 1001)["blocker"] == "ETH_POLL_STALE"
    assert quality([row, poll], max_block_age_ms=100050)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, poll], max_block_age_ms=100049)["blocker"] == "ETH_BLOCK_STALE"
    assert result["complete_feed_available"] is False
    assert result["continuous_history_complete"] is False
    assert result["finality_independently_verified"] is False
    assert result["trading_approval"] is False


def test_repeat_poll_preserves_original_observation_age_and_block_time():
    row = gas()
    poll = success(row)
    repeat = success(row, POLLED + 1000, RECEIVED + 1000)
    result = quality([row, poll, repeat], POLLED + 1000)
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["response_age_ms"] == 50
    assert result["observation_age_ms"] == 1050
    assert result["block_age_ms"] == 101050
    assert result["accepted_observed_at_ms"] == RECEIVED
    assert result["health_event_id"] == repeat.event_id


def test_latest_failure_and_recovery_receipt_boundaries():
    row = gas()
    poll = success(row)
    failure = eth_gas_poll_health("publicnode", POLLED + 100, ok=False, reason="RPC_UNAVAILABLE")
    recovered = success(row, POLLED + 200, RECEIVED + 200)
    rows = [row, poll, failure, recovered]
    assert quality(rows, POLLED + 99)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality(rows, POLLED + 100)["blocker"] == "ETH_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 199)["blocker"] == "ETH_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 200)["health_event_id"] == recovered.event_id
    assert quality(rows, POLLED + 200)["state"] == "POLL_OBSERVED_VERIFIED"


def test_legacy_success_remains_unbound_and_failure_blocks():
    row = gas()
    result = quality([row, legacy()])
    assert result["blocker"] == "ETH_HEALTH_UNBOUND"
    assert result["legacy_health_rows"] == 1
    assert (
        quality([row, legacy(), legacy(False, POLLED + 1)], POLLED + 1)["blocker"]
        == "ETH_FEED_UNAVAILABLE"
    )


@pytest.mark.parametrize(
    "mutation",
    [
        {"ok": 1},
        {"code": True},
        {"accepted_observed_at_ms": True},
        {"response_received_at_ms": "1100000"},
        {"accepted_event_id": "invalid"},
        {"accepted_event_id": None},
        {"reason": "RPC_UNAVAILABLE"},
        {"unknown": "extra"},
        {"response_received_at_ms": POLLED + 1},
        {"accepted_observed_at_ms": RECEIVED + 1},
    ],
)
def test_malformed_noncanonical_bound_health_blocks(mutation):
    row = gas()
    assert quality([row, changed(success(row), **mutation)])["state"] == "BLOCKED"


@pytest.mark.parametrize(
    "field,value",
    [("event_id", "wrong"), ("timestamp_kind", "source"), ("event_time_ms", POLLED - 1)],
)
def test_health_identity_and_receipt_clock_corruption_blocks(field, value):
    row = gas()
    result = quality([row, success(row).model_copy(update={field: value})])
    assert result["blocker"] == "ETH_HEALTH_CANONICAL"


def test_missing_wrong_receipt_late_and_future_references():
    row = gas()
    poll = success(row)
    assert quality([poll])["blocker"] == "ETH_REFERENCED_GAS_MISSING"
    wrong = gas(RECEIVED - 1)
    assert wrong.event_id == row.event_id
    assert quality([wrong, poll])["blocker"] == "ETH_REFERENCED_GAS_RECEIPT_MISMATCH"
    late = gas(RECEIVED + 1)
    assert quality([late, poll])["blocker"] == "ETH_REFERENCED_GAS_RECEIPT_MISMATCH"
    future = row.model_copy(update={"event_time_ms": POLLED + 1})
    assert quality([future, poll])["blocker"] == "ETH_REFERENCED_GAS_MISSING"
    future = row.model_copy(update={"observed_at_ms": POLLED + 1})
    assert quality([future, poll])["blocker"] == "ETH_REFERENCED_GAS_MISSING"


def test_exact_duplicates_ambiguous_latest_and_id_conflicts():
    row = gas()
    poll = success(row)
    assert quality([row, row, poll, poll])["state"] == "POLL_OBSERVED_VERIFIED"
    tied = eth_gas_poll_health("publicnode", POLLED, ok=False, reason="RPC_UNAVAILABLE")
    assert quality([row, poll, tied])["blocker"] == "ETH_HEALTH_AMBIGUOUS"
    assert quality([row, gas(RECEIVED + 1), poll])["blocker"] == "ETH_EVENT_ID_CONFLICT"


@pytest.mark.parametrize("provider", ["publicnode", "cloudflare", "drpc"])
def test_provider_scope_is_exact(provider):
    row = gas(provider=provider)
    poll = success(row, provider=provider)
    assert quality([row, poll], provider=provider)["state"] == "POLL_OBSERVED_VERIFIED"
    other = "drpc" if provider != "drpc" else "publicnode"
    assert quality([row, poll], provider=other)["blocker"] == "ETH_HEALTH_MISSING"
    failed = eth_gas_poll_health(other, POLLED + 1, ok=False, reason="RPC_UNAVAILABLE")
    assert (
        quality([row, poll, failed], POLLED + 1, provider=provider)["state"]
        == "POLL_OBSERVED_VERIFIED"
    )


@pytest.mark.parametrize(
    "change",
    [
        {"gas_used": "9999"},
        {"base_fee_per_gas_wei": "-1"},
        {"chain_id": True},
        {"execution_quote": 0},
        {"unknown": "extra"},
    ],
)
def test_gas_payload_corruption_blocks(change):
    row = gas()
    assert quality([changed(row, **change), success(row)])["state"] == "BLOCKED"


def test_progression_conflicts_block_even_with_other_bound_poll():
    row = gas()
    later = gas(
        RECEIVED + 10, number="0x65", timestamp=hex(BLOCK // 1000 + 1), hash="0x" + "33" * 32
    )
    assert (
        quality([row, later, success(later, POLLED, RECEIVED + 10)])["blocker"]
        == "ETH_PARENT_HASH_MISMATCH"
    )


def test_eligible_row_budget_counts_duplicates_and_stops():
    row = gas()
    poll = success(row)
    assert quality([row, poll], max_rows=2)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, row, poll], max_rows=2)["blocker"] == "ETH_QUALITY_OVER_BUDGET"

    def bounded():
        yield row
        yield poll
        raise AssertionError("consumed beyond rejected eligible row")

    assert quality(bounded(), max_rows=1)["blocker"] == "ETH_QUALITY_OVER_BUDGET"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"provider": True},
        {"provider": []},
        {"provider": "invalid"},
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_poll_age_ms": 0},
        {"max_poll_age_ms": 1.5},
        {"max_block_age_ms": False},
        {"max_rows": True},
        {"max_rows": 0},
        {"max_rows": 100001},
    ],
)
def test_invalid_policy_preflight_never_consumes_iterable(kwargs):
    def never():
        raise AssertionError("policy read rows")
        yield

    policy = {
        "provider": "publicnode",
        "as_of_ms": POLLED,
        "max_poll_age_ms": 1000,
        "max_block_age_ms": 200000,
        **kwargs,
    }
    with pytest.raises(ValueError):
        eth_gas_quality(never(), **policy)


@pytest.mark.parametrize(
    "updates",
    [
        {"ok": 1},
        {"reason": "UNKNOWN"},
        {"code": True},
        {"response_received_at_ms": True},
        {"response_received_at_ms": POLLED + 1},
        {"accepted_event_id": "x"},
        {"accepted_observed_at_ms": RECEIVED + 1},
        {"error_type": "fixture"},
    ],
)
def test_health_helper_rejects_invalid_success(updates):
    row = gas()
    args = {
        "ok": True,
        "reason": "FINALIZED_GAS_OBSERVED",
        "accepted_event_id": row.event_id,
        "accepted_observed_at_ms": row.observed_at_ms,
        "response_received_at_ms": RECEIVED,
        **updates,
    }
    with pytest.raises((ValueError, IntegrityError)):
        eth_gas_poll_health("publicnode", POLLED, **args)
