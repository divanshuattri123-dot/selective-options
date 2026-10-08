"""Stablecoin collector/quality fixtures never constitute public-success evidence."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.stablecoin_supply import (
    DECIMALS_SELECTOR,
    MAX_RESPONSE_BYTES,
    SOURCE,
    STEPS,
    SUPPLY_SELECTOR,
    SYMBOL,
    URL,
    USDC_ADDRESS,
    USDT_ADDRESS,
    StablecoinSupplyCollector,
    check_supply_progress,
    rpc_request,
    stablecoin_poll_health,
    stablecoin_supply_quality,
    supply_event,
    validate_supply_event,
)

BLOCK_TIME = 1_000_000
RECEIVED = 1_100_000
POLLED = RECEIVED + 100


def word(value):
    return "0x" + f"{value:064x}"


def header(**updates):
    return {
        "number": "0x64",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": hex(BLOCK_TIME // 1000),
        **updates,
    }


def results(offset=0, **changes):
    block = header(**changes)
    values = ["0x1", block, word(2**256 - 1), word(6), word(9007199254740993), word(6), block]
    return [(value, RECEIVED - 6 + i + offset) for i, value in enumerate(values)]


def snapshot(offset=0, **changes):
    return supply_event(results(offset, **changes), 1800000)


def health(row, polled=POLLED):
    return stablecoin_poll_health(
        polled,
        ok=True,
        reason="SUPPLY_SNAPSHOT_OBSERVED",
        accepted_event_id=row.event_id,
        accepted_observed_at_ms=row.observed_at_ms,
        response_received_at_ms=row.observed_at_ms,
    )


def quality(rows, as_of=POLLED, **kwargs):
    return stablecoin_supply_quality(rows, as_of_ms=as_of, **kwargs)


def mutate(row, **updates):
    return row.model_copy(update={"payload_json": json.dumps({**row.payload(), **updates})})


def test_native_uint256_abi_units_receipts_and_poll_revisions():
    row = snapshot()
    body = validate_supply_event(row, POLLED)
    assert body.usdc.total_supply_base_units == str(2**256 - 1)
    assert body.usdt.total_supply_base_units == "9007199254740993"
    assert body.usdc.total_supply_abi_word == word(2**256 - 1)
    assert body.usdc.decimals == body.usdt.decimals == 6
    assert body.usdc.units == "token base units"
    assert row.event_time_ms == BLOCK_TIME and row.observed_at_ms == RECEIVED
    assert body.usdc.contract_address == USDC_ADDRESS and body.usdt.contract_address == USDT_ADDRESS
    assert body.chain_received_at_ms == RECEIVED - 6 and body.reference_received_at_ms == RECEIVED
    repeat = snapshot(1000)
    assert row.event_id != repeat.event_id and row.observed_at_ms != repeat.observed_at_ms
    assert row.event_time_ms == repeat.event_time_ms
    check_supply_progress(row, repeat)
    check_supply_progress(None, row)
    assert body.coverage.startswith("Ethereum mainnet")
    assert body.execution_quote is False


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "0x2"),
        (0, True),
        (0, "0x01"),
        (2, "0x"),
        (2, "0x1"),
        (2, 1),
        (2, "0x" + "f" * 65),
        (2, "0x" + "z" * 64),
        (3, word(5)),
        (3, word(256)),
        (4, None),
        (5, word(18)),
    ],
)
def test_wrong_chain_abi_width_types_and_decimals_rejected(index, value):
    rows = results()
    rows[index] = (value, rows[index][1])
    with pytest.raises((IntegrityError, ValidationError)):
        supply_event(rows, 1800000)


@pytest.mark.parametrize(
    "change",
    [
        {"number": "0x0"},
        {"number": hex(2**64)},
        {"number": "0x01"},
        {"timestamp": "0x0"},
        {"timestamp": hex(5000)},
        {"hash": "0xabc"},
        {"parentHash": None},
    ],
)
def test_invalid_headers_rejected(change):
    with pytest.raises((IntegrityError, ValidationError)):
        snapshot(**change)


def test_complete_pair_and_ordered_response_receipts_required():
    rows = results()
    with pytest.raises(IntegrityError):
        supply_event(rows[:-1], 1800000)
    rows[6] = (header(hash="0x" + "33" * 32), RECEIVED)
    with pytest.raises(IntegrityError, match="REFERENCED_HEADER_MISMATCH"):
        supply_event(rows, 1800000)
    rows = results()
    rows[3] = (word(6), RECEIVED + 1)
    with pytest.raises(IntegrityError, match="CLOCK_REGRESSION"):
        supply_event(rows, 1800000)
    rows = results()
    rows[0] = ("0x1", True)
    with pytest.raises(IntegrityError):
        supply_event(rows, 1800000)

    def too_many():
        yield from results()
        yield ("0x1", RECEIVED)
        raise AssertionError("read past bounded complete poll budget")

    with pytest.raises(IntegrityError):
        supply_event(too_many(), 1800000)


@pytest.mark.parametrize("age", [True, 0, -1, 1.1])
def test_invalid_builder_policy(age):
    with pytest.raises(ValueError):
        supply_event(results(), age)


def test_block_age_exact_boundary_and_zero_supply():
    assert supply_event(results(), 100000).observed_at_ms == RECEIVED
    with pytest.raises(IntegrityError):
        supply_event(results(), 99999)
    rows = results()
    rows[2] = (word(0), rows[2][1])
    assert supply_event(rows, 1800000).payload()["usdc"]["total_supply_base_units"] == "0"


@pytest.mark.parametrize("step", STEPS)
def test_seven_fixed_rpc_steps(step):
    request = rpc_request(step, None if step in {"chain", "finalized"} else 100)
    assert request["id"] == STEPS.index(step) + 1 and request["jsonrpc"] == "2.0"
    if step == "chain":
        assert request["method"] == "eth_chainId" and request["params"] == []
    elif step in {"finalized", "reference"}:
        assert request["method"] == "eth_getBlockByNumber"
        assert request["params"] == ["finalized" if step == "finalized" else "0x64", False]
    else:
        assert request["method"] == "eth_call"
        assert request["params"] == [
            {
                "to": USDC_ADDRESS if step.startswith("usdc") else USDT_ADDRESS,
                "data": SUPPLY_SELECTOR if step.endswith("supply") else DECIMALS_SELECTOR,
            },
            "0x64",
        ]


@pytest.mark.parametrize(
    "step,number",
    [
        ("eth_sendRawTransaction", 100),
        ("balanceOf", 100),
        ([], None),
        ("usdc_supply", "latest"),
        ("usdt_decimals", True),
        ("reference", 0),
        ("reference", 2**64),
        ("chain", 100),
    ],
)
def test_rpc_rejects_arbitrary_methods_and_non_numeric_blocks(step, number):
    with pytest.raises(ValueError):
        rpc_request(step, number)


class Response:
    def __init__(self, status=200, raw=None):
        self.status, self.content = status, self
        self.body = (
            raw
            if isinstance(raw, bytes)
            else json.dumps(raw or {"jsonrpc": "2.0", "id": 1, "result": "0x1"}).encode()
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        yield self.body


class Session:
    def __init__(self, response):
        self.response = response

    def post(self, url, **kwargs):
        assert url == URL and kwargs["allow_redirects"] is False
        assert kwargs["json"] == rpc_request("chain")
        return self.response


@pytest.fixture
def collector():
    return StablecoinSupplyCollector(AppConfig(), AsyncMock())


@pytest.mark.parametrize(
    "status,reason",
    [
        (401, "ACCESS_DENIED"),
        (403, "ACCESS_DENIED"),
        (451, "ACCESS_DENIED"),
        (429, "RATE_LIMITED"),
        (302, "HTTP_UNAVAILABLE"),
        (500, "HTTP_UNAVAILABLE"),
    ],
)
async def test_response_hook_preserves_body_before_http_failure(collector, status, reason):
    collector.response_received = AsyncMock()
    response = Response(status)
    with pytest.raises(FeedUnavailable) as failure:
        await collector.request(Session(response), "chain")
    assert failure.value.reason == reason
    collector.response_received.assert_awaited_once()
    assert collector.response_received.await_args.args[:3] == (
        rpc_request("chain"),
        status,
        response.body,
    )


@pytest.mark.parametrize(
    "raw",
    [
        {"jsonrpc": "1.0", "id": 1, "result": "0x1"},
        {"jsonrpc": "2.0", "id": True, "result": "0x1"},
        {"jsonrpc": "2.0", "id": 2, "result": "0x1"},
        {"jsonrpc": "2.0", "id": 1, "result": None},
        {"jsonrpc": "2.0", "id": 1},
    ],
)
async def test_rpc_identity_and_null_results_rejected(collector, raw):
    with pytest.raises(IntegrityError):
        await collector.request(Session(Response(raw=raw)), "chain")


async def test_rpc_success_size_rpc_error_and_write_denial(collector):
    assert (await collector.request(Session(Response()), "chain"))[0] == "0x1"
    with pytest.raises(IntegrityError):
        await collector.request(Session(Response(raw=b" " * (MAX_RESPONSE_BYTES + 1))), "chain")
    with pytest.raises(FeedUnavailable, match="RPC_UNAVAILABLE"):
        await collector.request(
            Session(Response(raw={"jsonrpc": "2.0", "id": 1, "error": {"code": -1}})), "chain"
        )
    with pytest.raises(ValueError):
        await collector.request(None, "eth_sendRawTransaction")


async def test_collector_success_binds_complete_pair_after_sink_and_repeats_revisions(
    collector, monkeypatch
):
    collector.request = AsyncMock(side_effect=[*results(), *results(1000)])
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED + 1000)
    assert await collector.collect(None)
    original = collector.previous
    assert await collector.collect(None)
    rows = [call.args[0] for call in collector.sink.await_args_list]
    assert [row.kind for row in rows] == ["on_chain", "feed_health", "on_chain", "feed_health"]
    assert rows[0] == original and rows[0].event_id != rows[2].event_id
    assert rows[1].payload()["accepted_event_id"] == rows[0].event_id
    assert rows[3].payload()["accepted_event_id"] == rows[2].event_id
    assert collector.previous == rows[2]
    assert quality([rows[0], rows[2], rows[3]], POLLED + 1000)["state"] == "POLL_OBSERVED_VERIFIED"
    assert collector.request.await_count == 14
    for index, call in enumerate(collector.request.await_args_list):
        assert call.args[1] == STEPS[index % 7]
        assert len(call.args) == (2 if index % 7 < 2 else 3)
        if index % 7 >= 2:
            assert call.args[2] == 100


@pytest.mark.parametrize(
    "error,reason",
    [
        (TimeoutError(), "TRANSPORT_UNAVAILABLE"),
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (IntegrityError("bad"), "INVALID_DATA"),
    ],
)
async def test_failure_emits_only_canonical_failed_health(collector, error, reason):
    collector.request = AsyncMock(side_effect=error)
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    status = collector.sink.await_args.args[0]
    assert status.payload()["reason"] == reason
    assert status.payload()["accepted_event_id"] is None
    assert quality([status], status.observed_at_ms)["blocker"] == "STABLECOIN_FEED_UNAVAILABLE"
    assert collector.previous is None


@pytest.mark.parametrize(
    "block", [header(number="0x0"), header(timestamp="0x0"), header(timestamp=hex(5000))]
)
async def test_bad_finalized_header_fails_before_contract_requests(collector, block):
    collector.request = AsyncMock(side_effect=[("0x1", RECEIVED - 6), (block, RECEIVED - 5)])
    assert not await collector.collect(None)
    assert collector.request.await_count == 2
    assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


async def test_partial_pair_and_cursor_regressions_never_publish(collector):
    collector.previous = snapshot()
    previous = collector.previous
    older = results(-1000)
    collector.request = AsyncMock(side_effect=older)
    assert not await collector.collect(None)
    assert collector.previous is previous
    collector.sink.assert_awaited_once()
    collector.sink.reset_mock()
    collector.request = AsyncMock(side_effect=[*results()[:4], TimeoutError()])
    assert not await collector.collect(None)
    assert collector.previous is previous
    collector.sink.assert_awaited_once()


async def test_sink_failure_and_cancellation_never_advance(collector):
    collector.request = AsyncMock(side_effect=results())
    collector.sink.side_effect = IntegrityError("store")
    with pytest.raises(IntegrityError, match="store"):
        await collector.collect(None)
    assert collector.previous is None
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)


async def test_run_poll_interval_and_whole_poll_deadline(collector, monkeypatch):
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    collector.collect = AsyncMock(return_value=False)
    monkeypatch.setattr("selective_options.stablecoin_supply.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(600)
    other = StablecoinSupplyCollector(AppConfig(), AsyncMock())

    async def forever(*args):
        await asyncio.Event().wait()

    other.request = forever
    monkeypatch.setattr("selective_options.stablecoin_supply.POLL_TIMEOUT_S", 0.01)
    assert not await other.collect(None)
    assert other.sink.await_args.args[0].payload()["error_type"] == "TimeoutError"


def test_quality_clocks_age_boundaries_and_limited_scope():
    row = snapshot()
    poll = health(row)
    assert quality([row, poll], RECEIVED - 1)["blocker"] == "STABLECOIN_HEALTH_MISSING"
    assert quality([row, poll], POLLED - 1)["blocker"] == "STABLECOIN_HEALTH_MISSING"
    result = quality([row, poll])
    assert result["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["poll_age_ms"] == 0 and result["response_age_ms"] == 100
    assert result["block_age_ms"] == 100100
    assert (
        quality([row, poll], RECEIVED + 1000, max_poll_age_ms=1000)["state"]
        == "POLL_OBSERVED_VERIFIED"
    )
    assert (
        quality([row, poll], RECEIVED + 1001, max_poll_age_ms=1000)["blocker"]
        == "STABLECOIN_POLL_STALE"
    )
    assert quality([row, poll], max_block_age_ms=100100)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, poll], max_block_age_ms=100099)["blocker"] == "STABLECOIN_BLOCK_STALE"
    for key in [
        "trading_approval",
        "continuous_history_complete",
        "all_chain_supply_complete",
        "exchange_balance_coverage",
        "finality_independently_verified",
        "first_publication_independently_verified",
    ]:
        assert result[key] is False


def test_latest_failure_exact_recovery_legacy_and_tied_latest_block():
    row = snapshot()
    poll = health(row)
    fail = stablecoin_poll_health(POLLED + 10, ok=False, reason="RPC_UNAVAILABLE")
    recovered = health(snapshot(20), POLLED + 20)
    rows = [row, poll, fail, snapshot(20), recovered]
    assert quality(rows, POLLED + 9)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality(rows, POLLED + 10)["blocker"] == "STABLECOIN_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 19)["blocker"] == "STABLECOIN_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 20)["state"] == "POLL_OBSERVED_VERIFIED"
    assert (
        quality([row, poll, stablecoin_poll_health(POLLED, ok=False, reason="RPC_UNAVAILABLE")])[
            "blocker"
        ]
        == "STABLECOIN_HEALTH_AMBIGUOUS"
    )
    old = make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="feed_health",
        event_time_ms=POLLED,
        observed_at_ms=POLLED,
        timestamp_kind="receipt",
        payload={"ok": True, "reason": "SUPPLY_SNAPSHOT_OBSERVED", "code": 0, "error_type": None},
    )
    assert quality([row, old])["blocker"] == "STABLECOIN_HEALTH_UNBOUND"


@pytest.mark.parametrize(
    "update",
    [
        {"accepted_event_id": "bad"},
        {"ok": 1},
        {"code": True},
        {"response_received_at_ms": True},
        {"response_received_at_ms": POLLED + 1},
        {"accepted_observed_at_ms": RECEIVED - 1},
        {"unknown": "extra"},
        {"reason": "RPC_UNAVAILABLE"},
    ],
)
def test_malformed_bound_health_blocks(update):
    row = snapshot()
    assert quality([row, mutate(health(row), **update)])["state"] == "BLOCKED"


@pytest.mark.parametrize(
    "kind", ["id", "units", "amount", "receipt", "contract", "decimal", "block", "scope"]
)
def test_snapshot_corruption_blocks(kind):
    row = snapshot()
    payload = row.payload()
    if kind == "id":
        bad = row.model_copy(update={"event_id": "wrong"})
    elif kind == "scope":
        bad = row.model_copy(update={"timestamp_kind": "receipt"})
    else:
        if kind == "block":
            payload["call_block_quantity"] = "0x65"
        else:
            token = dict(payload["usdc"])
            token[
                {
                    "units": "units",
                    "amount": "total_supply_base_units",
                    "receipt": "total_supply_received_at_ms",
                    "contract": "contract_address",
                    "decimal": "decimals",
                }[kind]
            ] = {
                "units": "USD",
                "amount": "1",
                "receipt": True,
                "contract": USDT_ADDRESS,
                "decimal": True,
            }[kind]
            payload["usdc"] = token
        bad = row.model_copy(update={"payload_json": json.dumps(payload)})
    assert quality([bad, health(row)])["state"] == "BLOCKED"


def test_missing_late_or_future_exact_ref_conflicting_ids_and_unscoped_rows():
    row = snapshot()
    poll = health(row)
    assert quality([poll])["blocker"] == "STABLECOIN_REFERENCED_SNAPSHOT_MISSING"
    late = row.model_copy(update={"observed_at_ms": RECEIVED + 1})
    assert quality([late, poll])["state"] == "BLOCKED"
    future = row.model_copy(update={"observed_at_ms": POLLED + 1})
    assert quality([future, poll])["blocker"] == "STABLECOIN_REFERENCED_SNAPSHOT_MISSING"
    future = row.model_copy(update={"event_time_ms": POLLED + 1})
    assert quality([future, poll])["blocker"] == "STABLECOIN_REFERENCED_SNAPSHOT_MISSING"
    assert quality([row, row, poll, poll])["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, late, poll])["blocker"] == "STABLECOIN_EVENT_ID_CONFLICT"
    ignored = stablecoin_poll_health(POLLED + 1, ok=False, reason="RPC_UNAVAILABLE").model_copy(
        update={"symbol": "ETH_MAINNET_GAS"}
    )
    assert quality([row, poll, ignored], POLLED + 1)["state"] == "POLL_OBSERVED_VERIFIED"


def test_same_block_economics_conflict_linked_successor_and_gap():
    previous = snapshot()
    rows = results(1000)
    rows[2] = (word(123), rows[2][1])
    conflict = supply_event(rows, 1800000)
    with pytest.raises(IntegrityError, match="SAME_BLOCK_CONFLICT"):
        check_supply_progress(previous, conflict)
    assert (
        quality([previous, conflict, health(conflict, POLLED + 1000)], POLLED + 1000)["blocker"]
        == "STABLECOIN_SAME_BLOCK_CONFLICT"
    )
    for number, parent in [(101, header()["hash"]), (120, header()["parentHash"])]:
        check_supply_progress(
            previous,
            snapshot(
                1000,
                number=hex(number),
                timestamp=hex(1010),
                hash="0x" + "33" * 32,
                parentHash=parent,
            ),
        )


def test_row_budget_stops_and_counts_exact_duplicate_eligible_rows():
    row = snapshot()
    poll = health(row)
    assert quality([row, poll], max_rows=2)["state"] == "POLL_OBSERVED_VERIFIED"
    assert quality([row, row, poll], max_rows=2)["blocker"] == "STABLECOIN_QUALITY_OVER_BUDGET"

    def finite():
        yield row
        yield poll
        raise AssertionError("read beyond rejected eligible row")

    assert quality(finite(), max_rows=1)["blocker"] == "STABLECOIN_QUALITY_OVER_BUDGET"


@pytest.mark.parametrize(
    "change",
    [
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_poll_age_ms": False},
        {"max_poll_age_ms": 1.5},
        {"max_block_age_ms": 0},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
def test_quality_policy_preflight_precedes_iteration(change):
    def never():
        raise AssertionError("invalid policy read rows")
        yield

    with pytest.raises(ValueError):
        stablecoin_supply_quality(never(), **{"as_of_ms": POLLED, **change})
