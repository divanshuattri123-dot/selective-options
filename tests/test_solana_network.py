import asyncio
import json
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from pydantic import ValidationError

from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.solana_network import GENESIS, URL, SolanaNetworkCollector, network_events
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore

RECEIPTS = (1000100, 1000200, 1000300, 1000400)
SCOPE = "performance and vote-account stake; sink accepted, no atomic commit claim"


def sample(**changes):
    return {
        "slot": 1000,
        "numTransactions": 9007199254740993,
        "numSlots": 100,
        "samplePeriodSecs": 60,
        "numNonVoteTransactions": 12,
        **changes,
    }


def vote(suffix="2", **changes):
    return {
        "votePubkey": "1" * 31 + suffix,
        "nodePubkey": "1" * 32,
        "activatedStake": 9007199254740993,
        "epochVoteAccount": True,
        "lastVote": 999,
        "rootSlot": 998,
        "commission": 5,
        **changes,
    }


def votes():
    return {"current": [vote()], "delinquent": [vote("3", activatedStake=42)]}


def pair():
    return network_events([sample()], 1000, votes(), RECEIPTS, 180000)


def health(events=(), at=1000500, ok=True, **changes):
    return make_event(
        source="solana_public",
        environment="public_reference",
        asset="SOL",
        symbol="SOL_NETWORK",
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=at,
        observed_at_ms=at,
        payload={
            "ok": ok,
            "reason": "NETWORK_SAMPLES_ACCEPTED" if ok else "RATE_LIMITED",
            "code": 0 if ok else 429,
            "error_type": None,
            "event_ids": [e.event_id for e in events],
            "scope": SCOPE,
            **changes,
        },
    )


def test_exact_native_units_clocks_and_revisions():
    perf, stake = pair()
    assert perf.event_time_ms == 1000000 and perf.observed_at_ms == 1000300
    assert perf.payload()["transactions_per_second_numerator"] == "9007199254740993"
    assert perf.payload()["transactions_per_second_denominator"] == 60
    assert stake.event_time_ms == stake.observed_at_ms == 1000400
    assert stake.timestamp_kind == "receipt"
    assert stake.payload()["total_visible_activated_stake_lamports"] == "9007199254741035"
    assert stake.payload()["vote_accounts"]["current"][0]["activatedStake"] == 9007199254740993
    assert pair() == (perf, stake)
    later = network_events([sample()], 1000, votes(), tuple(r + 1000 for r in RECEIPTS), 180000)
    assert later[0].event_id != perf.event_id and later[1].event_id != stake.event_id
    assert perf.payload()["finality_verified"] is False


@pytest.mark.parametrize("value", [None, "missing"])
def test_unknown_non_vote_count_remains_unknown(value):
    raw = sample(numNonVoteTransactions=value)
    if value == "missing":
        del raw["numNonVoteTransactions"]
    event, _ = network_events([raw], 1000, votes(), RECEIPTS, 180000)
    assert event.payload()["non_vote_transactions_per_second_numerator"] is None


@pytest.mark.parametrize(
    "changes",
    [
        {"slot": 0},
        {"slot": True},
        {"slot": 2**64},
        {"numTransactions": 1.5},
        {"numTransactions": "12"},
        {"numNonVoteTransactions": -1},
        {"numNonVoteTransactions": 2**64},
        {"numTransactions": 11},
        {"numSlots": 1002},
        {"numSlots": 0},
        {"samplePeriodSecs": 0},
        {"samplePeriodSecs": 65536},
    ],
)
def test_invalid_performance(changes):
    with pytest.raises((ValidationError, IntegrityError)):
        network_events([sample(**changes)], 1000, votes(), RECEIPTS, 180000)


@pytest.mark.parametrize("raw", [[], [sample(), sample()], None, {}])
def test_missing_or_extra_samples(raw):
    with pytest.raises((ValidationError, IntegrityError)):
        network_events(raw, 1000, votes(), RECEIPTS, 180000)


@pytest.mark.parametrize("time", [None, True, 1000.0, "1000", 0, -1, 800, 1003])
def test_unavailable_stale_future_or_coerced_block_time(time):
    with pytest.raises((ValidationError, IntegrityError)):
        network_events([sample()], time, votes(), RECEIPTS, 180000)


@pytest.mark.parametrize(
    "receipts,age",
    [
        (RECEIPTS, 0),
        (RECEIPTS, True),
        ((0, *RECEIPTS[1:]), 180000),
        ((1000201, *RECEIPTS[1:]), 180000),
        ((True, *RECEIPTS[1:]), 180000),
        ((1000100,), 180000),
    ],
)
def test_receipt_policy(receipts, age):
    with pytest.raises(IntegrityError, match="SOL_RECEIPT_POLICY"):
        network_events([sample()], 1000, votes(), receipts, age)


@pytest.mark.parametrize(
    "changes",
    [
        {"votePubkey": "0" * 32},
        {"nodePubkey": "1" * 31},
        {"activatedStake": 3.0},
        {"activatedStake": True},
        {"activatedStake": -1},
        {"activatedStake": 2**64},
        {"epochVoteAccount": 1},
        {"lastVote": "99"},
        {"rootSlot": -1},
        {"commission": 101},
        {"commission": False},
    ],
)
def test_invalid_vote_records(changes):
    with pytest.raises(ValidationError):
        network_events(
            [sample()], 1000, {"current": [vote(**changes)], "delinquent": []}, RECEIPTS, 180000
        )


@pytest.mark.parametrize(
    "raw",
    [
        {"current": [], "delinquent": []},
        {"current": [vote()], "delinquent": [vote()]},
        {"current": [vote(activatedStake=2**64 - 1)], "delinquent": [vote("3", activatedStake=1)]},
        {"current": [vote()] * 10001, "delinquent": []},
        {"current": [vote()] * 5001, "delinquent": [vote("3")] * 5000},
    ],
)
def test_duplicate_empty_or_unbounded_stake(raw):
    with pytest.raises((ValidationError, IntegrityError)):
        network_events([sample()], 1000, raw, RECEIPTS, 180000)


async def test_actual_store_receipt_boundaries_replay_outbox_and_quality(tmp_path):
    events = pair()
    rows = (*events, health(events))
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        for event in rows:
            before = (
                await store.snapshot("SOL", event.observed_at_ms - 1, "public_reference")
            ).rows()
            at = (await store.snapshot("SOL", event.observed_at_ms, "public_reference")).rows()
            assert event not in before and event in at
        current = (await store.snapshot("SOL", 1000500, "public_reference")).rows()
        assert set(current) == set(rows)
        assert network_quality(current, 1000500)["state"] == "NETWORK_SAMPLE_VERIFIED"
        assert await store.export_parquet(tmp_path / "events.parquet") == 3
    finally:
        await store.close()


@pytest.fixture
def collector(monkeypatch):
    monkeypatch.setattr("selective_options.solana_network.now_ms", lambda: 1000500)
    monkeypatch.setattr("selective_options.solana_network.asyncio.sleep", AsyncMock())
    return SolanaNetworkCollector(AppConfig(), AsyncMock())


def responses():
    return [
        (GENESIS, RECEIPTS[0]),
        ([sample()], RECEIPTS[1]),
        (1000, RECEIPTS[2]),
        (votes(), RECEIPTS[3]),
    ]


async def test_complete_poll_stages_all_then_publishes_exact_pair(collector):
    async def request(*args):
        assert not collector.sink.await_args_list
        return queue.pop(0)

    queue = responses()
    collector.request = AsyncMock(side_effect=request)
    assert await collector.collect(None)
    rows = [call.args[0] for call in collector.sink.await_args_list]
    assert tuple(rows) == (*pair(), health(pair()))
    assert collector.request.await_args_list[2].args[2] == 1000


@pytest.mark.parametrize("stage", range(4))
@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("RATE_LIMITED", 429), "RATE_LIMITED"),
        (OSError(), "TRANSPORT_UNAVAILABLE"),
        (aiohttp.ClientError(), "TRANSPORT_UNAVAILABLE"),
        (IntegrityError("bad data"), "INVALID_DATA"),
    ],
)
async def test_failed_stage_emits_only_failure_health(collector, stage, error, reason):
    collector.request = AsyncMock(side_effect=[*responses()[:stage], error])
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    body = collector.sink.await_args.args[0].payload()
    assert body["reason"] == reason and body["ok"] is False and body["event_ids"] == []
    assert collector.request.await_count == stage + 1


@pytest.mark.parametrize(
    "queue",
    [
        [("wrong", RECEIPTS[0])],
        [(GENESIS, RECEIPTS[0]), ([], RECEIPTS[1])],
        [*responses()[:3], ({"current": [], "delinquent": []}, RECEIPTS[3])],
        [(GENESIS, RECEIPTS[1] + 1), *responses()[1:]],
    ],
)
async def test_invalid_complete_or_early_data_never_publishes_prefix(collector, queue):
    collector.request = AsyncMock(side_effect=queue)
    assert not await collector.collect(None)
    collector.sink.assert_awaited_once()
    assert collector.sink.await_args.args[0].payload()["reason"] == "INVALID_DATA"


@pytest.mark.parametrize("position", [0, 1, 2])
async def test_sink_failure_is_not_relabelled_transport_success(collector, position):
    collector.request = AsyncMock(side_effect=responses())
    collector.sink.side_effect = [None] * position + [OSError("sink failed")]
    with pytest.raises(OSError, match="sink failed"):
        await collector.collect(None)
    assert collector.sink.await_count == position + 1


async def test_cancellation_propagates_without_false_health(collector):
    collector.request = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await collector.collect(None)
    collector.sink.assert_not_awaited()


async def test_deadline_stops_poll_without_market_prefix(collector):
    collector.config = AppConfig.model_validate({"ingestion": {"solana_network_timeout_s": 1}})
    collector.request = AsyncMock(side_effect=lambda *args: None)

    async def never(*args):
        await asyncio.Event().wait()

    collector.request.side_effect = never
    assert not await collector.collect(None)
    assert collector.sink.await_args.args[0].payload()["error_type"] == "TimeoutError"


async def test_health_clock_regression_refuses_success(collector):
    future = pair()[0].model_copy(update={"observed_at_ms": 1000501})
    with pytest.raises(IntegrityError, match="SOL_HEALTH_CLOCK_REGRESSION"):
        await collector.health(True, "ok", events=(future,))
    collector.sink.assert_not_awaited()


class Response:
    def __init__(self, raw=None, status=200):
        self.status, self.content = status, self
        self.body = (
            raw
            if isinstance(raw, bytes)
            else json.dumps(
                raw if raw is not None else {"jsonrpc": "2.0", "id": 1, "result": GENESIS}
            ).encode()
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def iter_chunked(self, size):
        for start in range(0, len(self.body), size):
            yield self.body[start : start + size]


@pytest.mark.parametrize(
    "method,slot,params,rpc_id",
    [
        ("getGenesisHash", None, [], 1),
        ("getRecentPerformanceSamples", None, [1], 2),
        ("getBlockTime", 1000, [1000], 3),
        (
            "getVoteAccounts",
            None,
            [{"commitment": "finalized", "keepUnstakedDelinquents": True}],
            4,
        ),
    ],
)
async def test_fixed_read_only_routing(collector, method, slot, params, rpc_id):
    session = Mock()
    session.post.return_value = Response({"jsonrpc": "2.0", "id": rpc_id, "result": "response"})
    assert await collector.request(session, method, slot) == ("response", 1000500)
    session.post.assert_called_once_with(
        URL,
        json={"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params},
        allow_redirects=False,
    )


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
async def test_http_refusal(collector, status, reason):
    session = Mock()
    session.post.return_value = Response(status=status)
    with pytest.raises(FeedUnavailable) as caught:
        await collector.request(session, "getGenesisHash")
    assert caught.value.reason == reason and caught.value.code == status


@pytest.mark.parametrize(
    "raw,error",
    [
        (b"not json", ValidationError),
        (b"[1]", ValidationError),
        pytest.param(b"x" * 4000001, IntegrityError, id="oversized-response"),
        ({"jsonrpc": "2.0", "id": True, "result": 1}, IntegrityError),
        ({"jsonrpc": "1.0", "id": 1, "result": 1}, IntegrityError),
        ({"jsonrpc": "2.0", "id": 2, "result": 1}, IntegrityError),
        ({"jsonrpc": "2.0", "id": 1, "error": {}, "result": 1}, IntegrityError),
        ({"jsonrpc": "2.0", "id": 1, "error": {}}, FeedUnavailable),
        ({"jsonrpc": "2.0", "id": 1}, IntegrityError),
        ({"jsonrpc": "2.0", "id": 1, "result": None}, IntegrityError),
    ],
)
async def test_rpc_envelope_integrity(collector, raw, error):
    session = Mock()
    session.post.return_value = Response(raw)
    with pytest.raises(error):
        await collector.request(session, "getGenesisHash")


@pytest.mark.parametrize(
    "method,slot",
    [
        ("sendTransaction", None),
        ("getBlockTime", None),
        ("getBlockTime", True),
        ("getBlockTime", 0),
        ("getBlockTime", 2**64),
        ("getGenesisHash", 1),
    ],
)
async def test_request_preflight_rejects_before_network(collector, method, slot):
    session = Mock()
    with pytest.raises(ValueError):
        await collector.request(session, method, slot)
    session.post.assert_not_called()


@pytest.mark.parametrize("ok,pause", [(True, 300), (False, 600)])
async def test_poll_pause_on_success_and_failure(collector, monkeypatch, ok, pause):
    collector.collect = AsyncMock(return_value=ok)
    sleep = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr("selective_options.solana_network.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await collector.run()
    sleep.assert_awaited_once_with(pause)


def test_quality_pit_latest_outage_and_recovery():
    rows = pair()
    success = health(rows)
    failed = health(at=1000600, ok=False)
    recovery = health(rows, at=1000700)
    assert (
        network_quality([*rows, success, failed, recovery], 1000499)["blocker"]
        == "SOL_HEALTH_MISSING"
    )
    assert (
        network_quality([*rows, success, failed, recovery], 1000500)["state"]
        == "NETWORK_SAMPLE_VERIFIED"
    )
    assert (
        network_quality([*rows, success, failed, recovery], 1000600)["blocker"]
        == "SOL_FEED_UNAVAILABLE"
    )
    assert (
        network_quality([*rows, success, failed, recovery], 1000700)["state"]
        == "NETWORK_SAMPLE_VERIFIED"
    )
    assert network_quality([*rows, success, success], 1000500)["trading_approval"] is False


@pytest.mark.parametrize(
    "receipt_age,sample_age,blocker",
    [
        (499, 999999, "SOL_HEALTH_STALE"),
        (850, 999999, "SOL_SNAPSHOT_RECEIPT_STALE"),
        (999999, 999, "SOL_PERFORMANCE_SOURCE_STALE"),
    ],
)
def test_health_rpc_receipts_and_source_time_expire_independently(receipt_age, sample_age, blocker):
    rows = pair()
    result = network_quality([*rows, health(rows)], 1001000, receipt_age, sample_age)
    assert result["blocker"] == blocker


def test_quality_missing_partial_and_conflicting_snapshots():
    rows = pair()
    assert (
        network_quality([rows[0], health(rows)], 1000500)["blocker"]
        == "SOL_REFERENCED_SNAPSHOT_MISSING_OR_CONFLICTING"
    )
    conflicting = rows[0].model_copy(update={"payload_json": "{}"})
    assert (
        network_quality([*rows, conflicting, health(rows)], 1000500)["blocker"]
        == "SOL_REFERENCED_SNAPSHOT_MISSING_OR_CONFLICTING"
    )
    assert (
        network_quality([*rows, health(rows), health(at=1000500, ok=False)], 1000500)["blocker"]
        == "SOL_HEALTH_AMBIGUOUS"
    )


@pytest.mark.parametrize(
    "changes,blocker",
    [
        ({"event_ids": []}, "SOL_HEALTH_REFERENCES"),
        ({"event_ids": ["x", "x"]}, "SOL_HEALTH_REFERENCES"),
        ({"ok": "yes"}, "SOL_INVALID_EVIDENCE"),
        ({"code": True}, "SOL_INVALID_EVIDENCE"),
        ({"scope": "all staking"}, "SOL_INVALID_EVIDENCE"),
        ({"unexpected": True}, "SOL_HEALTH_CANONICAL"),
    ],
)
def test_health_strict_schema_and_canonical_identity(changes, blocker):
    rows = pair()
    assert network_quality([*rows, health(rows, **changes)], 1000500)["blocker"] == blocker


@pytest.mark.parametrize(
    "field,value",
    [("timestamp_kind", "source"), ("event_id", "tampered"), ("event_time_ms", 1000400)],
)
def test_health_metadata_tampering(field, value):
    rows = pair()
    bad = health(rows).model_copy(update={field: value})
    assert network_quality([*rows, bad], 1000500)["blocker"] == "SOL_HEALTH_CANONICAL"


def test_quality_rebuilds_pair_not_just_hashes():
    perf, stake = pair()
    bad = perf.model_copy(update={"observed_at_ms": 1000301})
    assert (
        network_quality([bad, stake, health((bad, stake))], 1000500)["blocker"]
        == "SOL_SNAPSHOT_CANONICAL_OR_HEALTH_ORDER"
    )
    body = perf.payload()
    del body["sample"]
    bad = perf.model_copy(update={"payload_json": json.dumps(body)})
    assert (
        network_quality([bad, stake, health((bad, stake))], 1000500)["blocker"]
        == "SOL_INVALID_EVIDENCE"
    )


def test_scope_and_future_rows_are_filtered_before_budget():
    rows = pair()
    good = health(rows)
    ignored = good.model_copy(update={"source": "other", "payload_json": "{}"})
    future = good.model_copy(update={"event_time_ms": 1000501, "payload_json": "{}"})
    result = network_quality([*rows, good, *([ignored, future] * 5)], 1000500, max_rows=3)
    assert result["state"] == "NETWORK_SAMPLE_VERIFIED"
    assert (
        network_quality([*rows, good], 1000500, max_rows=2)["blocker"] == "SOL_QUALITY_OVER_BUDGET"
    )


@pytest.mark.parametrize(
    "policy",
    [
        {"as_of_ms": True},
        {"max_receipt_age_ms": 0},
        {"max_sample_age_ms": -1},
        {"max_rows": 100001},
    ],
)
def test_invalid_quality_policy(policy):
    with pytest.raises(ValueError):
        network_quality([], **{"as_of_ms": 1000500, **policy})


@pytest.mark.parametrize(
    "field,value",
    [
        ("solana_network_poll_interval_s", 119),
        ("solana_network_poll_interval_s", float("inf")),
        ("solana_network_timeout_s", 301),
        ("solana_network_max_sample_age_s", 59),
        ("solana_network_max_sample_age_s", 3601),
    ],
)
def test_config_limits(field, value):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {field: value}})
