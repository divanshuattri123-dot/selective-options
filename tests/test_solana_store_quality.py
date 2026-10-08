"""Real bounded SOL scope/PIT queries retain failure and restart semantics."""

import asyncio
import threading

import pytest

from selective_options.events import make_event
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore
from tests.test_solana_network import RECEIPTS, health, pair


async def test_sol_scope_and_pit_match_pure_before_overflow_and_after_reopen(tmp_path):
    perf, stake = pair()
    polled = health((perf, stake))
    failed = health(at=1000600, ok=False)
    recovered = health((perf, stake), at=1000700)
    noise = [
        make_event(
            source="solana_public",
            environment="public_reference",
            asset="SOL",
            symbol="IGNORED",
            kind="on_chain",
            event_time_ms=i + 1,
            observed_at_ms=i + 1,
            payload={"fixture_origin": "UNRELATED_TEST_SCOPE"},
        )
        for i in range(100)
    ]
    noise.append(
        make_event(
            source="solana_public",
            environment="public_reference",
            asset="SOL",
            symbol="SOL_PERFORMANCE",
            kind="feed_health",
            event_time_ms=1000500,
            observed_at_ms=1000500,
            payload={},
        )
    )
    rows = [perf, stake, polled, failed, recovered, *noise]
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(rows, exact=True)
        for clock in (*RECEIPTS, 1000499, 1000500, 1000599, 1000600, 1000699, 1000700):
            assert await store.solana_network_quality(clock) == network_quality(rows, clock)
        assert (await store.solana_network_quality(1000500, max_rows=3))[
            "state"
        ] == "NETWORK_SAMPLE_VERIFIED"
        assert (await store.solana_network_quality(1000700, max_rows=4))[
            "blocker"
        ] == "SOL_QUALITY_OVER_BUDGET"
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        assert (await store.solana_network_quality(1000600))["blocker"] == "SOL_FEED_UNAVAILABLE"
        assert (await store.solana_network_quality(1000700))["state"] == "NETWORK_SAMPLE_VERIFIED"
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"as_of_ms": True},
        {"max_receipt_age_ms": 0},
        {"max_sample_age_ms": True},
        {"max_rows": 0},
        {"max_rows": 100001},
    ],
)
async def test_sol_invalid_policy_precedes_database_access(tmp_path, monkeypatch, changes):
    store = await DuckStore.open(tmp_path / "events.duckdb")

    def forbidden(*args):
        raise AssertionError("invalid SOL policy accessed SQL")

    monkeypatch.setattr(store, "_select", forbidden)
    try:
        with pytest.raises(ValueError, match="policy"):
            await store.solana_network_quality(**{"as_of_ms": 1000700, **changes})
    finally:
        await store.close()


async def test_sol_both_clocks_filter_before_limit_and_overflow_sentinel(tmp_path, monkeypatch):
    perf, stake = pair()
    poll = health((perf, stake))
    future_observation = perf.model_copy(update={"event_id": "future", "observed_at_ms": 1000501})
    future_source = stake.model_copy(update={"event_id": "future-source", "event_time_ms": 1000501})
    store = await DuckStore.open(tmp_path / "events.duckdb")
    calls = []
    original = store._select

    def tracked(sql, params):
        result = original(sql, params)
        calls.append((params[-1], len(result)))
        return result

    monkeypatch.setattr(store, "_select", tracked)
    try:
        await store.append([perf, stake, poll, future_observation, future_source])
        assert (await store.solana_network_quality(1000500, max_rows=3))[
            "state"
        ] == "NETWORK_SAMPLE_VERIFIED"
        assert calls == [(4, 3)]
        assert (await store.solana_network_quality(1000501, max_rows=3))[
            "blocker"
        ] == "SOL_QUALITY_OVER_BUDGET"
        assert calls[-1] == (4, 4)
    finally:
        await store.close()


async def test_sol_cancelled_query_drains_before_waiting_writer(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    started, release = threading.Event(), threading.Event()
    original = store._select

    def blocked(sql, params):
        started.set()
        assert release.wait(5)
        return original(sql, params)

    monkeypatch.setattr(store, "_select", blocked)
    query = asyncio.create_task(store.solana_network_quality(1000700))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        query.cancel()
        await asyncio.sleep(0)
        query.cancel()
        append = asyncio.create_task(store.append(list(pair())))
        close = asyncio.create_task(store.close())
        await asyncio.sleep(0)
        assert not query.done() and not append.done() and not close.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await query
        await append
        await close
    finally:
        release.set()
        await store.close()
