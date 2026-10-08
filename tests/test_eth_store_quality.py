"""Bound gas queries preserve both clocks, failures and committed restart evidence."""

import asyncio
import threading

import pytest

from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.eth_gas import EthereumGasCollector, eth_gas_poll_health, gas_event
from selective_options.eth_gas_quality import eth_gas_quality
from selective_options.events import make_event
from selective_options.ingestion import capture
from selective_options.store import DuckStore
from tests.test_eth_checkpoint import raw

RECEIVED = 1_100_000
POLLED = RECEIVED + 10


def success(row, clock=POLLED):
    return eth_gas_poll_health(
        "drpc",
        clock,
        ok=True,
        reason="FINALIZED_GAS_OBSERVED",
        accepted_event_id=row.event_id,
        accepted_observed_at_ms=row.observed_at_ms,
        response_received_at_ms=clock - 1,
    )


async def test_scoped_gas_query_matches_pure_pit_failures_and_closed_restart(tmp_path):
    gas = gas_event(raw(), RECEIVED, 1_800_000, provider="drpc")
    polled = success(gas)
    failed = eth_gas_poll_health(
        "drpc", POLLED + 10, ok=False, reason="TRANSPORT_UNAVAILABLE", error_type="TimeoutError"
    )
    recovered = success(gas, POLLED + 20)
    ignored = [
        gas_event(raw(), RECEIVED, 1_800_000, provider="publicnode"),
        make_event(
            source="ethereum_drpc",
            environment="testnet",
            asset="ETH",
            symbol="ETH_MAINNET_GAS",
            kind="feed_health",
            event_time_ms=POLLED,
            observed_at_ms=POLLED,
            payload={},
        ),
        make_event(
            source="ethereum_drpc",
            environment="public_reference",
            asset="ETH",
            symbol="OTHER",
            kind="feed_health",
            event_time_ms=POLLED,
            observed_at_ms=POLLED,
            payload={},
        ),
    ]
    rows = [gas, polled, failed, recovered, *ignored]
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(rows, exact=True)
        for clock in (
            RECEIVED - 1,
            RECEIVED,
            POLLED - 1,
            POLLED,
            POLLED + 9,
            POLLED + 10,
            POLLED + 19,
            POLLED + 20,
        ):
            assert await store.eth_gas_quality("drpc", clock, 600000, 1800000) == eth_gas_quality(
                rows,
                provider="drpc",
                as_of_ms=clock,
                max_poll_age_ms=600000,
                max_block_age_ms=1800000,
            )
        assert (await store.eth_gas_quality("drpc", POLLED + 20, 600000, 1800000, max_rows=3))[
            "blocker"
        ] == "ETH_QUALITY_OVER_BUDGET"
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        result = await store.eth_gas_quality("drpc", POLLED + 20, 600000, 1800000)
        assert result["state"] == "POLL_OBSERVED_VERIFIED"
        assert await store.read_events([gas.event_id]) == (gas,)
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"provider": True},
        {"provider": "unknown"},
        {"as_of_ms": True},
        {"max_poll_age_ms": 0},
        {"max_block_age_ms": True},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
async def test_invalid_gas_policy_precedes_sql(tmp_path, monkeypatch, changes):
    store = await DuckStore.open(tmp_path / "events.duckdb")

    def forbidden(*args):
        raise AssertionError("invalid policy reached SQL")

    monkeypatch.setattr(store, "_select", forbidden)
    try:
        with pytest.raises(ValueError, match="policy"):
            await store.eth_gas_quality(
                **{
                    "provider": "drpc",
                    "as_of_ms": POLLED,
                    "max_poll_age_ms": 600000,
                    "max_block_age_ms": 1800000,
                    **changes,
                }
            )
    finally:
        await store.close()


async def test_both_clocks_precede_gas_fetch_sentinel(tmp_path, monkeypatch):
    gas = gas_event(raw(), RECEIVED, 1800000, provider="drpc")
    polled = success(gas)
    future = gas.model_copy(update={"event_id": "future", "observed_at_ms": POLLED + 2})
    future_source = gas.model_copy(
        update={"event_id": "future-source", "event_time_ms": POLLED + 2}
    )
    store = await DuckStore.open(tmp_path / "events.duckdb")
    calls = []
    original = store._select

    def tracked(sql, params):
        result = original(sql, params)
        calls.append((params[-1], len(result)))
        return result

    monkeypatch.setattr(store, "_select", tracked)
    try:
        await store.append([gas, polled, success(gas, POLLED + 1), future, future_source])
        assert (await store.eth_gas_quality("drpc", POLLED, 600000, 1800000, 2))[
            "state"
        ] == "POLL_OBSERVED_VERIFIED"
        assert calls == [(3, 2)]
        assert (await store.eth_gas_quality("drpc", POLLED + 1, 600000, 1800000, 2))[
            "blocker"
        ] == "ETH_QUALITY_OVER_BUDGET"
        assert calls[-1] == (3, 3)
    finally:
        await store.close()


async def test_cancelled_gas_read_drains_before_queued_append_and_close(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    started, release = threading.Event(), threading.Event()
    original = store._select

    def blocked(sql, params):
        started.set()
        assert release.wait(5)
        return original(sql, params)

    monkeypatch.setattr(store, "_select", blocked)
    query = asyncio.create_task(store.eth_gas_quality("drpc", POLLED, 600000, 1800000))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        query.cancel()
        await asyncio.sleep(0)
        query.cancel()
        append = asyncio.create_task(
            store.append([gas_event(raw(), RECEIVED, 1800000, provider="drpc")])
        )
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


@pytest.mark.parametrize("ok", [True, False])
async def test_capture_exposes_failure_aware_gas_quality_without_readiness(
    tmp_path, monkeypatch, ok
):
    async def idle(*args):
        await asyncio.Event().wait()

    async def produce(self):
        received = now_ms()
        raw_block = raw(timestamp=hex(received // 1000))
        gas = gas_event(raw_block, received, 1800000, provider="drpc")
        await self.sink(gas)
        await self.sink(
            eth_gas_poll_health(
                "drpc",
                now_ms(),
                ok=ok,
                reason="FINALIZED_GAS_OBSERVED" if ok else "TRANSPORT_UNAVAILABLE",
                accepted_event_id=gas.event_id if ok else None,
                accepted_observed_at_ms=received if ok else None,
                response_received_at_ms=received if ok else None,
                error_type=None if ok else "TimeoutError",
            )
        )
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(EthereumGasCollector, "run", produce)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["ETH"]},
            "ingestion": {
                "eth_gas_enabled": True,
                "eth_gas_provider": "drpc",
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert report["eth_gas_quality"]["state"] == ("POLL_OBSERVED_VERIFIED" if ok else "BLOCKED")
    assert report["eth_gas_quality"]["trading_approval"] is False
    assert report["trading_enabled"] is False and report["ready_for_signals"] is False
