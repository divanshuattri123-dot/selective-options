"""Scoped bounded RSS reads feed capture diagnostics without erasing failures/history."""

import asyncio
import threading

import pytest

from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.events import make_event
from selective_options.ingestion import capture
from selective_options.news import news_poll_health
from selective_options.news_quality import policy_news_quality
from selective_options.store import DuckStore
from tests.test_news_quality import POLLED, RECEIVED, headline, successful


async def test_bounded_scoped_sql_matches_pure_pit_and_preserves_outbox(tmp_path):
    row = headline()
    poll = successful(row)
    failed = news_poll_health("federal_reserve", POLLED + 10, error_type="TimeoutError")
    recovered = successful(row, POLLED + 20, RECEIVED + 20)
    ignored = [
        make_event(
            source="fixture",
            environment="public_reference",
            asset="GLOBAL",
            symbol="OTHER",
            kind="news",
            event_time_ms=i + 1,
            observed_at_ms=i + 1,
            payload={},
        )
        for i in range(100)
    ]
    rows = [row, poll, failed, recovered, *ignored]
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(rows, exact=True)
        for cutoff in (
            RECEIVED - 1,
            RECEIVED,
            POLLED - 1,
            POLLED,
            POLLED + 9,
            POLLED + 10,
            POLLED + 19,
            POLLED + 20,
        ):
            assert await store.policy_news_quality(
                "federal_reserve", cutoff
            ) == policy_news_quality(rows, source="federal_reserve", as_of_ms=cutoff)
        assert (await store.policy_news_quality("ecb", POLLED + 20))[
            "blocker"
        ] == "NEWS_HEALTH_MISSING"
        assert (await store.policy_news_quality("federal_reserve", POLLED + 20, max_rows=3))[
            "blocker"
        ] == "NEWS_QUALITY_OVER_BUDGET"
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        assert (await store.policy_news_quality("federal_reserve", POLLED + 20))[
            "first_observed_at_ms"
        ] == [RECEIVED]
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"source": "unknown"},
        {"as_of_ms": True},
        {"max_rows": True},
        {"max_rows": 100001},
        {"max_poll_age_ms": 0},
        {"max_publication_age_ms": True},
    ],
)
async def test_news_policy_refuses_before_sql(tmp_path, monkeypatch, kwargs):
    store = await DuckStore.open(tmp_path / "events.duckdb")

    def forbidden(*args):
        raise AssertionError("invalid policy accessed database")

    monkeypatch.setattr(store, "_select", forbidden)
    try:
        with pytest.raises(ValueError, match="policy"):
            await store.policy_news_quality(
                **{"source": "federal_reserve", "as_of_ms": POLLED, **kwargs}
            )
    finally:
        await store.close()


async def test_fetch_budget_sentinel_and_both_clocks_precede_limit(tmp_path, monkeypatch):
    row = headline()
    visible = successful(row)
    rows = [row, visible, successful(row, POLLED + 1, RECEIVED + 1)]
    # Source time alone does not make an undelivered observation visible.
    future_receipt = row.model_copy(update={"event_id": "future", "observed_at_ms": POLLED + 2})
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([*rows, future_receipt])
        calls = []
        original_select = store._select

        def tracked(sql, params):
            result = original_select(sql, params)
            calls.append((params[-1], len(result)))
            return result

        monkeypatch.setattr(store, "_select", tracked)
        assert (await store.policy_news_quality("federal_reserve", POLLED, max_rows=2))[
            "state"
        ] == "POLL_OBSERVED_VERIFIED"
        assert calls == [(3, 2)]
        assert (await store.policy_news_quality("federal_reserve", POLLED + 1, max_rows=2))[
            "blocker"
        ] == "NEWS_QUALITY_OVER_BUDGET"
        assert calls[-1] == (3, 3)
    finally:
        await store.close()


async def test_cancelled_news_query_drains_before_append_close(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    started, release = threading.Event(), threading.Event()
    original_select = store._select

    def blocked(sql, params):
        started.set()
        assert release.wait(5)
        return original_select(sql, params)

    monkeypatch.setattr(store, "_select", blocked)
    query = asyncio.create_task(store.policy_news_quality("federal_reserve", POLLED))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        query.cancel()
        await asyncio.sleep(0)
        query.cancel()
        append = asyncio.create_task(store.append([headline()]))
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


@pytest.mark.parametrize("outcome", ["success", "empty", "failure"])
async def test_capture_records_exact_news_diagnostic_without_readiness(
    tmp_path, monkeypatch, outcome
):
    async def idle(*args):
        await asyncio.Event().wait()

    async def produce(self):
        received = now_ms()
        row = headline(received)
        if outcome == "success":
            await self.sink(row)
        await self.sink(
            news_poll_health(
                self.source,
                now_ms(),
                response_received_at_ms=received,
                accepted_event_ids=(row.event_id,) if outcome == "success" else (),
                error_type="TimeoutError" if outcome == "failure" else None,
            )
        )
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr("selective_options.ingestion.PolicyNewsCollector.run", produce)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": ["federal_reserve"],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    (result,) = report["policy_news_quality"]
    assert result["source"] == "federal_reserve"
    if outcome == "failure":
        assert result["blocker"] == "NEWS_FEED_UNAVAILABLE"
    else:
        assert result["state"] == "POLL_OBSERVED_VERIFIED"
        assert result["headline_state"] == (
            "EMPTY_POLL" if outcome == "empty" else "PUBLICATION_STALE"
        )
    assert result["complete_feed_available"] is False
    assert result["trading_approval"] is False
    assert report["trading_enabled"] is False and report["ready_for_signals"] is False
