"""Exact scoped SQL and capture integration; all feed/native outcomes are fixtures."""

import asyncio
from datetime import UTC, datetime
from email.utils import format_datetime

import duckdb
import pytest
from pydantic import ValidationError

from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.crypto_news import (
    CryptoNewsCollector,
    crypto_news_quality,
    news_poll_health,
    parse_feed,
)
from selective_options.ingestion import capture
from selective_options.store import DuckStore


def rows_at(received=1791100000000):
    published = format_datetime(datetime.fromtimestamp((received - 60000) / 1000, UTC))
    body = (
        '<rss version="2.0"><channel><item><title> Explicit fixture headline </title>'
        "<link>https://www.coindesk.com/markets/fixture-a</link>"
        f"<guid>fixture-a</guid><pubDate>{published}</pubDate></item></channel></rss>"
    ).encode()
    native, rejected = parse_feed(body, received)
    assert len(native) == 1 and rejected == 0
    healthy = news_poll_health(
        received + 1,
        accepted_event_ids=(native[0].event_id,),
        response_received_at_ms=received,
        http_status=200,
    )
    failed = news_poll_health(received + 100, error_type="TimeoutError")
    recovered = news_poll_health(
        received + 200,
        accepted_event_ids=(native[0].event_id,),
        response_received_at_ms=received + 199,
        http_status=200,
    )
    return [*native, healthy, failed, recovered], received


async def test_scope_pit_fetch_budget_failure_and_recovery_persist_exactly(tmp_path, monkeypatch):
    native, clock = rows_at()
    row = native[0]
    noise = [
        row.model_copy(update={"event_id": f"noise-{i}", field: value})
        for i, (field, value) in enumerate(
            [
                ("source", "federal_reserve"),
                ("asset", "BTC"),
                ("environment", "testnet"),
                ("kind", "index"),
                ("symbol", "POLICY_NEWS"),
            ]
        )
    ]
    future = [
        row.model_copy(update={"event_id": f"future-{field}", field: clock + 201})
        for field in ("event_time_ms", "observed_at_ms")
    ]
    all_rows = [*native, *noise, *future]
    database, archive = tmp_path / "events.duckdb", tmp_path / "events.parquet"
    store = await DuckStore.open(database)
    select = store._select
    calls = []

    def tracked(sql, params):
        result = select(sql, params)
        calls.append((params[-1] if params else None, len(result)))
        return result

    monkeypatch.setattr(store, "_select", tracked)
    try:
        await store.append(all_rows, exact=True)
        for at in (clock - 1, clock, clock + 1, clock + 99, clock + 100, clock + 199, clock + 200):
            assert await store.crypto_news_quality(at, max_rows=4) == crypto_news_quality(
                all_rows, as_of_ms=at, max_rows=4
            )
        assert calls[-1] == (5, 4)
        assert (await store.crypto_news_quality(clock + 1, max_rows=2))[
            "state"
        ] == "POLL_OBSERVED_VERIFIED"
        assert calls[-1] == (3, 2)
        assert (await store.crypto_news_quality(clock + 100))["state"] == "BLOCKED"
        assert (await store.crypto_news_quality(clock + 200))["state"] == "POLL_OBSERVED_VERIFIED"
        assert (await store.crypto_news_quality(clock + 200, max_rows=3))[
            "blocker"
        ] == "CRYPTO_NEWS_QUALITY_OVER_BUDGET"
        assert calls[-1] == (4, 4)
        assert await store.pending_delivery_count() == len(all_rows)
        count = await store.export_parquet(archive)
        await seal_archive(archive, count)
    finally:
        await store.close()
    with duckdb.connect(str(database), read_only=True) as connection:
        assert (
            connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(archive)]
            ).fetchall()
        )
    store = await DuckStore.open(database)
    try:
        assert (await store.crypto_news_quality(clock + 100))["state"] == "BLOCKED"
        assert (await store.crypto_news_quality(clock + 200))["state"] == "POLL_OBSERVED_VERIFIED"
        assert await store.pending_delivery_count() == len(all_rows)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"as_of_ms": True},
        {"max_poll_age_ms": 0},
        {"max_publication_age_ms": True},
        {"max_rows": 0},
        {"max_rows": 100001},
    ],
)
async def test_invalid_policy_refuses_before_query(tmp_path, monkeypatch, changes):
    store = await DuckStore.open(tmp_path / "events.duckdb")

    def forbidden(*args):
        raise AssertionError("invalid policy reached SQL")

    monkeypatch.setattr(store, "_select", forbidden)
    try:
        with pytest.raises(ValueError):
            await store.crypto_news_quality(**{"as_of_ms": 1791100000000, **changes})
    finally:
        await store.close()


@pytest.mark.parametrize("healthy", [False, True])
async def test_capture_optional_news_reports_match_canonical_without_readiness(
    tmp_path, monkeypatch, healthy
):
    rows, _clock = rows_at(now_ms() - 1000)

    async def idle(*args):
        await asyncio.Event().wait()

    async def produce(self):
        for row in rows[:2] if healthy else rows[:3]:
            await self.sink(row)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(CryptoNewsCollector, "run", produce)
    config = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "crypto_news_enabled": True,
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
        },
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert (report["crypto_news_quality"]["state"] == "POLL_OBSERVED_VERIFIED") == healthy
    assert report["crypto_news_quality"]["trading_approval"] is False
    assert not report["ready_for_signals"] and not report["trading_enabled"]
    assert any(item["source"] == "coindesk_rss" for item in report["freshness"])


@pytest.mark.parametrize("value", [0, 299, float("inf"), float("nan"), 86401])
def test_news_opt_in_defaults_and_invalid_poll_policy(value):
    assert not AppConfig().ingestion.crypto_news_enabled
    with pytest.raises(ValidationError):
        AppConfig(ingestion={"crypto_news_poll_interval_s": value})
