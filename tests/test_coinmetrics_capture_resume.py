"""Offline capture handoff from bounded, receipt-bound Coin Metrics history."""

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.cli import main, parser
from selective_options.coinmetrics import (
    PollHealth,
    canonical_coinmetrics_health,
    coinmetrics_poll_health,
)
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/coinmetrics-public-01/events.duckdb"
SOURCE_REPORT = json.loads((ROOT / "evidence/coinmetrics-public-01/report.json").read_text())
NOW = SOURCE_REPORT["as_of_ms"]


def config(*, assets=("BTC", "ETH"), **changes):
    return AppConfig(
        system={"assets": list(assets)},
        ingestion={
            "coinmetrics_enabled": True,
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
            **changes,
        },
    )


async def idle(*_args, **_kwargs):
    await asyncio.Event().wait()


async def database_rows(path: Path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(list(rows), exact=True)
    finally:
        await store.close()
    return path


def quiet(monkeypatch, at_ms=NOW):
    monkeypatch.setattr("selective_options.ingestion.now_ms", lambda: at_ms)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)


def current_health(path: Path, asset: str):
    with duckdb.connect(str(path), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events WHERE source='coinmetrics_community' AND environment='public_reference' "
            "AND asset=? AND symbol='COINMETRICS_DAILY_REGIME' AND kind='feed_health' "
            "ORDER BY observed_at_ms DESC LIMIT 1",
            [asset],
        )
        columns = [field[0] for field in cursor.description]
        rows = cursor.fetchall()
    assert rows
    from selective_options.events import MarketEvent

    event = MarketEvent.model_validate(dict(zip(columns, rows[0], strict=True)))
    return canonical_coinmetrics_health(event)


def failed_health(asset: str, at_ms: int, prior: PollHealth):
    health = PollHealth(
        frequency="1d",
        window_start_ms=prior.window_start_ms,
        window_end_ms=prior.window_end_ms,
        ok=False,
        reason="HTTP_UNAVAILABLE",
        code=503,
        error_type=None,
        poll_started_at_ms=at_ms - 1,
        poll_finished_at_ms=at_ms,
        response_received_at_ms=None,
        accepted_observations=(),
    )
    return coinmetrics_poll_health(asset, health)


@pytest.mark.skipif(not SOURCE.is_file(), reason="private restored Coin Metrics proof is required")
async def test_saved_public_history_is_copied_exactly_and_reloaded_before_producers(
    monkeypatch, tmp_path
):
    from selective_options.coinmetrics import CoinMetricsCollector
    from selective_options.coinmetrics_checkpoint import load_coinmetrics_resume
    from selective_options.events import MarketEvent

    quiet(monkeypatch)
    original_hash = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    seed = await load_coinmetrics_resume(
        SOURCE,
        NOW,
        max_poll_age_ms=7_200_000,
        max_data_age_ms=259_200_000,
    )
    assert len(seed) == 16
    assert all(isinstance(event, MarketEvent) for event in seed)
    assert {event.asset for event in seed} == {"BTC", "ETH"}
    started = []

    async def mocked_collector(self):
        started.append(self)
        await idle()

    monkeypatch.setattr(CoinMetricsCollector, "run", mocked_collector)
    output = tmp_path / "capture"
    report = await capture(config(), output, 0.04, resume_coinmetrics_from=SOURCE)
    assert len(started) == 1
    resumed = report["coinmetrics_resume"]
    assert resumed["scope"] == ["BTC", "ETH"]
    assert resumed["copied_events"] == len(seed)
    assert resumed["copied_outbox_pending"] is True
    assert resumed["original_native_values_and_receipts_preserved"] is True
    assert resumed["history_complete"] is False and resumed["trading_enabled"] is False
    assert report["coinmetrics_quality"]["history_complete"] is False
    assert report["trading_enabled"] is False

    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY observed_at_ms,event_time_ms,event_id"
        )
        columns = [field[0] for field in cursor.description]
        persisted = [
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        ]
        assert sorted(persisted, key=lambda event: event.event_id) == sorted(
            seed, key=lambda event: event.event_id
        )
        assert connection.execute(
            "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
        ).fetchone()[0] == len(seed)
        assert (
            connection.execute("SELECT count(*) FROM delivery_outbox WHERE delivered").fetchone()[0]
            == 0
        )
        assert (
            connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(output / "events.parquet")]
            ).fetchall()
        )
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest() == original_hash
    assert (
        await load_coinmetrics_resume(
            output / "events.duckdb",
            NOW,
            max_poll_age_ms=7_200_000,
            max_data_age_ms=259_200_000,
        )
        == seed
    )


@pytest.mark.skipif(not SOURCE.is_file(), reason="private restored Coin Metrics proof is required")
@pytest.mark.parametrize("case", ["latest_failure", "stale", "over_budget"])
async def test_invalid_seed_refuses_before_creating_output_or_starting_producers(
    monkeypatch, tmp_path, case
):
    from selective_options.coinmetrics import CoinMetricsCollector
    from selective_options.coinmetrics_checkpoint import load_coinmetrics_resume

    at_ms = NOW + 1
    quiet(monkeypatch, at_ms)
    if case == "stale":
        at_ms = NOW + 7_200_001
        quiet(monkeypatch, at_ms)
    source = SOURCE
    if case == "latest_failure":
        source = tmp_path / "failed.duckdb"
        events = list(
            await load_coinmetrics_resume(
                SOURCE,
                NOW,
                max_poll_age_ms=7_200_000,
                max_data_age_ms=259_200_000,
            )
        )
        prior = current_health(SOURCE, "BTC")
        events.append(failed_health("BTC", at_ms, prior))
        await database_rows(source, events)

    producer = AsyncMock()
    monkeypatch.setattr(CoinMetricsCollector, "run", producer)
    if case == "over_budget":
        original = load_coinmetrics_resume

        async def capped(path, selected_at, **policy):
            policy["max_rows"] = 7
            return await original(path, selected_at, **policy)

        monkeypatch.setattr("selective_options.ingestion.load_coinmetrics_resume", capped)

    output = tmp_path / "refused"
    with pytest.raises(IntegrityError):
        await capture(config(), output, 0.03, resume_coinmetrics_from=source)
    assert not output.exists()
    producer.assert_not_awaited()


async def test_resume_flag_requires_opt_in_and_btc_eth_scope_before_read(tmp_path, monkeypatch):
    forbidden = AsyncMock(side_effect=AssertionError("seed loader must not run"))
    monkeypatch.setattr("selective_options.ingestion.load_coinmetrics_resume", forbidden)
    with pytest.raises(ValueError, match="enabled Coin Metrics"):
        await capture(
            AppConfig(), tmp_path / "disabled", 0.03, resume_coinmetrics_from=tmp_path / "absent"
        )
    with pytest.raises(ValueError, match="BTC and ETH"):
        await capture(
            config(assets=("BTC",)),
            tmp_path / "wrong-scope",
            0.03,
            resume_coinmetrics_from=tmp_path / "absent",
        )
    assert not (tmp_path / "disabled").exists()
    assert not (tmp_path / "wrong-scope").exists()
    forbidden.assert_not_awaited()


def test_cli_threads_explicit_coinmetrics_resume_without_changing_defaults(monkeypatch, tmp_path):
    args = parser().parse_args(
        ["capture", "--output", "new", "--resume-coinmetrics-from", "old.duckdb"]
    )
    assert args.resume_coinmetrics_from == Path("old.duckdb")
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", operation)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--resume-coinmetrics-from",
            "old.duckdb",
        ],
    )
    assert main() == 0
    assert operation.await_args.kwargs["resume_coinmetrics_from"] == Path("old.duckdb")
