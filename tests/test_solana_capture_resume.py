"""Finite SOL handoff integration with explicitly synthetic native-shape fixtures."""

import asyncio
import hashlib
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.cli import main, parser
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture
from selective_options.solana_checkpoint import load_solana_resume
from selective_options.solana_network import SolanaNetworkCollector
from selective_options.store import DuckStore
from tests.test_solana_network import health, pair

NOW = 1_000_500


def config(*, assets=("SOL",), **changes):
    return AppConfig(
        system={"assets": list(assets)},
        ingestion={
            "solana_network_enabled": True,
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


def quiet(monkeypatch, at_ms=NOW):
    monkeypatch.setattr("selective_options.ingestion.now_ms", lambda: at_ms)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)


async def seed_database(tmp_path, *, rows=None):
    native = pair()
    selected = [*native, health(native)] if rows is None else rows
    path = tmp_path / "source.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(selected, exact=True)
    finally:
        await store.close()
    return path


async def test_exact_closed_seed_revalidated_before_sol_constructor_or_any_producer(
    tmp_path, monkeypatch
):
    quiet(monkeypatch)
    source = await seed_database(tmp_path)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    output = tmp_path / "capture"
    seed = await load_solana_resume(source, NOW)
    stages = []

    async def observed_loader(path, cutoff, **policy):
        result = await load_solana_resume(path, cutoff, **policy)
        if path == output / "events.duckdb":
            # A distinct read-only connection is permitted only after the write
            # store closed; check the actual committed seed and new pending outbox.
            with duckdb.connect(str(path), read_only=True) as connection:
                assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3
                assert (
                    connection.execute(
                        "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
                    ).fetchone()[0]
                    == 3
                )
            stages.append("closed_seed_revalidated")
        return result

    class ObservedSol:
        def __init__(self, *args, **kwargs):
            assert stages == ["closed_seed_revalidated"]
            stages.append("sol_constructed")

        async def run(self):
            stages.append("sol_started")
            await idle()

    async def producer(*args):
        assert stages[0] == "closed_seed_revalidated"
        await idle()

    monkeypatch.setattr("selective_options.ingestion.load_solana_resume", observed_loader)
    monkeypatch.setattr("selective_options.ingestion.SolanaNetworkCollector", ObservedSol)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", producer)
    report = await capture(config(), output, 0.03, resume_solana_from=source)
    assert stages == ["closed_seed_revalidated", "sol_constructed", "sol_started"]
    resumed = report["solana_resume"]
    assert resumed["copied_events"] == 3
    assert resumed["closed_seed_revalidated_before_producers"] is True
    assert resumed["copied_outbox_pending"] is True
    assert resumed["delivery_acknowledgements_restored"] is False
    assert resumed["history_complete"] is False
    assert resumed["transport_resumed_by_copy"] is False
    assert resumed["ready_for_signals"] is False
    assert report["trading_enabled"] is False
    assert report["solana_network_quality"]["state"] == "NETWORK_SAMPLE_VERIFIED"
    assert await load_solana_resume(output / "events.duckdb", NOW) == seed
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
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
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("case", ["latest_failure", "stale", "budget", "missing_pair"])
async def test_invalid_seed_fails_preflight_before_output_and_producers(
    tmp_path, monkeypatch, case
):
    native = pair()
    rows = [*native, health(native)]
    at_ms = NOW
    if case == "latest_failure":
        at_ms += 1
        rows.append(health(at=at_ms, ok=False))
    elif case == "missing_pair":
        rows = [native[0], health(native)]
    elif case == "stale":
        at_ms += 600_001
    source = await seed_database(tmp_path, rows=rows)
    quiet(monkeypatch, at_ms)
    producer = AsyncMock(side_effect=AssertionError("refused seed started transport"))
    monkeypatch.setattr(SolanaNetworkCollector, "run", producer)
    if case == "budget":

        async def capped(path, cutoff, **policy):
            policy["max_rows"] = 2
            return await load_solana_resume(path, cutoff, **policy)

        monkeypatch.setattr("selective_options.ingestion.load_solana_resume", capped)
    output = tmp_path / "refused"
    with pytest.raises(IntegrityError):
        await capture(config(), output, 0.03, resume_solana_from=source)
    assert not output.exists()
    producer.assert_not_awaited()


@pytest.mark.parametrize("assets,enabled", [(("BTC",), True), (("SOL",), False)])
async def test_sol_config_required_before_seed_read(tmp_path, monkeypatch, assets, enabled):
    loader = AsyncMock(side_effect=AssertionError("invalid config read checkpoint"))
    monkeypatch.setattr("selective_options.ingestion.load_solana_resume", loader)
    output = tmp_path / "refused"
    with pytest.raises(ValueError, match="enabled SOL network"):
        await capture(
            config(assets=assets, solana_network_enabled=enabled),
            output,
            0.03,
            resume_solana_from=tmp_path / "absent",
        )
    assert not output.exists()
    loader.assert_not_awaited()


@pytest.mark.parametrize("case", ["reload_refusal", "changed_seed", "pending_outbox"])
async def test_closed_destination_validation_failure_prevents_all_producer_starts(
    tmp_path, monkeypatch, case
):
    quiet(monkeypatch)
    source = await seed_database(tmp_path)
    output = tmp_path / "capture"
    producer = AsyncMock(side_effect=AssertionError("invalid copied seed started producer"))
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", producer)
    monkeypatch.setattr(SolanaNetworkCollector, "run", producer)
    original_pending = DuckStore.pending_delivery_count
    pending_calls = 0

    async def observed_loader(path, cutoff, **policy):
        result = await load_solana_resume(path, cutoff, **policy)
        if path == output / "events.duckdb":
            if case == "reload_refusal":
                raise IntegrityError("FIXTURE_CLOSED_SEED_REFUSED")
            if case == "changed_seed":
                return result[:-1]
        return result

    async def changed_pending(self):
        nonlocal pending_calls
        value = await original_pending(self)
        pending_calls += 1
        return value + 1 if case == "pending_outbox" and pending_calls == 2 else value

    monkeypatch.setattr("selective_options.ingestion.load_solana_resume", observed_loader)
    monkeypatch.setattr(DuckStore, "pending_delivery_count", changed_pending)
    with pytest.raises(IntegrityError):
        await capture(config(), output, 0.03, resume_solana_from=source)
    producer.assert_not_awaited()
    # Failure cleanup closes the actual write handle as well.
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 3


async def test_capture_default_does_not_read_any_solana_checkpoint(tmp_path, monkeypatch):
    quiet(monkeypatch)
    loader = AsyncMock(side_effect=AssertionError("default capture loaded checkpoint"))
    monkeypatch.setattr("selective_options.ingestion.load_solana_resume", loader)
    monkeypatch.setattr(SolanaNetworkCollector, "run", idle)
    report = await capture(config(), tmp_path / "plain", 0.03)
    assert report["solana_resume"] is None
    loader.assert_not_awaited()


def test_explicit_sol_cli_option_and_default_compatibility(tmp_path, monkeypatch):
    assert parser().parse_args(["capture", "--output", "new"]).resume_solana_from is None
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", operation)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--resume-solana-from",
            "old.duckdb",
        ],
    )
    assert main() == 0
    assert operation.await_args.kwargs["resume_solana_from"] == Path("old.duckdb")
