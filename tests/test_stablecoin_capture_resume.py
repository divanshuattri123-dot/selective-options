"""Explicit healthy-pair capture restore; mocked transport never grants readiness."""

import asyncio
import hashlib
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.cli import main, parser
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture
from selective_options.stablecoin_checkpoint import load_stablecoin_resume
from selective_options.stablecoin_supply import StablecoinSupplyCollector, stablecoin_poll_health
from selective_options.store import DuckStore
from tests.test_stablecoin_checkpoint import database, health, snapshot

CLOCK = 1_102_000


def config(**changes):
    return AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "stablecoin_supply_enabled": True,
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
            **changes,
        },
    )


async def idle(*args):
    await asyncio.Event().wait()


def quiet(monkeypatch):
    monkeypatch.setattr("selective_options.ingestion.now_ms", lambda: CLOCK)
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: CLOCK)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)


async def test_pair_snapshot_and_latest_bound_health_are_one_readonly_selection(tmp_path):
    row = snapshot()
    first = health(row)
    later = health(row, 1_101_000)
    path = await database(tmp_path / "source.duckdb", [row, first, later])
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    assert await load_stablecoin_resume(path, CLOCK) == (row, later)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load_stablecoin_resume(path, CLOCK, 2)


@pytest.mark.parametrize(
    "policy",
    [
        {"max_poll_age_ms": False},
        {"max_poll_age_ms": 0},
        {"max_block_age_ms": 0},
        {"max_block_age_ms": True},
        {"max_rows": 0},
        {"max_rows": 100001},
    ],
)
async def test_pair_policy_before_database_open(tmp_path, monkeypatch, policy):
    monkeypatch.setattr(
        "selective_options.stablecoin_checkpoint.duckdb.connect",
        lambda *a, **k: pytest.fail("opened"),
    )
    with pytest.raises(ValueError):
        await load_stablecoin_resume(tmp_path / "missing", CLOCK, **policy)


@pytest.mark.parametrize(
    "case", ["failed", "stale_poll", "stale_block", "overbudget", "missing_health", "tied"]
)
async def test_refused_history_never_creates_capture_or_starts_producers(
    tmp_path, monkeypatch, case
):
    quiet(monkeypatch)
    row = snapshot()
    rows = [row, health(row)]
    changes = {}
    if case == "failed":
        rows.append(stablecoin_poll_health(CLOCK - 1, ok=False, reason="RPC_UNAVAILABLE"))
    if case == "stale_poll":
        monkeypatch.setattr("selective_options.ingestion.now_ms", lambda: 3_000_000)
    if case == "stale_block":
        changes["stablecoin_supply_max_block_age_s"] = 60
    if case == "overbudget":
        # Deliberately lowered loader budget is a fixture for capture's preflight refusal.
        real = load_stablecoin_resume

        async def capped(path, at, **policy):
            return await real(path, at, max_rows=1, **policy)

        monkeypatch.setattr("selective_options.ingestion.load_stablecoin_resume", capped)
    if case == "missing_health":
        rows = [row]
    if case == "tied":
        rows.append(
            stablecoin_poll_health(health(row).observed_at_ms, ok=False, reason="RPC_UNAVAILABLE")
        )
    path = await database(tmp_path / "source.duckdb", rows)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    run = AsyncMock()
    monkeypatch.setattr(StablecoinSupplyCollector, "run", run)
    with pytest.raises(IntegrityError):
        await capture(config(**changes), tmp_path / "capture", 0.1, resume_stablecoin_from=path)
    assert not (tmp_path / "capture").exists()
    run.assert_not_awaited()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_disabled_supply_preflight_requires_optin_and_creates_nothing(tmp_path):
    with pytest.raises(ValueError, match="enabled supply"):
        await capture(
            AppConfig(), tmp_path / "capture", 0.1, resume_stablecoin_from=tmp_path / "missing"
        )
    assert not (tmp_path / "capture").exists()


@pytest.mark.parametrize("case", ["mismatch", "cancel"])
async def test_destination_revalidation_failure_closes_before_any_producer(
    tmp_path, monkeypatch, case
):
    quiet(monkeypatch)
    pair = (snapshot(), health(snapshot()))
    path = await database(tmp_path / "source.duckdb", list(pair))
    replacement = (
        asyncio.CancelledError() if case == "cancel" else (snapshot(1000), health(snapshot(1000)))
    )
    loader = AsyncMock(side_effect=[pair, replacement])
    monkeypatch.setattr("selective_options.ingestion.load_stablecoin_resume", loader)
    run = AsyncMock()
    monkeypatch.setattr(StablecoinSupplyCollector, "run", run)
    with pytest.raises(asyncio.CancelledError if case == "cancel" else IntegrityError):
        await capture(config(), tmp_path / "capture", 0.1, resume_stablecoin_from=path)
    run.assert_not_awaited()
    assert await load_stablecoin_resume(tmp_path / "capture/events.duckdb", CLOCK) == pair
    # Successful copied transaction is exact and remains pending after failed startup.
    store = await DuckStore.open(tmp_path / "capture/events.duckdb")
    try:
        assert await store.pending_delivery_count() == 2
    finally:
        await store.close()


@pytest.mark.parametrize("outcome", ["quiet", "repeat", "failed"])
async def test_capture_exact_copy_first_poll_seed_and_quality_persistence(
    tmp_path, monkeypatch, outcome
):
    quiet(monkeypatch)
    seed = snapshot()
    pair = (seed, health(seed))
    path = await database(tmp_path / "source.duckdb", list(pair))
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    checked = []

    async def run(self):
        assert self.previous == seed
        checked.append(self.previous)
        if outcome != "quiet":
            self._collect = AsyncMock(
                return_value=snapshot(1000, **({"number": "0x63"} if outcome == "failed" else {}))
            )
            assert await self.collect(None) == (outcome == "repeat")
        await idle()

    monkeypatch.setattr(StablecoinSupplyCollector, "run", run)
    report = await capture(config(), tmp_path / "capture", 0.1, resume_stablecoin_from=path)
    assert checked == [seed] and not report["ready_for_signals"] and not report["trading_enabled"]
    resumed = report["stablecoin_resume"]
    assert resumed["copied_events"] == 2 and resumed["anchor_event_id"] == seed.event_id
    assert (
        resumed["health_observed_at_ms"] == pair[1].observed_at_ms
        and resumed["selection_as_of_ms"] == CLOCK
    )
    assert not resumed["complete_history_copied"] and not resumed["transport_resumed_by_copy"]
    expected_rows = 2 if outcome == "quiet" else (4 if outcome == "repeat" else 3)
    db = tmp_path / "capture/events.duckdb"
    with duckdb.connect(str(db), read_only=True) as c:
        cursor = c.execute("SELECT * FROM events ORDER BY event_id")
        columns = [v[0] for v in cursor.description]
        rows = [dict(zip(columns, r, strict=True)) for r in cursor.fetchall()]
        assert len(rows) == expected_rows
        for original in pair:
            assert original.model_dump(mode="json") in rows
        assert (
            c.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[0]
            == expected_rows
        )
        assert (
            c.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == c.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(tmp_path / "capture/events.parquet")],
            ).fetchall()
        )
    if outcome == "failed":
        assert report["stablecoin_supply_quality"]["state"] == "BLOCKED"
        with pytest.raises(IntegrityError, match="FEED_UNAVAILABLE"):
            await load_stablecoin_resume(db, CLOCK)
    else:
        assert report["stablecoin_supply_quality"]["state"] == "POLL_OBSERVED_VERIFIED"
        selected = await load_stablecoin_resume(db, CLOCK)
        assert selected[0] == (seed if outcome == "quiet" else snapshot(1000))
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_cli_arg_forwards_without_arming(monkeypatch, tmp_path):
    args = parser().parse_args(
        ["capture", "--output", "new", "--resume-stablecoin-from", "old.duckdb"]
    )
    assert str(args.resume_stablecoin_from) == "old.duckdb"
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", operation)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path),
            "--resume-stablecoin-from",
            "old.duckdb",
        ],
    )
    assert (
        main() == 0 and str(operation.await_args.kwargs["resume_stablecoin_from"]) == "old.duckdb"
    )


async def test_seed_constructor_refusal_closes_store_before_producer_start(tmp_path, monkeypatch):
    quiet(monkeypatch)
    pair = (snapshot(), health(snapshot()))
    path = await database(tmp_path / "source.duckdb", list(pair))
    producer = AsyncMock()
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)

    def refuse(*args, **kwargs):
        raise IntegrityError("FIXTURE_SEED_REFUSED")

    monkeypatch.setattr(StablecoinSupplyCollector, "__init__", refuse)
    with pytest.raises(IntegrityError, match="SEED_REFUSED"):
        await capture(config(), tmp_path / "capture", 0.1, resume_stablecoin_from=path)
    producer.assert_not_awaited()
    assert await load_stablecoin_resume(tmp_path / "capture/events.duckdb", CLOCK) == pair
