"""Read-only checkpoint safety, exact pair scope and worker cleanup checks."""

import asyncio
import hashlib
import json
import threading

import duckdb
import pytest

from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.stablecoin_checkpoint import load_stablecoin_checkpoint
from selective_options.stablecoin_supply import stablecoin_poll_health, supply_event
from selective_options.store import DuckStore


def snapshot(offset=0, **changes):
    header = {
        "number": "0x64",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": "0x3e8",
        **changes,
    }

    def word(value):
        return "0x" + f"{value:064x}"

    values = ["0x1", header, word(123456), word(6), word(789012), word(6), header]
    return supply_event(
        [(value, 1_100_000 + i + offset) for i, value in enumerate(values)], 1800000
    )


def health(row, clock=None):
    return stablecoin_poll_health(
        clock or row.observed_at_ms + 1,
        ok=True,
        reason="SUPPLY_SNAPSHOT_OBSERVED",
        accepted_event_id=row.event_id,
        accepted_observed_at_ms=row.observed_at_ms,
        response_received_at_ms=row.observed_at_ms,
    )


async def database(path, rows):
    store = await DuckStore.open(path)
    await store.append(rows)
    await store.close()
    return path


async def test_bound_latest_exact_readonly_and_budget(tmp_path):
    first, second = snapshot(), snapshot(1000)
    path = await database(
        tmp_path / "events.duckdb", [first, health(first), second, health(second)]
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert await load_stablecoin_checkpoint(path, health(first).observed_at_ms, 2) == first
    assert await load_stablecoin_checkpoint(path, health(second).observed_at_ms, 4) == second
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load_stablecoin_checkpoint(path, health(second).observed_at_ms, 3)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


async def test_scope_and_both_clocks_before_limit(tmp_path):
    row = snapshot()
    noise = [
        row.model_copy(update={"event_id": f"noise-{i}", field: value})
        for i, (field, value) in enumerate(
            [
                ("asset", "ETH"),
                ("symbol", "OTHER"),
                ("source", "other"),
                ("environment", "testnet"),
                ("kind", "spot"),
                ("event_time_ms", 1_100_020),
                ("observed_at_ms", 1_100_020),
            ]
        )
    ]
    # Store accepts a limited source-clock skew; both future rows must remain excluded.
    path = await database(tmp_path / "events.duckdb", [row, health(row), *noise])
    assert await load_stablecoin_checkpoint(path, 1_100_010, 2) == row


@pytest.mark.parametrize("clock", [0, -1, True, 1.5, "10"])
async def test_invalid_clock_before_open(tmp_path, monkeypatch, clock):
    monkeypatch.setattr(
        "selective_options.stablecoin_checkpoint.duckdb.connect",
        lambda *a, **k: pytest.fail("opened"),
    )
    with pytest.raises(ValueError):
        await load_stablecoin_checkpoint(tmp_path / "absent", clock)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "10", 100001])
async def test_invalid_budget_before_open(tmp_path, monkeypatch, limit):
    monkeypatch.setattr(
        "selective_options.stablecoin_checkpoint.duckdb.connect",
        lambda *a, **k: pytest.fail("opened"),
    )
    with pytest.raises(ValueError):
        await load_stablecoin_checkpoint(tmp_path / "absent", 1_100_010, limit)


async def test_missing_and_empty_do_not_create(tmp_path):
    absent = tmp_path / "absent.duckdb"
    with pytest.raises(FileNotFoundError):
        await load_stablecoin_checkpoint(absent, 1_100_010)
    assert not absent.exists()
    empty = await database(tmp_path / "empty.duckdb", [])
    with pytest.raises(IntegrityError, match="HEALTH_MISSING"):
        await load_stablecoin_checkpoint(empty, 1_100_010)


async def test_latest_failure_no_healthy_fallback_and_recovery_boundaries(tmp_path):
    row = snapshot()
    failed = stablecoin_poll_health(1_100_100, ok=False, reason="RPC_UNAVAILABLE")
    repeated = snapshot(200)
    recovered = health(repeated)
    path = await database(
        tmp_path / "events.duckdb", [row, health(row), failed, repeated, recovered]
    )
    assert await load_stablecoin_checkpoint(path, failed.observed_at_ms - 1) == row
    with pytest.raises(IntegrityError, match="FEED_UNAVAILABLE"):
        await load_stablecoin_checkpoint(path, failed.observed_at_ms)
    with pytest.raises(IntegrityError, match="FEED_UNAVAILABLE"):
        await load_stablecoin_checkpoint(path, recovered.observed_at_ms - 1)
    assert await load_stablecoin_checkpoint(path, recovered.observed_at_ms) == repeated


@pytest.mark.parametrize(
    "case,blocker",
    [
        ("missing_health", "HEALTH_MISSING"),
        ("missing_snapshot", "REFERENCED_SNAPSHOT_MISSING"),
        ("tie", "HEALTH_AMBIGUOUS"),
        ("receipt", "REFERENCED_RECEIPT_MISMATCH"),
        ("payload", "INVALID_EVIDENCE"),
        ("identity", "SNAPSHOT_CANONICAL"),
        ("legacy", "HEALTH_UNBOUND"),
        ("regression", "REGRESSION"),
        ("conflict", "SAME_BLOCK_CONFLICT"),
    ],
)
async def test_unusable_history_refused(tmp_path, case, blocker):
    row = snapshot()
    bound = health(row)
    rows = [row, bound]
    if case == "missing_health":
        rows = [row]
    elif case == "missing_snapshot":
        rows = [bound]
    elif case == "tie":
        rows.append(
            stablecoin_poll_health(bound.observed_at_ms, ok=False, reason="RPC_UNAVAILABLE")
        )
    elif case == "receipt":
        rows[1] = stablecoin_poll_health(
            bound.observed_at_ms,
            ok=True,
            reason="SUPPLY_SNAPSHOT_OBSERVED",
            accepted_event_id=row.event_id,
            accepted_observed_at_ms=row.observed_at_ms - 1,
            response_received_at_ms=row.observed_at_ms - 1,
        )
    elif case == "payload":
        rows[0] = row.model_copy(
            update={"payload_json": json.dumps({**row.payload(), "extra": True})}
        )
    elif case == "identity":
        rows[0] = row.model_copy(update={"event_id": "corrupt"})
    elif case == "legacy":
        rows[1] = make_event(
            source=row.source,
            environment=row.environment,
            asset=row.asset,
            symbol=row.symbol,
            kind="feed_health",
            timestamp_kind="receipt",
            event_time_ms=bound.observed_at_ms,
            observed_at_ms=bound.observed_at_ms,
            payload={
                "ok": True,
                "reason": "SUPPLY_SNAPSHOT_OBSERVED",
                "code": 0,
                "error_type": None,
            },
        )
    elif case == "regression":
        rows.append(snapshot(100, number="0x63"))
    else:
        rows.append(snapshot(100, hash="0x" + "33" * 32))
    path = await database(tmp_path / "events.duckdb", rows)
    with pytest.raises(IntegrityError, match=blocker):
        await load_stablecoin_checkpoint(path, 1_100_500)


@pytest.mark.parametrize("as_of,blocker", [(2_300_008, "POLL_STALE"), (2_800_001, "POLL_STALE")])
async def test_stale_checkpoint_refused(tmp_path, as_of, blocker):
    row = snapshot()
    path = await database(tmp_path / "events.duckdb", [row, health(row)])
    with pytest.raises(IntegrityError, match=blocker):
        await load_stablecoin_checkpoint(path, as_of)


async def test_repeated_cancellation_drains_connection_and_preserves_file(tmp_path, monkeypatch):
    row = snapshot()
    path = await database(tmp_path / "events.duckdb", [row, health(row)])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    real_connect = duckdb.connect
    started, release, closed = threading.Event(), threading.Event(), threading.Event()

    class DelayedConnection:
        def __enter__(self):
            self.connection = real_connect(str(path), read_only=True)
            started.set()
            assert release.wait(5)
            return self.connection

        def __exit__(self, *args):
            self.connection.close()
            closed.set()

    monkeypatch.setattr(
        "selective_options.stablecoin_checkpoint.duckdb.connect",
        lambda *a, **k: DelayedConnection(),
    )
    task = asyncio.create_task(load_stablecoin_checkpoint(path, 1_100_010))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not closed.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
