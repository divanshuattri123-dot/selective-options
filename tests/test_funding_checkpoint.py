import asyncio
import hashlib
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import duckdb
import pytest

from scripts import verify_binance_funding_history as verifier
from selective_options import funding_checkpoint
from selective_options.binance_funding_view import funding_history_at
from selective_options.errors import IntegrityError
from selective_options.funding_checkpoint import load_funding_seed
from selective_options.store import DuckStore
from tests.test_binance_funding_view import event

POLICY = dict(asset="BTC", symbol="BTCUSDT", start_ms=1000, end_ms=9500, as_of_ms=14000)


async def committed(path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(rows)
    finally:
        await store.close()


async def test_all_revisions_read_only_and_old_pit(tmp_path):
    path = tmp_path / "source.duckdb"
    a, b, again = event(), event(receipt=11000, rate="-0.00020000"), event(receipt=12000)
    await committed(path, [a, b, again, event(8000, 15000), event(500, 10000)])
    digest = hashlib.sha256(path.read_bytes()).digest()
    rows = await load_funding_seed(path, **POLICY)
    assert rows == (a, b, again)
    assert funding_history_at(rows, **{**POLICY, "as_of_ms": 10000}) == (a,)
    assert funding_history_at(rows, **POLICY) == (again,)
    assert hashlib.sha256(path.read_bytes()).digest() == digest
    with duckdb.connect(str(path), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 5


async def test_budget_counts_observations_not_selected_charges(tmp_path):
    path = tmp_path / "source.duckdb"
    await committed(path, [event(), event(receipt=11000)])
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load_funding_seed(path, **POLICY, max_rows=1)


@pytest.mark.parametrize("mode", ["empty", "scope", "schema", "corrupt", "tie"])
async def test_bad_seed_refuses(mode, tmp_path):
    path = tmp_path / "source.duckdb"
    rows = [] if mode == "empty" else [event()]
    if mode == "tie":
        rows.append(event(rate="0.00020000"))
    await committed(path, rows)
    if mode in {"schema", "corrupt"}:
        with duckdb.connect(str(path)) as db:
            db.execute(
                "DROP TABLE events" if mode == "schema" else "UPDATE events SET payload_json='{}'"
            )
    policy = {**POLICY, "asset": "ETH"} if mode == "scope" else POLICY
    with pytest.raises(IntegrityError):
        await load_funding_seed(path, **policy)


async def test_missing_file_and_invalid_policy_do_not_create_database(tmp_path):
    path = tmp_path / "missing.duckdb"
    with pytest.raises(ValueError):
        await load_funding_seed(path, **{**POLICY, "max_rows": True})
    with pytest.raises(FileNotFoundError):
        await load_funding_seed(path, **POLICY)
    assert not path.exists()


async def test_scope_filter_precedes_validation_and_bad_typed_row_refuses(tmp_path):
    path = tmp_path / "source.duckdb"
    good = event()
    future = event(8000, 15000)
    await committed(path, [good, future])
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE events SET payload_json='{}' WHERE event_id=?", [future.event_id])
    assert await load_funding_seed(path, **POLICY) == (good,)
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE events SET kind='invalid' WHERE event_id=?", [future.event_id])
        db.execute("UPDATE events SET timestamp_kind='invalid' WHERE event_id=?", [good.event_id])
    with pytest.raises(IntegrityError, match="SCHEMA_OR_ROW_INVALID"):
        await load_funding_seed(path, **POLICY)


async def test_cancelled_seed_worker_finishes_before_propagation(tmp_path, monkeypatch):
    path = tmp_path / "source.duckdb"
    await committed(path, [event()])
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = funding_checkpoint._load

    def held(*args):
        started.set()
        assert release.wait(5)
        rows = original(*args)
        finished.set()
        return rows

    monkeypatch.setattr(funding_checkpoint, "_load", held)
    task = asyncio.create_task(load_funding_seed(path, **POLICY))
    assert await asyncio.to_thread(started.wait, 5)
    try:
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    # Read-only worker has closed: a differently configured writable connection can reopen.
    with duckdb.connect(str(path)) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 1


async def setup_resume(tmp_path, monkeypatch, fresh=None):
    clock = 180_000_000
    monkeypatch.setattr(verifier, "now_ms", lambda: clock)
    source = tmp_path / "prior.duckdb"
    old = event(clock - 10000, clock - 9000)
    await committed(source, [old])
    plan = SimpleNamespace(events=tuple(fresh or []), pages=1, termination="EMPTY_PAGE")
    fetch = AsyncMock(return_value=plan)
    monkeypatch.setattr(verifier, "collect_binance_funding_history", fetch)
    return source, old, fetch, clock


async def test_resume_preserves_receipts_and_closed_reload(tmp_path, monkeypatch, capsys):
    source, old, fetch, clock = await setup_resume(tmp_path, monkeypatch)
    fresh = event(old.event_time_ms, clock, rate="-0.00020000")
    fetch.return_value.events = (fresh,)
    digest = hashlib.sha256(source.read_bytes()).digest()
    output = tmp_path / "new"
    await verifier.verify(output, resume_from=source)
    report = json.loads((output / "report.json").read_text())
    assert report["rows"] == 2 and report["seed_rows_copied"] == 1
    assert report["seed_committed_reload_verified"] is True
    assert json.loads(capsys.readouterr().out) == report
    rows = await load_funding_seed(
        output / "events.duckdb", **{**POLICY, "start_ms": 1, "end_ms": clock, "as_of_ms": clock}
    )
    assert rows == (old, fresh)
    assert hashlib.sha256(source.read_bytes()).digest() == digest
    assert funding_history_at(
        rows, asset="BTC", symbol="BTCUSDT", start_ms=1, end_ms=clock, as_of_ms=clock - 1
    ) == (old,)


async def test_invalid_seed_fails_before_output_or_fetch(tmp_path, monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(verifier, "collect_binance_funding_history", fetch)
    output = tmp_path / "new"
    with pytest.raises(FileNotFoundError):
        await verifier.verify(output, resume_from=tmp_path / "missing")
    assert not output.exists()
    fetch.assert_not_awaited()


@pytest.mark.parametrize(
    "failure", [RuntimeError("denied"), IntegrityError("TRUNCATED"), asyncio.CancelledError()]
)
async def test_failed_fetch_publishes_no_seed(tmp_path, monkeypatch, failure):
    source, old, fetch, clock = await setup_resume(tmp_path, monkeypatch)
    digest = hashlib.sha256(source.read_bytes()).digest()
    fetch.side_effect = failure
    output = tmp_path / "new"
    with pytest.raises(type(failure)):
        await verifier.verify(output, resume_from=source)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["rows"] == 0
    assert report["seed_rows_validated"] == 1 and report["seed_rows_copied"] == 0
    assert report["seed_committed_reload_verified"] is False
    assert hashlib.sha256(source.read_bytes()).digest() == digest


async def test_combined_ambiguous_revision_refuses_publication(tmp_path, monkeypatch):
    source, old, fetch, clock = await setup_resume(tmp_path, monkeypatch)
    fetch.return_value.events = (event(old.event_time_ms, old.observed_at_ms, rate="-0.00020000"),)
    output = tmp_path / "new"
    with pytest.raises(IntegrityError, match="AMBIGUOUS"):
        await verifier.verify(output, resume_from=source)
    report = json.loads((output / "report.json").read_text())
    assert report["rows"] == 0 and report["failure_phase"] == "FETCH_AND_VALIDATE"


@pytest.mark.parametrize("failure", [IntegrityError("RELOAD_FAILED"), asyncio.CancelledError()])
async def test_reload_failure_retains_committed_rows_and_records_failure(
    tmp_path, monkeypatch, failure
):
    source, old, fetch, clock = await setup_resume(tmp_path, monkeypatch)
    loader = AsyncMock(side_effect=[(old,), failure])
    monkeypatch.setattr(verifier, "load_funding_seed", loader)
    output = tmp_path / "new"
    with pytest.raises(type(failure)):
        await verifier.verify(output, resume_from=source)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["failure_phase"] == "COMMITTED_RELOAD"
    assert report["rows"] == 1 and report["seed_rows_copied"] == 1
    assert report["seed_committed_reload_verified"] is False
    rows = await funding_checkpoint.load_funding_seed(
        output / "events.duckdb",
        asset="BTC",
        symbol="BTCUSDT",
        start_ms=1,
        end_ms=clock,
        as_of_ms=clock,
    )
    assert rows == (old,)
