"""Scalar JSON binds preserve exact identity, transaction and worker-lock semantics."""

import asyncio
import threading

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.store import DuckStore


def event(key, receipt=1100):
    # Explicitly synthetic identities test SQL literals independently of hash generation.
    return make_event(
        source="fixture",
        environment="testnet",
        asset="BTC",
        symbol=key,
        kind="status",
        event_time_ms=1000,
        observed_at_ms=receipt,
        payload={"fixture_origin": "SYNTHETIC_SQL_LITERAL", "native": key},
    ).model_copy(update={"event_id": key})


async def test_literals_duplicates_missing_empty_and_closed_reload(tmp_path):
    path = tmp_path / "events.duckdb"
    keys = [
        "a'quoted",
        "\\backslash",
        "line\nfeed",
        "€-unicode",
        "?parameter",
        "'); DELETE FROM events; --",
        "",
        '"quoted"',
    ]
    rows = [event(key) for key in keys]
    store = await DuckStore.open(path)
    try:
        await store.append(rows + rows, exact=True)
        await store.append(rows, exact=True)
        requested = keys + keys + ["absent"]
        result = await store.read_events(requested)
        assert len(result) == len(rows) and set(result) == set(rows)
        assert await store.read_events([]) == ()
        assert await store.read_events(["absent"]) == ()
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        assert set(await store.read_events(keys)) == set(rows)
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "change",
    [
        {"observed_at_ms": 1101},
        {"payload_json": '{"changed":true}'},
        {"source": "other"},
        {"environment": "public_reference"},
    ],
)
async def test_existing_conflict_rolls_back_new_rows_and_outbox(tmp_path, change):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    original, new = event("original"), event("new")
    try:
        await store.append([original], exact=True)
        await store.acknowledge_delivery([original.event_id])
        with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
            await store.append([new, original.model_copy(update=change)], exact=True)
        assert await store.read_events([original.event_id, new.event_id]) == (original,)
        assert await store.pending_delivery_count() == 0
        await store.append([original], exact=True)
        assert await store.pending_delivery_count() == 0
    finally:
        await store.close()


async def test_within_batch_conflict_is_atomic(tmp_path):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    original = event("original")
    try:
        with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
            await store.append([event("new"), original, event("original", 1101)], exact=True)
        assert await store.read_events(["new", "original"]) == ()
        assert await store.pending_delivery_count() == 0
    finally:
        await store.close()


async def test_exact_20001_id_boundary_and_prequery_refusal(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    rows = [event(str(i)) for i in range(20001)]
    try:
        await store.append(rows, exact=True)
        await store.append(rows, exact=True)
        result = await store.read_events([row.event_id for row in rows])
        assert len(result) == 20001 and set(result) == set(rows)
        assert await store.pending_delivery_count() == 20001

        def forbidden(*args):
            raise AssertionError("oversized input touched SQL")

        monkeypatch.setattr(store, "_select", forbidden)
        with pytest.raises(ValueError, match="budget"):
            await store.read_events(["0"] * 20002)
    finally:
        await store.close()


async def test_cancelled_lookup_drains_worker_before_append_or_close(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    started, release = threading.Event(), threading.Event()
    original_select = store._select

    def blocked(sql, params):
        started.set()
        assert release.wait(5)
        return original_select(sql, params)

    monkeypatch.setattr(store, "_select", blocked)
    lookup = asyncio.create_task(store.read_events(["one"]))
    try:
        assert await asyncio.to_thread(started.wait, 5)
        lookup.cancel()
        await asyncio.sleep(0)
        lookup.cancel()
        append = asyncio.create_task(store.append([event("one")], exact=True))
        close = asyncio.create_task(store.close())
        await asyncio.sleep(0)
        assert not lookup.done() and not append.done() and not close.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await lookup
        await append
        await close
    finally:
        release.set()
        await store.close()
