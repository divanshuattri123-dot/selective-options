"""Exact stopped-capture WS membership, closed artifacts and cancellation proofs."""

import asyncio
import hashlib
import json
import threading
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from selective_options import bar_checkpoint as checkpoint
from selective_options.bar_state import decode_bar_state, encode_bar_state, restore_bar_state
from selective_options.bars import TradeBars
from selective_options.errors import IntegrityError
from selective_options.spot_trades import KrakenTrade, stored_trade, trade_event
from selective_options.store import DuckStore

CUTOFF = 620000


def tick(timestamp=301000, identifier=100, price=100.0, qty=0.1, receipt=None, asset="BTC"):
    trade = KrakenTrade(
        symbol=f"{asset}/USD",
        side="buy",
        price=price,
        qty=qty,
        trade_id=identifier,
        timestamp=datetime.fromtimestamp(timestamp / 1000, UTC),
    )
    return trade_event(trade, asset, receipt if receipt is not None else timestamp + 100)


def book(asset="BTC", *, empty=False):
    rows = tuple(
        tick(time, 100 + index, 100.0 + index, (index + 1) / 10, asset=asset)
        for index, time in enumerate((301000, 302000, 601000, 602000))
    )
    builder = TradeBars((300, 900), 300000, 5000)
    for row in rows:
        builder.add(row)
    cutoff = 1000000 if empty else CUTOFF
    builder.close_ready(cutoff)
    return checkpoint.CheckpointBook(
        state_blob=encode_bar_state(builder, cutoff),
        input_events=() if empty else rows,
        tail_event=rows[-1],
    ), builder


async def source(tmp_path, books=None):
    books = books or (book()[0],)
    path = tmp_path / "source's events.duckdb"
    store = await DuckStore.open(path)
    union = {row.event_id: row for item in books for row in (*item.input_events, item.tail_event)}
    await store.append(list(union.values()), exact=True)
    return store, path, books


async def load(output, **kwargs):
    return await checkpoint.load_bar_checkpoint(
        output, 1000000, ("BTC", "ETH"), (300, 900), 5000, **kwargs
    )


def rewrite_manifest(output, change):
    path = output / "checkpoint.json"
    value = json.loads(path.read_bytes())
    change(value)
    path.write_text(json.dumps(value), encoding="utf-8")


async def test_exact_replay_long_bucket_decimal_minimal_union_ack_and_readonly_resume(tmp_path):
    item, original = book()
    before_builder = deepcopy(vars(original))
    store, source_path, _ = await source(tmp_path, (item,))
    output = tmp_path / "checkpoint's output"
    # Stored raw-only historical trades do not become claimed consumed membership.
    old = tick(100000, 99)
    await store.append([old])
    await store.acknowledge_delivery([item.input_events[0].event_id])
    before_rows = await store.snapshot("BTC", CUTOFF, "public_reference")
    before_pending = await store.pending_delivery_count()
    try:
        manifest = await checkpoint.create_bar_checkpoint(store, output, (item,), CUTOFF)
        assert not manifest["history_complete"] and not manifest["trading_enabled"]
        assert manifest["source_database_hash_claimed"] is False
        assert (
            await store.snapshot("BTC", CUTOFF, "public_reference")
        ).rows() == before_rows.rows()
        assert await store.pending_delivery_count() == before_pending
        assert vars(original) == before_builder
    finally:
        await store.close()
    source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    database_hash = hashlib.sha256((output / "events.duckdb").read_bytes()).hexdigest()
    resumed = await load(output)
    assert resumed.books == (item,)
    assert set(resumed.events) == set(item.input_events)
    assert len(resumed.events) == 4
    restored = restore_bar_state(
        resumed.books[0].state_blob, 1000000, timeframes_s=(300, 900), lateness_ms=5000
    )
    long = restored.buckets[(900000, 0)]
    assert long.volume == Decimal("1.0") and long.count == 4
    assert long.first == (301000, 100) and long.last == (602000, 103)
    assert long.prototype == item.input_events[0]
    assert all(not bucket.complete for bucket in restored.buckets.values())
    assert restored.watermark_ms == 615000 and restored.coverage_start_ms == 1000000
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
        ).fetchone() == (4,)
    assert hashlib.sha256(source_path.read_bytes()).hexdigest() == source_hash
    assert hashlib.sha256((output / "events.duckdb").read_bytes()).hexdigest() == database_hash


async def test_multiple_books_and_empty_initialized_book_keep_watermark_and_tail(tmp_path):
    btc = book(empty=True)[0]
    eth = book("ETH", empty=True)[0]
    store, _, _ = await source(tmp_path, (btc, eth))
    try:
        await checkpoint.create_bar_checkpoint(store, tmp_path / "output", (btc, eth), 1000000)
    finally:
        await store.close()
    resumed = await load(tmp_path / "output")
    assert len(resumed.books) == len(resumed.events) == 2
    for item in resumed.books:
        state = decode_bar_state(item.state_blob)
        assert state.buckets == () and state.watermark_ms == 995000
        assert item.input_events == ()


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "extra",
        "order",
        "duplicate",
        "aggregate",
        "future",
        "scope",
        "native",
        "bad_type",
        "cutoff",
        "duplicate_book",
    ],
)
async def test_invalid_supplied_ledger_refused_before_output(tmp_path, case):
    item, _ = book()
    store, _, _ = await source(tmp_path, (item,))
    rows = item.input_events
    if case == "missing":
        item = item.model_copy(update={"input_events": rows[1:]})
    elif case == "extra":
        item = item.model_copy(update={"input_events": (tick(100000, 99), *rows)})
    elif case == "order":
        item = item.model_copy(update={"input_events": tuple(reversed(rows))})
    elif case == "duplicate":
        item = item.model_copy(update={"input_events": (*rows, rows[-1])})
    elif case == "aggregate":
        value = json.loads(item.state_blob)
        value["buckets"][-1]["count"] += 1
        item = item.model_copy(update={"state_blob": json.dumps(value).encode()})
    elif case == "future":
        item = item.model_copy(
            update={"tail_event": rows[-1].model_copy(update={"observed_at_ms": CUTOFF + 1})}
        )
    elif case == "scope":
        item = item.model_copy(update={"tail_event": tick(602000, 103, asset="ETH")})
    elif case == "native":
        item = item.model_copy(
            update={"tail_event": rows[-1].model_copy(update={"source": "kraken_rest"})}
        )
    elif case == "bad_type":
        item = item.model_copy(update={"input_events": list(rows)})
    elif case == "cutoff":
        value = json.loads(item.state_blob)
        value["as_of_ms"] += 1
        item = item.model_copy(update={"state_blob": json.dumps(value).encode()})
    try:
        items = (item, item) if case == "duplicate_book" else (item,)
        with pytest.raises((IntegrityError, ValueError)):
            await checkpoint.create_bar_checkpoint(store, tmp_path / "output", items, CUTOFF)
        assert not (tmp_path / "output").exists()
        assert await store.pending_delivery_count() == 4
    finally:
        await store.close()


@pytest.mark.parametrize(
    "case", ["absent", "receipt", "conflict", "later_tail", "closed", "transaction"]
)
async def test_source_commitment_and_unique_tail_refusal_without_source_mutation(tmp_path, case):
    item, _ = book()
    store = await DuckStore.open(tmp_path / "source.duckdb")
    rows = item.input_events
    await store.append(list(rows[1:] if case == "absent" else rows))
    if case == "receipt":
        store._connection.execute(
            "UPDATE events SET observed_at_ms=? WHERE event_id=?", [CUTOFF + 1, rows[0].event_id]
        )
    elif case == "conflict":
        await store.append([tick(301000, 100, price=999.0)])
    elif case == "later_tail":
        await store.append([tick(603000, 104)])
    elif case == "closed":
        await store.close()
    elif case == "transaction":
        store._connection.execute("BEGIN TRANSACTION")
    try:
        with pytest.raises(IntegrityError):
            await checkpoint.create_bar_checkpoint(store, tmp_path / "output", (item,), CUTOFF)
        assert not (tmp_path / "output").exists()
        if case == "transaction":
            # Refusal must preserve, rather than roll back, the operator transaction.
            first = store._connection.execute("SELECT current_transaction_id()").fetchone()
            second = store._connection.execute("SELECT current_transaction_id()").fetchone()
            assert first == second
            store._connection.execute("ROLLBACK")
    finally:
        await store.close()


async def test_future_both_clock_rows_do_not_displace_declared_eligible_tail(tmp_path):
    store, _, items = await source(tmp_path)
    await store.append(
        [tick(CUTOFF + 1, 104, receipt=CUTOFF), tick(603000, 105, receipt=CUTOFF + 1)]
    )
    try:
        await checkpoint.create_bar_checkpoint(
            store, tmp_path / "output", items, CUTOFF, max_events=4
        )
    finally:
        await store.close()
    assert len((await load(tmp_path / "output", max_events=4)).events) == 4


@pytest.mark.parametrize(
    "field,value",
    [
        ("as_of_ms", True),
        ("max_events", 0),
        ("max_books", False),
        ("max_bytes", -1),
        ("max_events", 100001),
    ],
)
async def test_policy_refuses_before_output(tmp_path, field, value):
    store, _, items = await source(tmp_path)
    args = {"as_of_ms": CUTOFF, field: value}
    try:
        with pytest.raises(ValueError):
            await checkpoint.create_bar_checkpoint(store, tmp_path / "output", items, **args)
        assert not (tmp_path / "output").exists()
    finally:
        await store.close()


@pytest.mark.parametrize("budget", ["events", "books", "encoded_bytes", "artifact_bytes"])
async def test_finite_budget_overflow_never_publishes_success(tmp_path, budget):
    store, _, items = await source(tmp_path)
    args = (
        {"max_events": 3}
        if budget == "events"
        else {"max_bytes": 10 if budget == "encoded_bytes" else 10000}
    )
    if budget == "books":
        items = (*items, book("ETH")[0])
        args = {"max_books": 1}
    try:
        with pytest.raises(IntegrityError, match="BUDGET"):
            await checkpoint.create_bar_checkpoint(
                store, tmp_path / "output", items, CUTOFF, **args
            )
        assert not (tmp_path / "output/checkpoint.json").exists()
        if budget == "artifact_bytes":
            assert (tmp_path / "output/failure.json").exists()
            assert (tmp_path / "output/events.duckdb").exists()
    finally:
        await store.close()


@pytest.mark.parametrize(
    "case",
    [
        "blob",
        "database",
        "membership",
        "payload_rehash",
        "ack_rehash",
        "claim",
        "unknown",
        "duplicate_json",
        "extra_file",
    ],
)
async def test_loader_refuses_tampering_even_with_updated_hash(tmp_path, case):
    store, _, items = await source(tmp_path)
    output = tmp_path / "output"
    try:
        await checkpoint.create_bar_checkpoint(store, output, items, CUTOFF)
    finally:
        await store.close()
    if case == "blob":
        (output / "state-000000.json").write_bytes(b"{}")
    elif case == "database":
        with (output / "events.duckdb").open("ab") as stream:
            stream.write(b"corrupt")
    elif case == "membership":
        rewrite_manifest(output, lambda value: value["books"][0]["input_ids"].pop())
    elif case in {"payload_rehash", "ack_rehash"}:
        path = output / "events.duckdb"
        with duckdb.connect(str(path)) as connection:
            if case == "payload_rehash":
                connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1")
            else:
                connection.execute("UPDATE delivery_outbox SET delivered=TRUE")
        rewrite_manifest(
            output,
            lambda value: value.update(
                events_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                events_bytes=path.stat().st_size,
            ),
        )
    elif case == "claim":
        rewrite_manifest(output, lambda value: value.update(history_complete=True))
    elif case == "unknown":
        rewrite_manifest(output, lambda value: value.update(unexpected=True))
    elif case == "duplicate_json":
        path = output / "checkpoint.json"
        path.write_bytes(path.read_bytes().rstrip()[:-1] + b',"format_version":1}')
    else:
        (output / "unexplained").write_text("extra")
    with pytest.raises((IntegrityError, ValueError)):
        await load(output)


async def test_restart_configuration_before_cutoff_and_forged_resume_refusal(tmp_path):
    store, _, items = await source(tmp_path)
    try:
        await checkpoint.create_bar_checkpoint(store, tmp_path / "output", items, CUTOFF)
    finally:
        await store.close()
    resumed = await load(tmp_path / "output")
    for now, assets, intervals, lateness in [
        (CUTOFF - 1, ("BTC",), (300, 900), 5000),
        (1000000, ("ETH",), (300, 900), 5000),
        (1000000, ("BTC",), (300,), 5000),
        (1000000, ("BTC",), (300, 900), 0),
    ]:
        with pytest.raises(IntegrityError):
            await checkpoint.load_bar_checkpoint(
                tmp_path / "output", now, assets, intervals, lateness
            )
    for fake in [
        resumed.model_copy(update={"as_of_ms": True}),
        resumed.model_copy(update={"events": tuple(reversed(resumed.events))}),
        resumed.model_copy(update={"books": list(resumed.books)}),
    ]:
        with pytest.raises((IntegrityError, ValueError)):
            checkpoint.validate_bar_resume(fake, 1000000, ("BTC",), (300, 900), 5000)
    clean = checkpoint.validate_bar_resume(resumed, 1000000, ("BTC",), (300, 900), 5000)
    assert clean == resumed and clean is not resumed


@pytest.mark.parametrize("case", ["valid", "extra", "ack", "wal", "forged"])
async def test_seed_validation_exact_closed_union_and_pending_outbox(tmp_path, case):
    store, _, items = await source(tmp_path)
    try:
        await checkpoint.create_bar_checkpoint(store, tmp_path / "output", items, CUTOFF)
    finally:
        await store.close()
    resumed = await load(tmp_path / "output")
    path = tmp_path / "seed.duckdb"
    seed = await DuckStore.open(path)
    try:
        await seed.append(list(resumed.events), exact=True)
        if case == "extra":
            await seed.append([tick(603000, 104)])
        elif case == "ack":
            await seed.acknowledge_delivery([resumed.events[0].event_id])
    finally:
        await seed.close()
    if case == "wal":
        path.with_suffix(".duckdb.wal").write_bytes(b"active marker")
    elif case == "forged":
        resumed = resumed.model_copy(update={"events": resumed.events[:-1]})
    before = path.read_bytes()
    if case == "valid":
        report = await checkpoint.verify_bar_checkpoint_seed(path, resumed, 1000000)
        assert report["events"] == 4 and report["source_mutated"] is False
    else:
        with pytest.raises(IntegrityError):
            await checkpoint.verify_bar_checkpoint_seed(path, resumed, 1000000)
    assert path.read_bytes() == before


async def test_path_reparse_traversal_and_existing_output_refuse(tmp_path, monkeypatch):
    store, _, items = await source(tmp_path)
    ancestor = tmp_path / "reparse"
    ancestor.mkdir()
    original = Path.lstat
    mode = ancestor.lstat().st_mode
    monkeypatch.setattr(
        Path,
        "lstat",
        lambda self: (
            SimpleNamespace(st_mode=mode, st_file_attributes=0x400)
            if self == ancestor
            else original(self)
        ),
    )
    try:
        with pytest.raises(ValueError, match="reparse"):
            await checkpoint.create_bar_checkpoint(store, ancestor / "new", items, CUTOFF)
        assert not (ancestor / "new").exists()
        with pytest.raises(ValueError, match="traversal"):
            await checkpoint.create_bar_checkpoint(store, tmp_path / ".." / "new", items, CUTOFF)
        with pytest.raises(FileExistsError):
            await checkpoint.create_bar_checkpoint(store, tmp_path, items, CUTOFF)
    finally:
        await store.close()


async def test_conservative_gap_flag_for_future_skew_bucket_is_never_promoted(tmp_path):
    row = tick(300001, 1, receipt=299999)
    builder = TradeBars((300, 900), 0, 5000)
    builder.add(row)
    builder.mark_gap(300000)
    builder.close_ready(301000)
    assert builder.buckets[(300000, 300000)].complete is False
    item = checkpoint.CheckpointBook(
        state_blob=encode_bar_state(builder, 301000), input_events=(row,), tail_event=row
    )
    store, _, _ = await source(tmp_path, (item,))
    try:
        await checkpoint.create_bar_checkpoint(store, tmp_path / "output", (item,), 301000)
    finally:
        await store.close()
    resumed = await load(tmp_path / "output")
    assert all(
        not bucket.complete for bucket in decode_bar_state(resumed.books[0].state_blob).buckets
    )


async def test_pure_resume_refuses_conflicting_tail_with_same_native_input_id(tmp_path):
    store, _, items = await source(tmp_path)
    try:
        await checkpoint.create_bar_checkpoint(store, tmp_path / "output", items, CUTOFF)
    finally:
        await store.close()
    resumed = await load(tmp_path / "output")
    conflicting = tick(602000, 103, price=999.0)
    item = resumed.books[0].model_copy(update={"tail_event": conflicting})
    rows = tuple(sorted((*resumed.events, conflicting), key=lambda row: row.event_id))
    forged = resumed.model_copy(update={"books": (item,), "events": rows})
    with pytest.raises(IntegrityError, match="CONFLICTING_TAIL_NATIVE_ID"):
        checkpoint.validate_bar_resume(forged, 1000000, ("BTC",), (300, 900), 5000)


async def test_duplicate_symbol_across_assets_refuses_before_output_and_on_resume(tmp_path):
    first = book()[0]
    rows = tuple(
        trade_event(stored_trade(row), "ETH", row.observed_at_ms) for row in first.input_events
    )
    builder = TradeBars((300, 900), 300000, 5000)
    for row in rows:
        builder.add(row)
    builder.close_ready(CUTOFF)
    second = checkpoint.CheckpointBook(
        state_blob=encode_bar_state(builder, CUTOFF), input_events=rows, tail_event=rows[-1]
    )
    store, _, _ = await source(tmp_path, (first, second))
    try:
        with pytest.raises(IntegrityError, match="DUPLICATE_BOOK_SYMBOL"):
            await checkpoint.create_bar_checkpoint(
                store, tmp_path / "output", (first, second), CUTOFF
            )
        assert not (tmp_path / "output").exists()
    finally:
        await store.close()
    forged = checkpoint.BarCheckpointResume(
        as_of_ms=CUTOFF,
        books=(first, second),
        events=tuple(sorted((*first.input_events, *rows), key=lambda row: row.event_id)),
    )
    with pytest.raises(IntegrityError, match="DUPLICATE_BOOK_SYMBOL"):
        checkpoint.validate_bar_resume(forged, 1000000, ("BTC", "ETH"), (300, 900), 5000)


@pytest.mark.parametrize("phase", ["state_fsync", "manifest_link", "mutation_before_publish"])
async def test_write_failure_retains_staging_and_never_success(tmp_path, monkeypatch, phase):
    store, _, items = await source(tmp_path)
    output = tmp_path / "output"
    if phase == "state_fsync":

        def failure(descriptor):
            raise OSError("injected state sync failure")

        monkeypatch.setattr(checkpoint.os, "fsync", failure)
    elif phase == "manifest_link":

        def failure(*args, **kwargs):
            raise OSError("injected final link failure")

        monkeypatch.setattr(checkpoint.os, "link", failure)
    else:
        original = checkpoint._publish

        def failure(*args, **kwargs):
            (output / "state-000000.json").write_bytes(b"{}")
            return original(*args, **kwargs)

        monkeypatch.setattr(checkpoint, "_publish", failure)
    try:
        with pytest.raises((OSError, IntegrityError)):
            await checkpoint.create_bar_checkpoint(store, output, items, CUTOFF)
        assert not (output / "checkpoint.json").exists()
        assert (output / "events.duckdb").is_file()
        assert (output / "failure.json").is_file()
        assert await store.pending_delivery_count() == 4
        await store.append([tick(603000, 104)])
        assert await store.pending_delivery_count() == 5
    finally:
        await store.close()


async def test_loader_refuses_resealed_wrong_sql_clock_type(tmp_path):
    store, _, items = await source(tmp_path)
    output = tmp_path / "output"
    try:
        await checkpoint.create_bar_checkpoint(store, output, items, CUTOFF)
    finally:
        await store.close()
    path = output / "events.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "CREATE TABLE changed AS SELECT * REPLACE(CAST(schema_version AS BIGINT) AS schema_version) FROM events"
        )
        connection.execute("DROP TABLE events")
        connection.execute("ALTER TABLE changed RENAME TO events")
    rewrite_manifest(
        output,
        lambda value: value.update(
            events_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            events_bytes=path.stat().st_size,
        ),
    )
    with pytest.raises(IntegrityError, match="SCHEMA"):
        await load(output)


@pytest.mark.parametrize("phase", ["stage", "publish"])
async def test_repeated_cancellation_drains_worker_and_revokes_late_marker(
    tmp_path, monkeypatch, phase
):
    store, _, items = await source(tmp_path)
    entered, release = threading.Event(), threading.Event()
    output = tmp_path / "output"
    name = "_stage" if phase == "stage" else "_publish"
    original = getattr(checkpoint, name)

    def blocked(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(10)
        return result

    monkeypatch.setattr(checkpoint, name, blocked)
    task = asyncio.create_task(checkpoint.create_bar_checkpoint(store, output, items, CUTOFF))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        if phase == "stage":
            assert store._lock.locked()
            assert not (output / "checkpoint.json").exists()
        else:
            assert (output / "checkpoint.json").exists()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not store._lock.locked()
        assert not (output / "checkpoint.json").exists()
        assert (output / "failure.json").exists()
        assert await store.pending_delivery_count() == 4
        with pytest.raises(FileNotFoundError):
            await load(output)
    finally:
        release.set()
        await store.close()
