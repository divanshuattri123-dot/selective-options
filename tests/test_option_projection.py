"""Real retained storage verifies bounded projection, both clocks, ties and cancellation."""

import asyncio
import threading

import duckdb
import pytest

from selective_options.collectors import ChannelRegistry, Notification
from selective_options.events import make_event
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore
from tests.test_multiasset_coverage_verifier import sample
from tests.test_option_coverage import control, fixture, replace_payload


def quote(original, timestamp, receipt, **changes):
    body = original.payload() | {"timestamp": timestamp} | changes
    registry = ChannelRegistry()
    registry.add_instrument(original.asset, original.symbol, "btc_usd")
    channel = (
        f"ticker.{original.symbol}.100ms"
        if original.kind == "ticker"
        else f"book.{original.symbol}.none.20.100ms"
    )
    return registry.normalize(
        Notification.model_validate(
            {
                "jsonrpc": "2.0",
                "method": "subscription",
                "params": {"channel": channel, "data": body},
            }
        ),
        receipt,
    )[0]


async def test_retained_history_does_not_exhaust_projection_and_all_bytes_export(
    tmp_path, instrument, ticker
):
    rows = fixture(instrument, ticker)
    history = [quote(rows[-2], timestamp, 1300) for timestamp in range(1, 101)]
    # Corrupt retired-symbol payloads are not selected legs and cannot satisfy membership.
    history.extend(
        make_event(
            source="deribit",
            environment="testnet",
            asset="BTC",
            symbol="RETIRED",
            kind="ticker",
            event_time_ms=t,
            observed_at_ms=1300,
            payload={},
        )
        for t in range(1, 20)
    )
    expected = coverage_at(rows, "BTC", 1500, max_rows=6)
    assert (
        coverage_at([*rows, *history], "BTC", 1500, max_rows=6)["blocker"]
        == "OPTION_COVERAGE_OVER_BUDGET"
    )
    path = tmp_path / "history.duckdb"
    store = await DuckStore.open(path)
    calls, select = [], store._select

    def observed_select(sql, params):
        result = select(sql, params)
        calls.append(len(result))
        return result

    store._select = observed_select
    try:
        await store.append([*rows, *history])
        await store.append([*rows, *history])
        assert await store.option_coverage("BTC", 1500, max_rows=6) == expected
        assert calls == [3, 1, 2]
        assert await store.export_parquet(tmp_path / "history.parquet") == len(rows) + len(history)
        assert await store.pending_delivery_count() == len(rows) + len(history)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        assert await store.option_coverage("BTC", 1500, max_rows=6) == expected
    finally:
        await store.close()
    with duckdb.connect(str(path), read_only=True) as db:
        native = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        assert (
            native
            == db.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(tmp_path / "history.parquet")],
            ).fetchall()
        )


@pytest.mark.parametrize("cutoff", [1199, 1200, 1300, 1399, 1400, 1500, 1599, 1600, 1799, 1800])
async def test_both_clocks_and_session_filter_match_full_oracle(
    tmp_path, instrument, ticker, cutoff
):
    rows = fixture(instrument, ticker)
    rows.extend(
        [
            quote(rows[-2], 1600, 1300, mark_iv=60),
            quote(rows[-1], 1300, 1800, change_id=2),
            quote(rows[-2], 1700, 1199, mark_iv=65),
            control("STREAM_RESYNC", 1150, connection=True),
            control("IGNORED", 1450),
        ]
    )
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        assert await store.option_coverage("BTC", cutoff) == coverage_at(rows, "BTC", cutoff)
    finally:
        await store.close()


@pytest.mark.parametrize("budget", [1, 2, 3, 4, 5, 6])
async def test_bound_includes_all_control_metadata_and_tied_legs(
    tmp_path, instrument, ticker, budget
):
    rows = fixture(instrument, ticker)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    calls, select = [], store._select

    def observed_select(sql, params):
        values = select(sql, params)
        calls.append(len(values))
        return values

    store._select = observed_select
    try:
        await store.append(rows)
        assert await store.option_coverage("BTC", 1500, max_rows=budget) == coverage_at(
            rows, "BTC", 1500, max_rows=budget
        )
        assert sum(calls) <= budget + 1
    finally:
        await store.close()


async def test_old_controls_are_not_pruned_to_fit_budget(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    rows.extend(control("STREAM_RESYNC", 100 + i, connection=2 + i) for i in range(10))
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        assert (await store.option_coverage("BTC", 1500, max_rows=8))[
            "blocker"
        ] == "OPTION_COVERAGE_OVER_BUDGET"
    finally:
        await store.close()


@pytest.mark.parametrize(
    "change",
    [
        {"connection": True},
        {"channels": 2},
        {"universe_event_ids": ["missing"]},
        {"connection": 2},
        {"unexpected": True},
    ],
)
async def test_invalid_ack_same_blocker_without_quote_query(tmp_path, instrument, ticker, change):
    rows = fixture(instrument, ticker)
    rows[3] = replace_payload(rows[3], **change)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    select, calls = store._select, []

    def observed_select(sql, params):
        calls.append(sql)
        return select(sql, params)

    store._select = observed_select
    try:
        await store.append(rows)
        assert await store.option_coverage("BTC", 1500) == coverage_at(rows, "BTC", 1500)
        assert len(calls) == 1
    finally:
        await store.close()


@pytest.mark.parametrize(
    "scenario",
    [
        "tied",
        "identity",
        "payload",
        "inactive",
        "expired",
        "missing",
        "legacy",
        "control_tie",
        "universe_stale",
    ],
)
async def test_conflicts_and_failures_are_not_hidden(tmp_path, instrument, ticker, scenario):
    rows = fixture(
        instrument | ({"expiration_timestamp": 1500} if scenario == "expired" else {}), ticker
    )
    if scenario == "tied":
        rows.append(quote(rows[-1], 1300, 1300, change_id=2))
    elif scenario == "identity":
        rows[-1] = rows[-1].model_copy(update={"event_id": "bad"})
    elif scenario == "payload":
        rows[-1] = rows[-1].model_copy(update={"payload_json": "{}"})
    elif scenario == "inactive":
        rows.append(control("STREAM_DISCONNECTED", 1500))
    elif scenario == "missing":
        rows.pop()
    elif scenario == "legacy":
        body = rows[3].payload()
        body.pop("universe_event_ids")
        rows[3] = control(body.pop("reason"), 1400, **body)
    elif scenario == "control_tie":
        rows.append(control("STREAM_REFRESH_DUE", 1400, connection=1))
    store = await DuckStore.open(tmp_path / "events.duckdb")
    age = 399 if scenario == "universe_stale" else 3600000
    try:
        await store.append(rows)
        assert await store.option_coverage("BTC", 1500, max_universe_age_ms=age) == coverage_at(
            rows, "BTC", 1500, max_universe_age_ms=age
        )
    finally:
        await store.close()


@pytest.mark.parametrize("payload", ["invalid", "[]", "true", "null"])
async def test_corrupt_unrelated_status_root_is_not_silently_discarded(
    tmp_path, instrument, ticker, payload
):
    rows = fixture(instrument, ticker)
    extra = control("IGNORED", 1450)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([*rows, extra])
        store._connection.execute(
            "UPDATE events SET payload_json=? WHERE event_id=?", [payload, extra.event_id]
        )
        expected = coverage_at(
            [*rows, extra.model_copy(update={"payload_json": payload})], "BTC", 1500
        )
        assert expected["blocker"] == "OPTION_INVALID_EVIDENCE"
        assert await store.option_coverage("BTC", 1500) == expected
    finally:
        await store.close()


@pytest.mark.parametrize("asset", ["ETH", "SOL", "PAXG"])
async def test_multiasset_shared_control_and_empty_universe(tmp_path, instrument, ticker, asset):
    rows = sample(instrument, ticker, empty=True)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        assert await store.option_coverage(asset, 1500) == coverage_at(rows, asset, 1500)
    finally:
        await store.close()


@pytest.mark.parametrize(
    "policy",
    [
        {"asset": "XAUT"},
        {"as_of_ms": True},
        {"max_rows": 0},
        {"max_rows": 200001},
        {"max_leg_age_ms": 0},
        {"max_universe_age_ms": -1},
    ],
)
async def test_policy_preflight_no_worker_or_database_read(monkeypatch, policy):
    store = DuckStore(None, 5000)

    def unexpected(*args):
        raise AssertionError("no query is allowed")

    monkeypatch.setattr(store, "_option_coverage", unexpected)
    with pytest.raises(ValueError):
        await store.option_coverage(**{"asset": "BTC", "as_of_ms": 1500, **policy})


async def test_repeated_cancellation_drains_projection_before_append_or_close(
    tmp_path, monkeypatch, instrument, ticker
):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    rows = fixture(instrument, ticker)
    await store.append(rows)
    original = store._option_coverage
    started, release = threading.Event(), threading.Event()

    def gated(*args):
        started.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(store, "_option_coverage", gated)
    task = asyncio.create_task(store.option_coverage("BTC", 1500))
    append = closing = None
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        append = asyncio.create_task(store.append([control("IGNORED", 1600)]))
        closing = asyncio.create_task(store.close())
        await asyncio.sleep(0)
        assert not task.done() and not append.done() and not closing.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        if append is not None:
            await append
        if closing is not None:
            await closing
        await store.close()


async def test_closed_database_failure_propagates(tmp_path):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    await store.close()
    with pytest.raises(duckdb.ConnectionException):
        await store.option_coverage("BTC", 1500)
