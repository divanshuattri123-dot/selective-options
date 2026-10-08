"""Actual bounded queries retain canonical success/failure across durable reloads.

Native values and transport outcomes here are explicitly synthetic fixtures.
"""

import json

import duckdb
import pytest

from selective_options.archive import seal_archive
from selective_options.liquidations import health_event, native_event, streams_for
from selective_options.store import DuckStore
from tests.test_new_feed_store import assess, query
from tests.test_stablecoin_supply import health, snapshot, stablecoin_poll_health
from tests.test_tips_yield import healthy, tips_yield_health
from tests.test_tips_yield import snapshot as tips_snapshot


def fixture_rows(feed):
    if feed == "stablecoin_supply":
        native = snapshot()
        clock = native.observed_at_ms + 100
        success = health(native, clock)
        failed = stablecoin_poll_health(
            clock + 100, ok=False, reason="TRANSPORT_UNAVAILABLE", error_type="OSError"
        )
        recovered = health(native, clock + 200)
    elif feed == "tips_yield":
        native = tips_snapshot()
        clock = native.observed_at_ms + 10
        success = healthy(native, clock)
        failed = tips_yield_health(clock + 100, error_type="OSError")
        recovered = healthy(native, clock + 200)
    else:
        clock = 1100100
        native = native_event(
            {
                "e": "forceOrder",
                "E": clock - 50,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "o": "LIMIT",
                    "f": "IOC",
                    "q": "0.0100",
                    "p": "60000.00",
                    "ap": "60001.00",
                    "X": "FILLED",
                    "l": "0.0100",
                    "z": "0.0100",
                    "T": clock - 60,
                },
            },
            clock - 25,
            "BTC",
        )

        def make_health(at, reason, *, session="a" * 32, referenced=True):
            return health_event(
                "BTC",
                at,
                state="DISCONNECTED" if reason == "CONNECTION_CLOSED" else "CONNECTED",
                ok=reason != "CONNECTION_CLOSED",
                reason=reason,
                session_id=session,
                session_started_at_ms=clock - 100 if session == "a" * 32 else clock + 150,
                streams=streams_for(["BTC"]),
                ignored_unrequested_frames=0,
                accepted_event_id=native.event_id if referenced else None,
                accepted_observed_at_ms=native.observed_at_ms if referenced else None,
            )

        ack = make_health(clock - 90, "SUBSCRIPTION_ACKNOWLEDGED", referenced=False)
        success = make_health(clock, "FRAME_OBSERVED")
        failed = make_health(clock + 100, "CONNECTION_CLOSED")
        recovered = make_health(
            clock + 200, "SUBSCRIPTION_ACKNOWLEDGED", session="b" * 32, referenced=False
        )
        return [ack, native, success, failed, recovered], clock
    return [native, success, failed, recovered], clock


@pytest.mark.parametrize("feed", ["stablecoin_supply", "tips_yield", "liquidations"])
async def test_success_failure_recovery_pit_archive_reopen_and_outbox(tmp_path, feed):
    rows, clock = fixture_rows(feed)
    database, archive = tmp_path / "events.duckdb", tmp_path / "events.parquet"
    store = await DuckStore.open(database)
    views = {}
    try:
        await store.append(rows, exact=True)
        await store.append(rows, exact=True)
        for at in (clock - 1, clock, clock + 99, clock + 100, clock + 199, clock + 200):
            views[at] = await query(store, feed, at)
            assert views[at] == assess(feed, rows, at)
        assert views[clock]["state"] != "BLOCKED"
        assert views[clock + 100]["state"] == "BLOCKED"
        assert views[clock + 200]["state"] != "BLOCKED"
        if feed == "liquidations":
            # Recovery acknowledges a new quiet session: no native event is invented.
            assert views[clock + 200]["observation_state"] == "UNOBSERVED"
        assert (await query(store, feed, clock + 200, max_rows=len(rows) - 1))["state"] == "BLOCKED"
        assert await store.pending_delivery_count() == len(rows)
        count = await store.export_parquet(archive)
        assert count == len(rows)
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
        for at, view in views.items():
            assert await query(store, feed, at) == view
        assert await store.pending_delivery_count() == len(rows)
    finally:
        await store.close()


@pytest.mark.parametrize("feed", ["stablecoin_supply", "tips_yield", "liquidations"])
async def test_corrupted_latest_success_cannot_clear_persisted_failure(tmp_path, feed):
    rows, clock = fixture_rows(feed)
    recovered = rows[-1]
    body = recovered.payload()
    body["unsupported_claim"] = "SYNTHETIC_CORRUPTION_MUST_BLOCK"
    corrupted = recovered.model_copy(update={"payload_json": json.dumps(body)})
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([*rows[:-1], corrupted], exact=True)
        view = await query(store, feed, clock + 200)
        assert view == assess(feed, [*rows[:-1], corrupted], clock + 200)
        assert view["state"] == "BLOCKED" and view["trading_approval"] is False
    finally:
        await store.close()
