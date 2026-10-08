"""Independent full BTC subscription/PIT/leg/archive proof from saved data; no network."""

import asyncio
import hashlib
import json
from collections import Counter
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent
from selective_options.models import Instrument
from selective_options.option_coverage import coverage_at, status_canonical
from selective_options.store import DuckStore

SOURCE = Path(__file__).resolve().parent
REPLAY = SOURCE.parent / "reused-validation-replay-01"
PHASES = {"SOCKET_RECEIVE", "NORMALIZE", "SINK_PUBLISH"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def main():
    paths = [
        SOURCE / name
        for name in (
            "events.duckdb",
            "events.parquet",
            "events.parquet.manifest.json",
            "report.json",
            "verification.json",
            "config.sanitized.json",
        )
    ]
    before = {path.name: digest(path) for path in paths}
    captured = json.loads((SOURCE / "report.json").read_text())
    config = json.loads((SOURCE / "config.sanitized.json").read_text())
    assert config["ingestion"]["max_stream_instruments_per_asset"] is None
    assert config["ingestion"]["subscription_batch_size"] == 100
    assert config["venue"]["request_timeout_s"] == 15
    cutoff = captured["ended_at_ms"]
    with duckdb.connect(str(SOURCE / "events.duckdb"), read_only=True) as db:
        query = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in query.description]
        native = query.fetchall()
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
        )
        assert len(rows) == captured["rows"] == 62089 == len({row.event_id for row in rows})
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(rows)
        assert (
            db.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(SOURCE / "events.parquet")]
            ).fetchall()
            == native
        )
        controls = sorted(
            (row for row in rows if row.kind == "status"), key=lambda row: row.observed_at_ms
        )
        boundaries = 0
        for row in controls:
            status_canonical(row)
            for point, expected in ((row.observed_at_ms - 1, 0), (row.observed_at_ms, 1)):
                assert (
                    db.execute(
                        "SELECT count(*) FROM events WHERE event_id = ? AND event_time_ms <= ? AND observed_at_ms <= ?",
                        [row.event_id, point, point],
                    ).fetchone()[0]
                    == expected
                )
                boundaries += 1
        discovered = next(
            row for row in controls if row.payload()["reason"] == "UNIVERSE_DISCOVERED"
        )
        starts = [row for row in controls if row.payload()["reason"] == "STREAM_RESYNC"]
        acks = [row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIBED"]
        assert len(starts) == len(acks) == 1
        assert not any(row.payload()["reason"] == "STREAM_DISCONNECTED" for row in controls)
        start, ack = starts[0], acks[0]
        manifest = discovered.payload()
        assert manifest["selected"] == manifest["available"] == 1010
        assert set(manifest["instruments"]) == set(manifest["selected_instruments"])
        metadata = {row.event_id: row for row in rows if row.kind == "instrument"}
        indices = set()
        for symbol, event_id in zip(
            manifest["instruments"], manifest["metadata_event_ids"], strict=True
        ):
            row = metadata[event_id]
            instrument = Instrument.model_validate(row.payload())
            assert row.symbol == instrument.instrument_name == symbol
            assert row.asset == instrument.base_currency == "BTC"
            assert instrument.is_active and instrument.expiration_timestamp > cutoff
            assert (
                row.event_time_ms
                == row.observed_at_ms
                <= discovered.observed_at_ms
                <= start.observed_at_ms
            )
            indices.add(instrument.price_index)
        required = 3 * len(metadata) + len(indices)
        assert required == ack.payload()["channels"] == 3031
        assert ack.payload()["universe_event_ids"] == [discovered.event_id]
        assert {(row["asset"], row["symbol"]) for row in ack.payload()["instruments"]} == {
            ("BTC", symbol) for symbol in manifest["selected_instruments"]
        }
        progress = [
            row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIPTION_BATCH_ACK"
        ]
        assert len(progress) == 31
        prior = {key: 0 for key in PHASES}
        completed = 0
        for index, row in enumerate(progress, 1):
            body = row.payload()
            assert body["connection"] == 1 and body["total_channels"] == required
            assert body["request_id"] == completed + 1
            size = min(100, required - completed)
            completed += size
            assert body["batch_channels"] == size
            assert (
                body["acknowledged_channels"] == completed and body["acknowledged_batches"] == index
            )
            total, delta, elapsed = (
                body["receive_timing_ns"],
                body["response_timing_ns"],
                body["response_elapsed_ns"],
            )
            assert set(total) == set(delta) == PHASES
            assert all(
                type(delta[key]) is int
                and delta[key] >= 0
                and total[key] - prior[key] == delta[key]
                for key in PHASES
            )
            assert type(elapsed) is int and elapsed >= sum(delta.values())
            assert start.observed_at_ms <= row.observed_at_ms <= ack.observed_at_ms
            prior = total
        assert completed == required
        before_ack = coverage_at(rows, "BTC", ack.observed_at_ms - 1, 30000)
        at_ack = coverage_at(rows, "BTC", ack.observed_at_ms, 30000)
        assert before_ack["blocker"] == "OPTION_STREAM_INACTIVE"
        assert at_ack["full_discovered_membership"] is True
        view = coverage_at(rows, "BTC", cutoff, 30000)
        assert view == captured["option_stream_coverage"][0]
        assert (
            view["state"] == "INCOMPLETE_LEG_OBSERVATIONS"
            and view["full_discovered_membership"] is True
        )
        assert (
            view["expected_leg_observations"] == 2020 and view["observed_leg_observations"] == 1946
        )
        # Independently select both-clock-eligible latest rows, including tied conflicts.
        latest = db.execute(
            "SELECT event_id,symbol,kind,event_time_ms,observed_at_ms FROM events WHERE source='deribit' AND environment='testnet' AND asset='BTC' AND kind IN ('ticker','book') AND event_time_ms <= ? AND observed_at_ms BETWEEN ? AND ? QUALIFY dense_rank() OVER (PARTITION BY symbol,kind ORDER BY event_time_ms DESC,observed_at_ms DESC)=1",
            [cutoff, start.observed_at_ms, cutoff],
        ).fetchall()
        assert len(latest) == 2020
        independent = {
            (symbol, kind): (
                event_id,
                "STALE" if cutoff - source_time > 30000 or cutoff - receipt > 30000 else "OBSERVED",
            )
            for event_id, symbol, kind, source_time, receipt in latest
        }
        for leg in view["legs"]:
            assert independent[(leg["symbol"], leg["kind"])] == (leg["event_id"], leg["state"])
        counts = Counter((row["kind"], row["state"]) for row in view["legs"])
        assert counts == {
            ("ticker", "OBSERVED"): 1010,
            ("book", "OBSERVED"): 936,
            ("book", "STALE"): 74,
        }
    seal = json.loads((SOURCE / "events.parquet.manifest.json").read_text())
    assert seal["sha256"] == before["events.parquet"] and seal["rows"] == len(rows)
    assert seal["bytes"] == (SOURCE / "events.parquet").stat().st_size
    REPLAY.mkdir(exist_ok=False)
    store = await DuckStore.open(REPLAY / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        asset = await store.snapshot("BTC", cutoff)
        system = await store.snapshot("SYSTEM", cutoff)
        assert coverage_at([*asset.rows(), *system.rows()], "BTC", cutoff, 30000) == view
        assert await store.export_parquet(REPLAY / "events.parquet") == len(rows)
    finally:
        await store.close()
    with duckdb.connect(str(REPLAY / "events.duckdb"), read_only=True) as db:
        assert db.execute("SELECT * FROM events ORDER BY event_id").fetchall() == native
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(rows)
    assert before == {path.name: digest(path) for path in paths}
    report = {
        "status": "FULL_BTC_SUBSCRIPTION_BINDING_VERIFIED_WITH_STALE_BOOKS_WITHHELD",
        "saved_rows": len(rows),
        "pending_outbox_rows": len(rows),
        "discovered_and_selected_options": 1010,
        "required_and_acknowledged_channels": 3031,
        "batch_progress_records": 31,
        "fully_acknowledged_sessions": 1,
        "disconnects": 0,
        "full_ack_after_capture_start_ms": ack.observed_at_ms - captured["started_at_ms"],
        "full_ack_after_resync_ms": ack.observed_at_ms - start.observed_at_ms,
        "completed_subscription_prefix_receive_timing_ns": progress[-1].payload()[
            "receive_timing_ns"
        ],
        "sql_control_pit_boundaries": boundaries,
        "ack_visibility_checks": 2,
        "independent_sql_latest_leg_checks": 2020,
        "fresh_tickers": 1010,
        "fresh_books": 936,
        "stale_books": 74,
        "missing_legs": 0,
        "freshness_age_bound_ms": 30000,
        "stale_book_source_or_receipt_age_not_relaxed": True,
        "exact_metadata_subscription_and_timing_bindings_verified": True,
        "closed_reload_replay_and_sealed_archive_verified": True,
        "source_bytes_unchanged": True,
        "original_sha256": before,
        "new_network_requests": 0,
        "finite_uncapped_btc_subscriptions_verified": True,
        "full_fresh_option_stream_verified": False,
        "all_required_assets_or_feeds_verified": False,
        "trade_history_continuity_verified": False,
        "sustained_capacity_verified": False,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (SOURCE / "independent-proof.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "original_sha256"}, indent=2
        )
    )


asyncio.run(main())
