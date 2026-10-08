"""Independent saved multiasset metadata/PIT/leg/archive verification, zero network."""

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
REPLAY = SOURCE.parent / "multiasset-options-replay-01"
PHASES = {"SOCKET_RECEIVE", "NORMALIZE", "SINK_PUBLISH"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def main():
    assert not REPLAY.exists() and not (SOURCE / "independent-proof.json").exists()
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
    verified = json.loads((SOURCE / "verification.json").read_text())
    config = json.loads((SOURCE / "config.sanitized.json").read_text())
    assets = config["system"]["assets"]
    assert assets == ["ETH", "SOL", "PAXG"] == verified["assets"]
    assert config["ingestion"]["max_stream_instruments_per_asset"] is None
    assert config["ingestion"]["subscription_batch_size"] == 100
    assert config["venue"]["request_timeout_s"] == 15
    cutoff = captured["ended_at_ms"]
    age = int(config["data"]["max_staleness_s"] * 1000)
    assert age == 30000
    with duckdb.connect(str(SOURCE / "events.duckdb"), read_only=True) as db:
        query = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in query.description]
        native = query.fetchall()
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
        )
        assert (
            len(rows)
            == captured["rows"]
            == verified["saved_rows"]
            == len({row.event_id for row in rows})
        )
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
                        "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                        [row.event_id, point, point],
                    ).fetchone()[0]
                    == expected
                )
                boundaries += 1
        starts = [row for row in controls if row.payload()["reason"] == "STREAM_RESYNC"]
        acks = [row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIBED"]
        assert len(starts) == len(acks) == 1
        assert not any(row.payload()["reason"] == "STREAM_DISCONNECTED" for row in controls)
        start, ack = starts[0], acks[0]
        manifests = {
            row.payload()["asset"]: row
            for row in controls
            if row.payload()["reason"] == "UNIVERSE_DISCOVERED"
        }
        assert set(manifests) == set(assets)
        assert ack.payload()["universe_event_ids"] == [
            manifests[asset].event_id for asset in assets
        ]
        metadata = {row.event_id: row for row in rows if row.kind == "instrument"}
        native_channels, members, bound_ids = set(), set(), set()
        scoped = []
        total_leg_checks = 0
        for asset, view in zip(assets, captured["option_stream_coverage"], strict=True):
            discovered = manifests[asset]
            manifest = discovered.payload()
            symbols = set(manifest["selected_instruments"])
            assert len(symbols) == manifest["available"] == manifest["selected"]
            assert symbols == set(manifest["instruments"])
            for symbol, event_id in zip(
                manifest["instruments"], manifest["metadata_event_ids"], strict=True
            ):
                row = metadata[event_id]
                instrument = Instrument.model_validate(row.payload())
                assert row.symbol == instrument.instrument_name == symbol
                assert row.asset == instrument.base_currency == asset
                assert instrument.is_active and instrument.expiration_timestamp > cutoff
                assert (
                    row.event_time_ms
                    == row.observed_at_ms
                    <= discovered.observed_at_ms
                    <= start.observed_at_ms
                )
                native_channels.update(
                    {
                        f"ticker.{symbol}.100ms",
                        f"book.{symbol}.none.20.100ms",
                        f"trades.{symbol}.100ms",
                        f"deribit_price_index.{instrument.price_index}",
                    }
                )
                members.add((asset, symbol))
                bound_ids.add(event_id)
            assert coverage_at(rows, asset, cutoff, age) == view
            assert (
                coverage_at(rows, asset, ack.observed_at_ms - 1, age)["blocker"]
                == "OPTION_STREAM_INACTIVE"
            )
            at_ack = coverage_at(rows, asset, ack.observed_at_ms, age)
            counts = Counter()
            if symbols:
                assert at_ack["full_discovered_membership"] is True
                assert view["full_discovered_membership"] is True and view["blocker"] is None
                latest = db.execute(
                    "SELECT event_id,symbol,kind,event_time_ms,observed_at_ms FROM events WHERE source='deribit' AND environment='testnet' AND asset=? AND kind IN ('ticker','book') AND event_time_ms<=? AND observed_at_ms BETWEEN ? AND ? QUALIFY dense_rank() OVER (PARTITION BY symbol,kind ORDER BY event_time_ms DESC,observed_at_ms DESC)=1",
                    [asset, cutoff, start.observed_at_ms, cutoff],
                ).fetchall()
                independent = {}
                for identity, symbol, kind, source_time, receipt in latest:
                    key = (symbol, kind)
                    assert key not in independent
                    independent[key] = (
                        identity,
                        "STALE"
                        if cutoff - source_time > age or cutoff - receipt > age
                        else "OBSERVED",
                    )
                assert view["expected_leg_observations"] == 2 * len(symbols)
                for leg in view["legs"]:
                    assert independent.get((leg["symbol"], leg["kind"]), (None, "MISSING")) == (
                        leg["event_id"],
                        leg["state"],
                    )
                    counts[(leg["kind"], leg["state"])] += 1
                    total_leg_checks += 1
                observed = sum(
                    count for (kind, state), count in counts.items() if state == "OBSERVED"
                )
                assert observed == view["observed_leg_observations"]
                assert view["state"] == (
                    "SELECTED_FEEDS_OBSERVED"
                    if observed == 2 * len(symbols)
                    else "INCOMPLETE_LEG_OBSERVATIONS"
                )
            else:
                assert at_ack["blocker"] == view["blocker"] == "OPTION_SELECTED_UNIVERSE_EMPTY"
                assert manifest["coverage"] == "PARTIAL_OR_ABSENT"
            scoped.append(
                {
                    "asset": asset,
                    "available_and_selected_options": len(symbols),
                    "empty_chain_is_blocked": not bool(symbols),
                    "coverage_state": view["state"],
                    "blocker": view["blocker"],
                    "leg_counts": {
                        f"{kind}_{state}": count for (kind, state), count in sorted(counts.items())
                    },
                }
            )
        assert bound_ids == set(metadata)
        assert members == {(item["asset"], item["symbol"]) for item in ack.payload()["instruments"]}
        required = len(native_channels)
        assert required == ack.payload()["channels"]
        progress = [
            row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIPTION_BATCH_ACK"
        ]
        assert len(progress) == (required + 99) // 100
        prior, completed = {key: 0 for key in PHASES}, 0
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
    seal = json.loads((SOURCE / "events.parquet.manifest.json").read_text())
    assert seal["sha256"] == before["events.parquet"] and seal["rows"] == len(rows)
    assert seal["bytes"] == (SOURCE / "events.parquet").stat().st_size
    REPLAY.mkdir(exist_ok=False)
    store = await DuckStore.open(REPLAY / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        system = await store.snapshot("SYSTEM", cutoff)
        for asset, view in zip(assets, captured["option_stream_coverage"], strict=True):
            saved = await store.snapshot(asset, cutoff)
            assert coverage_at([*saved.rows(), *system.rows()], asset, cutoff, age) == view
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
        "status": "FINITE_MULTI_ASSET_NATIVE_SUBSCRIPTION_AND_COVERAGE_VERIFIED",
        "assets": scoped,
        "saved_rows": len(rows),
        "pending_outbox_rows": len(rows),
        "required_and_acknowledged_channels": required,
        "batch_progress_records": len(progress),
        "fully_acknowledged_sessions": 1,
        "disconnects": 0,
        "full_ack_after_capture_start_ms": ack.observed_at_ms - captured["started_at_ms"],
        "full_ack_after_resync_ms": ack.observed_at_ms - start.observed_at_ms,
        "sql_control_pit_boundaries": boundaries,
        "ack_visibility_checks": 2 * len(assets),
        "independent_sql_latest_leg_checks": total_leg_checks,
        "metadata_bindings_checked": len(bound_ids),
        "freshness_age_bound_ms": age,
        "completed_subscription_receive_timing_ns": prior,
        "original_sha256": before,
        "source_bytes_unchanged": True,
        "closed_reload_replay_and_sealed_archive_verified": True,
        "new_network_requests": 0,
        "stale_bounds_not_relaxed": True,
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
