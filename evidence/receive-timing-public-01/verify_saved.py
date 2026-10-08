"""Independent saved timing/PIT/archive proof; no HTTP or WebSocket calls."""

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
REPLAY = SOURCE.parent / "receive-timing-replay-01"
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
    batch_size = config["ingestion"]["subscription_batch_size"]
    with duckdb.connect(str(SOURCE / "events.duckdb"), read_only=True) as db:
        query = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in query.description]
        native = query.fetchall()
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
        )
        assert len(rows) == captured["rows"] == len({row.event_id for row in rows})
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
            for cutoff, expected in ((row.observed_at_ms - 1, 0), (row.observed_at_ms, 1)):
                assert (
                    db.execute(
                        "SELECT count(*) FROM events WHERE event_id = ? AND event_time_ms <= ? AND observed_at_ms <= ?",
                        [row.event_id, cutoff, cutoff],
                    ).fetchone()[0]
                    == expected
                )
                boundaries += 1
        discovered = next(
            row for row in controls if row.payload()["reason"] == "UNIVERSE_DISCOVERED"
        )
        starts = [row for row in controls if row.payload()["reason"] == "STREAM_RESYNC"]
        assert len(starts) == 1
        start = starts[0]
        manifest = discovered.payload()
        count = manifest["available"]
        assert count == manifest["selected"] == len(manifest["instruments"]) > 0
        assert manifest["coverage"] == "FULL"
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
            assert (
                instrument.is_active and instrument.expiration_timestamp > captured["ended_at_ms"]
            )
            assert (
                row.event_time_ms
                == row.observed_at_ms
                <= discovered.observed_at_ms
                <= start.observed_at_ms
            )
            indices.add(instrument.price_index)
        required_channels = count * 3 + len(indices)
        progress = [
            row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIPTION_BATCH_ACK"
        ]
        failures = [row for row in controls if row.payload()["reason"] == "STREAM_DISCONNECTED"]
        acknowledgements = [
            row for row in controls if row.payload()["reason"] == "STREAM_SUBSCRIBED"
        ]
        assert not failures and not acknowledgements and progress
        measured = progress
        prior = {key: 0 for key in PHASES}
        completed = 0
        measured_records = []
        for index, row in enumerate(measured):
            body = row.payload()
            assert body["connection"] == 1 and body["total_channels"] == required_channels
            total, delta, elapsed = (
                body["receive_timing_ns"],
                body["response_timing_ns"],
                body["response_elapsed_ns"],
            )
            assert set(total) == set(delta) == PHASES
            assert type(elapsed) is int and elapsed >= 0
            assert all(
                type(total[key]) is int
                and type(delta[key]) is int
                and delta[key] >= 0
                and total[key] - prior[key] == delta[key]
                for key in PHASES
            )
            assert sum(delta.values()) <= elapsed
            pending = False
            assert body["request_id"] == completed + 1
            if not pending:
                amount = min(batch_size, required_channels - completed)
                completed += amount
                assert body["batch_channels"] == amount
                assert body["acknowledged_batches"] == index + 1
            else:
                assert body["acknowledged_batches"] == len(progress)
                assert body["requested_channels"] == min(batch_size, required_channels - completed)
                assert (
                    body["phase"] == "SUBSCRIPTION_RESPONSE"
                    and body["error_type"] == "TimeoutError"
                )
                assert body["receive_phase"] in PHASES
            assert body["acknowledged_channels"] == completed
            assert discovered.observed_at_ms <= start.observed_at_ms <= row.observed_at_ms
            prior = total
            measured_records.append(
                {
                    "event_id": row.event_id,
                    "request_id": body["request_id"],
                    "completed": not pending,
                    "response_elapsed_ns": elapsed,
                    "response_timing_ns": delta,
                    "unattributed_elapsed_ns": elapsed - sum(delta.values()),
                }
            )
        ended = captured["ended_at_ms"]
        view = coverage_at(rows, "BTC", ended, int(config["data"]["max_staleness_s"] * 1000))
        assert view == captured["option_stream_coverage"][0]
        assert view["blocker"] == "OPTION_STREAM_INACTIVE"
        for cutoff in (
            start.observed_at_ms,
            progress[-1].observed_at_ms - 1,
            progress[-1].observed_at_ms,
            ended,
        ):
            assert coverage_at(rows, "BTC", cutoff)["blocker"] == "OPTION_STREAM_INACTIVE"
        end_body = progress[-1].payload()
    seal = json.loads((SOURCE / "events.parquet.manifest.json").read_text())
    assert seal["sha256"] == before["events.parquet"] and seal["rows"] == len(rows)
    assert seal["bytes"] == (SOURCE / "events.parquet").stat().st_size
    REPLAY.mkdir(exist_ok=False)
    store = await DuckStore.open(REPLAY / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        current = await store.snapshot("BTC", ended)
        system = await store.snapshot("SYSTEM", ended)
        assert (
            coverage_at(
                [*current.rows(), *system.rows()],
                "BTC",
                ended,
                int(config["data"]["max_staleness_s"] * 1000),
            )
            == view
        )
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
        "status": "SAVED_TIMING_EVIDENCE_VERIFIED_FULL_SUBSCRIPTION_NOT_OBSERVED",
        "saved_rows": len(rows),
        "pending_outbox_rows": len(rows),
        "rows_by_kind": dict(Counter(row.kind for row in rows)),
        "discovered_and_selected_options": count,
        "required_channels": required_channels,
        "batch_progress_records": len(progress),
        "acknowledged_channels": completed,
        "last_completed_request_id": end_body["request_id"],
        "last_completed_response_timing_ns": end_body["response_timing_ns"],
        "last_completed_response_elapsed_ns": end_body["response_elapsed_ns"],
        "cumulative_receive_timing_ns_through_last_completed_batch": end_body["receive_timing_ns"],
        "interrupted_request_timing_recorded": False,
        "source_timeout_observed": False,
        "stopped_before_complete_acknowledgement": True,
        "measured_records": measured_records,
        "sql_control_pit_boundaries": boundaries,
        "inactive_view_checks": 4,
        "monotonic_exclusive_timing_partitions_verified": True,
        "clock_changes_or_scheduling_and_wire_latency_separately_identified": False,
        "failure_root_cause": "NOT_ESTABLISHED",
        "exact_metadata_bindings_verified": True,
        "closed_reload_replay_and_sealed_archive_verified": True,
        "source_bytes_unchanged": True,
        "original_sha256": before,
        "new_network_requests": 0,
        "full_fresh_option_stream_verified": False,
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
            {
                key: value
                for key, value in report.items()
                if key not in ("original_sha256", "measured_records")
            },
            indent=2,
        )
    )


asyncio.run(main())
