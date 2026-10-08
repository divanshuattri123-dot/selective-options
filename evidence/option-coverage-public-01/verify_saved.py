"""Read-only original evidence and exact offline replay; zero HTTP/WebSocket calls."""

import asyncio
import hashlib
import json
from collections import Counter
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent
from selective_options.models import Instrument
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore

SOURCE = Path(__file__).resolve().parent
REPLAY = SOURCE.parent / "option-coverage-replay-01"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def main():
    original_paths = [
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
    before = {path.name: digest(path) for path in original_paths}
    captured = json.loads((SOURCE / "report.json").read_text())
    with duckdb.connect(str(SOURCE / "events.duckdb"), read_only=True) as db:
        query = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in query.description]
        native = query.fetchall()
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in native
        )
        archived = db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(SOURCE / "events.parquet")]
        ).fetchall()
        assert native == archived
        assert len(rows) == captured["rows"] == 21445
        assert len({row.event_id for row in rows}) == len(rows)
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(rows)
        controls = sorted(
            (row for row in rows if row.kind == "status"), key=lambda row: row.observed_at_ms
        )
        boundaries = 0
        for control in controls:
            for cutoff, expected in ((control.observed_at_ms - 1, 0), (control.observed_at_ms, 1)):
                assert (
                    db.execute(
                        "SELECT count(*) FROM events WHERE event_id = ? AND event_time_ms <= ? AND observed_at_ms <= ?",
                        [control.event_id, cutoff, cutoff],
                    ).fetchone()[0]
                    == expected
                )
                boundaries += 1
        discovered, start, disconnected = controls
        assert [row.payload()["reason"] for row in controls] == [
            "UNIVERSE_DISCOVERED",
            "STREAM_RESYNC",
            "STREAM_DISCONNECTED",
        ]
        manifest = discovered.payload()
        assert manifest["available"] == manifest["selected"] == 1010
        assert manifest["coverage"] == "FULL"
        assert set(manifest["instruments"]) == set(manifest["selected_instruments"])
        metadata = {row.event_id: row for row in rows if row.kind == "instrument"}
        assert len(metadata) == 1010
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
        assert disconnected.payload()["error_type"] == "TimeoutError"
        assert not any(row.payload().get("reason") == "STREAM_SUBSCRIBED" for row in controls)
        ended = captured["ended_at_ms"]
        saved_view = coverage_at(rows, "BTC", ended, 30000)
        assert saved_view == captured["option_stream_coverage"][0]
        assert saved_view["blocker"] == "OPTION_STREAM_INACTIVE"
        for cutoff in (
            start.observed_at_ms,
            disconnected.observed_at_ms - 1,
            disconnected.observed_at_ms,
            ended,
        ):
            assert coverage_at(rows, "BTC", cutoff, 30000)["blocker"] == "OPTION_STREAM_INACTIVE"
        observed_symbols = {
            kind: len({row.symbol for row in rows if row.kind == kind})
            for kind in ("ticker", "book")
        }
        assert all(
            row.symbol in set(manifest["instruments"])
            for row in rows
            if row.kind in {"ticker", "book"}
        )
    seal = json.loads((SOURCE / "events.parquet.manifest.json").read_text())
    assert seal["sha256"] == before["events.parquet"] and seal["rows"] == len(rows)
    assert seal["bytes"] == (SOURCE / "events.parquet").stat().st_size
    REPLAY.mkdir(exist_ok=False)
    store = await DuckStore.open(REPLAY / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)
        snap = await store.snapshot("BTC", ended)
        system = await store.snapshot("SYSTEM", ended)
        assert coverage_at([*snap.rows(), *system.rows()], "BTC", ended, 30000) == saved_view
        assert await store.export_parquet(REPLAY / "events.parquet") == len(rows)
    finally:
        await store.close()
    with duckdb.connect(str(REPLAY / "events.duckdb"), read_only=True) as db:
        assert db.execute("SELECT * FROM events ORDER BY event_id").fetchall() == native
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(rows)
    after = {path.name: digest(path) for path in original_paths}
    assert before == after
    report = {
        "status": "SAVED_EVIDENCE_VERIFIED_UNCAPPED_ACKNOWLEDGEMENT_NOT_OBSERVED",
        "saved_rows": len(rows),
        "pending_outbox_rows": len(rows),
        "rows_by_kind": dict(Counter(row.kind for row in rows)),
        "discovered_and_selected_options": 1010,
        "distinct_symbols_with_any_saved_observation": observed_symbols,
        "sql_control_pit_boundaries": boundaries,
        "inactive_view_checks": 4,
        "acknowledged_sessions": 0,
        "stream_failure": "TimeoutError",
        "failure_cause_or_exact_batch": "NOT_ESTABLISHED",
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
            {key: value for key, value in report.items() if key != "original_sha256"}, indent=2
        )
    )


asyncio.run(main())
