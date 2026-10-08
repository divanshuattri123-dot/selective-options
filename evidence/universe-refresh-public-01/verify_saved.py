"""Offline SQL/PIT and immutable archive checks for the finite capped public rotation."""

import hashlib
import json
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent
from selective_options.quality import FreshnessMonitor

ROOT = Path(__file__).resolve().parent


def verify():
    original = hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest()
    report = json.loads((ROOT / "report.json").read_text())
    runtime = json.loads((ROOT / "verification.json").read_text())
    with duckdb.connect(str(ROOT / "events.duckdb"), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY observed_at_ms, rowid")
        columns = [c[0] for c in result.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        assert len(rows) == report["rows"] == runtime["saved_rows"] == 4286
        assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
            0
        ] == len(rows)
        controls = [
            row
            for row in rows
            if row.kind == "status"
            and row.payload().get("reason")
            in {"STREAM_RESYNC", "STREAM_SUBSCRIBED", "STREAM_REFRESH_DUE"}
        ]
        assert [row.payload()["reason"] for row in controls] == [
            "STREAM_RESYNC",
            "STREAM_SUBSCRIBED",
            "STREAM_REFRESH_DUE",
            "STREAM_RESYNC",
            "STREAM_SUBSCRIBED",
        ]
        assert [row.payload()["connection"] for row in controls] == [1, 1, 1, 2, 2]
        assert controls[2].observed_at_ms - controls[1].observed_at_ms >= 60000
        boundaries = 0
        for row in controls:
            for cutoff, expected in ((row.observed_at_ms - 1, 0), (row.observed_at_ms, 1)):
                found = db.execute(
                    "SELECT count(*) FROM events WHERE event_id = ? AND event_time_ms <= ? AND observed_at_ms <= ?",
                    [row.event_id, cutoff, cutoff],
                ).fetchone()[0]
                assert found == expected
                boundaries += 1

    def view(cutoff):
        monitor = FreshnessMonitor(5000)
        for row in rows:
            if row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff:
                monitor.observe(row)
        return monitor, monitor.option_freshness(cutoff, 300000)

    due = controls[2].observed_at_ms
    before_monitor, before = view(due - 1)
    after_monitor, after = view(due)
    assert before_monitor.stream_state == "STREAM_SUBSCRIBED"
    assert after_monitor.stream_state == "STREAM_REFRESH_DUE"
    assert before and len(after) == 4 and all(row["state"] == "STREAM_INACTIVE" for row in after)
    second_start = controls[3].observed_at_ms
    acknowledged = controls[4].observed_at_ms
    second_monitor, second = view(acknowledged)
    assert second_monitor.stream_state == "STREAM_SUBSCRIBED"
    for _asset, symbol in second_monitor.expected_options:
        for kind in ("ticker", "book"):
            row = second_monitor.by_instrument.get(("deribit", symbol, kind))
            assert row is None or row.observed_at_ms >= second_start
    with duckdb.connect() as db:
        result = db.execute("SELECT * FROM read_parquet(?)", [str(ROOT / "events.parquet")])
        columns = [c[0] for c in result.description]
        restored = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        assert set(rows) == set(restored)
    assert (
        hashlib.sha256((ROOT / "events.parquet").read_bytes()).hexdigest()
        == report["parquet_sha256"]
    )
    assert hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest() == original
    proof = {
        "status": "SAVED_PUBLIC_LIFECYCLE_PIT_AND_ARCHIVE_VERIFIED",
        "new_public_requests": 0,
        "saved_rows": len(rows),
        "pending_outbox_rows": len(rows),
        "sql_control_pit_boundaries": boundaries,
        "stream_controls_match": True,
        "actual_refresh_after_ms": controls[2].observed_at_ms - controls[1].observed_at_ms,
        "inactive_leg_checks": len(after),
        "second_session_precludes_previous_session_cached_quotes": True,
        "exact_archive_and_closed_reload": True,
        "source_database_unchanged": True,
        "source_database_sha256": original,
        "stream_cap_per_asset": 2,
        "listing_or_expiry_change_proven": False,
        "trade_history_continuity_verified": False,
        "full_universe_stream_verified": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "independent-proof.json").write_text(
        json.dumps(proof, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    verify()
