"""Independent read-only native/archive audit; reconstructed frames are not original bytes."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb

from selective_options.events import MarketEvent

ROOT = Path.cwd()
PROOF = ROOT / "evidence/spot-batch-saved-public-01"
OUTPUT = ROOT / "evidence/spot-batch-parent-validation-0344-02.json"


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def hashes(path):
    return {
        p.relative_to(path).as_posix(): digest(p) for p in sorted(path.rglob("*")) if p.is_file()
    }


def events(conn, sql, params=None):
    cursor = conn.execute(sql, params or [])
    names = [column[0] for column in cursor.description]
    return [
        MarketEvent.model_validate(dict(zip(names, row, strict=True)), strict=True)
        for row in cursor.fetchall()
    ]


def pit(conn, rows):
    for row in rows:
        boundary = max(row.event_time_ms, row.observed_at_ms)
        for at, count in ((boundary - 1, 0), (boundary, 1)):
            assert conn.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row.event_id, at, at],
            ).fetchone() == (count,)
    return 2 * len(rows)


def audit_archive(conn, path, rows):
    manifest = read(path.with_suffix(path.suffix + ".manifest.json"))
    assert manifest["sha256"] == digest(path) and manifest["bytes"] == path.stat().st_size
    assert manifest["rows"] == len(rows)
    assert (
        events(
            conn,
            "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
            [str(path)],
        )
        == rows
    )


def audit_outbox(conn, rows):
    expected = sorted(row.event_id for row in rows)
    assert conn.execute("SELECT event_id FROM delivery_outbox ORDER BY event_id").fetchall() == [
        (identifier,) for identifier in expected
    ]
    assert conn.execute("SELECT count(*) FROM delivery_outbox WHERE delivered").fetchone() == (0,)


def audit_bar(bar, input_rows):
    payload = bar.payload()
    opening, closing = payload["open_time_ms"], payload["close_time_ms"]
    members = [row for row in input_rows if opening <= row.event_time_ms < closing]
    assert members
    members.sort(key=lambda row: (row.event_time_ms, row.payload()["trade_id"]))
    prices = [row.payload()["price"] for row in members]
    assert payload["open"] == prices[0] and payload["close"] == prices[-1]
    assert payload["high"] == max(prices) and payload["low"] == min(prices)
    assert payload["trade_count"] == len(members)
    assert payload["volume"] == float(
        sum((Decimal(str(row.payload()["qty"])) for row in members), Decimal(0))
    )
    assert payload["closed"] is True and payload["volume_unit"] == "base_asset"
    assert bar.event_time_ms == closing <= bar.observed_at_ms
    identity = json.dumps(
        [
            bar.source,
            bar.environment,
            bar.asset,
            bar.symbol,
            "bar",
            None,
            bar.event_time_ms,
            bar.payload_json,
        ],
        separators=(",", ":"),
    )
    assert hashlib.sha256(identity.encode()).hexdigest() == bar.event_id


def baseline_check():
    baseline = read((ROOT / "../../work/spot-native-baseline-0344.json").resolve())
    for name, expected in baseline["previous_proof_and_native_files"].items():
        assert hashes(ROOT / "evidence" / name) == expected
    for name, expected in baseline["unchanged_source_sha256"].items():
        assert digest(ROOT / name) == expected
    assert digest(ROOT.parent / "selective-options-0.3.43.zip") == baseline["previous_zip_sha256"]
    return baseline


def main():
    assert not OUTPUT.exists()
    baseline = baseline_check()
    before = hashes(PROOF)
    report = read(PROOF / "report.json")
    assert report["status"] == "OFFLINE_SAVED_SPOT_BATCH_STAGING_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    assert report["saved_native_trade_rows"] == report["replayed_native_trade_rows"] == 55
    for field in (
        "original_websocket_frames_available",
        "original_frame_boundaries_verified",
        "continuity_across_native_gap_claimed",
        "redis_delivery_claimed",
        "current_feed_health_verified",
        "complete_history_verified",
        "seven_day_validation_complete",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert report[field] is False
    assert report["consumer_queue_drained"] is True
    assert report["source_native_gap_trade_id"] == 109230677
    assert (
        report["source_hashes_before"]
        == report["source_hashes_after"]
        == hashes(ROOT / "evidence/live-gap-02")
    )
    assert report["code_sha256_before"] == report["code_sha256_after"]
    for name, expected in report["code_sha256_before"].items():
        assert digest(ROOT / "src/selective_options" / name) == expected
    assert (
        report["verifier_sha256"]
        == digest(ROOT / "scripts/verify_spot_batch.py")
        == digest(PROOF / "verification-script.py")
    )
    source = ROOT / "evidence/live-gap-02/events.duckdb"
    source_digest = digest(source)
    with duckdb.connect(str(source), read_only=True) as conn:
        native = events(
            conn,
            "SELECT * FROM events WHERE source='kraken' AND environment='public_reference' AND asset='BTC' AND symbol='BTC/USD' AND kind='trade' ORDER BY event_id",
        )
    assert len(native) == 55 and digest(source) == source_digest
    native_map = {row.event_id: row for row in native}
    native_ids = sorted(row.payload()["trade_id"] for row in native)
    assert native_ids == [*range(109230627, 109230677), *range(109230678, 109230683)]
    path = PROOF / "events.duckdb"
    db_digest = digest(path)
    with duckdb.connect(str(path), read_only=True) as conn:
        rows = events(conn, "SELECT * FROM events ORDER BY event_id")
        by_id = {row.event_id: row for row in rows}
        assert (
            len(rows) == len(by_id) == report["persisted_rows"] == report["pending_delivery_rows"]
        )
        assert [row.event_id for row in rows] == report["persisted_event_ids"]
        assert [row for row in rows if row.event_id in native_map] == native
        assert not any(row.source == "kraken_rest" for row in rows)
        audit_archive(conn, PROOF / "events.parquet", rows)
        audit_outbox(conn, rows)
        pit_checks = pit(conn, rows)
        for check in report["pit_checks"]:
            at = check["as_of_ms"]
            ids = [
                row.event_id for row in rows if row.event_time_ms <= at and row.observed_at_ms <= at
            ]
            assert ids == check["event_ids"] and len(ids) == check["available_rows"]
            assert conn.execute(
                "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                [at, at],
            ).fetchall() == [(identifier,) for identifier in ids]
        rejected = report["synthetic_rejected_batch"]
        assert rejected["emitted_rows"] == 0 and rejected["database_unchanged"] is True
        assert rejected["state_sha256_before"] == rejected["state_sha256_after"]
        assert "LATE_TRADE" in rejected["error"]
        assert rejected["candidate_event_ids"][1] not in by_id
        assert rejected["candidate_event_ids"][0] in by_id
        derived_ids = set(report["derived_event_provenance"])
        assert set(by_id) == set(native_map) | derived_ids
        assert set(native_map).isdisjoint(derived_ids)
        all_inputs = []
        all_bars = []
        synthetic_inputs = []
        synthetic_bars = []
        for index, cohort in enumerate(report["cohorts"]):
            assert cohort["cohort"] == index and cohort["native_rows"] == (50 if index == 0 else 5)
            assert (
                cohort["continuity_across_cohorts_claimed"] is False
                and cohort["overlap_reemitted_rows"] == 0
            )
            assert cohort["timeframe_s"] == 300
            input_rows = [
                native_map[identifier] for identifier in cohort["accepted_input_event_ids"]
            ]
            assert len(input_rows) == cohort["native_rows"]
            assert [row.payload()["trade_id"] for row in input_rows] == list(
                range(cohort["first_trade_id"], cohort["last_trade_id"] + 1)
            )
            assert cohort["coverage_start_ms"] == min(row.observed_at_ms for row in input_rows)
            all_inputs.extend(cohort["accepted_input_event_ids"])
            for identifier in cohort["derived_bar_event_ids"]:
                bar = by_id[identifier]
                audit_bar(bar, input_rows)
                assert bar.observed_at_ms == cohort["flush_observed_at_ms"]
                assert bar.payload()["timeframe_s"] == 300 and bar.payload()["lateness_ms"] == 0
                assert bar.payload()["coverage_start_ms"] == cohort["coverage_start_ms"]
                assert bar.payload()["complete"] == (
                    bar.payload()["open_time_ms"] >= cohort["coverage_start_ms"]
                )
                all_bars.append(identifier)
            added = [
                by_id[identifier]
                for identifier in cohort.get("synthetic_accepted_trade_event_ids", [])
            ]
            synthetic_inputs.extend(row.event_id for row in added)
            for identifier in cohort.get("synthetic_derived_bar_event_ids", []):
                bar = by_id[identifier]
                audit_bar(bar, added)
                assert bar.observed_at_ms == cohort["synthetic_flush_observed_at_ms"]
                synthetic_bars.append(identifier)
        assert len(all_inputs) == len(set(all_inputs)) == 55 and set(all_inputs) == set(native_map)
        assert len(synthetic_inputs) == len(synthetic_bars) == 1
        assert synthetic_inputs == [rejected["candidate_event_ids"][0]]
        assert set(all_bars + synthetic_bars) == {row.event_id for row in rows if row.kind == "bar"}
        container_ids = []
        for container in report["reconstructed_containers"]:
            assert (
                container["original_frame_bytes_available"]
                is container["original_frame_boundaries_verified"]
                is False
            )
            expected = [native_map[identifier] for identifier in container["event_ids"]]
            container_ids.extend(container["event_ids"])
            wire = {item["trade_id"]: item for item in container["body"]["data"]}
            assert len(wire) == len(expected)
            for row in expected:
                body = row.payload()
                entry = wire[body["trade_id"]]
                assert row.observed_at_ms == container["received_at_ms"]
                assert entry == {
                    "symbol": row.symbol,
                    "price": body["price"],
                    "qty": body["qty"],
                    "side": body["side"],
                    "trade_id": body["trade_id"],
                    "timestamp": body["source_timestamp"],
                }
        assert len(container_ids) == len(set(container_ids)) == 55 and set(container_ids) == set(
            native_map
        )
    assert digest(path) == db_digest and hashes(PROOF) == before
    sink_dir = ROOT / "evidence/spot-sink-failure-fixture-0344-02"
    sink_before = hashes(sink_dir)
    sink = read(sink_dir / "report.json")
    assert sink["status"] == "SYNTHETIC_CAPTURE_SINK_FAILSTOP_VERIFIED"
    assert sink["verifier_sha256"] == digest(sink_dir / "verification-script.py")
    assert digest(sink_dir / "capture/events.duckdb") == sink["database_sha256"]
    assert (
        sink["websocket_connections"] == 1
        and sink["collector_state_unchanged"] is sink["collector_reuse_refused"] is True
    )
    capture = read(sink_dir / "capture/report.json")
    assert capture["status"] == "FAILED" and capture["failure_type"] == "IntegrityError"
    assert capture["trading_enabled"] is capture["milestone_2_complete"] is False
    with duckdb.connect(str(sink_dir / "capture/events.duckdb"), read_only=True) as conn:
        sink_rows = events(conn, "SELECT * FROM events ORDER BY event_id")
        assert len(sink_rows) == sink["rows"] == 3
        assert [row.payload()["trade_id"] for row in sink_rows if row.kind == "trade"] == [1]
        assert not any(row.kind == "bar" for row in sink_rows)
        assert any(row.payload().get("detail") == "SPOT_EMISSION_FAILED" for row in sink_rows)
        audit_archive(conn, sink_dir / "capture/events.parquet", sink_rows)
        audit_outbox(conn, sink_rows)
        sink_pit = pit(conn, sink_rows)
    assert hashes(sink_dir) == sink_before
    sources = {
        p.relative_to(ROOT).as_posix(): digest(p)
        for p in sorted((ROOT / "src/selective_options").glob("*.py"))
    }
    assert sources == sink["tested_source_sha256"] and len(sources) == 63
    assert (
        sources["src/selective_options/spot_trades.py"]
        == "b7ca0845aabdfccf8a80da5a643b3f2ebed79b83342e28eef97b0d5ea03b680c"
    )
    assert baseline_check() == baseline
    result = {
        "status": "INDEPENDENT_SPOT_BATCH_NATIVE_VERIFIED",
        "version": "0.3.44",
        "new_public_requests": 0,
        "saved_native_trades": 55,
        "native_cohort_rows": [50, 5],
        "source_native_gap_trade_id": 109230677,
        "source_native_gap_continuity_claimed": False,
        "original_frames_available": False,
        "reconstructed_native_containers": len(report["reconstructed_containers"]),
        "native_derived_bars": len(all_bars),
        "synthetic_accepted_trades": len(synthetic_inputs),
        "synthetic_derived_bars": len(synthetic_bars),
        "persisted_records": len(rows),
        "per_record_sql_pit_checks": pit_checks + sink_pit,
        "complete_sql_cutoffs": len(report["pit_checks"]),
        "native_and_fixture_archives_exact": True,
        "pending_outbox_membership_exact": True,
        "bar_ohlc_decimal_volume_counts_verified": True,
        "native_rows_exact_with_original_receipts": True,
        "frozen_native_proof_sha256": digest(PROOF / "report.json"),
        "frozen_sink_proof_sha256": digest(sink_dir / "report.json"),
        "previous_zip_sha256": baseline["previous_zip_sha256"],
        "previous_proof_native_files_unchanged": True,
        "unchanged_production_modules": len(baseline["unchanged_source_sha256"]),
        "tested_source_sha256": sources,
        "parent_verifier_sha256": digest(Path(__file__)),
        "milestone": 2,
        "milestone_status": "PARTIAL",
        "trading_enabled": False,
    }
    OUTPUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps({key: value for key, value in result.items() if key != "tested_source_sha256"})
    )


if __name__ == "__main__":
    main()
