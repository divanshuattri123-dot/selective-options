"""Independent finite SQL proof of preserved latest identities/clocks and grouped history."""

import hashlib
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.audit_option_freshness import audit_capture  # noqa: E402


def digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main():
    output = ROOT / "evidence/option-freshness-01/independent-proof.json"
    if output.exists():
        raise FileExistsError("immutable proof already exists")
    findings = []
    for capture_name, assets, name in [
        ("reused-validation-public-01", ("BTC",), "btc"),
        ("multiasset-options-public-01", ("ETH", "SOL", "PAXG"), "eth-sol-paxg"),
    ]:
        capture = ROOT / "evidence" / capture_name
        preserved = json.loads(
            (ROOT / "evidence/option-freshness-01" / name / "report.json").read_text()
        )
        rerun = audit_capture(capture, assets, ROOT / "work/option-freshness-audit" / name)
        assert rerun == preserved
        for filename, expected in preserved["source_hashes"].items():
            assert digest(Path(filename)) == expected
        saved = json.loads((capture / "report.json").read_text())
        saved_views = {view["asset"]: view for view in saved["option_stream_coverage"]}
        with duckdb.connect(str(capture / "events.duckdb"), read_only=True) as db:
            for report in preserved["assets"]:
                asset, cutoff = report["asset"], report["as_of_ms"]
                assert report["coverage"] == saved_views[asset]
                if asset == "PAXG":
                    assert report["coverage"]["blocker"] == "OPTION_SELECTED_UNIVERSE_EMPTY"
                    assert not report["legs"] and report["selected_session"] is None
                    findings.append({"asset": asset, "blocker": report["coverage"]["blocker"]})
                    continue
                start = report["selected_session"]["resync_at_ms"]
                threshold = cutoff - report["max_leg_age_ms"]
                symbols = sorted({leg["symbol"] for leg in report["legs"]})
                eligible = """SELECT * FROM events WHERE source='deribit' AND environment='testnet'
                    AND asset=? AND kind IN ('ticker','book') AND event_time_ms<=?
                    AND observed_at_ms>=? AND observed_at_ms<=?
                    AND symbol IN (SELECT value ->> '$' FROM json_each(?))"""
                bindings = [asset, cutoff, start, cutoff, json.dumps(symbols)]
                native = db.execute(
                    "WITH eligible AS ("
                    + eligible
                    + """), newest_source AS (
                        SELECT symbol,kind,max(event_time_ms) AS event_time_ms
                        FROM eligible GROUP BY symbol,kind), newest_pair AS (
                        SELECT e.symbol,e.kind,e.event_time_ms,max(e.observed_at_ms) AS observed_at_ms
                        FROM eligible e JOIN newest_source s USING(symbol,kind,event_time_ms)
                        GROUP BY e.symbol,e.kind,e.event_time_ms)
                        SELECT e.symbol,e.kind,e.event_id,e.event_time_ms,e.observed_at_ms,
                            e.timestamp_kind,e.payload_json
                        FROM eligible e JOIN newest_pair p USING(symbol,kind,event_time_ms,observed_at_ms)
                        ORDER BY e.symbol,e.kind,e.event_id""",
                    bindings,
                ).fetchall()
                by_key = {}
                for symbol, kind, identity, source, receipt, timestamp_kind, payload in native:
                    body = json.loads(payload)
                    assert type(body["timestamp"]) is int and body["timestamp"] == source
                    by_key.setdefault((symbol, kind), []).append(
                        (
                            identity,
                            source,
                            receipt,
                            timestamp_kind,
                            hashlib.sha256(payload.encode()).hexdigest(),
                        )
                    )
                aggregate = db.execute(
                    "WITH eligible AS ("
                    + eligible
                    + """ ) SELECT symbol,kind,count(*),
                        count(DISTINCT event_time_ms),min(event_time_ms),max(event_time_ms),
                        min(observed_at_ms),max(observed_at_ms),
                        min(observed_at_ms-event_time_ms),max(observed_at_ms-event_time_ms),
                        sum(CASE WHEN observed_at_ms<event_time_ms THEN 1 ELSE 0 END),
                        sum(CASE WHEN event_time_ms>=? THEN 1 ELSE 0 END),
                        sum(CASE WHEN observed_at_ms>=? THEN 1 ELSE 0 END),
                        sum(CASE WHEN observed_at_ms>=? AND event_time_ms<? THEN 1 ELSE 0 END)
                        FROM eligible GROUP BY symbol,kind""",
                    [*bindings, threshold, threshold, threshold, threshold],
                ).fetchall()
                stats = {(row[0], row[1]): row[2:] for row in aggregate}
                keys = [
                    "stored_observation_count",
                    "distinct_native_timestamp_count",
                    "first_source_ms",
                    "last_source_ms",
                    "first_receipt_ms",
                    "last_receipt_ms",
                    "min_receipt_minus_source_ms",
                    "max_receipt_minus_source_ms",
                    "negative_difference_count",
                    "recent_source_count",
                    "recent_receipt_count",
                    "recent_receipts_with_old_source_count",
                ]
                assert (
                    sum(values[0] for values in stats.values())
                    == report["history_rows_scanned_for_statistics"]
                )
                assert len(native) == len(report["legs"])
                for leg in report["legs"]:
                    key = leg["symbol"], leg["kind"]
                    candidates = leg["latest_pair_candidates"]
                    assert [
                        (
                            r["event_id"],
                            r["event_time_ms"],
                            r["observed_at_ms"],
                            r["timestamp_kind"],
                            r["payload_sha256"],
                        )
                        for r in candidates
                    ] == by_key[key]
                    assert (
                        tuple(leg["selected_session_statistics"][key] for key in keys) == stats[key]
                    )
                    for row in candidates:
                        assert row["source_age_ms"] == cutoff - row["event_time_ms"]
                        assert row["receipt_age_ms"] == cutoff - row["observed_at_ms"]
                        assert (
                            row["receipt_minus_source_ms"]
                            == row["observed_at_ms"] - row["event_time_ms"]
                        )
                        assert row["negative_clock_difference"] == (
                            row["receipt_minus_source_ms"] < 0
                        )
                        assert (
                            row["native_timestamp_matches_event"]
                            and row["payload_json_object_valid"]
                        )
                        expected = (
                            "STALE"
                            if max(row["source_age_ms"], row["receipt_age_ms"]) > 30000
                            else "OBSERVED"
                        )
                        assert leg["canonical_state"] == expected
                        if expected == "STALE":
                            assert row["source_age_ms"] > 30000 and row["receipt_age_ms"] > 30000
                            assert leg["selected_session_statistics"]["recent_source_count"] == 0
                            assert leg["selected_session_statistics"]["recent_receipt_count"] == 0
                summaries = {}
                for kind in ("ticker", "book"):
                    legs = [leg for leg in report["legs"] if leg["kind"] == kind]
                    chosen = [leg["latest_pair_candidates"][0] for leg in legs]
                    summaries[kind] = {
                        "selected_legs": len(legs),
                        "stale_legs": sum(leg["canonical_state"] == "STALE" for leg in legs),
                        "source_age_range_ms": [
                            min(r["source_age_ms"] for r in chosen),
                            max(r["source_age_ms"] for r in chosen),
                        ],
                        "receipt_age_range_ms": [
                            min(r["receipt_age_ms"] for r in chosen),
                            max(r["receipt_age_ms"] for r in chosen),
                        ],
                        "selected_receipt_minus_source_range_ms": [
                            min(r["receipt_minus_source_ms"] for r in chosen),
                            max(r["receipt_minus_source_ms"] for r in chosen),
                        ],
                        "negative_selected_difference_count": sum(
                            r["negative_clock_difference"] for r in chosen
                        ),
                        "stored_selected_session_observations": sum(
                            leg["selected_session_statistics"]["stored_observation_count"]
                            for leg in legs
                        ),
                        "recent_receipts_with_old_source_count": sum(
                            leg["selected_session_statistics"][
                                "recent_receipts_with_old_source_count"
                            ]
                            for leg in legs
                        ),
                    }
                findings.append(
                    {"asset": asset, "independent_latest_rows": len(native), "by_kind": summaries}
                )
        for filename, expected in preserved["source_hashes"].items():
            assert digest(Path(filename)) == expected
    proof = {
        "latest_current_script_rerun_exact": True,
        "original_source_hashes_unchanged": True,
        "saved_coverage_exact": True,
        "independent_latest_identity_both_clock_native_provenance_and_statistics_exact": True,
        "assets": findings,
        "new_network_requests": 0,
        "orders_submitted": 0,
        "milestone_2_complete": False,
        "trading_enabled": False,
    }
    output.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    main()
