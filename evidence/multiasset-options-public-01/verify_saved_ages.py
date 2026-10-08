"""Exact saved source/receipt age audit; no new data or invented update timestamps."""

import hashlib
import json
from collections import Counter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "age-proof.json"
assert not OUTPUT.exists()
files = [ROOT / name for name in ("events.duckdb", "report.json", "verification.json")]
original = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
capture = json.loads((ROOT / "report.json").read_text())
cutoff, started = capture["ended_at_ms"], capture["started_at_ms"]
report = []
with duckdb.connect(str(ROOT / "events.duckdb"), read_only=True) as db:
    for view in capture["option_stream_coverage"]:
        if "legs" not in view:
            report.append({"asset": view["asset"], "blocker": view["blocker"], "selected_legs": 0})
            continue
        for kind in ("ticker", "book"):
            selected = [leg for leg in view["legs"] if leg["kind"] == kind]
            source_ages, receipt_ages, latency = [], [], []
            causes, types = Counter(), Counter()
            source_before_capture, receipt_before_capture = 0, 0
            for leg in selected:
                assert leg["event_id"] is not None
                row = db.execute(
                    "SELECT asset,symbol,kind,event_time_ms,observed_at_ms,payload_json FROM events WHERE event_id=?",
                    [leg["event_id"]],
                ).fetchone()
                assert row is not None and row[:3] == (view["asset"], leg["symbol"], kind)
                source, receipt = row[3:5]
                assert source <= cutoff and receipt <= cutoff
                source_age, receipt_age = cutoff - source, cutoff - receipt
                expected = "STALE" if source_age > 30000 or receipt_age > 30000 else "OBSERVED"
                assert leg["state"] == expected
                if expected == "STALE":
                    causes[
                        "BOTH_AGES"
                        if source_age > 30000 and receipt_age > 30000
                        else "SOURCE_AGE_ONLY"
                        if source_age > 30000
                        else "RECEIPT_AGE_ONLY"
                    ] += 1
                source_ages.append(source_age)
                receipt_ages.append(receipt_age)
                latency.append(receipt - source)
                source_before_capture += int(source < started)
                receipt_before_capture += int(receipt < started)
                native = json.loads(row[5])
                assert native["timestamp"] == source
                types[str(native.get("type", "ABSENT"))] += 1
            assert len(source_ages) == len(selected)
            report.append(
                {
                    "asset": view["asset"],
                    "kind": kind,
                    "selected_legs": len(selected),
                    "age_failures": dict(causes),
                    "native_type_field_counts": dict(types),
                    "source_age_min_max_ms": [min(source_ages), max(source_ages)],
                    "receipt_age_min_max_ms": [min(receipt_ages), max(receipt_ages)],
                    "receipt_minus_source_min_max_ms": [min(latency), max(latency)],
                    "source_timestamp_before_capture": source_before_capture,
                    "receipt_before_capture": receipt_before_capture,
                }
            )
assert original == {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
result = {
    "status": "EXACT_SAVED_SOURCE_AND_RECEIPT_AGES_VERIFIED",
    "as_of_ms": cutoff,
    "freshness_age_bound_ms": 30000,
    "legs": report,
    "original_sha256": original,
    "new_network_requests": 0,
    "original_bytes_unchanged": True,
    "timestamps_or_bounds_relaxed": False,
    "underlying_age_failure_cause_established": False,
    "trading_approval": False,
}
OUTPUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(
    json.dumps({key: value for key, value in result.items() if key != "original_sha256"}, indent=2)
)
