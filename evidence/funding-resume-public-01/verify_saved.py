"""Independent read-only SQL verification; no network or collector/parser calls."""

import hashlib
import json
import zipfile
from pathlib import Path

import duckdb

root = Path(__file__).resolve().parents[2]
folder = Path(__file__).resolve().parent
source = root / "evidence/binance-funding-public-01/events.duckdb"
source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
with zipfile.ZipFile(root.parent / "selective-options-0.3.25.zip") as archive:
    preserved = archive.read("selective-options/evidence/binance-funding-public-01/events.duckdb")
assert hashlib.sha256(preserved).hexdigest() == source_hash
report = json.loads((folder / "report.json").read_text())
responses = json.loads((folder / "responses.json").read_text())
with duckdb.connect(str(source), read_only=True) as db:
    original = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
    seeds = db.execute(
        "SELECT * FROM events WHERE event_time_ms BETWEEN ? AND ? AND observed_at_ms<=? ORDER BY event_id",
        [report["start_ms"], report["end_ms"], report["end_ms"]],
    ).fetchall()
with duckdb.connect(str(folder / "events.duckdb"), read_only=True) as db:
    actual = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
    by_id = {row[0]: row for row in actual}
    assert all(by_id[row[0]] == row for row in seeds)
    assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[
        0
    ] == len(actual)
    fresh = [row for row in actual if row[0] not in {seed[0] for seed in seeds}]
    raw = [
        (item, response["observed_at_ms"])
        for response in responses
        if response["endpoint"] == "fundingRate"
        for item in response["response"]
    ]
    assert len(fresh) == len(raw) == 6
    for row in fresh:
        payload = json.loads(row[10])
        item, receipt = next(
            (item, receipt) for item, receipt in raw if item["fundingTime"] == row[6]
        )
        assert row[7] == receipt == payload["observation_received_at_ms"]
        assert payload["funding_rate"] == item["fundingRate"]
        assert payload["mark_price_at_charge"] == item["markPrice"]
        assert payload["reported_rate_type"] == item.get("rateType")
        assert (
            db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row[0], receipt - 1, receipt - 1],
            ).fetchone()[0]
            == 0
        )
        assert (
            db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row[0], receipt, receipt],
            ).fetchone()[0]
            == 1
        )
    selected = db.execute(
        "SELECT event_id FROM events QUALIFY row_number() OVER (PARTITION BY event_time_ms ORDER BY observed_at_ms DESC)=1 ORDER BY event_time_ms"
    ).fetchall()
    assert [r[0] for r in selected] == report["funding_revision_view"]["selected_event_ids"]
    archived = db.execute(
        "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(folder / "events.parquet")]
    ).fetchall()
    assert actual == archived
assert len(original) == 6 and len(seeds) == 5 and len(actual) == 11
assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
assert report["seed_committed_reload_verified"] is True
result = dict(
    source_sha256=source_hash,
    original_source_rows=6,
    seed_rows_preserved_exactly=5,
    outside_window_rows_preserved_in_original=1,
    fresh_public_observations=6,
    combined_rows=11,
    selected_charges=6,
    independent_pit_boundaries=12,
    original_release_bytes_unchanged=True,
    archive_exact_match=True,
    outbox_count_matches=True,
    new_network_requests_by_this_verifier=0,
    schedule_completeness_proven=False,
    trading_enabled=False,
)
(folder / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps(result, indent=2))
