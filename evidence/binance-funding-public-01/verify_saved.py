"""Independent saved-response/SQL/archive verification; no collector, parser or network."""

import hashlib
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
responses = json.loads((ROOT / "responses.json").read_text(encoding="utf-8"))
report = json.loads((ROOT / "report.json").read_text(encoding="utf-8"))
assert report["status"] == "PUBLIC_SETTLEMENT_ARCHIVE_VERIFIED"
assert len(responses) == report["public_requests_attempted"] == 5
assert responses[0]["endpoint"] == "exchangeInfo" and responses[0]["params"] == {}
selected = [r for r in responses[0]["response"]["symbols"] if r["symbol"] == "BTCUSDT"]
assert len(selected) == 1
instrument = {
    k: selected[0][k]
    for k in ["symbol", "baseAsset", "quoteAsset", "marginAsset", "contractType", "status"]
}
assert instrument == {
    "symbol": "BTCUSDT",
    "baseAsset": "BTC",
    "quoteAsset": "USDT",
    "marginAsset": "USDT",
    "contractType": "PERPETUAL",
    "status": "TRADING",
}
cursor = report["start_ms"]
expected = {}
prior_receipt = responses[0]["observed_at_ms"]
for page in responses[1:]:
    assert page["endpoint"] == "fundingRate"
    assert page["params"] == {
        "symbol": "BTCUSDT",
        "startTime": str(cursor),
        "endTime": str(report["end_ms"]),
        "limit": "2",
    }
    receipt = page["observed_at_ms"]
    assert receipt >= prior_receipt
    prior_receipt = receipt
    for raw in page["response"]:
        ts = raw["fundingTime"]
        assert cursor <= ts <= min(report["end_ms"], receipt)
        assert ts not in expected and raw["symbol"] == "BTCUSDT"
        body = {
            "funding_rate": raw["fundingRate"],
            "mark_price_at_charge": raw["markPrice"],
            "price_units": "USDT",
            "units": "fraction_per_reported_charge",
            "rate_kind": "settled_history",
            "reported_rate_type": raw.get("rateType"),
            "settlement_currency": "USDT",
            "funding_interval_minutes": None,
            "interval_basis": "historical interval unavailable; current metadata not applied",
            "observation_received_at_ms": receipt,
            "instrument": instrument,
            "timestamp_basis": "venue funding timestamp and actual receipt; publication time unavailable",
            "finality_verified": False,
            "aggregation_policy": "poll observations are not independent settlements; select observed revisions before aggregation",
        }
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
        identity = json.dumps(
            [
                "binance_history",
                "public_reference",
                "BTC",
                "BTCUSDT",
                "funding",
                str(ts),
                ts,
                encoded,
            ],
            separators=(",", ":"),
        )
        expected[ts] = {
            "event_id": hashlib.sha256(identity.encode()).hexdigest(),
            "payload_json": encoded,
            "observed_at_ms": receipt,
        }
        cursor = ts + 1
assert responses[-1]["response"] == [] and len(expected) == 6
before = hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest()
checked = []
with duckdb.connect(str(ROOT / "events.duckdb"), read_only=True) as db:
    result = db.execute("SELECT * FROM events ORDER BY event_id")
    columns = [c[0] for c in result.description]
    database_rows = result.fetchall()
    assert len(database_rows) == 6
    for values in database_rows:
        row = dict(zip(columns, values, strict=True))
        ts = row["event_time_ms"]
        assert row["source"] == "binance_history" and row["environment"] == "public_reference"
        assert row["asset"] == "BTC" and row["symbol"] == "BTCUSDT" and row["kind"] == "funding"
        assert row["timestamp_kind"] == "source" and row["schema_version"] == 1
        assert {k: row[k] for k in ["event_id", "payload_json", "observed_at_ms"]} == expected[ts]
        receipt = row["observed_at_ms"]
        for clock, count in [(receipt - 1, 0), (receipt, 1)]:
            assert db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row["event_id"], clock, clock],
            ).fetchone() == (count,)
        checked.append(
            {"event_id": row["event_id"], "funding_time_ms": ts, "observed_at_ms": receipt}
        )
    assert db.execute("SELECT count(*) FROM delivery_outbox").fetchone() == (6,)
with duckdb.connect() as db:
    assert (
        db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(ROOT / "events.parquet")]
        ).fetchall()
        == database_rows
    )
assert hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest() == before
assert (
    hashlib.sha256((ROOT / "events.parquet").read_bytes()).hexdigest() == report["parquet_sha256"]
)
verification = {
    "status": "SAVED_PUBLIC_SETTLEMENT_RESPONSES_INDEPENDENTLY_VERIFIED",
    "network_requests": 0,
    "public_settlement_rows": 6,
    "public_pages": 4,
    "sql_pit_checks": 12,
    "canonical_identity_checks": 6,
    "all_reported_fields_exact": True,
    "database_sha256_unchanged": before,
    "parquet_exact_match": True,
    "checked": checked,
    "historical_interval_available": False,
    "historical_schedule_completeness": "NOT_PROVEN",
    "finality_verified": False,
    "trading_enabled": False,
}
(ROOT / "verification.json").write_text(json.dumps(verification, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"status": verification["status"], "rows": 6, "sql_pit_checks": 12}))
