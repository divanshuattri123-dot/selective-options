"""Independent saved-response/database check; no network and no collector/parser calls."""

import json
from pathlib import Path

import duckdb

root = Path(__file__).resolve().parent
responses = json.loads((root / "responses.json").read_text(encoding="utf-8"))
assert len(responses) == 5
discovery = responses[0]
assert discovery["endpoint"] == "exchangeInfo"
instruments = {r["symbol"]: r for r in discovery["response"]["symbols"]}
by_symbol = {r["params"]["symbol"]: r for r in responses[1:]}
assert set(by_symbol) == {"BTCUSDT", "ETHUSDT", "SOLUSDT", "PAXGUSDT"}
checked = []
with duckdb.connect(str(root / "events.duckdb"), read_only=True) as db:
    result = db.execute("SELECT * FROM events ORDER BY event_id")
    columns = [c[0] for c in result.description]
    database_rows = result.fetchall()
    for values in database_rows:
        row = dict(zip(columns, values, strict=True))
        assert row["source"] == "binance_accounts" and row["environment"] == "public_reference"
        if row["kind"] != "account_ratio":
            continue
        payload = json.loads(row["payload_json"])
        response = by_symbol[row["symbol"]]
        assert response["endpoint"] == "globalLongShortAccountRatio"
        assert response["params"] == {"symbol": row["symbol"], "period": "5m", "limit": "3"}
        matches = [r for r in response["response"] if r["timestamp"] == payload["period_end_ms"]]
        assert len(matches) == 1
        raw = matches[0]
        assert (
            payload["reported_longAccount"],
            payload["reported_shortAccount"],
            payload["reported_longShortRatio"],
        ) == (
            raw["longAccount"],
            raw["shortAccount"],
            raw["longShortRatio"],
        )
        assert row["event_time_ms"] == payload["period_end_ms"] == raw["timestamp"]
        assert payload["period_start_ms"] == raw["timestamp"] - 300_000
        assert row["observed_at_ms"] == response["observed_at_ms"] >= row["event_time_ms"]
        assert discovery["observed_at_ms"] <= row["observed_at_ms"]
        native = instruments[row["symbol"]]
        assert native["baseAsset"] == row["asset"]
        assert native["quoteAsset"] == native["marginAsset"] == "USDT"
        assert native["contractType"] == "PERPETUAL" and native["status"] == "TRADING"
        for as_of, expected in [(row["observed_at_ms"] - 1, 0), (row["observed_at_ms"], 1)]:
            assert db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row["event_id"], as_of, as_of],
            ).fetchone() == (expected,)
        checked.append(
            {
                "event_id": row["event_id"],
                "asset": row["asset"],
                "period_end_ms": raw["timestamp"],
                "observed_at_ms": row["observed_at_ms"],
            }
        )
with duckdb.connect() as db:
    assert (
        db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(root / "events.parquet")]
        ).fetchall()
        == database_rows
    )
assert len(database_rows) == 16 and len(checked) == 12
report = {
    "status": "SAVED_PUBLIC_RESPONSES_INDEPENDENTLY_VERIFIED",
    "network_requests": 0,
    "flow_rows": len(checked),
    "database_rows": len(database_rows),
    "exact_reported_decimal_values": True,
    "period_and_receipt_checks": True,
    "point_in_time_checks": 24,
    "database_parquet_exact_match": True,
    "observations": checked,
    "continuous_history_verified": False,
    "account_field_semantics_independently_verified": False,
    "trading_enabled": False,
}
(root / "verification.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps({k: v for k, v in report.items() if k != "observations"}))
