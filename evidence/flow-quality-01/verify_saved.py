"""Read-only saved-public flow-window checks with clearly synthetic outage/recovery rows."""

import hashlib
import json
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.events import MarketEvent, make_event
from selective_options.flow_quality import flow_quality
from selective_options.flow_view import SOURCES

ROOT = Path(__file__).resolve().parent
checks = []
synthetic_rows = []
hashes = {}


def check(rows, kind, asset, clock):
    return flow_quality(rows, kind=kind, asset=asset, symbol=asset + "USDT", as_of_ms=clock)


def synthetic_health(kind, clock, asset, ok):
    row = make_event(
        source=SOURCES[kind],
        environment="public_reference",
        asset=asset,
        symbol="USDT_PERPETUAL",
        kind="feed_health",
        event_time_ms=clock,
        observed_at_ms=clock,
        timestamp_kind="receipt",
        payload={"ok": ok, "reason": "OK" if ok else "HTTP_ERROR", "code": 0 if ok else 403},
    )
    synthetic_rows.append(row.model_dump(mode="json"))
    return row


for kind, directory in [
    ("taker_flow", "binance-taker-public-02"),
    ("account_ratio", "binance-accounts-public-01"),
]:
    path = ROOT.parent / directory / "events.duckdb"
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [c[0] for c in result.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, values, strict=True)))
            for values in result.fetchall()
        ]
        assert len(rows) == 16
        for asset in ["BTC", "ETH", "SOL", "PAXG"]:
            original = [r for r in rows if r.asset == asset]
            receipt = max(r.observed_at_ms for r in original)
            available = check(rows, kind, asset, receipt)
            assert available["state"] == "OBSERVED_WINDOW_AVAILABLE" and available["blockers"] == []
            ends = [
                r[0]
                for r in db.execute(
                    "SELECT event_time_ms FROM events WHERE asset=? AND kind=? ORDER BY event_time_ms",
                    [asset, kind],
                ).fetchall()
            ]
            assert len(ends) == 3 and ends[1] - ends[0] == ends[2] - ends[1] == 300_000
            expired = check(rows, kind, asset, receipt + 600_001)
            assert set(expired["blockers"]) == {
                "STALE_FLOW_PERIOD",
                "STALE_FLOW_OBSERVATION",
                "STALE_HEALTH",
            }
            checks.append(
                {
                    "kind": kind,
                    "asset": asset,
                    "original_public_window": available,
                    "simulated_later_expiry": expired,
                }
            )
    clock = max(r.observed_at_ms for r in rows) + 1000
    outage = synthetic_health(kind, clock, "GLOBAL", False)
    assert all(
        "SOURCE_UNAVAILABLE" in check(rows + [outage], kind, asset, clock)["blockers"]
        for asset in ["BTC", "ETH", "SOL", "PAXG"]
    )
    assert check(rows + [outage], kind, "BTC", clock - 1)["state"] == "OBSERVED_WINDOW_AVAILABLE"
    recovery = synthetic_health(kind, clock + 1000, "BTC", True)
    assert (
        check(rows + [outage, recovery], kind, "BTC", clock + 1000)["state"]
        == "OBSERVED_WINDOW_AVAILABLE"
    )
    assert (
        "SOURCE_UNAVAILABLE"
        in check(rows + [outage, recovery], kind, "ETH", clock + 1000)["blockers"]
    )
    middle = sorted(
        (r for r in rows if r.kind == kind and r.asset == "BTC"), key=lambda r: r.event_time_ms
    )[1]
    missing = check([r for r in rows if r.event_id != middle.event_id], kind, "BTC", clock)
    assert missing["missing_period_ends_ms"] == [middle.event_time_ms]
    assert "MISSING_RECENT_PERIODS" in missing["blockers"]
    checks.append(
        {
            "kind": kind,
            "synthetic_global_outage": check(rows + [outage], kind, "BTC", clock),
            "synthetic_asset_recovery": check(rows + [outage, recovery], kind, "BTC", clock + 1000),
            "synthetic_removed_period": missing,
        }
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    hashes[str(path.relative_to(ROOT.parent))] = before

report = {
    "version": __version__,
    "status": "OFFLINE_FLOW_WINDOW_QUALITY_VERIFIED",
    "network_requests": 0,
    "original_public_flow_rows": 24,
    "original_public_health_rows": 8,
    "original_public_database_sha256_unchanged": hashes,
    "checks": checks,
    "synthetic_health_rows": len(synthetic_rows),
    "synthetic_scope": "Future evaluation clocks, global outages, BTC health recovery and removed middle periods are simulated; no new public request or actual outage is claimed",
    "continuous_history_verified": False,
    "field_semantics_and_volume_units_verified": False,
    "trading_enabled": False,
    "orders_submitted": 0,
}
(ROOT / "synthetic-health.json").write_text(
    json.dumps(synthetic_rows, indent=2) + "\n", encoding="utf-8"
)
(ROOT / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(
    json.dumps(
        {
            "status": report["status"],
            "original_public_rows": 32,
            "synthetic_health_rows": len(synthetic_rows),
        }
    )
)
