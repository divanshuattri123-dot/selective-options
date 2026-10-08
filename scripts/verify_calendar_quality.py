"""Offline replay of saved public BEA content with explicitly simulated health/failure events."""

import argparse
import json
from pathlib import Path

from selective_options import __version__
from selective_options.bea_calendar import parse_calendar
from selective_options.calendar_quality import calendar_quality
from selective_options.events import make_event


def verify(source: Path, output: Path) -> None:
    original = json.loads((source / "report.json").read_text(encoding="utf-8"))
    receipt = int(original["observed_at_ms"])
    snapshot = parse_calendar((source / "response.json").read_bytes(), receipt)
    health = make_event(
        source="bea",
        environment="public_reference",
        asset="GLOBAL",
        symbol="RELEASE_SCHEDULE",
        kind="feed_health",
        event_time_ms=receipt,
        observed_at_ms=receipt,
        timestamp_kind="receipt",
        payload={"ok": True, "fixture": True},
    )
    failure = make_event(
        source="bea",
        environment="public_reference",
        asset="GLOBAL",
        symbol="RELEASE_SCHEDULE",
        kind="feed_health",
        event_time_ms=receipt + 1000,
        observed_at_ms=receipt + 1000,
        timestamp_kind="receipt",
        payload={"ok": False, "fixture": True},
    )
    rows = [snapshot, health, failure]
    cases = {
        "unobserved": calendar_quality(rows, receipt - 1, 7200000),
        "missing_health": calendar_quality([snapshot], receipt, 7200000),
        "before_failure": calendar_quality(rows, receipt + 999, 7200000),
        "at_failure": calendar_quality(rows, receipt + 1000, 7200000),
        "stale": calendar_quality([snapshot, health], receipt + 7200001, 7200000),
    }
    assert cases["unobserved"]["blockers"] == ["MISSING_SNAPSHOT", "MISSING_HEALTH"]
    assert cases["missing_health"]["blockers"] == ["MISSING_HEALTH"]
    assert cases["before_failure"]["state"] == "OBSERVED_SCOPE_AVAILABLE"
    assert cases["at_failure"]["blockers"] == ["SOURCE_UNAVAILABLE"]
    assert cases["stale"]["blockers"] == ["STALE_HEALTH", "STALE_SNAPSHOT"]
    report = {
        "version": __version__,
        "status": "OFFLINE_REPLAY_VERIFIED",
        "source_directory": source.name,
        "snapshot_body_sha256": snapshot.payload()["body_sha256"],
        "network_requests": 0,
        "health_events": "SIMULATED_FIXTURES_NOT_PUBLIC_EVIDENCE",
        "cases": cases,
        "orders_submitted": 0,
        "trading_enabled": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as file:
        file.write(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "cases": len(cases), "network_requests": 0}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify(args.source, args.output)
