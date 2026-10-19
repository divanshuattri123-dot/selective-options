"""Saved-native swing warmup and independent causal pivot fixture proof."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import cast

from selective_options import __version__
from selective_options.archive import file_digest
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.research import _path
from selective_options.swing_structure_features import compute_swing_structure
from selective_options.swing_structure_research import research_swing_structure

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/ingestion-pilot-04/events.duckdb"
DATABASE_SHA256 = "691fd0d9e9b36544b4844d3743fe5941c144d7b7e365ee695872d5660891393b"
CUTOFF_MS = 1_790_002_505_185
HISTORY_START_MS = 1_790_002_200_000
OUTPUT = ROOT / "evidence/swing-structure-saved-native-01"


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("expected JSON object")
    return cast(dict[str, object], value)


def _bar(index: int, high: int, low: int) -> MarketEvent:
    opening = 300_000 + index * 300_000
    return make_event(
        source="independent_swing_fixture",
        environment="public_reference",
        asset="BTC",
        symbol="FIXTURE-BTC/USD",
        kind="bar",
        event_time_ms=opening + 300_000,
        observed_at_ms=opening + 300_005,
        payload={
            "open_time_ms": opening,
            "close_time_ms": opening + 300_000,
            "timeframe_s": 300,
            "open": 100.0,
            "high": float(high),
            "low": float(low),
            "close": 100.0,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": True,
        },
    )


def _code_hashes() -> dict[str, str]:
    names = (
        "src/selective_options/swing_structure_features.py",
        "src/selective_options/swing_structure_research.py",
        "src/selective_options/features.py",
        "src/selective_options/research.py",
        "src/selective_options/events.py",
        "src/selective_options/cli.py",
        "scripts/verify_swing_structure.py",
    )
    return {name: file_digest(_path(ROOT / name)) for name in names}


def verify(output: Path, database_path: Path = DATABASE) -> dict[str, object]:
    database = _path(database_path)
    database_digest = hashlib.sha256(database.read_bytes()).hexdigest()
    require(database_digest == DATABASE_SHA256, "NATIVE_SOURCE_HASH_MISMATCH")
    output = _path(output, must_exist=False)
    work = (ROOT / "work").resolve()
    require(output == OUTPUT.resolve() or output.is_relative_to(work), "OUTPUT_PATH_REFUSED")
    output.mkdir(parents=True, exist_ok=False)
    native = research_swing_structure(
        database,
        output / "native-report",
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=300,
        as_of_ms=CUTOFF_MS,
        history_start_ms=HISTORY_START_MS,
        max_latest_age_ms=600_000,
    )
    native_scope = _mapping(native["source_scope_and_cutoff"])
    native_report = _mapping(native["swing_structure_report"])
    native_base = _mapping(native_report["base"])
    native_pivots = native_report["pivots"]
    require(native_scope["materialized_rows"] == 2, "NATIVE_ROW_SCOPE_MISMATCH")
    require(native_base["blockers"] == [], "NATIVE_QUALITY_BLOCKERS")
    require(native_report["state"] == "WARMUP", "NATIVE_SHORT_HISTORY_MUST_REMAIN_WARMUP")
    if not isinstance(native_pivots, list):
        raise ValueError("NATIVE_PIVOTS_MUST_BE_A_LIST")
    require(not native_pivots, "NATIVE_WARMUP_MUST_NOT_INVENT_PIVOTS")
    require(native["source_mutated"] is False, "NATIVE_SOURCE_MUTATED")
    require(native["trading_enabled"] is False, "TRADING_MUST_REMAIN_DISABLED")

    highs = [100, 101, 105, 102, 101]
    lows = [99, 98, 97.5, 97, 96]
    rows = [
        _bar(index, high, int(low))
        for index, (high, low) in enumerate(zip(highs, lows, strict=True))
    ]
    snapshot = PointInTimeSnapshot(rows[-1].observed_at_ms, "BTC", "public_reference", tuple(rows))
    fixture = compute_swing_structure(
        snapshot,
        source="independent_swing_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        left_bars=1,
        right_bars=2,
        max_latest_age_ms=600_000,
    )
    require(fixture.state == "AVAILABLE", "FIXTURE_MUST_BE_AVAILABLE")
    require(len(fixture.pivots) == 1, "FIXTURE_PIVOT_COUNT_MISMATCH")
    pivot = fixture.pivots[0]
    require(pivot.kind == "SWING_HIGH", "FIXTURE_PIVOT_KIND_MISMATCH")
    require(pivot.pivot_event_id == rows[2].event_id, "FIXTURE_PIVOT_INDEX_MISMATCH")
    require(pivot.confirmation_event_id == rows[4].event_id, "FIXTURE_CONFIRMATION_MISMATCH")
    require(pivot.confirmation_close_ms > pivot.pivot_close_ms, "PIVOT_NOT_CAUSAL")
    require(fixture.ready_for_signals is fixture.trading_enabled is False, "SIGNAL_GATE_OPEN")
    require(hashlib.sha256(database.read_bytes()).hexdigest() == database_digest, "SOURCE_CHANGED")

    report = {
        "version": __version__,
        "status": "SAVED_NATIVE_WARMUP_AND_CAUSAL_FIXTURE_VERIFIED",
        "native_source": "evidence/ingestion-pilot-04/events.duckdb",
        "native_source_sha256": database_digest,
        "native_source_unchanged": True,
        "native_rows": native_scope["materialized_rows"],
        "native_state": native_report["state"],
        "native_pivots": len(native_pivots),
        "fixture_provenance": "independent_analytical_fixture_not_market_data",
        "fixture_pivot": {
            "kind": pivot.kind,
            "pivot_event_id": pivot.pivot_event_id,
            "confirmation_event_id": pivot.confirmation_event_id,
            "pivot_close_ms": pivot.pivot_close_ms,
            "confirmation_close_ms": pivot.confirmation_close_ms,
        },
        "source_code_sha256": _code_hashes(),
        "real_network_requests": 0,
        "ready_for_signals": False,
        "trading_enabled": False,
        "orders_submitted": 0,
        "milestone_2_complete": False,
    }
    (output / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--database", type=Path, default=DATABASE)
    args = parser.parse_args()
    print(json.dumps(verify(args.output, args.database), indent=2))


if __name__ == "__main__":
    main()
