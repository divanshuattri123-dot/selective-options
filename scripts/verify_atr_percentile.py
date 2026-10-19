"""Offline saved-bar ATR-percentile warmup and exact-rational fixture proof."""

import argparse
import hashlib
import json
from fractions import Fraction
from pathlib import Path
from typing import cast

from selective_options import __version__
from selective_options.archive import file_digest
from selective_options.atr_percentile_features import compute_atr_percentile
from selective_options.atr_percentile_research import research_atr_percentile
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.research import _path

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/ingestion-pilot-04/events.duckdb"
DATABASE_SHA256 = "691fd0d9e9b36544b4844d3743fe5941c144d7b7e365ee695872d5660891393b"
CUTOFF_MS = 1_790_002_505_185
HISTORY_START_MS = 1_790_002_200_000
TIMEFRAME_S = 300
OUTPUT = ROOT / "evidence/atr-percentile-saved-native-01"


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("expected JSON object")
    return cast(dict[str, object], value)


def _exact_atrs(ranges: list[int], period: int) -> list[Fraction]:
    true_ranges = [Fraction(value) for value in ranges[1:]]
    if len(true_ranges) < period:
        return []
    current = sum(true_ranges[:period], Fraction(0)) / period
    output = [current]
    for true_range in true_ranges[period:]:
        current = (current * (period - 1) + true_range) / period
        output.append(current)
    return output


def _bar(index: int, true_range: int) -> MarketEvent:
    opening = 300_000 + index * 300_000
    return make_event(
        source="synthetic_fixture",
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
            "high": 100.0 + true_range / 2,
            "low": 100.0 - true_range / 2,
            "close": 100.0,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": True,
        },
    )


def _code_hashes() -> dict[str, str]:
    names = (
        "src/selective_options/atr_percentile_features.py",
        "src/selective_options/atr_percentile_research.py",
        "src/selective_options/features.py",
        "src/selective_options/research.py",
        "src/selective_options/events.py",
        "src/selective_options/cli.py",
        "scripts/verify_atr_percentile.py",
    )
    return {name: file_digest(_path(ROOT / name)) for name in names}


def verify(output: Path) -> dict[str, object]:
    database = _path(DATABASE)
    database_digest = hashlib.sha256(database.read_bytes()).hexdigest()
    require(database_digest == DATABASE_SHA256, "NATIVE_SOURCE_HASH_MISMATCH")
    output = _path(output, must_exist=False)
    expected_output = OUTPUT.resolve()
    work = (ROOT / "work").resolve()
    require(output == expected_output or output.is_relative_to(work), "OUTPUT_PATH_REFUSED")
    output.mkdir(parents=True, exist_ok=False)
    native = research_atr_percentile(
        database,
        output / "native-report",
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=TIMEFRAME_S,
        as_of_ms=CUTOFF_MS,
        history_start_ms=HISTORY_START_MS,
        max_latest_age_ms=600_000,
    )
    native_scope = _mapping(native["source_scope_and_cutoff"])
    native_report = _mapping(native["atr_percentile_report"])
    native_base = _mapping(native_report["base"])
    native_feature = _mapping(native_report["feature"])
    require(native_scope["materialized_rows"] == 2, "NATIVE_ROW_SCOPE_MISMATCH")
    require(native_base["blockers"] == [], "NATIVE_QUALITY_BLOCKERS")
    require(native_feature["state"] == "WARMUP", "NATIVE_SHORT_HISTORY_MUST_REMAIN_WARMUP")
    require(native_feature["value"] is None, "NATIVE_WARMUP_VALUE_MUST_BE_NULL")
    require(native_feature["bars_required"] == 35, "NATIVE_REQUIRED_HISTORY_MISMATCH")
    require(native["source_mutated"] is False, "NATIVE_SOURCE_MUTATED")
    require(native["trading_enabled"] is False, "TRADING_MUST_REMAIN_DISABLED")

    ranges = [99, 1, 2, 4, 1, 3, 2, 8]
    rows = [_bar(index, value) for index, value in enumerate(ranges)]
    snapshot = PointInTimeSnapshot(rows[-1].observed_at_ms, "BTC", "public_reference", tuple(rows))
    fixture = compute_atr_percentile(
        snapshot,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=TIMEFRAME_S,
        atr_period=3,
        percentile_window=4,
        percentile_min_history=4,
        max_latest_age_ms=600_000,
    )
    atrs = _exact_atrs(ranges, 3)
    current, prior = atrs[-1], atrs[:-1]
    exact = (
        100
        * (
            sum(value < current for value in prior)
            + Fraction(1, 2) * sum(value == current for value in prior)
        )
        / len(prior)
    )
    value = fixture.feature.value
    if value is None:
        raise ValueError("SYNTHETIC_FEATURE_UNAVAILABLE")
    require(abs(value - float(exact)) <= 1e-12, "SYNTHETIC_EXACT_REFERENCE_MISMATCH")
    require(fixture.reference.observations == 4, "SYNTHETIC_REFERENCE_SCOPE_MISMATCH")
    latest_reference = fixture.reference.latest_close_ms
    if latest_reference is None:
        raise ValueError("SYNTHETIC_REFERENCE_BOUNDARY_MISSING")
    require(latest_reference < rows[-1].event_time_ms, "CURRENT_NOT_EXCLUDED")
    require(fixture.ready_for_signals is fixture.trading_enabled is False, "SIGNAL_GATE_OPEN")
    require(hashlib.sha256(database.read_bytes()).hexdigest() == database_digest, "SOURCE_CHANGED")

    report = {
        "version": __version__,
        "status": "SAVED_NATIVE_WARMUP_AND_EXACT_FIXTURE_VERIFIED",
        "native_source": "evidence/ingestion-pilot-04/events.duckdb",
        "native_source_sha256": database_digest,
        "native_source_unchanged": True,
        "native_rows": native_scope["materialized_rows"],
        "native_state": native_feature["state"],
        "native_value": native_feature["value"],
        "native_required_bars": native_feature["bars_required"],
        "synthetic_provenance": "independent_exact_fraction_fixture_not_market_data",
        "synthetic_true_ranges": ranges,
        "synthetic_atr_period": 3,
        "synthetic_prior_observations": fixture.reference.observations,
        "synthetic_current_excluded": fixture.reference.current_excluded,
        "synthetic_exact_percentile": str(exact),
        "synthetic_computed_percentile": value,
        "synthetic_matches_exact_reference": True,
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
    args = parser.parse_args()
    print(json.dumps(verify(args.output), indent=2))


if __name__ == "__main__":
    main()
