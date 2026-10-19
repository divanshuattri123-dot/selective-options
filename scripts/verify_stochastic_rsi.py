"""Offline native-bar warmup proof plus a separate exact synthetic golden."""

import argparse
import hashlib
import json
from fractions import Fraction
from pathlib import Path
from typing import cast

from selective_options.archive import file_digest
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.research import _path
from selective_options.stochastic_rsi_features import compute_stochastic_rsi
from selective_options.stochastic_rsi_research import research_stochastic_rsi

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/ingestion-pilot-04/events.duckdb"
DATABASE_SHA256 = "691fd0d9e9b36544b4844d3743fe5941c144d7b7e365ee695872d5660891393b"
CUTOFF_MS = 1_790_002_505_185
HISTORY_START_MS = 1_790_002_200_000
TIMEFRAME_S = 300


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("expected a JSON object")
    return cast(dict[str, object], value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("expected a JSON array")
    return value


def _bar(index: int, close: int) -> MarketEvent:
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
            "open": close,
            "high": close + 2,
            "low": close - 2,
            "close": close,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": True,
        },
    )


def _exact_reference(closes: list[int]) -> tuple[Fraction, Fraction, Fraction]:
    """Independently expand Wilder seeds/updates using exact rationals."""
    period, stochastic_period, smooth_k, smooth_d = 14, 14, 3, 3
    values = [Fraction(value) for value in closes]
    changes = [right - left for left, right in zip(values, values[1:], strict=False)]
    gains: list[Fraction] = [value if value > 0 else Fraction(0) for value in changes]
    losses: list[Fraction] = [-value if value < 0 else Fraction(0) for value in changes]

    def wilder(items: list[Fraction]) -> list[Fraction]:
        average = sum(items[:period], Fraction(0)) / period
        output = [average]
        for item in items[period:]:
            average = (average * (period - 1) + item) / period
            output.append(average)
        return output

    rsi: list[Fraction | None] = [None] * period
    for gain, loss in zip(wilder(gains), wilder(losses), strict=True):
        rsi.append(100 * gain / (gain + loss))
    raw: list[Fraction | None] = [None] * len(rsi)
    for end in range(stochastic_period - 1, len(rsi)):
        window = rsi[end - stochastic_period + 1 : end + 1]
        concrete = [value for value in window if value is not None]
        if len(concrete) != stochastic_period:
            continue
        low, high = min(concrete), max(concrete)
        raw[end] = 100 * (concrete[-1] - low) / (high - low)

    def sma(series: list[Fraction | None], window: int) -> list[Fraction | None]:
        result: list[Fraction | None] = [None] * len(series)
        for end in range(window - 1, len(series)):
            values = [value for value in series[end - window + 1 : end + 1] if value is not None]
            if len(values) == window:
                result[end] = sum(values, Fraction(0)) / window
        return result

    k_values = sma(raw, smooth_k)
    d_values = sma(k_values, smooth_d)
    assert raw[-1] is not None and k_values[-1] is not None and d_values[-1] is not None
    return raw[-1], k_values[-1], d_values[-1]


def _code_hashes() -> dict[str, str]:
    names = (
        "src/selective_options/stochastic_rsi_features.py",
        "src/selective_options/stochastic_rsi_research.py",
        "src/selective_options/features.py",
        "src/selective_options/research.py",
        "src/selective_options/events.py",
        "src/selective_options/cli.py",
        "scripts/verify_stochastic_rsi.py",
    )
    return {name: file_digest(_path(ROOT / name)) for name in names}


def verify(output: Path) -> dict[str, object]:
    database = _path(DATABASE)
    database_digest = hashlib.sha256(database.read_bytes()).hexdigest()
    require(database_digest == DATABASE_SHA256, "NATIVE_SOURCE_HASH_MISMATCH")
    output = _path(output, must_exist=False)
    output.mkdir(parents=True, exist_ok=False)
    native = research_stochastic_rsi(
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
    native_report = _mapping(native["stochastic_rsi_report"])
    native_base = _mapping(native_report["base"])
    native_features = [_mapping(item) for item in _array(native_report["features"])]
    require(native_base["bars_available"] == 2, "NATIVE_ROW_SCOPE_MISMATCH")
    require(native_base["blockers"] == [], "NATIVE_QUALITY_BLOCKERS")
    require(len(_array(native_report["input_event_ids"])) == 2, "NATIVE_IDS_MISSING")
    require(
        all(
            feature["state"] == "WARMUP" and feature["value"] is None for feature in native_features
        ),
        "NATIVE_SHORT_HISTORY_MUST_REMAIN_WARMUP",
    )
    require(native_report["trading_enabled"] is False, "TRADING_MUST_REMAIN_DISABLED")

    closes = [100 + (index * 7) % 13 + index // 6 for index in range(48)]
    rows = [_bar(index, close) for index, close in enumerate(closes)]
    snapshot = PointInTimeSnapshot(rows[-1].observed_at_ms, "BTC", "public_reference", tuple(rows))
    fixture = compute_stochastic_rsi(
        snapshot,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=TIMEFRAME_S,
    )
    exact = _exact_reference(closes)
    measured: list[float] = []
    for name in ("stoch_rsi", "stoch_rsi_k", "stoch_rsi_d"):
        value = fixture.feature(name).value
        if value is None:
            raise ValueError("SYNTHETIC_FEATURE_UNAVAILABLE")
        measured.append(value)
    require(
        all(
            abs(float(value) - float(reference)) <= 1e-12
            for value, reference in zip(measured, exact, strict=True)
        ),
        "SYNTHETIC_EXACT_REFERENCE_MISMATCH",
    )
    require(
        fixture.ready_for_signals is fixture.trading_enabled is False,
        "SIGNAL_GATE_MUST_STAY_CLOSED",
    )
    result: dict[str, object] = {
        "status": "VERIFIED_OFFLINE_RESEARCH_ONLY",
        "source_database_sha256": database_digest,
        "source_scope": "two original native Kraken BTC five-minute closed bars; explicit cutoff and history start",
        "native_report": native,
        "native_numeric_feature_values_claimed": False,
        "synthetic_fixture": {
            "provenance": "synthetic_fixture",
            "bars": len(rows),
            "formula_reference": "independent exact Fraction expansion",
            "features": fixture.model_dump(mode="json"),
        },
        "source_mutated": False,
        "current_feed_health_verified": False,
        "trading_enabled": False,
        "code_sha256": _code_hashes(),
    }
    with (output / "verification.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = verify(args.output)
    print(json.dumps({"status": report["status"], "output": str(args.output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
