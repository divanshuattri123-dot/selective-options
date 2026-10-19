"""Offline native API/CLI replay plus a labelled synthetic exact-rational golden.

Standard 20/10/2 channels require 20 bars. The native pilot's explicitly selected
two complete BTC bars therefore establish only honest WARMUP, never a numeric
channel or a sustained-history claim. Its original partial prefix is refused.
"""

import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
from fractions import Fraction
from pathlib import Path
from typing import Literal

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.events import Environment, MarketEvent, PointInTimeSnapshot, make_event
from selective_options.keltner_features import KeltnerFeatureReport, compute_keltner_features
from selective_options.keltner_research import research_keltner
from selective_options.research import MAX_SOURCE_BYTES, _path, load_bar_snapshot

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/ingestion-pilot-04/events.duckdb"
SOURCE_SHA256 = "691fd0d9e9b36544b4844d3743fe5941c144d7b7e365ee695872d5660891393b"
CUTOFF = 1790002505185
HISTORY_START = 1790002200000
EMA_PERIOD = 20
ATR_PERIOD = 10
ATR_MULTIPLIER = 2.0
MAX_BARS = 2000
MAX_LATEST_AGE_MS = 600_000
NAMES = ("keltner_center", "keltner_atr", "keltner_upper", "keltner_lower")
Provenance = Literal["saved_native_capture", "synthetic_fixture"]


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError(reason)


def code_hashes() -> dict[str, str]:
    names = (
        "src/selective_options/keltner_features.py",
        "src/selective_options/keltner_research.py",
        "src/selective_options/features.py",
        "src/selective_options/research.py",
        "src/selective_options/events.py",
        "src/selective_options/cli.py",
        "scripts/verify_keltner.py",
    )
    return {name: file_digest(_path(ROOT / name)) for name in names}


def exact_reference(rows: tuple[MarketEvent, ...]) -> tuple[Fraction | None, ...]:
    """Exact finite geometric expansion; no production EMA/ATR helper or recurrence."""
    bodies = [row.payload() for row in rows]
    closes = [Fraction(str(body["close"])) for body in bodies]
    ranges = [
        max(
            Fraction(str(right["high"])) - Fraction(str(right["low"])),
            abs(Fraction(str(right["high"])) - Fraction(str(left["close"]))),
            abs(Fraction(str(right["low"])) - Fraction(str(left["close"]))),
        )
        for left, right in zip(bodies, bodies[1:], strict=False)
    ]

    def expansion(values: list[Fraction], period: int, alpha: Fraction) -> Fraction:
        beta = 1 - alpha
        seed_weight = beta ** (len(values) - period)
        return Fraction(
            sum(values[:period]) / period * seed_weight
            + sum(
                alpha * value * beta ** (len(values) - 1 - index)
                for index, value in enumerate(values[period:], start=period)
            )
        )

    center = (
        expansion(closes, EMA_PERIOD, Fraction(2, EMA_PERIOD + 1))
        if len(closes) >= EMA_PERIOD
        else None
    )
    atr = (
        expansion(ranges, ATR_PERIOD, Fraction(1, ATR_PERIOD))
        if len(ranges) >= ATR_PERIOD
        else None
    )
    return (
        center,
        atr,
        center + 2 * atr if center is not None and atr is not None else None,
        center - 2 * atr if center is not None and atr is not None else None,
    )


def _price(value: object) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise IntegrityError("KELTNER_PROOF_INVALID_OHLC")
    try:
        price = float(value)
    except (OverflowError, ValueError) as exc:
        raise IntegrityError("KELTNER_PROOF_INVALID_OHLC") from exc
    require(math.isfinite(price) and price > 0, "KELTNER_PROOF_INVALID_OHLC")
    return price


def check_report(
    report: KeltnerFeatureReport,
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
) -> dict[str, object]:
    """Independent identity/clock/history/metadata/shape and rational value audit."""
    rows = tuple(
        sorted(
            (
                row
                for row in snapshot.rows(source, "bar")
                if row.symbol == symbol and row.payload()["timeframe_s"] == timeframe_s
            ),
            key=lambda row: row.event_time_ms,
        )
    )
    require(
        len({row.event_id for row in rows}) == len(rows)
        and len({row.event_time_ms for row in rows}) == len(rows),
        "KELTNER_PROOF_DUPLICATE_OR_TIED_INPUT",
    )
    for row in rows:
        body = row.payload()
        try:
            canonical = make_event(
                source=row.source,
                environment=row.environment,
                asset=row.asset,
                symbol=row.symbol,
                kind="bar",
                event_time_ms=row.event_time_ms,
                observed_at_ms=row.observed_at_ms,
                payload=body,
            )
        except (ValueError, TypeError) as exc:
            raise IntegrityError("KELTNER_PROOF_NATIVE_IDENTITY_OR_CLOCKS") from exc
        require(
            max(row.event_time_ms, row.observed_at_ms) <= snapshot.as_of_ms
            and row.asset == snapshot.asset
            and row.environment == snapshot.environment
            and row == canonical,
            "KELTNER_PROOF_NATIVE_IDENTITY_OR_CLOCKS",
        )
        opening, high, low, close = (
            _price(body[name]) for name in ("open", "high", "low", "close")
        )
        require(
            body["closed"] is True
            and type(body["complete"]) is bool
            and body["close_time_ms"] == row.event_time_ms
            and body["open_time_ms"] == row.event_time_ms - timeframe_s * 1000
            and low <= min(opening, close)
            and max(opening, close) <= high,
            "KELTNER_PROOF_NATIVE_INTERVAL_OR_CONTROL",
        )
    blockers: list[str] = []
    if len(rows) > MAX_BARS:
        blockers.append("HISTORY_BUDGET_EXCEEDED")
    if not rows:
        blockers.append("NO_CLOSED_BARS")
    if any(not row.payload()["complete"] for row in rows):
        blockers.append("INCOMPLETE_BAR")
    if any(
        right.event_time_ms - left.event_time_ms != timeframe_s * 1000
        for left, right in zip(rows, rows[1:], strict=False)
    ):
        blockers.append("MISSING_INTERIOR_INTERVAL")
    if rows and snapshot.as_of_ms - rows[-1].event_time_ms > MAX_LATEST_AGE_MS:
        blockers.append("STALE_LATEST_BAR")
    digest = hashlib.sha256(
        json.dumps(
            [[row.event_id, row.event_time_ms, row.observed_at_ms] for row in rows],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    base = report.base
    require(
        report.formula_version == "keltner-bars-v1"
        and (report.ema_period, report.atr_period, report.atr_multiplier) == (20, 10, 2)
        and report.center_price == "close"
        and report.ema_seed == "first_period_arithmetic_mean"
        and report.atr_seed == "first_period_previous_close_true_ranges_mean"
        and report.atr_smoothing == "wilder_alpha_1_over_period",
        "KELTNER_PROOF_FORMULA_POLICY",
    )
    require(
        report.state == ("UNAVAILABLE" if blockers else "KELTNER_COMPUTED")
        and base.state == ("UNAVAILABLE" if blockers else "FEATURES_COMPUTED")
        and base.formula_version == "closed-bars-v1"
        and base.blockers == tuple(blockers)
        and (
            base.asset,
            base.environment,
            base.source,
            base.symbol,
            base.timeframe_s,
            base.as_of_ms,
        )
        == (snapshot.asset, snapshot.environment, source, symbol, timeframe_s, snapshot.as_of_ms)
        and base.bars_available == len(rows)
        and base.bars_used == (0 if blockers else len(rows))
        and base.history_sha256 == digest
        and base.history_budget == MAX_BARS
        and base.max_latest_age_ms == MAX_LATEST_AGE_MS
        and base.annualization_periods_per_year is None
        and base.first_close_ms == (rows[0].event_time_ms if rows else None)
        and base.latest_close_ms == (rows[-1].event_time_ms if rows else None)
        and base.latest_bar_age_ms == (snapshot.as_of_ms - rows[-1].event_time_ms if rows else None)
        and report.input_event_ids == (() if blockers else tuple(row.event_id for row in rows)),
        "KELTNER_PROOF_SCOPE_OR_HISTORY",
    )
    require(
        report.windows_are_bar_counts
        and not any(
            (
                report.current_feed_health_verified,
                report.source_frontier_complete,
                report.regime_classification_complete,
                report.ready_for_signals,
                report.trading_enabled,
                base.history_truncated,
                base.source_frontier_complete,
                base.ready_for_signals,
                base.trading_enabled,
            )
        ),
        "KELTNER_PROOF_UNSUPPORTED_APPROVAL",
    )
    require(
        tuple(item.name for item in report.features) == NAMES, "KELTNER_PROOF_OUTPUT_MEMBERSHIP"
    )
    exact = exact_reference(rows) if not blockers else None
    for index, feature in enumerate(report.features):
        required = (20, 11, 20, 20)[index]
        state = "UNAVAILABLE" if blockers else "WARMUP" if len(rows) < required else "AVAILABLE"
        reason = (
            blockers[0] if blockers else "INSUFFICIENT_CLOSED_BARS" if state == "WARMUP" else None
        )
        require(
            feature.state == state
            and feature.reason == reason
            and feature.bars_required == required
            and feature.unit == "price",
            "KELTNER_PROOF_FEATURE_POLICY",
        )
        if state == "AVAILABLE":
            require(exact is not None, "KELTNER_PROOF_REFERENCE_HISTORY")
            assert exact is not None
            expected = exact[index]
            require(expected is not None, "KELTNER_PROOF_REFERENCE_HISTORY")
            assert expected is not None
            require(
                feature.value is not None
                and math.isfinite(feature.value)
                and math.isclose(feature.value, float(expected), rel_tol=1e-12, abs_tol=0),
                "KELTNER_PROOF_NUMERIC_MISMATCH",
            )
        else:
            require(feature.value is None, "KELTNER_PROOF_INVENTED_VALUE")
    return {
        "selected_bars": len(rows),
        "complete_bars": sum(row.payload()["complete"] is True for row in rows),
        "partial_bars": sum(row.payload()["complete"] is False for row in rows),
        "history_sha256": digest,
        "blockers": blockers,
        "feature_states": {feature.name: feature.state for feature in report.features},
        "rational_reference": {
            name: str(value) if value is not None else None
            for name, value in zip(NAMES, exact, strict=True)
        }
        if exact
        else None,
    }


def golden_snapshot() -> PointInTimeSnapshot:
    rows = []
    for index in range(37):
        opening = 300_000 + index * 300_000
        close = 100.0 + (index * 7) % 31 - index / 4
        rows.append(
            make_event(
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
        )
    return PointInTimeSnapshot(rows[-1].observed_at_ms, "BTC", "public_reference", tuple(rows))


def _json_write(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")


def verify(
    database: Path,
    output: Path,
    *,
    expected_source_sha256: str = SOURCE_SHA256,
    asset: str = "BTC",
    environment: Environment = "public_reference",
    source: str = "kraken",
    symbol: str = "BTC/USD",
    timeframe_s: int = 300,
    as_of_ms: int = CUTOFF,
    history_start_ms: int = HISTORY_START,
    provenance: Provenance = "saved_native_capture",
) -> dict[str, object]:
    if (
        type(expected_source_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", expected_source_sha256) is None
        or provenance not in ("saved_native_capture", "synthetic_fixture")
        or type(as_of_ms) is not int
        or not 0 < as_of_ms < 1 << 63
        or type(history_start_ms) is not int
        or not 0 < history_start_ms <= as_of_ms
        or type(timeframe_s) is not int
        or timeframe_s != 300
    ):
        raise ValueError("proof requires explicit digest, provenance and inclusive history start")
    database, output = _path(database), _path(output, must_exist=False)
    if output.exists() or output.is_relative_to(database.parent):
        raise ValueError("proof requires fresh output outside the source directory")
    require(database.stat().st_size <= MAX_SOURCE_BYTES, "KELTNER_PROOF_SOURCE_BYTE_BUDGET")
    require(file_digest(database) == expected_source_sha256, "KELTNER_PROOF_SOURCE_HASH")
    require(
        not database.with_suffix(database.suffix + ".wal").exists(),
        "KELTNER_PROOF_SOURCE_NOT_CLOSED",
    )
    before_code = code_hashes()
    scopes = []
    for start in (None, history_start_ms):
        snapshot, digest = load_bar_snapshot(
            database,
            asset=asset,
            environment=environment,
            source=source,
            symbol=symbol,
            as_of_ms=as_of_ms,
            max_bars=MAX_BARS,
            history_start_ms=start,
        )
        require(digest == expected_source_sha256, "KELTNER_PROOF_LOADER_SOURCE_HASH")
        scopes.append((start, snapshot))
    output.mkdir(parents=True, exist_ok=False)
    checks: list[dict[str, object]] = []
    artifacts: dict[str, str] = {}
    for index, (start, snapshot) in enumerate(scopes):
        api_output, cli_output = output / f"api-{index}", output / f"cli-{index}"
        value = research_keltner(
            database,
            api_output,
            asset=asset,
            environment=environment,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            as_of_ms=as_of_ms,
            ema_period=EMA_PERIOD,
            atr_period=ATR_PERIOD,
            atr_multiplier=ATR_MULTIPLIER,
            max_latest_age_ms=MAX_LATEST_AGE_MS,
            max_bars=MAX_BARS,
            history_start_ms=start,
        )
        require(
            value["source_database_sha256"] == expected_source_sha256
            and value["source_mutated"] is False
            and value["ready_for_signals"] is False
            and value["trading_enabled"] is False
            and value["source_scope_and_cutoff"]
            == {
                "asset": asset,
                "environment": environment,
                "source": source,
                "symbol": symbol,
                "as_of_ms": as_of_ms,
                "explicit_history_start_ms": start,
                "materialized_rows": len(snapshot.rows()),
                "full_history_or_source_authenticity_verified": False,
            },
            "KELTNER_PROOF_WRAPPER_PROVENANCE",
        )
        report = KeltnerFeatureReport.model_validate(value["keltner_report"])
        audit = check_report(
            report, snapshot, source=source, symbol=symbol, timeframe_s=timeframe_s
        )
        audit.update({"explicit_history_start_ms": start, "data_kind": provenance})
        checks.append(audit)
        command = [
            sys.executable,
            "-m",
            "selective_options.cli",
            "research-keltner",
            "--database",
            str(database),
            "--output",
            str(cli_output),
            "--asset",
            asset,
            "--environment",
            environment,
            "--source",
            source,
            "--symbol",
            symbol,
            "--timeframe-s",
            str(timeframe_s),
            "--as-of-ms",
            str(as_of_ms),
            "--ema-period",
            str(EMA_PERIOD),
            "--atr-period",
            str(ATR_PERIOD),
            "--atr-multiplier",
            str(ATR_MULTIPLIER),
            "--max-latest-age-ms",
            str(MAX_LATEST_AGE_MS),
            "--max-bars",
            str(MAX_BARS),
        ]
        if start is not None:
            command.extend(["--history-start-ms", str(start)])
        completed = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True, timeout=60, check=False
        )
        require(completed.returncode == 0, "KELTNER_PROOF_CLI_FAILED")
        require(
            json.loads(completed.stdout) == value
            and json.loads((cli_output / "keltner.json").read_text()) == value
            and json.loads((api_output / "keltner.json").read_text()) == value,
            "KELTNER_PROOF_API_CLI_MISMATCH",
        )
        for folder in (api_output, cli_output):
            artifact = folder / "keltner.json"
            artifacts[artifact.relative_to(output).as_posix()] = file_digest(artifact)
    synthetic = golden_snapshot()
    golden = compute_keltner_features(
        synthetic,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        max_latest_age_ms=MAX_LATEST_AGE_MS,
        max_bars=MAX_BARS,
    )
    golden_audit = check_report(
        golden, synthetic, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
    )
    _json_write(
        output / "synthetic-golden.json",
        {
            "data_kind": "synthetic_fixture",
            "audit": golden_audit,
            "report": golden.model_dump(mode="json"),
        },
    )
    artifacts["synthetic-golden.json"] = file_digest(output / "synthetic-golden.json")
    require(
        file_digest(database) == expected_source_sha256
        and not database.with_suffix(database.suffix + ".wal").exists(),
        "KELTNER_PROOF_SOURCE_CHANGED",
    )
    require(code_hashes() == before_code, "KELTNER_PROOF_SOURCE_CODE_CHANGED")
    result: dict[str, object] = {
        "schema_version": 1,
        "verification_passed": True,
        "source_database_sha256": expected_source_sha256,
        "source_mutated": False,
        "source_code_sha256": before_code,
        "source_checks": checks,
        "synthetic_golden": golden_audit,
        "artifacts_sha256": artifacts,
        "native_numeric_channel_established": False,
        "sustained_history_verified": False,
        "current_feed_health_verified": False,
        "ready_for_signals": False,
        "regime_classification_complete": False,
        "trading_enabled": False,
    }
    _json_write(output / "verification.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-source-sha256", default=SOURCE_SHA256)
    args = parser.parse_args()
    result = verify(args.database, args.output, expected_source_sha256=args.expected_source_sha256)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
