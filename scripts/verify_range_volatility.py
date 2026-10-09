"""Bounded saved-bar replay and Decimal OHLC audit through real API/CLI.

The default source is an immutable native Kraken capture. Its explicitly scoped
two complete five-minute bars establish window 2 arithmetic, while windows 3/20
remain WARMUP. The original partial prefix is separately retained and refused.
The three-bar analytical sample is synthetic and never certifies native history,
7/14/30-day coverage, source health, a strategy, or trading readiness.
"""

import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Literal

from pydantic import JsonValue

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.events import Environment, MarketEvent, PointInTimeSnapshot, make_event
from selective_options.range_volatility import RangeVolatilityReport, compute_range_volatility
from selective_options.range_volatility_research import research_range_volatility
from selective_options.research import MAX_SOURCE_BYTES, _path, load_bar_snapshot

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence/ingestion-pilot-04/events.duckdb"
SOURCE_SHA256 = "691fd0d9e9b36544b4844d3743fe5941c144d7b7e365ee695872d5660891393b"
CUTOFF = 1790002505185
HISTORY_START = 1790002200000
WINDOWS = (2, 3, 20)
ANNUALIZATION = float(365 * 24 * 12)
MAX_BARS = 2000
MAX_LATEST_AGE_MS = 600_000
ESTIMATORS = ("parkinson", "garman_klass")
Provenance = Literal["saved_native_capture", "synthetic_fixture"]


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError(reason)


def code_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): file_digest(_path(path))
        for path in sorted((ROOT / "src/selective_options").glob("*.py"))
    }


def _history_hash(rows: tuple[MarketEvent, ...]) -> str:
    return hashlib.sha256(
        json.dumps(
            [[row.event_id, row.event_time_ms, row.observed_at_ms] for row in rows],
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


def decimal_reference(rows: tuple[MarketEvent, ...], estimator: str) -> tuple[Decimal, Decimal]:
    """Independent primary-paper arithmetic on decoded OHLC, with 100-digit precision."""
    if not rows or estimator not in ESTIMATORS:
        raise ValueError("reference requires nonempty bars and a named estimator")
    with localcontext() as context:
        context.prec = 100
        contributions: list[Decimal] = []
        log_two = Decimal(2).ln()
        for row in rows:
            payload = row.payload()
            opening, high, low, close = (
                Decimal(str(payload[name])) for name in ("open", "high", "low", "close")
            )
            range_log = (high / low).ln()
            body_log = (close / opening).ln()
            squared_range = range_log * range_log
            contribution = (
                squared_range / (4 * log_two)
                if estimator == "parkinson"
                else squared_range / 2 - (2 * log_two - 1) * body_log * body_log
            )
            require(contribution >= 0, "RANGE_PROOF_REFERENCE_NEGATIVE_VARIANCE")
            contributions.append(contribution)
        variance = sum(contributions, Decimal(0)) / len(rows)
        return +variance, (variance * Decimal(str(ANNUALIZATION))).sqrt()


def _close(actual: float | None, expected: Decimal) -> bool:
    value = float(expected)
    return (
        actual is not None
        and math.isfinite(value)
        and math.isclose(actual, value, rel_tol=1e-12, abs_tol=0.0)
    )


def _price(value: JsonValue) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise IntegrityError("RANGE_PROOF_NATIVE_OHLC_OR_INTERVAL")
    try:
        parsed = float(value)
    except OverflowError as exc:
        raise IntegrityError("RANGE_PROOF_NATIVE_OHLC_OR_INTERVAL") from exc
    require(math.isfinite(parsed) and parsed > 0, "RANGE_PROOF_NATIVE_OHLC_OR_INTERVAL")
    return parsed


def check_report(
    report: RangeVolatilityReport,
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
) -> dict[str, object]:
    """Independently check membership, both clocks, refusal, window hash and formulas."""
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
        "RANGE_PROOF_DUPLICATE_OR_TIED_INPUT",
    )
    for row in rows:
        try:
            canonical = make_event(
                source=row.source,
                environment=row.environment,
                asset=row.asset,
                symbol=row.symbol,
                kind="bar",
                event_time_ms=row.event_time_ms,
                observed_at_ms=row.observed_at_ms,
                payload=row.payload(),
            )
        except (ValueError, TypeError) as exc:
            raise IntegrityError("RANGE_PROOF_NATIVE_IDENTITY_OR_CLOCKS") from exc
        require(
            max(row.event_time_ms, row.observed_at_ms) <= snapshot.as_of_ms
            and row.asset == snapshot.asset
            and row.environment == snapshot.environment
            and row == canonical,
            "RANGE_PROOF_NATIVE_IDENTITY_OR_CLOCKS",
        )
        body = row.payload()
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
            "RANGE_PROOF_NATIVE_OHLC_OR_INTERVAL",
        )
    blockers = []
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
    base = report.base
    require(
        report.formula_version == "range-volatility-v1"
        and report.windows == WINDOWS
        and report.state == ("UNAVAILABLE" if blockers else "RANGE_VOLATILITY_COMPUTED")
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
        and base.history_budget == MAX_BARS
        and base.max_latest_age_ms == MAX_LATEST_AGE_MS
        and base.annualization_periods_per_year == ANNUALIZATION
        and base.history_sha256 == _history_hash(rows)
        and base.first_close_ms == (rows[0].event_time_ms if rows else None)
        and base.latest_close_ms == (rows[-1].event_time_ms if rows else None)
        and base.latest_bar_age_ms == (snapshot.as_of_ms - rows[-1].event_time_ms if rows else None)
        and report.input_event_ids == (() if blockers else tuple(row.event_id for row in rows)),
        "RANGE_PROOF_SCOPE_OR_HISTORY",
    )
    require(
        report.windows_are_bar_counts
        and report.zero_drift_diffusion_assumed
        and not any(
            (
                report.zero_drift_verified,
                report.continuous_extrema_verified,
                report.opening_jumps_modelled,
                report.sampling_bias_corrected,
                report.annualization_calendar_verified,
                report.current_feed_health_verified,
                report.source_frontier_complete,
                report.ready_for_signals,
                report.trading_enabled,
                base.history_truncated,
                base.source_frontier_complete,
                base.ready_for_signals,
                base.trading_enabled,
            )
        ),
        "RANGE_PROOF_UNSUPPORTED_APPROVAL",
    )
    expected_names = tuple(
        name
        for window in WINDOWS
        for estimator in ESTIMATORS
        for name in (f"{estimator}_variance_per_bar_{window}", f"{estimator}_vol_{window}")
    )
    require(
        tuple(feature.name for feature in report.features) == expected_names
        and len(report.window_references) == len(WINDOWS),
        "RANGE_PROOF_OUTPUT_MEMBERSHIP",
    )
    checked = []
    for window, reference in zip(WINDOWS, report.window_references, strict=True):
        selected = rows[-window:] if not blockers and len(rows) >= window else ()
        require(
            reference.window == window
            and reference.bars_available == len(rows)
            and reference.bars_used == len(selected)
            and reference.first_open_ms
            == (selected[0].payload()["open_time_ms"] if selected else None)
            and reference.first_close_ms == (selected[0].event_time_ms if selected else None)
            and reference.latest_close_ms == (selected[-1].event_time_ms if selected else None)
            and reference.history_sha256 == (_history_hash(selected) if selected else None),
            "RANGE_PROOF_WINDOW_REFERENCE",
        )
        expected_state = "UNAVAILABLE" if blockers else "AVAILABLE" if selected else "WARMUP"
        expected_reason = (
            blockers[0] if blockers else None if selected else "INSUFFICIENT_CLOSED_BARS"
        )
        values = {}
        for estimator in ESTIMATORS:
            raw = report.feature(f"{estimator}_variance_per_bar_{window}")
            annualized = report.feature(f"{estimator}_vol_{window}")
            require(
                raw.state == annualized.state == expected_state
                and raw.reason == annualized.reason == expected_reason
                and raw.bars_required == annualized.bars_required == window
                and raw.unit == "log_return_variance_per_bar"
                and annualized.unit == "annualized_log_return_std",
                "RANGE_PROOF_FEATURE_STATE_OR_UNIT",
            )
            if selected:
                variance, volatility = decimal_reference(selected, estimator)
                require(
                    _close(raw.value, variance) and _close(annualized.value, volatility),
                    "RANGE_PROOF_DECIMAL_FORMULA",
                )
                values[estimator] = {
                    "decimal_variance_per_bar": str(variance),
                    "decimal_annualized_volatility": str(volatility),
                    "reported_variance_per_bar": raw.value,
                    "reported_annualized_volatility": annualized.value,
                }
            else:
                require(
                    raw.value is None and annualized.value is None, "RANGE_PROOF_WITHHELD_VALUE"
                )
        checked.append(
            {"window": window, "state": expected_state, "reason": expected_reason, "values": values}
        )
    return {
        "materialized_rows": len(snapshot.rows()),
        "selected_bars": len(rows),
        "complete_bars": sum(row.payload()["complete"] is True for row in rows),
        "partial_bars": sum(not row.payload()["complete"] for row in rows),
        "blockers": blockers,
        "windows": checked,
        "preserved_rows": [row.model_dump(mode="json") for row in rows],
    }


def golden_snapshot() -> PointInTimeSnapshot:
    """Explicit analytical fixture: three known OHLC bars, never native observations."""
    rows = tuple(
        make_event(
            source="synthetic_fixture",
            environment="public_reference",
            asset="BTC",
            symbol="FIXTURE-BTC/USD",
            kind="bar",
            event_time_ms=(index + 1) * 300_000,
            observed_at_ms=(index + 1) * 300_000 + 10,
            payload={
                "open_time_ms": index * 300_000,
                "close_time_ms": (index + 1) * 300_000,
                "timeframe_s": 300,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10.0,
                "trade_count": 5,
                "closed": True,
                "complete": True,
            },
        )
        for index, (opening, high, low, close) in enumerate(
            ((100.0, 110.0, 90.0, 105.0), (105.0, 130.0, 95.0, 120.0), (120.0, 125.0, 80.0, 85.0))
        )
    )
    return PointInTimeSnapshot(rows[-1].observed_at_ms, "BTC", "public_reference", rows)


def _json_write(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")


def _run_cli(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, cwd=ROOT, capture_output=True, text=True, timeout=60, check=False
    )


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
    """Exclusive artifact replay; two source scopes are explicit and never reseeded."""
    # Bound and validate before file access or output creation.
    if (
        type(expected_source_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", expected_source_sha256) is None
        or provenance not in ("saved_native_capture", "synthetic_fixture")
        or type(as_of_ms) is not int
        or not 0 < as_of_ms < 1 << 63
        or type(timeframe_s) is not int
        or timeframe_s != 300
        or type(history_start_ms) is not int
        or not 0 < history_start_ms <= as_of_ms
    ):
        raise ValueError("proof requires explicit digest, provenance and inclusive history start")
    compute_range_volatility(
        PointInTimeSnapshot(as_of_ms, asset, environment, ()),
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        windows=WINDOWS,
        max_latest_age_ms=MAX_LATEST_AGE_MS,
        max_bars=MAX_BARS,
        annualization_periods_per_year=ANNUALIZATION,
    )
    database, output = _path(database), _path(output, must_exist=False)
    if output.exists() or output.is_relative_to(database.parent):
        raise ValueError("proof requires fresh output outside the original source directory")
    require(database.stat().st_size <= MAX_SOURCE_BYTES, "RANGE_PROOF_SOURCE_BYTE_BUDGET")
    require(file_digest(database) == expected_source_sha256, "RANGE_PROOF_SOURCE_HASH")
    require(
        not database.with_suffix(database.suffix + ".wal").exists(), "RANGE_PROOF_SOURCE_NOT_CLOSED"
    )
    before_code = code_hashes()
    verifier_digest = file_digest(_path(Path(__file__)))
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
        require(digest == expected_source_sha256, "RANGE_PROOF_LOADER_SOURCE_HASH")
        scopes.append((start, snapshot))
    output.mkdir(parents=True, exist_ok=False)
    checks = []
    artifacts = {}
    for index, (start, snapshot) in enumerate(scopes):
        api_output = output / f"api-{index}"
        result = research_range_volatility(
            database,
            api_output,
            asset=asset,
            environment=environment,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            as_of_ms=as_of_ms,
            windows=WINDOWS,
            max_latest_age_ms=MAX_LATEST_AGE_MS,
            max_bars=MAX_BARS,
            annualization_periods_per_year=ANNUALIZATION,
            history_start_ms=start,
        )
        require(
            result["source_database_sha256"] == expected_source_sha256
            and result["source_mutated"] is False
            and result["ready_for_signals"] is False
            and result["trading_enabled"] is False
            and result["source_scope_and_cutoff"]
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
            "RANGE_PROOF_WRAPPER_PROVENANCE",
        )
        report = RangeVolatilityReport.model_validate(result["range_volatility_report"])
        checked = check_report(
            report, snapshot, source=source, symbol=symbol, timeframe_s=timeframe_s
        )
        checked["explicit_history_start_ms"] = start
        checked["data_kind"] = provenance
        checks.append(checked)
        cli_output = output / f"cli-{index}"
        command = [
            sys.executable,
            "-m",
            "selective_options.cli",
            "research-range-volatility",
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
            "--windows",
            *(str(window) for window in WINDOWS),
            "--max-latest-age-ms",
            str(MAX_LATEST_AGE_MS),
            "--max-bars",
            str(MAX_BARS),
            "--annualization-periods-per-year",
            str(ANNUALIZATION),
        ]
        if start is not None:
            command.extend(("--history-start-ms", str(start)))
        completed = _run_cli(command)
        require(completed.returncode == 0, "RANGE_PROOF_ACTUAL_CLI_FAILED")
        require(json.loads(completed.stdout) == result, "RANGE_PROOF_CLI_API_MISMATCH")
        for directory in (api_output, cli_output):
            path = directory / "range-volatility.json"
            require(
                json.loads(path.read_text(encoding="utf-8")) == result,
                "RANGE_PROOF_PUBLISHED_REPORT_MISMATCH",
            )
            artifacts[path.relative_to(output).as_posix()] = file_digest(path)
    fixture = golden_snapshot()
    golden = compute_range_volatility(
        fixture,
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        windows=WINDOWS,
        max_latest_age_ms=MAX_LATEST_AGE_MS,
        max_bars=MAX_BARS,
        annualization_periods_per_year=ANNUALIZATION,
    )
    golden_checks = check_report(
        golden, fixture, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
    )
    golden_value = {
        "data_kind": "synthetic_analytical_golden",
        "checks": golden_checks,
        "range_volatility_report": golden.model_dump(mode="json"),
    }
    _json_write(output / "synthetic-golden.json", golden_value)
    artifacts["synthetic-golden.json"] = file_digest(output / "synthetic-golden.json")
    require(
        file_digest(database) == expected_source_sha256
        and not database.with_suffix(database.suffix + ".wal").exists(),
        "RANGE_PROOF_ORIGINAL_SOURCE_CHANGED",
    )
    require(
        code_hashes() == before_code and file_digest(Path(__file__)) == verifier_digest,
        "RANGE_PROOF_EXECUTED_CODE_CHANGED",
    )
    value: dict[str, object] = {
        "status": "SAVED_BAR_RANGE_VOLATILITY_AUDIT_PASSED",
        "source_database": str(database),
        "source_database_sha256": expected_source_sha256,
        "data_kind": provenance,
        "as_of_ms": as_of_ms,
        "source_scopes": checks,
        "synthetic_golden": {"data_kind": "synthetic_analytical_golden", "checks": golden_checks},
        "windows_are_bar_counts": True,
        "annualization_periods_per_year": ANNUALIZATION,
        "annualization_assumption": "Explicit 365 days/year * 24 hours/day * 12 five-minute bars/hour; not a verified market calendar",
        "annualization_calendar_verified": False,
        "decimal_reference_precision": 100,
        "decimal_comparison_relative_tolerance": 1e-12,
        "decimal_comparison_absolute_tolerance": 0.0,
        "result_artifact_sha256": artifacts,
        "tested_source_sha256": before_code,
        "verifier_sha256": verifier_digest,
        "research_api_runs": 2,
        "actual_cli_runs": 2,
        "source_mutated": False,
        "source_clocks_payloads_ids_preserved": True,
        "new_public_requests": 0,
        "orders_submitted": 0,
        "new_strategy_trials": 0,
        "historical_strategy_validation": False,
        "complete_7_14_30_day_history_verified": False,
        "current_feed_health_verified": False,
        "milestone_2_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    _json_write(output / "verification.json", value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE)
    parser.add_argument("--source-sha256", default=SOURCE_SHA256)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--asset", default="BTC")
    parser.add_argument(
        "--environment", choices=("public_reference", "testnet"), default="public_reference"
    )
    parser.add_argument("--source", default="kraken")
    parser.add_argument("--symbol", default="BTC/USD")
    parser.add_argument("--timeframe-s", type=int, choices=(300,), default=300)
    parser.add_argument("--as-of-ms", type=int, default=CUTOFF)
    parser.add_argument("--history-start-ms", type=int, default=HISTORY_START)
    parser.add_argument(
        "--provenance",
        choices=("saved_native_capture", "synthetic_fixture"),
        default="saved_native_capture",
    )
    args = parser.parse_args()
    result = verify(
        args.database,
        args.output,
        expected_source_sha256=args.source_sha256,
        asset=args.asset,
        environment=args.environment,
        source=args.source,
        symbol=args.symbol,
        timeframe_s=args.timeframe_s,
        as_of_ms=args.as_of_ms,
        history_start_ms=args.history_start_ms,
        provenance=args.provenance,
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "actual_cli_runs": result["actual_cli_runs"],
                "trading_enabled": False,
            }
        )
    )


if __name__ == "__main__":
    main()
