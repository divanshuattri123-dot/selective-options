"""Replay discrete native delta risk-reversal research against a frozen saved cohort."""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.models import Instrument, Ticker
from selective_options.option_coverage import Universe
from selective_options.option_skew_research import research_option_skew
from selective_options.options_research import load_options_snapshots

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "evidence" / "reused-validation-public-01" / "events.duckdb"
OUTPUT = ROOT / "evidence" / "options-skew-saved-native-01"
AS_OF_MS = 1791050702961
DATABASE_SHA256 = "97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc"
POLICY = {
    "asset": "BTC",
    "as_of_ms": AS_OF_MS,
    "target_absolute_delta": 0.25,
    "maximum_delta_error": 0.05,
    "max_quote_age_ms": 10_000,
    "max_discovery_age_ms": 100_000,
    "max_clock_skew_ms": 10_000,
    "max_underlying_mismatch_fraction": 0.1,
    "max_rows": 100_000,
    "max_expiries": 128,
}


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise IntegrityError(code)


def _code_hashes() -> dict[str, str]:
    paths = [
        ROOT / "src" / "selective_options" / name
        for name in (
            "options_skew.py",
            "option_skew_research.py",
            "options_research.py",
            "options_features.py",
        )
    ]
    paths.append(Path(__file__).resolve())
    return {path.relative_to(ROOT).as_posix(): file_digest(path) for path in sorted(paths)}


def _latest_by_source_time(rows: list[Any]) -> Any:
    latest = max(row.event_time_ms for row in rows)
    choices = [row for row in rows if row.event_time_ms == latest]
    _require(len(choices) == 1, "VERIFY_SKEW_SOURCE_TIME_AMBIGUOUS")
    return choices[0]


def main() -> int:
    _require(file_digest(DATABASE) == DATABASE_SHA256, "VERIFY_SKEW_SOURCE_CHANGED")
    snapshot, discovery, source_digest = load_options_snapshots(
        DATABASE, asset="BTC", as_of_ms=AS_OF_MS, max_rows=100_000
    )
    _require(source_digest == DATABASE_SHA256, "VERIFY_SKEW_SOURCE_DIGEST")
    rows = {row.event_id: row for row in (*snapshot.rows(), *discovery.rows())}
    manifests = [
        row
        for row in discovery.rows()
        if row.kind == "status"
        and row.payload().get("reason") == "UNIVERSE_DISCOVERED"
        and row.payload().get("asset") == "BTC"
    ]
    _require(bool(manifests), "VERIFY_SKEW_MANIFEST_MISSING")
    manifest = max(manifests, key=lambda row: row.observed_at_ms)
    universe = Universe.model_validate(manifest.payload())
    _require(universe.coverage == "FULL", "VERIFY_SKEW_SOURCE_UNIVERSE_PARTIAL")
    metadata = {
        identity: Instrument.model_validate_json(rows[identity].payload_json, strict=True)
        for identity in universe.metadata_event_ids
    }
    by_symbol: dict[str, list[Any]] = {}
    for row in snapshot.rows(kind="ticker"):
        by_symbol.setdefault(row.symbol, []).append(row)

    if OUTPUT.exists():
        report_value = json.loads((OUTPUT / "risk-reversal.json").read_text(encoding="utf-8"))
        _require(
            report_value.get("source_database_sha256") == DATABASE_SHA256
            and report_value.get("source_scope_and_cutoff", {}).get("as_of_ms") == AS_OF_MS
            and report_value.get("policy", {}).get("target_absolute_delta") == 0.25,
            "VERIFY_SKEW_EXISTING_REPORT_SCOPE",
        )
    else:
        report_value = research_option_skew(
            DATABASE,
            OUTPUT,
            asset="BTC",
            as_of_ms=AS_OF_MS,
            target_absolute_delta=0.25,
            maximum_delta_error=0.05,
            max_quote_age_ms=10_000,
            max_discovery_age_ms=100_000,
            max_clock_skew_ms=10_000,
            max_underlying_mismatch_fraction=0.1,
            max_rows=100_000,
            max_expiries=128,
        )
    report_path = OUTPUT / "risk-reversal.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    _require(report == report_value, "VERIFY_SKEW_REPORT_RELOAD")
    feature = report["risk_reversal_report"]
    _require(
        feature["state"] == "PARTIAL"
        and feature["blockers"] == ["INCOMPLETE_EXPIRY_RISK_REVERSALS"],
        "VERIFY_SKEW_EXPECTED_PARTIAL_STATE",
    )
    _require(
        feature["manifest_event_id"] == manifest.event_id
        and feature["discovered_members"] == universe.available
        and feature["discovery_coverage"] == "FULL"
        and feature["input_sha256"]
        and feature["trading_enabled"] is False,
        "VERIFY_SKEW_DISCOVERY_OR_DISABLED_FLAGS",
    )
    available = [expiry for expiry in feature["expiries"] if expiry["state"] == "AVAILABLE"]
    unavailable = [expiry for expiry in feature["expiries"] if expiry["state"] == "UNAVAILABLE"]
    _require(len(available) == 12 and len(unavailable) == 1, "VERIFY_SKEW_EXPIRY_COUNTS")
    _require(
        unavailable[0]["reason"] == "PUT_DELTA_SIGN_INVALID",
        "VERIFY_SKEW_INVALID_MEMBER_NOT_EXPOSED",
    )
    checked = []
    target = Fraction("0.25")
    tolerance = Fraction("0.05")
    for expiry in available:
        expiry_ms = expiry["expiration_timestamp"]
        expected_members = {
            symbol
            for symbol, identity in zip(
                universe.instruments, universe.metadata_event_ids, strict=True
            )
            if metadata[identity].expiration_timestamp == expiry_ms
        }
        _require(set(expiry["members"]) == expected_members, "VERIFY_SKEW_MEMBER_SET")
        candidates: dict[str, list[tuple[Fraction, str, Any, Any]]] = {"call": [], "put": []}
        for symbol in sorted(expected_members):
            instrument = next(
                value for value in metadata.values() if value.instrument_name == symbol
            )
            history = by_symbol.get(symbol, [])
            _require(bool(history), "VERIFY_SKEW_MEMBER_QUOTE_MISSING")
            quote_row = _latest_by_source_time(history)
            _require(
                max(AS_OF_MS - quote_row.event_time_ms, AS_OF_MS - quote_row.observed_at_ms)
                <= POLICY["max_quote_age_ms"]
                and abs(quote_row.event_time_ms - quote_row.observed_at_ms)
                <= POLICY["max_clock_skew_ms"]
                and quote_row.observed_at_ms >= manifest.observed_at_ms,
                "VERIFY_SKEW_MEMBER_CLOCKS",
            )
            quote = Ticker.model_validate_json(quote_row.payload_json, strict=True)
            delta = Fraction(str(quote.greeks.delta))
            if instrument.option_type == "call":
                _require(delta > 0, "VERIFY_SKEW_CALL_DELTA_SIGN")
                error = abs(delta - target)
            else:
                _require(
                    instrument.option_type == "put" and delta < 0, "VERIFY_SKEW_PUT_DELTA_SIGN"
                )
                error = abs(delta + target)
            _require(0 < quote.mark_iv, "VERIFY_SKEW_MEMBER_MARK_INVALID")
            candidates[instrument.option_type].append((error, symbol, instrument, quote_row))
        selected: dict[str, tuple[Fraction, str, Any, Any]] = {}
        for side in ("call", "put"):
            ranked = sorted(
                (row for row in candidates[side] if row[0] <= tolerance),
                key=lambda row: (row[0], row[1]),
            )
            _require(bool(ranked), "VERIFY_SKEW_DELTA_TARGET_UNAVAILABLE")
            _require(
                len(ranked) == 1 or ranked[0][0] != ranked[1][0],
                "VERIFY_SKEW_TIE_IN_AVAILABLE_EXPIRY",
            )
            selected[side] = ranked[0]
        for side in ("call", "put"):
            selected_meta = selected[side][2]
            reported = expiry[side]
            quote_row = rows[reported["quote_event_id"]]
            native_quote = Ticker.model_validate_json(quote_row.payload_json, strict=True)
            _require(
                reported["symbol"] == selected[side][1]
                and reported["metadata_event_id"]
                == next(
                    identity
                    for identity, value in metadata.items()
                    if value.instrument_name == selected[side][1]
                )
                and reported["quote_event_id"] == selected[side][3].event_id == quote_row.event_id
                and reported["delta"] == native_quote.greeks.delta
                and reported["mark_iv_points"] == native_quote.mark_iv
                and reported["quote_source_time_ms"] == quote_row.event_time_ms
                and reported["quote_observed_at_ms"] == quote_row.observed_at_ms
                and selected_meta.option_type == side,
                "VERIFY_SKEW_NATIVE_SELECTION_BINDING",
            )
        call_iv = Decimal(str(expiry["call"]["mark_iv_points"]))
        put_iv = Decimal(str(expiry["put"]["mark_iv_points"]))
        expected_rr = float(call_iv - put_iv)
        _require(expiry["risk_reversal_points"] == expected_rr, "VERIFY_SKEW_RR_ARITHMETIC")
        checked.append(
            {
                "expiration_timestamp": expiry_ms,
                "call_symbol": expiry["call"]["symbol"],
                "call_delta": expiry["call"]["delta"],
                "call_iv_points": expiry["call"]["mark_iv_points"],
                "put_symbol": expiry["put"]["symbol"],
                "put_delta": expiry["put"]["delta"],
                "put_iv_points": expiry["put"]["mark_iv_points"],
                "risk_reversal_points": expected_rr,
            }
        )
    _require(
        feature["full_chain_verified"] is False
        and feature["current_feed_health_verified"] is False
        and feature["executable_quotes_verified"] is False
        and feature["interpolated"] is False
        and feature["ready_for_signals"] is False
        and report["trading_enabled"] is False,
        "VERIFY_SKEW_UNSUPPORTED_APPROVAL",
    )
    validation = {
        "status": "SAVED_NATIVE_OPTIONS_SKEW_VERIFIED_PARTIAL",
        "data_kind": "saved_deribit_testnet_cohort_offline_replay",
        "network_requests": 0,
        "orders": 0,
        "source_database_sha256": source_digest,
        "source_unchanged": file_digest(DATABASE) == source_digest,
        "source_database_rows_supplied": len(snapshot.rows()) + len(discovery.rows()),
        "discovered_full_members": universe.available,
        "as_of_ms": AS_OF_MS,
        "available_expiries": len(available),
        "unavailable_expiries": [
            {
                "expiration_timestamp": row["expiration_timestamp"],
                "reason": row["reason"],
            }
            for row in unavailable
        ],
        "checked_native_pairs": checked,
        "implementation_sha256": _code_hashes(),
        "limitations": [
            "One historical snapshot cohort; not current feed health or a time series.",
            "Discrete marks at the closest available signed delta within the declared tolerance; no interpolation.",
            "One expiry is withheld because its discovered put member reports an invalid positive delta.",
            "Mark-IV arithmetic only; spread, execution, Greeks validity, fit, signal, strategy and Gate 2 are not established.",
            "Risk reversal is call mark IV minus put mark IV in native percentage points; positive/negative signs are descriptive only.",
        ],
        "trading_enabled": False,
    }
    (OUTPUT / "verification.json").write_text(
        json.dumps(validation, indent=2) + "\n", encoding="utf-8"
    )
    _require(file_digest(DATABASE) == source_digest, "VERIFY_SKEW_SOURCE_MUTATED")
    print(json.dumps(validation, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
