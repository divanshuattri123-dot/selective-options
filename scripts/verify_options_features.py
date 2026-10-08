"""Bounded offline saved-native BTC ATM-IV proof through research API and real CLI.

Uses preserved evidence only; does not collect data, register a strategy trial,
place orders, or establish a complete chain/current feed/milestone validation.
"""

import argparse
import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path
from typing import Literal

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.models import Instrument, Ticker
from selective_options.option_coverage import Universe
from selective_options.options_features import OptionsFeatureReport
from selective_options.options_research import load_options_snapshots, research_options
from selective_options.research import _path

ROOT = Path(__file__).resolve().parents[1]
CUTOFF = 1791050702961
SOURCE_SHA = "97c10f3c2f59a67edff83104fceca3b0b195b1498432597abde62e509f4cabdc"
BASELINE_SHA = "bfcebe34564f69b2816e8b646cffa552bdd91b807b641ef021930768db8bf9f8"
MANIFEST_ID = "2c23e7e290a0051c15fcb72c08f334c152bce605c2e704264faec9ece85257fa"
MAX_ROWS = 100000


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError(reason)


def code_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): file_digest(_path(path))
        for path in sorted((ROOT / "src/selective_options").glob("*.py"))
    }


def _check_report(
    report: OptionsFeatureReport,
    rows: tuple[MarketEvent, ...],
    manifest: MarketEvent,
    universe: Universe,
    side: Literal["call", "put"],
) -> dict[str, object]:
    by_id = {row.event_id: row for row in rows}
    metadata = {
        identity: Instrument.model_validate(by_id[identity].payload())
        for identity in universe.metadata_event_ids
    }
    relevant = {
        identity: model for identity, model in metadata.items() if model.option_type == side
    }
    expected_expiries = {model.expiration_timestamp for model in relevant.values()}
    quote_history: dict[str, list[MarketEvent]] = {}
    for row in rows:
        if row.kind == "ticker":
            quote_history.setdefault(row.symbol, []).append(row)
    _require(
        report.asset == "BTC"
        and report.environment == "testnet"
        and report.as_of_ms == CUTOFF
        and report.formula_version == "options-term-v1",
        "PROOF_REPORT_SCOPE",
    )
    _require(report.manifest_event_id == manifest.event_id, "PROOF_MANIFEST_BINDING")
    _require(
        report.manifest_observed_at_ms == manifest.observed_at_ms
        and report.discovery_declared_coverage == universe.coverage,
        "PROOF_NATIVE_DISCOVERY_CHANGED",
    )
    _require(
        report.policy.model_dump()
        == {
            "option_type": side,
            "moneyness_method": "native_underlying_log_distance",
            "tie_policy": "exact_distance_refuse",
            "quote_order": "source_timestamp_ties_refuse",
            "max_quote_age_ms": 10000,
            "max_discovery_age_ms": 100000,
            "max_atm_log_moneyness": 0.2,
            "max_underlying_mismatch_fraction": 0.1,
            "flat_threshold_iv_points": 1.0,
            "max_rows": MAX_ROWS,
            "max_expiries": 128,
        },
        "PROOF_EXPLICIT_POLICY_CHANGED",
    )
    _require(
        0 <= CUTOFF - manifest.observed_at_ms <= 100000,
        "PROOF_NATIVE_DISCOVERY_AGE",
    )
    _require(
        report.discovered_members == 1010 and len(expected_expiries) == 13,
        "PROOF_NATIVE_UNIVERSE_SIZE",
    )
    _require(
        report.state == "DISCRETE_TERM_COMPUTED" and not report.blockers,
        "PROOF_OPTIONS_UNAVAILABLE",
    )
    _require(
        len(report.expiries) == 13
        and tuple(row.expiration_timestamp for row in report.expiries)
        == tuple(sorted(expected_expiries)),
        "PROOF_EXPIRY_SCOPE",
    )
    _require(
        not report.full_chain_verified
        and not report.ready_for_signals
        and not report.trading_enabled
        and not report.current_feed_health_verified
        and not report.interpolated,
        "PROOF_UNSUPPORTED_APPROVAL",
    )
    chosen = []
    for expiry in report.expiries:
        _require(expiry.state == "AVAILABLE" and expiry.reason is None, "PROOF_EXPIRY_UNAVAILABLE")
        if expiry.quote_event_id is None or expiry.metadata_event_id is None:
            raise IntegrityError("PROOF_NATIVE_CHOSEN_ID_MISSING")
        quote_row = by_id[expiry.quote_event_id]
        quote = Ticker.model_validate(quote_row.payload())
        instrument = metadata[expiry.metadata_event_id]
        members = {
            model.instrument_name
            for model in relevant.values()
            if model.expiration_timestamp == expiry.expiration_timestamp
        }
        _require(
            set(expiry.members) == members and expiry.eligible_quotes == len(members),
            "PROOF_EXPIRY_MEMBERSHIP",
        )
        _require(
            expiry.symbol == quote_row.symbol == instrument.instrument_name
            and instrument.option_type == side,
            "PROOF_NATIVE_SYMBOL",
        )
        _require(
            expiry.mark_iv_points == quote.mark_iv
            and expiry.strike == instrument.strike
            and expiry.underlying_price == quote.underlying_price,
            "PROOF_NATIVE_UNITS_CHANGED",
        )
        _require(
            expiry.underlying_index == quote_row.payload().get("underlying_index"),
            "PROOF_NATIVE_REFERENCE_CHANGED",
        )
        _require(
            expiry.quote_currency == instrument.quote_currency
            and expiry.settlement_currency == instrument.settlement_currency
            and expiry.instrument_type == instrument.instrument_type,
            "PROOF_NATIVE_UNIT_LABEL_CHANGED",
        )
        _require(
            expiry.quote_source_time_ms == quote_row.event_time_ms
            and expiry.quote_observed_at_ms == quote_row.observed_at_ms,
            "PROOF_NATIVE_CLOCKS_CHANGED",
        )
        _require(
            expiry.metadata_observed_at_ms == by_id[expiry.metadata_event_id].observed_at_ms,
            "PROOF_METADATA_RECEIPT_CHANGED",
        )
        # Independent logarithmic comparison on native positive numbers. Native
        # cohort has a strict margin; no reproduction of rational tie algorithm.
        alternatives = []
        for identity, model in relevant.items():
            if model.expiration_timestamp != expiry.expiration_timestamp:
                continue
            history = quote_history[model.instrument_name]
            latest = max(row.event_time_ms for row in history)
            candidates = [row for row in history if row.event_time_ms == latest]
            _require(len(candidates) == 1, "PROOF_NATIVE_TIMESTAMP_AMBIGUOUS")
            native_quote = Ticker.model_validate(candidates[0].payload())
            _require(
                manifest.observed_at_ms <= candidates[0].observed_at_ms <= CUTOFF
                and 0 <= CUTOFF - candidates[0].event_time_ms <= 10000
                and 0 <= CUTOFF - candidates[0].observed_at_ms <= 10000,
                "PROOF_NATIVE_CANDIDATE_AGE",
            )
            distance = abs(math.log(model.strike / native_quote.underlying_price))
            alternatives.append((distance, model.instrument_name, candidates[0].event_id, identity))
        ordered = sorted(alternatives)
        _require(
            ordered[0][1] == expiry.symbol and ordered[0][2] == expiry.quote_event_id,
            "PROOF_ATM_ANALYTIC_SELECTION",
        )
        if len(ordered) > 1:
            _require(ordered[1][0] - ordered[0][0] > 1e-12, "PROOF_NATIVE_ATM_MARGIN")
        _require(
            math.isclose(ordered[0][0], expiry.log_moneyness_distance or 0, abs_tol=1e-12),
            "PROOF_MONEYNESS_VALUE",
        )
        _require(
            quote_row.event_time_ms <= CUTOFF and quote_row.observed_at_ms <= CUTOFF,
            "PROOF_LOOKAHEAD",
        )
        chosen.append(expiry.model_dump(mode="json"))
    if side == "call":
        _require(
            report.expiries[0].symbol == "BTC-4OCT26-85000-C"
            and report.expiries[0].mark_iv_points == 12.28
            and report.expiries[0].underlying_price == 84976.58,
            "PROOF_KNOWN_NATIVE_FRONT_CALL",
        )
    native_ivs = [
        Ticker.model_validate(by_id[row.quote_event_id].payload()).mark_iv
        for row in report.expiries
        if row.quote_event_id is not None
    ]
    native_differences = tuple(
        right - left for left, right in zip(native_ivs, native_ivs[1:], strict=False)
    )
    _require(
        report.adjacent_iv_differences_points == native_differences, "PROOF_NATIVE_IV_DIFFERENCES"
    )
    _require(
        native_ivs
        == [
            12.28,
            20.53,
            24.05,
            25.29,
            28.71,
            30.8,
            31.87,
            32.83,
            35.44,
            36.52,
            37.5,
            38.54,
            38.91,
        ],
        "PROOF_KNOWN_NATIVE_IV_POINTS",
    )
    # These native marks rise monotonically, with both >one-point rises and
    # <=one-point FLAT increments. The explicit discrete policy calls that MIXED;
    # it does not imply a falling segment or a fitted curve's directional shape.
    _require(
        any(value > 1 for value in native_differences)
        and all(value > 0 for value in native_differences)
        and any(value <= 1 for value in native_differences)
        and report.policy.flat_threshold_iv_points == 1
        and report.term_shape == "MIXED",
        "PROOF_KNOWN_NATIVE_TERM_SHAPE",
    )
    return {
        "option_type": side,
        "term_shape": report.term_shape,
        "adjacent_iv_differences_points": list(report.adjacent_iv_differences_points),
        "same_type_members": len(relevant),
        "input_sha256": report.input_sha256,
        "policy": report.policy.model_dump(mode="json"),
        "expiries": chosen,
    }


def verify(source: Path, output: Path, baseline: Path) -> dict[str, object]:
    source, output, baseline = _path(source), _path(output, must_exist=False), _path(baseline)
    scratch = (ROOT.parents[1] / "work/options-features-0347").resolve()
    evidence = (ROOT / "evidence").resolve()
    if (
        output.exists()
        or output.is_relative_to(source.parent)
        or not (output.is_relative_to(scratch) or output.is_relative_to(evidence))
    ):
        raise ValueError("proof requires new output under named scratch or project evidence")
    _require(file_digest(source) == SOURCE_SHA, "PROOF_SAVED_SOURCE_HASH")
    _require(file_digest(baseline) == BASELINE_SHA, "PROOF_0346_BASELINE_HASH")
    _require(not source.with_suffix(source.suffix + ".wal").exists(), "PROOF_SOURCE_NOT_CLOSED")
    before_code = code_hashes()
    verifier_hash = file_digest(_path(Path(__file__)))
    snapshot, controls, digest = load_options_snapshots(
        source, asset="BTC", as_of_ms=CUTOFF, max_rows=MAX_ROWS
    )
    _require(digest == SOURCE_SHA, "PROOF_LOADER_SOURCE_HASH")
    manifests = [row for row in controls.rows() if row.event_id == MANIFEST_ID]
    _require(len(manifests) == 1, "PROOF_NATIVE_MANIFEST_MISSING")
    manifest = manifests[0]
    universe = Universe.model_validate(manifest.payload())
    _require(
        universe.available == universe.selected == 1010 and universe.coverage == "FULL",
        "PROOF_DECLARED_DISCOVERY_CHANGED",
    )
    _require(len(snapshot.rows()) + len(controls.rows()) <= MAX_ROWS, "PROOF_INPUT_BUDGET")
    supplied = tuple(sorted((*snapshot.rows(), *controls.rows()), key=lambda row: row.event_id))
    history_hash = hashlib.sha256(
        json.dumps(
            [row.model_dump(mode="json") for row in supplied],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    output.mkdir(parents=True, exist_ok=False)
    summaries = []
    artifacts = {}
    for side in ("call", "put"):
        api_output = output / f"api-{side}"
        result = research_options(
            source,
            api_output,
            asset="BTC",
            as_of_ms=CUTOFF,
            option_type=side,
            moneyness_method="native_underlying_log_distance",
            tie_policy="exact_distance_refuse",
            max_quote_age_ms=10000,
            max_discovery_age_ms=100000,
            max_atm_log_moneyness=0.2,
            max_underlying_mismatch_fraction=0.1,
            flat_threshold_iv_points=1.0,
            max_rows=MAX_ROWS,
            max_expiries=128,
        )
        report = OptionsFeatureReport.model_validate(result["options_report"])
        _require(
            report.input_event_ids == tuple(row.event_id for row in supplied)
            and report.input_sha256 == history_hash,
            "PROOF_ALL_INPUT_PROVENANCE",
        )
        summaries.append(_check_report(report, snapshot.rows(), manifest, universe, side))
        cli_output = output / f"cli-{side}"
        command = [
            sys.executable,
            "-m",
            "selective_options.cli",
            "research-options",
            "--database",
            str(source),
            "--output",
            str(cli_output),
            "--asset",
            "BTC",
            "--as-of-ms",
            str(CUTOFF),
            "--option-type",
            side,
            "--moneyness-method",
            "native_underlying_log_distance",
            "--tie-policy",
            "exact_distance_refuse",
            "--max-quote-age-ms",
            "10000",
            "--max-discovery-age-ms",
            "100000",
            "--max-atm-log-moneyness",
            "0.2",
            "--max-underlying-mismatch-fraction",
            "0.1",
            "--flat-threshold-iv-points",
            "1.0",
            "--max-rows",
            str(MAX_ROWS),
            "--max-expiries",
            "128",
        ]
        completed = subprocess.run(
            command, cwd=ROOT, capture_output=True, text=True, timeout=60, check=False
        )
        _require(completed.returncode == 0, "PROOF_ACTUAL_CLI_FAILED")
        _require(json.loads(completed.stdout) == result, "PROOF_CLI_API_REPORT_MISMATCH")
        _require(
            json.loads((api_output / "options-features.json").read_text(encoding="utf-8")) == result
            and json.loads((cli_output / "options-features.json").read_text(encoding="utf-8"))
            == result,
            "PROOF_PUBLISHED_REPORT_MISMATCH",
        )
        for directory in (api_output, cli_output):
            path = directory / "options-features.json"
            artifacts[path.relative_to(output).as_posix()] = file_digest(path)
    _require(
        file_digest(source) == SOURCE_SHA and file_digest(baseline) == BASELINE_SHA,
        "PROOF_ORIGINAL_SOURCE_CHANGED",
    )
    _require(not source.with_suffix(source.suffix + ".wal").exists(), "PROOF_SOURCE_NOT_CLOSED")
    _require(
        code_hashes() == before_code and file_digest(Path(__file__)) == verifier_hash,
        "PROOF_EXECUTED_CODE_CHANGED",
    )
    report_body: dict[str, object] = {
        "status": "SAVED_NATIVE_OPTIONS_FEATURES_VERIFIED",
        "source_database": str(source),
        "source_database_sha256": SOURCE_SHA,
        "original_0346_zip": str(baseline),
        "original_0346_zip_sha256": BASELINE_SHA,
        "tested_source_sha256": before_code,
        "verifier_sha256": verifier_hash,
        "as_of_ms": CUTOFF,
        "materialized_rows": len(snapshot.rows()) + len(controls.rows()),
        "manifest_event_id": MANIFEST_ID,
        "manifest_observed_at_ms": manifest.observed_at_ms,
        "declared_discovered_members": 1010,
        "declared_discovery_coverage": "FULL",
        "expiries_per_option_type": 13,
        "checks": summaries,
        "result_artifact_sha256": artifacts,
        "native_clocks_payloads_ids_preserved": True,
        "source_and_prior_release_unchanged": True,
        "actual_cli_runs": 2,
        "research_api_runs": 2,
        "iv_unit": "native_Deribit_percentage_points",
        "underlying_convention": "native_underlying_price_with_native_reference_name",
        "new_public_requests": 0,
        "orders_submitted": 0,
        "new_strategy_trials": 0,
        "historical_strategy_validation": False,
        "full_chain_verified": False,
        "current_feed_health_verified": False,
        "full_feature_layer_complete": False,
        "milestone_2_complete": False,
        "seven_day_validation_completed": False,
        "paper_days": 0,
        "paper_trades": 0,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    with (output / "report.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report_body, indent=2, allow_nan=False) + "\n")
    return report_body


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=ROOT / "evidence/reused-validation-public-01/events.duckdb"
    )
    parser.add_argument(
        "--baseline", type=Path, default=ROOT.parent / "selective-options-0.3.46.zip"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.source, args.output, args.baseline)
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "materialized_rows",
                    "expiries_per_option_type",
                    "actual_cli_runs",
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
