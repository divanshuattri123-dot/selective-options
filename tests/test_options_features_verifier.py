"""Independent verifier refusals using saved native bytes, with no public calls."""

import hashlib
import json
import math
import subprocess
from pathlib import Path

import pytest

from scripts import verify_options_features as proof
from selective_options.errors import IntegrityError
from selective_options.models import Instrument, Ticker
from selective_options.option_coverage import Universe
from selective_options.options_features import compute_options_features
from selective_options.options_research import load_options_snapshots


@pytest.fixture(scope="module")
def native():
    source = proof.ROOT / "evidence/reused-validation-public-01/events.duckdb"
    snapshot, controls, digest = load_options_snapshots(
        source, asset="BTC", as_of_ms=proof.CUTOFF, max_rows=proof.MAX_ROWS
    )
    assert digest == proof.SOURCE_SHA
    manifest = next(row for row in controls.rows() if row.event_id == proof.MANIFEST_ID)
    universe = Universe.model_validate(manifest.payload())
    reports = {
        side: compute_options_features(
            snapshot,
            controls,
            option_type=side,
            moneyness_method="native_underlying_log_distance",
            tie_policy="exact_distance_refuse",
            max_quote_age_ms=10000,
            max_discovery_age_ms=100000,
            max_atm_log_moneyness=0.2,
            max_underlying_mismatch_fraction=0.1,
            flat_threshold_iv_points=1.0,
            max_rows=proof.MAX_ROWS,
            max_expiries=128,
        )
        for side in ("call", "put")
    }
    return source, snapshot, controls, manifest, universe, reports


def check(native, report):
    _, snapshot, _, manifest, universe, _ = native
    return proof._check_report(report, snapshot.rows(), manifest, universe, "call")


def test_saved_native_scope_units_and_receipts_pass(native):
    report = native[-1]["call"]
    checked = check(native, report)
    assert checked["term_shape"] == "MIXED"
    assert len(checked["expiries"]) == 13
    assert report.expiries[0].quote_source_time_ms == 1791050701197
    assert report.expiries[0].quote_observed_at_ms == 1791050699929
    assert report.expiries[0].mark_iv_points == 12.28


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("mark_iv_points", 0.1228, "PROOF_NATIVE_UNITS_CHANGED"),
        ("strike", 84000.0, "PROOF_NATIVE_UNITS_CHANGED"),
        ("underlying_price", 85000.0, "PROOF_NATIVE_UNITS_CHANGED"),
        ("underlying_index", "invented-spot", "PROOF_NATIVE_REFERENCE_CHANGED"),
        ("quote_source_time_ms", 1791050699929, "PROOF_NATIVE_CLOCKS_CHANGED"),
        ("quote_observed_at_ms", 1791050701197, "PROOF_NATIVE_CLOCKS_CHANGED"),
        ("metadata_observed_at_ms", 1791050626810, "PROOF_METADATA_RECEIPT_CHANGED"),
        ("members", (), "PROOF_EXPIRY_MEMBERSHIP"),
        ("eligible_quotes", 0, "PROOF_EXPIRY_MEMBERSHIP"),
        ("quote_currency", "USD", "PROOF_NATIVE_UNIT_LABEL_CHANGED"),
        ("settlement_currency", "USD", "PROOF_NATIVE_UNIT_LABEL_CHANGED"),
        ("instrument_type", "linear", "PROOF_NATIVE_UNIT_LABEL_CHANGED"),
        ("log_moneyness_distance", 0.01, "PROOF_MONEYNESS_VALUE"),
        ("quote_event_id", None, "PROOF_NATIVE_CHOSEN_ID_MISSING"),
    ],
)
def test_native_expiry_field_tampering_refused(native, field, value, reason):
    report = native[-1]["call"]
    changed = report.expiries[0].model_copy(update={field: value})
    forged = report.model_copy(update={"expiries": (changed, *report.expiries[1:])})
    with pytest.raises(IntegrityError, match=reason):
        check(native, forged)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("asset", "ETH", "PROOF_REPORT_SCOPE"),
        ("as_of_ms", proof.CUTOFF + 1, "PROOF_REPORT_SCOPE"),
        ("formula_version", "invented-v2", "PROOF_REPORT_SCOPE"),
        ("manifest_observed_at_ms", 1, "PROOF_NATIVE_DISCOVERY_CHANGED"),
        ("discovery_declared_coverage", "PARTIAL", "PROOF_NATIVE_DISCOVERY_CHANGED"),
        ("adjacent_iv_differences_points", (), "PROOF_NATIVE_IV_DIFFERENCES"),
        ("term_shape", "CONTANGO", "PROOF_KNOWN_NATIVE_TERM_SHAPE"),
        ("full_chain_verified", True, "PROOF_UNSUPPORTED_APPROVAL"),
        ("current_feed_health_verified", True, "PROOF_UNSUPPORTED_APPROVAL"),
        ("ready_for_signals", True, "PROOF_UNSUPPORTED_APPROVAL"),
        ("trading_enabled", True, "PROOF_UNSUPPORTED_APPROVAL"),
        ("interpolated", True, "PROOF_UNSUPPORTED_APPROVAL"),
    ],
)
def test_native_report_tampering_refused(native, field, value, reason):
    with pytest.raises(IntegrityError, match=reason):
        check(native, native[-1]["call"].model_copy(update={field: value}))


def test_expiry_order_and_policy_cannot_be_changed(native):
    report = native[-1]["call"]
    with pytest.raises(IntegrityError, match="PROOF_EXPIRY_SCOPE"):
        check(native, report.model_copy(update={"expiries": report.expiries[::-1]}))
    policy = report.policy.model_copy(update={"max_quote_age_ms": 10001})
    with pytest.raises(IntegrityError, match="PROOF_EXPLICIT_POLICY_CHANGED"):
        check(native, report.model_copy(update={"policy": policy}))


def test_authentic_far_strike_refused_by_independent_atm_comparison(native):
    report = native[-1]["call"]
    expiry = report.expiries[0]
    by_id = {row.event_id: row for row in native[1].rows()}
    members = []
    for identity in native[4].metadata_event_ids:
        model = Instrument.model_validate(by_id[identity].payload())
        if (
            model.option_type == "call"
            and model.expiration_timestamp == expiry.expiration_timestamp
        ):
            members.append((model.strike, identity, model))
    _, identity, instrument = max(members, key=lambda entry: entry[0])
    quotes = [
        row
        for row in by_id.values()
        if row.kind == "ticker" and row.symbol == instrument.instrument_name
    ]
    row = max(quotes, key=lambda value: value.event_time_ms)
    quote = Ticker.model_validate(row.payload())
    # All native identities, clocks and field values are authentic. The actual
    # chosen contract alone is wrong, so comparisons to report values cannot pass.
    far = expiry.model_copy(
        update={
            "symbol": instrument.instrument_name,
            "strike": instrument.strike,
            "mark_iv_points": quote.mark_iv,
            "underlying_price": quote.underlying_price,
            "underlying_index": row.payload()["underlying_index"],
            "metadata_event_id": identity,
            "metadata_observed_at_ms": by_id[identity].observed_at_ms,
            "quote_event_id": row.event_id,
            "quote_source_time_ms": row.event_time_ms,
            "quote_observed_at_ms": row.observed_at_ms,
            "log_moneyness_distance": abs(math.log(instrument.strike / quote.underlying_price)),
        }
    )
    with pytest.raises(IntegrityError, match="PROOF_ATM_ANALYTIC_SELECTION"):
        check(native, report.model_copy(update={"expiries": (far, *report.expiries[1:])}))


def setup_preflight(tmp_path, monkeypatch):
    root = tmp_path / "project/outputs/selective-options"
    source = root / "evidence/old-native/events.duckdb"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"saved native placeholder")
    baseline = root.parent / "selective-options-0.3.46.zip"
    baseline.write_bytes(b"original zip placeholder")
    output = root / "evidence/new-proof"
    monkeypatch.setattr(proof, "ROOT", root)
    monkeypatch.setattr(proof, "SOURCE_SHA", hashlib.sha256(source.read_bytes()).hexdigest())
    monkeypatch.setattr(proof, "BASELINE_SHA", hashlib.sha256(baseline.read_bytes()).hexdigest())

    def forbidden_loader(*args, **kwargs):
        pytest.fail("preflight must refuse before reading/materializing a database")

    monkeypatch.setattr(proof, "load_options_snapshots", forbidden_loader)
    return source, output, baseline


@pytest.mark.parametrize(
    "case", ["source-hash", "baseline-hash", "wal", "existing", "source-child", "outside"]
)
def test_preflight_refusal_preserves_originals_and_creates_no_report(tmp_path, monkeypatch, case):
    source, output, baseline = setup_preflight(tmp_path, monkeypatch)
    original = source.read_bytes(), baseline.read_bytes()
    error = IntegrityError
    reason = "PROOF_"
    if case == "source-hash":
        monkeypatch.setattr(proof, "SOURCE_SHA", "0" * 64)
    elif case == "baseline-hash":
        monkeypatch.setattr(proof, "BASELINE_SHA", "0" * 64)
    elif case == "wal":
        source.with_suffix(".duckdb.wal").write_bytes(b"open database")
    else:
        error, reason = ValueError, "proof requires new output"
        if case == "existing":
            output.mkdir()
        elif case == "source-child":
            output = source.parent / "bad-proof"
        else:
            output = tmp_path / "outside"
    with pytest.raises(error, match=reason):
        proof.verify(source, output, baseline)
    assert not (output / "report.json").exists()
    assert (source.read_bytes(), baseline.read_bytes()) == original
    if case != "existing":
        assert not output.exists()


def test_shared_code_drift_retains_four_reports_but_never_publishes_success(
    native, tmp_path, monkeypatch
):
    source, snapshot, controls, _, _, reports = native
    baseline = proof.ROOT.parent / "selective-options-0.3.46.zip"
    before = proof.file_digest(source)
    output = tmp_path / "project/outputs/selective-options/evidence/proof"
    monkeypatch.setattr(proof, "ROOT", output.parent.parent)
    monkeypatch.setattr(
        proof, "load_options_snapshots", lambda *args, **kwargs: (snapshot, controls, before)
    )
    hashes = iter([{"module.py": "before"}, {"module.py": "after"}])
    monkeypatch.setattr(proof, "code_hashes", lambda: next(hashes))

    def result(side):
        return {"options_report": reports[side].model_dump(mode="json")}

    def research(source, directory, **policy):
        body = result(policy["option_type"])
        directory.mkdir(parents=True)
        (directory / "options-features.json").write_text(json.dumps(body), encoding="utf-8")
        return body

    def cli(command, **kwargs):
        side = command[command.index("--option-type") + 1]
        directory = Path(command[command.index("--output") + 1])
        body = research(source, directory, option_type=side)
        return subprocess.CompletedProcess(command, 0, json.dumps(body), "")

    monkeypatch.setattr(proof, "research_options", research)
    monkeypatch.setattr(proof.subprocess, "run", cli)
    with pytest.raises(IntegrityError, match="PROOF_EXECUTED_CODE_CHANGED"):
        proof.verify(source, output, baseline)
    assert len(list(output.glob("*/options-features.json"))) == 4
    assert not (output / "report.json").exists()
    assert proof.file_digest(source) == before
