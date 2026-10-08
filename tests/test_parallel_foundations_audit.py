"""A bounded independent audit succeeds only for the intact offline proof."""

import json
import shutil

import duckdb
import pytest

from scripts.audit_parallel_foundations import ROOT, AuditError, audit, digest, tree


@pytest.fixture(scope="session")
def intact_proof():
    final = ROOT / "evidence/parallel-foundations-01"
    scratch = ROOT.parents[1] / "work/parallel-foundations-0345/attempt-02"
    selected = final if final.is_dir() else scratch
    if not selected.is_dir():
        pytest.skip("saved-native parallel proof has not been published or staged")
    return selected


@pytest.fixture
def copied_proof(intact_proof, tmp_path):
    copy = tmp_path / "proof"
    shutil.copytree(intact_proof, copy)
    return copy


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


async def test_intact_saved_proof_independent_audit_is_read_only(intact_proof, tmp_path):
    before = tree(intact_proof)
    output = tmp_path / "audit.json"
    result = await audit(intact_proof, output, require_current_code=False)
    assert result["status"] == "PARALLEL_FOUNDATIONS_INDEPENDENTLY_AUDITED"
    assert result["proof_sha256"] == before == tree(intact_proof)
    assert result["native_cohort_lengths"] == [50, 5]
    assert result["derived_bar_count"] == 4
    assert result["registered_known_bad_fixture"]["closed_roundtrip_net_pnl"] == "-44"
    assert result["independent_future_receipt_refusal"] == "FEATURE_LOOKAHEAD_EVENT"
    assert result["synthetic_feature_bars"] == 240
    assert not result["trading_enabled"] and not result["full_capture_restart_verified"]
    assert json.loads(output.read_text())["proof_sha256"] == before
    with pytest.raises(AuditError, match="NEW_AND_SEPARATE"):
        await audit(intact_proof, output, require_current_code=False)


async def test_semantic_bucket_corruption_refused_even_when_report_hash_updated(
    copied_proof, tmp_path
):
    path = copied_proof / "bar-state-cohort-0.json"
    state = json.loads(path.read_text())
    state["buckets"][0]["count"] += 1
    write_json(path, state)
    report_path = copied_proof / "report.json"
    report = json.loads(report_path.read_text())
    report["bar_states"][0]["state_sha256"] = digest(path)
    write_json(report_path, report)
    output = tmp_path / "audit.json"
    with pytest.raises(AuditError, match="BAR_COUNT"):
        await audit(copied_proof, output, require_current_code=False)
    assert not output.exists()


async def test_changed_native_receipt_refused_before_success_report(copied_proof, tmp_path):
    with duckdb.connect(str(copied_proof / "native-and-bars.duckdb")) as connection:
        connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE kind='trade'")
    output = tmp_path / "audit.json"
    with pytest.raises(AuditError, match="PARQUET_TYPED_ROWS"):
        await audit(copied_proof, output, require_current_code=False)
    assert not output.exists()


async def test_analytical_feature_corruption_refused(copied_proof):
    path = copied_proof / "feature-result/features.json"
    value = json.loads(path.read_text())
    value["feature_report"]["features"][0]["value"] += 5
    write_json(path, value)
    with pytest.raises(AuditError, match="ANALYTICAL_FEATURE"):
        await audit(copied_proof, require_current_code=False)


async def test_torn_trial_chain_refused(copied_proof):
    path = copied_proof / "trials.jsonl"
    path.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(AuditError, match="TRIAL_INCOMPLETE"):
        await audit(copied_proof, require_current_code=False)


async def test_overclaim_missing_piece_and_code_mismatch_refuse(copied_proof):
    report_path = copied_proof / "report.json"
    report = json.loads(report_path.read_text())
    report["trading_enabled"] = True
    write_json(report_path, report)
    with pytest.raises(AuditError, match="OVERCLAIM"):
        await audit(copied_proof, require_current_code=False)
    report["trading_enabled"] = False
    report["tested_source_sha256"] = {}
    write_json(report_path, report)
    with pytest.raises(AuditError, match="FROZEN_CODE"):
        await audit(copied_proof)
    (copied_proof / "bar-state-cohort-0.json").unlink()
    with pytest.raises((FileNotFoundError, AuditError)):
        await audit(copied_proof, require_current_code=False)


async def test_duplicate_json_and_nested_output_refused(copied_proof):
    with pytest.raises(AuditError, match="NEW_AND_SEPARATE"):
        await audit(copied_proof, copied_proof / "audit.json", require_current_code=False)
    path = copied_proof / "report.json"
    path.write_text('{"status":"PARALLEL_FOUNDATIONS_VERIFIED","status":"FORGED"}')
    with pytest.raises(AuditError, match="DUPLICATE_JSON"):
        await audit(copied_proof, require_current_code=False)
