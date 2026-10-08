"""Independent proof audit rejects resealed corruption and drains cancellation."""

import asyncio
import json
import shutil
import subprocess
import sys
import threading

import duckdb
import pytest

from scripts import audit_bar_checkpoint as module
from scripts.audit_bar_checkpoint import ROOT, audit, digest, tree
from scripts.audit_parallel_foundations import AuditError


@pytest.fixture(scope="session")
def intact_proof():
    final = ROOT / "evidence/bar-checkpoint-resume-01"
    scratch = ROOT.parents[1] / "work/bar-checkpoint-0346/attempt-02"
    selected = final if final.is_dir() else scratch
    if not selected.is_dir():
        pytest.skip("bounded saved-native stopped-checkpoint proof not staged")
    return selected


@pytest.fixture
def copied_proof(intact_proof, tmp_path):
    copy = tmp_path / "proof"
    shutil.copytree(intact_proof, copy)
    return copy


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def reseal_phase(proof, index, phase):
    directory = proof / f"cohort-{index}-{phase}"
    manifest = json.loads((directory / "bar-checkpoint/checkpoint.json").read_text())
    capture_path = directory / "report.json"
    capture = json.loads(capture_path.read_text())
    capture["bar_checkpoint"] = manifest
    write(capture_path, capture)
    report_path = proof / "report.json"
    report = json.loads(report_path.read_text())
    entry = report["captures"][index][phase]
    entry["capture_report_sha256"] = digest(capture_path)
    entry["checkpoint_manifest_sha256"] = digest(directory / "bar-checkpoint/checkpoint.json")
    write(report_path, report)


async def test_intact_audit_binds_every_artifact_and_is_readonly(intact_proof, tmp_path):
    before = tree(intact_proof)
    output = tmp_path / "audit.json"
    result = await audit(intact_proof, output, require_current_code=False)
    assert result["status"] == "COMMITTED_BAR_HANDOFF_INDEPENDENTLY_AUDITED"
    assert result["native_trades"] == 55 and result["cohort_sizes"] == [50, 5]
    assert result["partial_bar_count"] == 4 and result["supervised_capture_artifacts"] == 4
    assert result["proof_sha256"] == before == tree(intact_proof)
    assert all(entry["resumed"]["closed_seed_barriers"] for entry in result["cohorts"])
    assert result["trend"]["analytical_values"]["adx_14"] == 100.0
    assert not result["trading_enabled"] and not result["full_ingestion_complete"]
    assert json.loads(output.read_text()) == result
    with pytest.raises(AuditError, match="NEW_AND_SEPARATE"):
        await audit(intact_proof, output, require_current_code=False)
    assert tree(intact_proof) == before


async def test_semantic_bucket_count_refused_after_all_related_seals_updated(
    copied_proof, tmp_path
):
    directory = copied_proof / "cohort-0-initial/bar-checkpoint"
    state_path = directory / "state-000000.json"
    state = json.loads(state_path.read_text())
    state["buckets"][0]["count"] += 1
    write(state_path, state)
    manifest_path = directory / "checkpoint.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["books"][0]["state_sha256"] = digest(state_path)
    manifest["books"][0]["state_bytes"] = state_path.stat().st_size
    write(manifest_path, manifest)
    reseal_phase(copied_proof, 0, "initial")
    output = tmp_path / "audit.json"
    with pytest.raises(AuditError, match="BAR_COUNT"):
        await audit(copied_proof, output, require_current_code=False)
    assert not output.exists()


async def test_closed_seed_ack_refused_after_digest_and_reports_resealed(copied_proof, tmp_path):
    directory = copied_proof / "cohort-0-resumed"
    database = directory / "seed-verified.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("UPDATE delivery_outbox SET delivered=true")
    capture_path = directory / "report.json"
    capture = json.loads(capture_path.read_text())
    capture["bar_resume"]["closed_seed_verification"]["database_sha256"] = digest(database)
    write(capture_path, capture)
    path = copied_proof / "report.json"
    report = json.loads(path.read_text())
    report["captures"][0]["resumed"]["closed_seed_observation_sha256"] = digest(database)
    report["captures"][0]["resumed"]["capture_report_sha256"] = digest(capture_path)
    write(path, report)
    output = tmp_path / "audit.json"
    with pytest.raises(AuditError, match="OUTBOX_EXACT_ALL_PENDING"):
        await audit(copied_proof, output, require_current_code=False)
    assert not output.exists()


async def test_native_receipt_change_refused_against_sealed_archive(copied_proof, tmp_path):
    with duckdb.connect(str(copied_proof / "cohort-0-resumed/events.duckdb")) as connection:
        connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE kind='trade'")
    with pytest.raises(AuditError, match="PARQUET_TYPED_ROWS"):
        await audit(copied_proof, tmp_path / "audit.json", require_current_code=False)
    assert not (tmp_path / "audit.json").exists()


async def test_rank_current_exclusion_refused_after_feature_seal_updated(copied_proof, tmp_path):
    path = copied_proof / "trend-result/features.json"
    result = json.loads(path.read_text())
    result["trend_report"]["percentile_references"][0]["current_excluded"] = False
    write(path, result)
    report_path = copied_proof / "report.json"
    report = json.loads(report_path.read_text())
    report["feature_result_sha256"] = digest(path)
    write(report_path, report)
    with pytest.raises(AuditError, match="TREND_EXACT_PRIOR_REFERENCE"):
        await audit(copied_proof, tmp_path / "audit.json", require_current_code=False)
    assert not (tmp_path / "audit.json").exists()


async def test_default_binding_refuses_fabricated_recorded_code(copied_proof, tmp_path):
    path = copied_proof / "report.json"
    report = json.loads(path.read_text())
    # Rebase only this copied report to isolate a digest defect from the expected
    # old-release module-set mismatch; immutable historical proof stays unchanged.
    report["code_hashes"] = {
        p.relative_to(ROOT).as_posix(): digest(p)
        for p in sorted((ROOT / "src/selective_options").glob("*.py"))
    }
    report["verifier_sha256"] = digest(ROOT / "scripts/verify_bar_checkpoint.py")
    report["code_hashes"]["src/selective_options/bar_checkpoint.py"] = "0" * 64
    write(path, report)
    with pytest.raises(AuditError, match="FROZEN_CODE_OR_PRODUCER_CHANGED"):
        await audit(copied_proof, tmp_path / "audit.json")
    assert not (tmp_path / "audit.json").exists()


async def test_historical_module_set_passes_only_explicit_old_code_policy(intact_proof, tmp_path):
    recorded = json.loads((intact_proof / "report.json").read_text())["code_hashes"]
    installed = {
        p.relative_to(ROOT).as_posix() for p in (ROOT / "src/selective_options").glob("*.py")
    }
    assert set(recorded) != installed  # Authentic .46 proof versus installed .47 modules.
    before = tree(intact_proof)
    with pytest.raises(AuditError, match="EXACT_CODE_FILE_SET"):
        await audit(intact_proof, tmp_path / "strict.json")
    assert not (tmp_path / "strict.json").exists() and tree(intact_proof) == before
    result = await audit(intact_proof, require_current_code=False)
    assert result["recorded_code_sha256"] == recorded
    assert result["current_code_binding_required"] is False
    assert result["proof_sha256"] == before == tree(intact_proof)


@pytest.mark.parametrize(
    "bad",
    [
        {},
        [],
        {"../outside.py": "0" * 64},
        {"src/selective_options/../outside.py": "0" * 64},
        {"src/selective_options\\store.py": "0" * 64},
        {"C:/Users/Divan/source.py": "0" * 64},
        {"src/selective_options/store.py": "0" * 63},
        {"src/selective_options/store.py": "A" * 64},
        {"src/selective_options/store.py": True},
    ],
)
async def test_historical_recorded_code_map_must_be_strict_safe(copied_proof, tmp_path, bad):
    path = copied_proof / "report.json"
    report = json.loads(path.read_text())
    report["code_hashes"] = bad
    write(path, report)
    output = tmp_path / "historical.json"
    with pytest.raises(AuditError, match="STRICT_RECORDED_CODE_MAP"):
        await audit(copied_proof, output, require_current_code=False)
    assert not output.exists()


async def test_repeated_cancel_during_audit_drains_and_never_publishes(
    intact_proof, tmp_path, monkeypatch
):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    real = module._audit

    def gated(*args):
        entered.set()
        assert release.wait(5)
        try:
            return real(*args)
        finally:
            finished.set()

    monkeypatch.setattr(module, "_audit", gated)
    output = tmp_path / "audit.json"
    task = asyncio.create_task(audit(intact_proof, output, require_current_code=False))
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set() and not output.exists()


async def test_cancelled_publication_drains_then_revokes_success(
    intact_proof, tmp_path, monkeypatch
):
    entered, release = threading.Event(), threading.Event()
    real = module._publish

    def gated(*args):
        real(*args)
        entered.set()
        assert release.wait(5)

    monkeypatch.setattr(module, "_publish", gated)
    output = tmp_path / "audit.json"
    task = asyncio.create_task(audit(intact_proof, output, require_current_code=False))
    assert await asyncio.to_thread(entered.wait, 20)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    report = json.loads(output.read_text())
    assert report["status"] == "AUDIT_PUBLICATION_CANCELLED"
    assert not report["audit_completed_for_caller"] and not report["trading_enabled"]


def test_actual_module_cli_supported_and_strict(intact_proof, tmp_path):
    # Actual entry point uses current frozen code; copy report binds only this
    # test's current producer snapshot, never rewriting preserved proof bytes.
    copy = tmp_path / "proof"
    shutil.copytree(intact_proof, copy)
    path = copy / "report.json"
    report = json.loads(path.read_text())
    report["code_hashes"] = {
        p.relative_to(ROOT).as_posix(): digest(p)
        for p in sorted((ROOT / "src/selective_options").glob("*.py"))
    }
    report["verifier_sha256"] = digest(ROOT / "scripts/verify_bar_checkpoint.py")
    write(path, report)
    output = tmp_path / "cli-audit.json"
    result = subprocess.run(
        [sys.executable, "-m", "scripts.audit_bar_checkpoint", str(copy), "--output", str(output)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "COMMITTED_BAR_HANDOFF_INDEPENDENTLY_AUDITED"
    assert json.loads(output.read_text())["current_code_binding_required"] is True
