"""Saved-native periodic proof and refusal boundaries; no public transport."""

import json
import shutil
from pathlib import Path

import pytest

from scripts import verify_periodic_bars as proof
from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.periodic_bar_runtime import load_bar_generation


async def selected(directory, digest, cutoff):
    return await load_bar_generation(
        directory,
        cutoff,
        ("BTC",),
        (300, 900),
        5000,
        expected_generation_sha256=digest,
    )


async def test_actual_saved_native_generations_resume_and_tamper_refusal(tmp_path):
    before = proof.saved.hashes(proof.SOURCE)
    output = tmp_path / "native-proof"
    result = await proof.verify(output)
    assert result == json.loads((output / "verification.json").read_text())
    assert result["status"] == "SAVED_NATIVE_ACTIVE_BAR_GENERATIONS_PASSED"
    assert result["application_version"] == "0.3.57"
    assert result["saved_native_trade_count"] == 55
    assert result["separate_contiguous_cohort_sizes"] == [50, 5]
    assert result["actual_capture_pipelines"] == 4
    assert result["active_committed_generations"] == result["derived_partial_bar_count"] == 4
    assert result["blocked_network_attempts"] == result["new_public_requests"] == 0
    assert result["source_hashes"] == before == proof.saved.hashes(proof.SOURCE)
    assert result["code_hashes"] == proof.saved.code_hashes()
    assert result["verifier_sha256"] == file_digest(Path(proof.__file__))
    assert result["reused_native_verifier_sha256"] == file_digest(
        proof.ROOT / "scripts/verify_bar_checkpoint.py"
    )
    for filename, digest in result["artifact_hashes"].items():
        assert file_digest(output / filename) == digest
    assert result["native_payloads_and_first_receipts_unchanged"] is True
    for flag in (
        "source_mutated",
        "missing_trade_repaired",
        "automatic_latest_generation_fallback",
        "os_signal_delivery_tested",
        "os_crash_tested",
        "periodic_crash_recovery",
        "continuous_schedule_or_service_soak_tested",
        "seven_day_validation_completed",
        "history_complete",
        "source_frontier_complete",
        "current_feed_health_verified",
        "milestone_2_complete",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert result[flag] is False
    for phase, counts in zip(result["phases"], ((25, 50), (2, 5)), strict=True):
        initial = phase["initial"]
        assert initial["closed_archive_exact"] and initial["pending_outbox_exact"]
        assert initial["first_generation_unchanged_after_new_commits"]
        assert initial["stop_after_second_generation"]
        assert initial["timeline"] == [
            "PREFIX_FRONTIER_INSTALLED",
            "GENERATION_0_VERIFIED_WHILE_WRITER_OPEN",
            "PRODUCER_ADVANCED_AFTER_FIRST_GENERATION",
            "FULL_COHORT_FRONTIER_INSTALLED",
            "GENERATION_1_VERIFIED_WHILE_WRITER_OPEN",
            "STOP_REQUESTED_AFTER_SECOND_GENERATION",
        ]
        assert [entry["native_count"] for entry in initial["generations"]] == list(counts)
        assert all(entry["writer_open_during_selection"] for entry in initial["generations"])
        assert all(entry["committed_native_membership_exact"] for entry in initial["generations"])
        resumed = phase["resumed"]
        assert resumed["restored"]["all_retained_partial"] is True
        assert resumed["trade_event_ids"] == initial["trade_event_ids"]
        assert len(resumed["partial_bar_ids"]) == 2
        assert resumed["closed_archive_exact"] and resumed["pending_outbox_exact"]
        assert resumed["closed_seed_observation_sha256"]
        chosen = phase["explicitly_selected_generation"]
        assert chosen == initial["generations"][-1] and chosen["index"] == 1

    # Copies are disposable test evidence. Original source and successful proof
    # stay unchanged; explicitly selected corruption must not pick an older run.
    phase = result["phases"][0]
    chosen = phase["explicitly_selected_generation"]
    original = output / "cohort-0-initial" / chosen["directory"]
    digest, cutoff = chosen["generation_sha256"], chosen["as_of_ms"]
    with pytest.raises(IntegrityError, match="SELECTION_DIGEST_MISMATCH"):
        await selected(original, "0" * 64, cutoff)
    for issue in ("pending", "failure", "state", "inner_marker", "missing_outer_marker"):
        directory = tmp_path / issue / original.name
        shutil.copytree(original, directory)
        if issue in {"pending", "failure"}:
            name = ".generation.pending.json" if issue == "pending" else "failure.json"
            (directory / name).write_text("EXPLICIT_SYNTHETIC_INTERRUPTED_ATTEMPT")
        elif issue == "missing_outer_marker":
            (directory / "generation.json").unlink()
        else:
            name = "state-000000.json" if issue == "state" else "checkpoint.json"
            with (directory / "checkpoint" / name).open("ab") as stream:
                stream.write(b"\n")
        with pytest.raises(IntegrityError):
            await selected(directory, digest, cutoff)
    assert proof.saved.hashes(proof.SOURCE) == before


@pytest.mark.parametrize("kind", ["directory", "file", "native_source", "native_child"])
async def test_output_must_be_new_and_separate_before_source_access(tmp_path, monkeypatch, kind):
    if kind == "directory":
        output = tmp_path / "existing"
        output.mkdir()
    elif kind == "file":
        output = tmp_path / "existing.txt"
        output.write_text("KEEP_EXISTING_BYTES")
    elif kind == "native_source":
        output = proof.SOURCE
    else:
        output = proof.SOURCE / "must-not-be-created"

    def forbidden(*args, **kwargs):
        pytest.fail("refused output reached source hashing or native reads")

    monkeypatch.setattr(proof.saved, "hashes", forbidden)
    with pytest.raises(ValueError):
        await proof.verify(output)
    if kind == "file":
        assert output.read_text() == "KEEP_EXISTING_BYTES"
    if kind == "native_child":
        assert not output.exists()


async def test_runtime_version_refuses_before_output_or_source_access(tmp_path, monkeypatch):
    monkeypatch.setattr(proof, "EXPECTED_VERSION", "UNSUPPORTED_SYNTHETIC_VERSION")

    def forbidden(*args, **kwargs):
        pytest.fail("unsupported version reached native source")

    monkeypatch.setattr(proof.saved, "hashes", forbidden)
    output = tmp_path / "wrong-version"
    with pytest.raises(IntegrityError, match="RUNTIME_VERSION_MISMATCH"):
        await proof.verify(output)
    assert not output.exists()


async def test_changed_native_database_digest_refuses_before_output(tmp_path, monkeypatch):
    original_digest = proof.file_digest
    monkeypatch.setattr(
        proof,
        "file_digest",
        lambda path: "0" * 64 if path == proof.SOURCE / "events.duckdb" else original_digest(path),
    )
    output = tmp_path / "changed-source"
    with pytest.raises(IntegrityError, match="NATIVE_SOURCE_DIGEST_MISMATCH"):
        await proof.verify(output)
    assert not output.exists()


@pytest.mark.parametrize("issue", ["missing", "scope", "identity"])
async def test_native_cohort_or_canonical_record_change_refuses_before_output(
    tmp_path, monkeypatch, issue
):
    rows = list(proof.saved.rows(proof.SOURCE / "events.duckdb"))
    index = next(
        index for index, row in enumerate(rows) if row.source == "kraken" and row.kind == "trade"
    )
    if issue == "missing":
        rows.pop(index)
        error = "NATIVE_COHORT_MEMBERSHIP"
    else:
        field, value = ("asset", "ETH") if issue == "scope" else ("event_id", "f" * 64)
        rows[index] = rows[index].model_copy(update={field: value})
        error = "NATIVE_RECORD_CHANGED"
    monkeypatch.setattr(proof.saved, "rows", lambda _: tuple(rows))
    output = tmp_path / issue
    with pytest.raises(IntegrityError, match=error):
        await proof.verify(output)
    assert not output.exists()
