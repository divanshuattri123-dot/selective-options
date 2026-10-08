"""Offline supervised running-store rotation verification checks."""

import importlib
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pytest

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError

ROOT = Path(__file__).resolve().parents[1]
NEWS = ROOT / "evidence/crypto-news-public-02"
SUPPLY = ROOT / "evidence/stablecoin-supply-public-01"


@pytest.fixture
def temporary():
    scratch = ROOT.parents[1] / "work/active-rotation-0343"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as path:
        yield Path(path)


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_active_rotation")


async def test_actual_supervised_mock_capture_two_rotations_native_exact(
    verifier, temporary, monkeypatch
):
    def no_network(*args, **kwargs):
        raise AssertionError("offline proof must not create network sessions")

    monkeypatch.setattr("aiohttp.ClientSession", no_network)
    before = {"news": verifier.source_hashes(NEWS), "supply": verifier.source_hashes(SUPPLY)}
    output = temporary / "proof"
    report = await verifier.verify(NEWS, SUPPLY, output)
    assert report["status"] == "OFFLINE_SUPERVISED_ACTIVE_ROTATION_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    assert report["saved_native_records"] == 28 and report["synthetic_records"] == 4
    assert report["captured_records"] == 32
    assert report["capture_report"]["status"] == "CAPTURE_FINISHED"
    assert report["capture_report"]["rows"] == 32
    assert report["capture_report"]["redis_delivery_pending"] == 31
    assert report["capture_report"]["archive_rotation"]["enabled"] is True
    assert len(report["periodic_rotations"]) == 2
    assert [entry["source_rows_at_snapshot_request"] for entry in report["periodic_rotations"]] == [
        30,
        31,
    ]
    assert [entry["snapshot_rows"] for entry in report["periodic_rotations"]] == [28, 31]
    assert [entry["restore"]["redis_delivery_pending"] for entry in report["restores"]] == [28, 31]
    assert all(entry["store_open_after_rotation"] for entry in report["periodic_rotations"])
    assert report["later_commit_before_first_partition_validation"]
    assert report["queue_empty_after_shutdown"]
    assert report["producer_cancelled_and_drained"] and report["runtime_cancelled_and_drained"]
    assert len(report["commit_barriers"]) >= 5
    assert all(entry["completed_after_storage"] for entry in report["commit_barriers"])
    assert len(report["pit_checks"]) >= 100
    assert len(report["shared_quality_checks"]) == 2
    assert all(len(item["checks"]) == 3 for item in report["shared_quality_checks"])
    assert report["source_hashes_before"] == report["source_hashes_after"] == before
    assert report["code_sha256_before"] == report["code_sha256_after"]
    assert report["legacy_release_sha256_before"] == report["legacy_release_sha256_after"]
    assert report["verifier_sha256"] == file_digest(output / "verification-script.py")
    for key in (
        "redis_delivery_claimed",
        "continuous_history_complete",
        "actual_provider_outage_recovery_verified",
        "seven_day_validation_complete",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert report[key] is False
    capture = output / "capture/events.duckdb"
    with duckdb.connect(str(capture), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*),count(*) FILTER(WHERE delivered) FROM delivery_outbox"
        ).fetchone() == (32, 1)
    saved = json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert saved["periodic_rotations"] == report["periodic_rotations"]
    assert (
        verifier.source_hashes(NEWS) == before["news"]
        and verifier.source_hashes(SUPPLY) == before["supply"]
    )


def test_native_source_preflight(verifier, temporary):
    saved = verifier.preflight(NEWS, SUPPLY, temporary / "proof")
    assert len(saved.rows) == 28
    assert saved.facts["news"]["metadata_items"] == 25
    assert saved.facts["supply"]["native_response_entities"] == 7


def test_synthetic_future_clocks_and_post_snapshot_commits_are_labelled(verifier, temporary):
    native = verifier.preflight(NEWS, SUPPLY, temporary / "proof").rows
    rows, first, second, ended = verifier.fixtures(native)
    assert len(rows) == 4 and first < second < ended
    assert rows[0].event_time_ms > first >= rows[0].observed_at_ms
    assert rows[1].observed_at_ms > first >= rows[1].event_time_ms
    assert all(row.event_time_ms <= second and row.observed_at_ms <= second for row in rows[:3])
    assert rows[3].observed_at_ms > second
    assert all(
        row.payload()["provenance"] == "synthetic_offline_verification_fixture" for row in rows
    )
    assert max(row.event_time_ms - row.observed_at_ms for row in rows) <= 5000


async def test_existing_output_refused_before_capture(verifier, temporary, monkeypatch):
    output = temporary / "existing"
    output.mkdir()

    async def forbidden(*args, **kwargs):
        raise AssertionError("capture must not run after preflight refusal")

    monkeypatch.setattr(verifier, "run_capture", forbidden)
    with pytest.raises(FileExistsError):
        await verifier.verify(NEWS, SUPPLY, output)
    assert list(output.iterdir()) == []


def test_output_outside_workspace_refused(verifier, tmp_path):
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(NEWS, SUPPLY, tmp_path / "outside")


@pytest.mark.parametrize("duration", [0, -1, float("nan"), float("inf"), 61])
async def test_bad_duration_refused_before_source_or_output(
    verifier, temporary, duration, monkeypatch
):
    def forbidden(*args, **kwargs):
        raise AssertionError("duration preflight must precede source reads")

    monkeypatch.setattr(verifier, "preflight", forbidden)
    output = temporary / "invalid"
    with pytest.raises(ValueError, match="duration must"):
        await verifier.verify(NEWS, SUPPLY, output, duration)
    assert not output.exists()


@pytest.mark.parametrize(
    "source_name,field,value,error",
    [
        ("news", "status", "FIXTURE", "NEWS_PUBLIC_PROVENANCE"),
        ("news", "real_public_requests_attempted", 0, "NEWS_PUBLIC_PROVENANCE"),
        ("supply", "status", "FIXTURE", "SUPPLY_PUBLIC_PROVENANCE"),
        ("supply", "real_public_requests_attempted", 0, "SUPPLY_PUBLIC_PROVENANCE"),
    ],
)
def test_native_source_provenance_cannot_be_promoted(
    verifier, temporary, source_name, field, value, error
):
    copied = temporary / "copied"
    shutil.copytree(NEWS if source_name == "news" else SUPPLY, copied)
    report_path = copied / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report[field] = value
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match=error):
        verifier.preflight(
            copied if source_name == "news" else NEWS,
            copied if source_name == "supply" else SUPPLY,
            temporary / "proof",
        )


@pytest.mark.parametrize(
    "source_name,filename,error",
    [
        ("news", "response.xml", "NEWS_NATIVE_ENTITY"),
        ("supply", "response-03.json", "SUPPLY_NATIVE_ENTITY"),
    ],
)
def test_changed_native_entity_refused(verifier, temporary, source_name, filename, error):
    copied = temporary / "copied"
    shutil.copytree(NEWS if source_name == "news" else SUPPLY, copied)
    with (copied / filename).open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(IntegrityError, match=error):
        verifier.preflight(
            copied if source_name == "news" else NEWS,
            copied if source_name == "supply" else SUPPLY,
            temporary / "proof",
        )


def test_source_database_archive_disagreement_refused(verifier, temporary):
    copied = temporary / "copied"
    shutil.copytree(NEWS, copied)
    with duckdb.connect(str(copied / "events.duckdb")) as connection:
        connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1")
    with pytest.raises(IntegrityError, match="DATABASE_ARCHIVE_MISMATCH"):
        verifier.preflight(copied, SUPPLY, temporary / "proof")


async def test_changed_code_refuses_final_proof_publication(verifier, temporary, monkeypatch):
    actual_digest = verifier.file_digest
    calls = 0

    def changing(path):
        nonlocal calls
        digest = actual_digest(path)
        if path.name == "rotation_runtime.py":
            calls += 1
            if calls > 1:
                return "0" * 64
        return digest

    monkeypatch.setattr(verifier, "file_digest", changing)
    output = temporary / "changed-code"
    with pytest.raises(IntegrityError, match="SOURCE_OR_CODE_CHANGED"):
        await verifier.verify(NEWS, SUPPLY, output)
    assert not (output / "report.json").exists()
    assert not (output / "verification-script.py").exists()


@pytest.mark.parametrize(
    "scenario,error",
    [
        ("snapshot_budget", "SNAPSHOT_BUDGET_EXHAUSTED"),
        ("partition_budget", "PARTITION_BUDGET"),
        ("collector_failure", "SYNTHETIC_OFFLINE_COLLECTOR_FAILURE"),
    ],
)
async def test_actual_capture_failure_drains_source_and_refuses_false_completion(
    verifier, temporary, scenario, error
):
    native = verifier.preflight(NEWS, SUPPLY, temporary / "unused").rows
    output = temporary / "failed-capture"
    options = (
        {"max_snapshots": 1}
        if scenario == "snapshot_budget"
        else {"partition_ms": 1000, "max_partitions": 1}
        if scenario == "partition_budget"
        else {"fail_after_snapshot": True}
    )
    with pytest.raises(IntegrityError, match=error):
        await verifier.run_capture(output, native, **options)
    report = json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["archive_rotation"]["status"] == "FAILED" or scenario == "collector_failure"
    assert report["ready_for_signals"] is False
    rows = verifier.read_rows(output / "events.duckdb", output / "events.parquet")
    native_ids = {row.event_id for row in native}
    assert tuple(row for row in rows if row.event_id in native_ids) == tuple(
        sorted(native, key=lambda row: row.event_id)
    )
    if scenario == "snapshot_budget":
        assert report["archive_rotation"]["completed_snapshots"] == 1
        assert report["archive_rotation"]["attempts"] == 1
        assert len(rows) == 31
        assert not (output / "archive-rotations/snapshot-000001").exists()
    else:
        assert report["archive_rotation"]["completed_snapshots"] == 0
        assert len(rows) == 30
        assert not (output / "archive-rotations/snapshot-000000/active-rotation.json").exists()
