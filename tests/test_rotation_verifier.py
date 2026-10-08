"""Independent offline saved-native rotated archive proof checks."""

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
    scratch = ROOT.parents[1] / "work/rotation-0342"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as path:
        yield Path(path)


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_rotation")


async def test_saved_native_rotation_restore_failure_and_clock_filter(
    verifier, temporary, monkeypatch
):
    def no_network(*args, **kwargs):
        raise AssertionError("offline rotation verifier must not create network sessions")

    monkeypatch.setattr("aiohttp.ClientSession", no_network)
    before = {"news": verifier.source_hashes(NEWS), "supply": verifier.source_hashes(SUPPLY)}
    output = temporary / "proof"
    report = await verifier.verify(NEWS, SUPPLY, output)
    assert report["status"] == "OFFLINE_SAVED_PUBLIC_ROTATION_RESTORE_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    assert report["saved_native_records"] == 28
    assert report["synthetic_records"] == 11
    assert report["source_records"] == 39 and report["eligible_records"] == 35
    assert report["excluded_future_clock_fixtures"] == 4
    assert len(report["partitions"]) == 2
    assert sum(part["rows"] for part in report["partitions"]) == 35
    assert report["partition_budget_refused_before_output_creation"]
    assert len(report["pit_checks"]) >= 50
    assert report["shared_quality_checks"][-1]["crypto_news"]["state"] == "BLOCKED"
    assert report["corrupt_restore"]["final_database_published"] is False
    assert report["source_hashes_before"] == report["source_hashes_after"] == before
    assert report["working_source_sha256_before"] == report["working_source_sha256_after"]
    assert report["source_outbox_before"] == report["source_outbox_after"]
    assert sum(delivered for _, delivered in report["source_outbox_before"]) == 1
    assert report["restore"]["redis_delivery_pending"] == 35
    assert report["ready_for_signals"] is False and report["trading_enabled"] is False
    assert verifier.source_hashes(NEWS) == before["news"]
    assert verifier.source_hashes(SUPPLY) == before["supply"]
    assert file_digest(output / "verification-script.py") == report["verifier_sha256"]
    assert (
        json.loads((output / "report.json").read_text(encoding="utf-8"))["pit_checks"]
        == report["pit_checks"]
    )
    with duckdb.connect(str(output / "restored/events.duckdb"), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
        ).fetchone() == (35,)


def test_original_source_native_metadata_and_abi_independent(verifier, temporary):
    saved = verifier.preflight(NEWS, SUPPLY, temporary / "proof")
    assert len(saved.rows) == 28
    assert saved.native_facts["news"]["metadata_items"] == 25
    assert saved.native_facts["news"]["first_publication_verified"] is False
    assert saved.native_facts["supply"]["native_response_entities"] == 7


def test_fixture_provenance_and_both_clocks_cross_budget(verifier, temporary):
    saved = verifier.preflight(NEWS, SUPPLY, temporary / "proof")
    fixtures, cutoff, base = verifier.fixture_events(saved.rows)
    visible = [
        row for row in fixtures if row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff
    ]
    assert len(visible) == 7
    assert {
        max(row.event_time_ms, row.observed_at_ms) // verifier.PARTITION_MS for row in visible
    } == {base // verifier.PARTITION_MS - 1, base // verifier.PARTITION_MS}
    futures = [row for row in fixtures if row not in visible]
    assert len(futures) == 4
    assert any(row.event_time_ms > cutoff and row.observed_at_ms <= cutoff for row in futures)
    assert any(row.observed_at_ms > cutoff and row.event_time_ms <= cutoff for row in futures)
    assert (
        len(
            {
                max(row.event_time_ms, row.observed_at_ms) // verifier.PARTITION_MS
                for row in fixtures
            }
        )
        > 2
    )
    assert all(
        row.payload()["provenance"] == "synthetic_offline_verification_fixture"
        for row in fixtures
        if row.kind == "status"
    )
    assert fixtures[-1].payload()["error_type"] == "SYNTHETIC_OFFLINE_FAILURE"


async def test_existing_output_refused_before_any_rotation(verifier, temporary, monkeypatch):
    output = temporary / "existing"
    output.mkdir()

    async def forbidden(*args, **kwargs):
        raise AssertionError("preflight refusal should occur before rotation")

    monkeypatch.setattr(verifier, "rotate_archive", forbidden)
    with pytest.raises(FileExistsError):
        await verifier.verify(NEWS, SUPPLY, output)
    assert list(output.iterdir()) == []


def test_output_outside_allowed_workspace_refused(verifier, tmp_path):
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(NEWS, SUPPLY, tmp_path / "outside")


@pytest.mark.parametrize(
    "source_name,field,value,error",
    [
        ("news", "status", "FIXTURE", "NEWS_PUBLIC_PROVENANCE"),
        ("news", "real_public_requests_attempted", 0, "NEWS_PUBLIC_PROVENANCE"),
        ("supply", "status", "FIXTURE", "SUPPLY_PUBLIC_PROVENANCE"),
        ("supply", "real_public_requests_attempted", 0, "SUPPLY_PUBLIC_PROVENANCE"),
    ],
)
def test_provenance_cannot_be_promoted(verifier, temporary, source_name, field, value, error):
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
def test_native_entity_changes_refused(verifier, temporary, source_name, filename, error):
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


def test_database_archive_difference_refused(verifier, temporary):
    copied = temporary / "copied"
    shutil.copytree(NEWS, copied)
    with duckdb.connect(str(copied / "events.duckdb")) as connection:
        connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1")
    with pytest.raises(IntegrityError, match="DATABASE_ARCHIVE_MISMATCH"):
        verifier.preflight(copied, SUPPLY, temporary / "proof")


def test_source_file_budget_refused_before_hashing(verifier, temporary):
    source = temporary / "too-many"
    source.mkdir()
    for index in range(verifier.MAX_FILES + 1):
        (source / f"file-{index}").write_bytes(b"fixture")
    with pytest.raises(IntegrityError, match="SOURCE_FILE_BUDGET"):
        verifier.source_hashes(source)


def test_source_byte_budget_refused_before_hashing(verifier, temporary, monkeypatch):
    source = temporary / "too-big"
    source.mkdir()
    (source / "fixture").write_bytes(b"too big")
    monkeypatch.setattr(verifier, "MAX_BYTES", 1)
    with pytest.raises(IntegrityError, match="SOURCE_BYTE_BUDGET"):
        verifier.source_hashes(source)


def test_non_regular_source_refused(verifier, temporary):
    source = temporary / "non-regular"
    source.mkdir()
    (source / "nested").mkdir()
    with pytest.raises(ValueError, match="regular files"):
        verifier.source_hashes(source)


def test_source_row_budget_refused(verifier, monkeypatch):
    monkeypatch.setattr(verifier, "MAX_ROWS", 10)
    with pytest.raises(IntegrityError, match="SOURCE_ROW_BUDGET"):
        verifier.read_rows(NEWS / "events.duckdb")


def test_native_rss_fields_audited_even_with_updated_entity_hash(verifier, temporary):
    copied = temporary / "copied"
    shutil.copytree(NEWS, copied)
    entity = copied / "response.xml"
    body = entity.read_bytes()
    changed = body.replace(b"Payments firm OpenPayd", b"Synthetic changed title", 1)
    assert changed != body
    entity.write_bytes(changed)
    report_path = copied / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["entities"][0]["sha256"] = file_digest(entity)
    report["entities"][0]["bytes"] = len(changed)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="NEWS_NATIVE_MISMATCH"):
        verifier.preflight(copied, SUPPLY, temporary / "proof")


@pytest.mark.parametrize(
    "response_index,error",
    [(1, "SUPPLY_NATIVE_HEADER"), (3, "SUPPLY_NATIVE_ABI"), (4, "SUPPLY_NATIVE_ABI")],
)
def test_abi_or_chain_native_fact_audited_even_with_updated_entity_hash(
    verifier, temporary, response_index, error
):
    copied = temporary / "copied"
    shutil.copytree(SUPPLY, copied)
    entity = copied / f"response-{response_index:02}.json"
    raw = json.loads(entity.read_bytes())
    raw["result"] = "0x2" if response_index == 1 else "0x" + "0" * 63 + "7"
    body = json.dumps(raw).encode()
    entity.write_bytes(body)
    report_path = copied / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    metadata = report["entities"][response_index - 1]
    metadata["sha256"], metadata["bytes"], metadata["body_hex"] = (
        file_digest(entity),
        len(body),
        body.hex(),
    )
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match=error):
        verifier.preflight(NEWS, copied, temporary / "proof")


async def test_verifier_refuses_completion_if_source_code_changes_during_proof(
    verifier, temporary, monkeypatch
):
    actual_digest = verifier.file_digest
    observed = 0

    def changing_digest(path):
        nonlocal observed
        digest = actual_digest(path)
        if path.name == "archive_rotation.py":
            observed += 1
            if observed > 1:
                return "0" * 64
        return digest

    monkeypatch.setattr(verifier, "file_digest", changing_digest)
    output = temporary / "changed-code"
    with pytest.raises(IntegrityError, match="CODE_CHANGED_DURING_VERIFICATION"):
        await verifier.verify(NEWS, SUPPLY, output)
    assert not (output / "report.json").exists()
    assert not (output / "verification-script.py").exists()
