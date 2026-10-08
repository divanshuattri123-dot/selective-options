"""Offline replay verifier checks; immutable source proof remains untouched."""

import importlib
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pytest

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.stablecoin_checkpoint import load_stablecoin_checkpoint

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/stablecoin-supply-public-01"


@pytest.fixture
def workspace_temp():
    scratch = ROOT.parents[1] / "work/stablecoin-restart-0340"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as temporary:
        yield Path(temporary)


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_stablecoin_restart")


def copy_source(verifier, temporary):
    copied = temporary / "source"
    copied.mkdir()
    for name in verifier.SOURCE_NAMES:
        shutil.copyfile(SOURCE / name, copied / name)
    return copied


async def test_actual_saved_native_replay_through_closed_failure_and_recovery(
    verifier, workspace_temp, monkeypatch
):
    def no_network(*args, **kwargs):
        raise AssertionError("offline verifier must not create a network session")

    monkeypatch.setattr("aiohttp.ClientSession", no_network)
    before = verifier.hashes(SOURCE)
    output = workspace_temp / "proof"
    report = await verifier.verify(SOURCE, output)
    assert report == json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "OFFLINE_SAVED_PUBLIC_SUPPLY_RESTART_VERIFIED"
    assert report["native_records"] == 2 and report["persisted_records"] == 7
    assert report["real_public_requests"] == 0 and report["mocked_http_attempts"] == 22
    assert (
        report["source_hashes_before"]
        == report["source_hashes_after"]
        == before
        == verifier.hashes(SOURCE)
    )
    assert report["exact_closed_reloads"] == 3 and report["three_archives_exact"]
    assert report["original_receipts_preserved"] and report["failure_checkpoint_refused"]
    assert report["outbox_pending_after_final_reopen"] == 7 and report["pit_checks"] == 14
    scenarios = report["quality_scenarios"]
    assert scenarios["original"]["state"] == "POLL_OBSERVED_VERIFIED"
    assert scenarios["before_failure"]["state"] == "POLL_OBSERVED_VERIFIED"
    assert scenarios["at_failure"] == scenarios["reopened_failure"]
    for name in (
        "at_failure",
        "before_recovery_response",
        "at_recovery_response",
        "before_recovery_health",
    ):
        assert scenarios[name]["blocker"] == "STABLECOIN_FEED_UNAVAILABLE"
    assert scenarios["at_recovery_health"] == scenarios["reopened_recovery"]
    assert scenarios["at_recovery_health"]["state"] == "POLL_OBSERVED_VERIFIED"
    clocks = report["simulated_health_clocks"]
    with pytest.raises(IntegrityError, match="FEED_UNAVAILABLE"):
        await load_stablecoin_checkpoint(output / "events.duckdb", clocks["failure"])
    final = await load_stablecoin_checkpoint(output / "events.duckdb", clocks["recovery"])
    initial = verifier.preflight(SOURCE, workspace_temp / "unused").snapshot
    assert final.event_id != initial.event_id and final.event_time_ms == initial.event_time_ms
    assert (
        final.payload()["usdc"]["total_supply_base_units"]
        == initial.payload()["usdc"]["total_supply_base_units"]
    )
    for field in (
        "redis_delivery_claim",
        "continuous_history_complete",
        "actual_outage_recovery_verified",
        "milestone_2_complete",
        "trading_approval",
    ):
        assert report[field] is False
    assert report["orders_submitted"] == 0


async def test_existing_output_refused_before_replay(verifier, workspace_temp, monkeypatch):
    output = workspace_temp / "existing"
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_bytes(b"immutable")
    monkeypatch.setattr(verifier, "replay", lambda *a, **k: pytest.fail("replayed"))
    with pytest.raises(FileExistsError):
        await verifier.verify(SOURCE, output)
    assert marker.read_bytes() == b"immutable"


def test_output_outside_owned_scope_refused(verifier, workspace_temp):
    with pytest.raises(ValueError, match="workspace scratch"):
        verifier.preflight(SOURCE, workspace_temp.parent.parent / "unowned")


@pytest.mark.parametrize(
    "case", ["request", "receipt", "identity", "body_hash", "body_hex", "provenance"]
)
def test_corrupt_native_evidence_rejected_without_output(verifier, workspace_temp, case):
    source = copy_source(verifier, workspace_temp)
    report_path = source / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    entry = report["entities"][0]
    if case == "request":
        entry["request"]["method"] = "eth_sendRawTransaction"
    elif case == "receipt":
        entry["received_at_ms"] = True
    elif case == "identity":
        response_path = source / "response-01.json"
        body = b'{"jsonrpc":"2.0","id":2,"result":"0x1"}'
        response_path.write_bytes(body)
        entry["sha256"], entry["bytes"], entry["body_hex"] = (
            file_digest(response_path),
            len(body),
            body.hex(),
        )
    elif case == "body_hash":
        entry["sha256"] = "0" * 64
    elif case == "body_hex":
        entry["body_hex"] = "00"
    else:
        report["status"] = "FIXTURE_ETHEREUM_SUPPLY_PAIR_VERIFIED"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    output = workspace_temp / "unused"
    with pytest.raises(IntegrityError):
        verifier.preflight(source, output)
    assert not output.exists()


def test_database_archive_mismatch_rejected(verifier, workspace_temp):
    source = copy_source(verifier, workspace_temp)
    with duckdb.connect(str(source / "events.duckdb")) as connection:
        connection.execute("DELETE FROM events WHERE kind='feed_health'")
    with pytest.raises(IntegrityError, match="ARCHIVE_MISMATCH"):
        verifier.preflight(source, workspace_temp / "unused")


def test_missing_native_body_rejected(verifier, workspace_temp):
    source = workspace_temp / "partial"
    source.mkdir()
    for name in verifier.SOURCE_NAMES:
        if name != "response-07.json":
            shutil.copyfile(SOURCE / name, source / name)
    with pytest.raises(ValueError, match="missing"):
        verifier.preflight(source, workspace_temp / "unused")
