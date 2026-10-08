"""Independent saved canonical Kraken event batch proof checks."""

import asyncio
import importlib
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pytest

from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/live-gap-02"


@pytest.fixture
def temporary():
    scratch = ROOT.parents[1] / "work/spot-batch-0344"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as path:
        yield Path(path)


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_spot_batch")


async def test_real_collector_bus_consumer_native_replay_and_invalid_batch(
    verifier, temporary, monkeypatch
):
    def no_network(*args, **kwargs):
        raise AssertionError("offline proof must not use a network session or socket")

    monkeypatch.setattr("aiohttp.ClientSession", no_network)
    monkeypatch.setattr("selective_options.spot_trades.connect", no_network)
    before = verifier.source_hashes(SOURCE)
    output = temporary / "proof"
    report = await verifier.verify(SOURCE, output)
    assert report["status"] == "OFFLINE_SAVED_SPOT_BATCH_STAGING_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    assert report["saved_native_trade_rows"] == report["replayed_native_trade_rows"] == 55
    assert report["persisted_rows"] == report["pending_delivery_rows"] == 61
    assert [cohort["native_rows"] for cohort in report["cohorts"]] == [50, 5]
    assert [cohort["first_trade_id"] for cohort in report["cohorts"]] == [109230627, 109230678]
    assert [cohort["last_trade_id"] for cohort in report["cohorts"]] == [109230676, 109230682]
    assert all(cohort["timeframe_s"] == 300 for cohort in report["cohorts"])
    assert all(cohort["overlap_reemitted_rows"] == 0 for cohort in report["cohorts"])
    assert report["cohorts"][1]["native_successor_within_cohort_verified"]
    assert all(cohort["continuity_across_cohorts_claimed"] is False for cohort in report["cohorts"])
    assert report["source_native_gap_trade_id"] == 109230677
    assert len(report["reconstructed_containers"]) == 3
    assert all(
        frame["provenance"] == "reconstructed_container_from_saved_canonical_events"
        for frame in report["reconstructed_containers"]
    )
    assert report["original_websocket_frames_available"] is False
    assert report["original_frame_boundaries_verified"] is False
    refused = report["synthetic_rejected_batch"]
    assert "LATE_TRADE" in refused["error"] and refused["emitted_rows"] == 0
    assert refused["state_sha256_before"] == refused["state_sha256_after"]
    assert refused["database_unchanged"]
    assert refused["candidate_event_ids"][0] in report["persisted_event_ids"]
    assert refused["candidate_event_ids"][1] not in report["persisted_event_ids"]
    accepted = report["cohorts"][0]["synthetic_accepted_trade_event_ids"]
    assert accepted == [refused["candidate_event_ids"][0]]
    assert (
        report["derived_event_provenance"][accepted[0]]["provenance"]
        == "synthetic_offline_accepted_trade_fixture"
    )
    assert len(report["pit_checks"]) >= 50
    assert report["pit_checks"][-1]["available_rows"] == 61
    assert report["consumer_queue_drained"]
    assert report["source_hashes_before"] == report["source_hashes_after"] == before
    assert report["code_sha256_before"] == report["code_sha256_after"]
    assert report["verifier_sha256"] == file_digest(output / "verification-script.py")
    for field in (
        "redis_delivery_claimed",
        "current_feed_health_verified",
        "complete_history_verified",
        "seven_day_validation_complete",
        "ready_for_signals",
        "trading_enabled",
        "continuity_across_native_gap_claimed",
    ):
        assert report[field] is False
    saved = verifier.preflight(SOURCE, temporary / "unused")
    persisted = verifier.read_rows(output / "events.duckdb", output / "events.parquet")
    native_ids = {row.event_id for row in saved.native}
    assert tuple(row for row in persisted if row.event_id in native_ids) == tuple(
        sorted(saved.native, key=lambda row: row.event_id)
    )
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*),count(*) FILTER(WHERE delivered) FROM delivery_outbox"
        ).fetchone() == (61, 0)
    assert (
        json.loads((output / "report.json").read_text(encoding="utf-8"))["cohorts"]
        == report["cohorts"]
    )
    assert verifier.source_hashes(SOURCE) == before


def test_preflight_exact_native_contiguous_cohorts(verifier, temporary):
    saved = verifier.preflight(SOURCE, temporary / "proof")
    assert len(saved.native) == 55 and [len(cohort) for cohort in saved.cohorts] == [50, 5]
    assert saved.omitted_id == 109230677
    assert len(saved.hashes) == 9


async def test_existing_output_refused_before_collector(verifier, temporary, monkeypatch):
    output = temporary / "existing"
    output.mkdir()

    async def forbidden(*args, **kwargs):
        raise AssertionError("collector must not run after preflight refusal")

    monkeypatch.setattr(SpotTradeCollector, "_trades", forbidden)
    with pytest.raises(FileExistsError):
        await verifier.verify(SOURCE, output)
    assert list(output.iterdir()) == []


def test_output_outside_workspace_refused(verifier, tmp_path):
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(SOURCE, tmp_path / "outside")


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "FIXTURE"),
        ("orders_submitted", 1),
        ("trading_enabled", True),
        ("evidence_type", "fixture"),
    ],
)
def test_source_provenance_cannot_be_promoted(verifier, temporary, field, value):
    copied = temporary / "source"
    shutil.copytree(SOURCE, copied)
    path = copied / "report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    report[field] = value
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="SOURCE_PROVENANCE"):
        verifier.preflight(copied, temporary / "proof")


def test_missing_native_gap_report_refused(verifier, temporary):
    copied = temporary / "source"
    shutil.copytree(SOURCE, copied)
    path = copied / "report.json"
    report = json.loads(path.read_text(encoding="utf-8"))
    report["omitted_trade_id"] = 1
    path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="NATIVE_COHORT_GAP"):
        verifier.preflight(copied, temporary / "proof")


def test_source_database_archive_disagreement_refused(verifier, temporary):
    copied = temporary / "source"
    shutil.copytree(SOURCE, copied)
    with duckdb.connect(str(copied / "events.duckdb")) as connection:
        connection.execute("UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE kind='trade'")
    with pytest.raises(IntegrityError, match="DATABASE_ARCHIVE_MISMATCH"):
        verifier.preflight(copied, temporary / "proof")


def test_archive_byte_change_refused(verifier, temporary):
    copied = temporary / "source"
    shutil.copytree(SOURCE, copied)
    with (copied / "events.parquet").open("ab") as stream:
        stream.write(b"fixture corruption")
    with pytest.raises(IntegrityError, match="ARCHIVE_CHECKSUM_MISMATCH"):
        verifier.preflight(copied, temporary / "proof")


def test_source_row_budget_refused(verifier, monkeypatch):
    monkeypatch.setattr(verifier, "MAX_ROWS", 10)
    with pytest.raises(IntegrityError, match="ROW_BUDGET"):
        verifier.read_rows(SOURCE / "events.duckdb")


def test_source_file_budget_refused(verifier, monkeypatch):
    monkeypatch.setattr(verifier, "MAX_FILES", 1)
    with pytest.raises(IntegrityError, match="SOURCE_FILE_BUDGET"):
        verifier.source_hashes(SOURCE)


def test_source_byte_budget_refused(verifier, monkeypatch):
    monkeypatch.setattr(verifier, "MAX_BYTES", 1)
    with pytest.raises(IntegrityError, match="SOURCE_BYTE_BUDGET"):
        verifier.source_hashes(SOURCE)


async def test_changed_code_refuses_success_report(verifier, temporary, monkeypatch):
    original_digest = verifier.file_digest
    calls = 0

    def changing(path):
        nonlocal calls
        actual = original_digest(path)
        if path.name == "spot_trades.py":
            calls += 1
            if calls > 1:
                return "0" * 64
        return actual

    monkeypatch.setattr(verifier, "file_digest", changing)
    output = temporary / "changed-code"
    with pytest.raises(IntegrityError, match="SOURCE_OR_CODE_CHANGED"):
        await verifier.verify(SOURCE, output)
    assert not (output / "report.json").exists()
    assert not (output / "verification-script.py").exists()


@pytest.mark.parametrize(
    "failure", [IntegrityError("SYNTHETIC_REPLAY_FAILURE"), asyncio.CancelledError()]
)
async def test_replay_failure_or_cancellation_closes_writer_and_store(
    verifier, temporary, monkeypatch, failure
):
    closed = []
    actual_close = DuckStore.close

    async def observe_close(store):
        await actual_close(store)
        closed.append(store._closed)

    async def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(SpotTradeCollector, "_trades", fail)
    monkeypatch.setattr(DuckStore, "close", observe_close)
    output = temporary / "failed"
    with pytest.raises(type(failure)):
        await verifier.verify(SOURCE, output)
    assert closed and all(closed)
    assert not (output / "report.json").exists()
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone() == (0,)


def test_output_nested_inside_source_refused_before_mutation(verifier, temporary):
    copied = temporary / "source"
    shutil.copytree(SOURCE, copied)
    before = verifier.source_hashes(copied)
    output = copied / "nested-proof"
    with pytest.raises(FileExistsError, match="original evidence"):
        verifier.preflight(copied, output)
    assert not output.exists()
    assert verifier.source_hashes(copied) == before
