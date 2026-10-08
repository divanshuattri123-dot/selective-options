"""Explicit mocked response fixtures; no public collection or historical sample."""

import asyncio
import hashlib
import json
import threading

import duckdb
import pytest

from scripts import audit_coinmetrics as auditor
from scripts import verify_coinmetrics as producer
from selective_options.coinmetrics import DAY_MS, native_time_ms
from selective_options.errors import IntegrityError

LABEL = native_time_ms("2026-10-05T00:00:00.000000000Z")
START = LABEL + 2 * DAY_MS + 100
RECEIPT = START + 10
FINISH = START + 20


@pytest.fixture
async def fixture_proof(tmp_path, monkeypatch):
    """Execute actual proof pipeline with explicitly tagged synthetic HTTP bytes."""
    output = tmp_path / "explicit-mock-proof"
    monkeypatch.setattr(producer, "preflight", lambda path: path)
    monkeypatch.setattr(producer, "now_ms", lambda: FINISH + 1)
    times = iter((START, FINISH))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(times))
    native = [
        {
            "asset": asset,
            "time": "2026-10-05T00:00:00.000000000Z",
            "CapMrktCurUSD": "123456789012345.123456789",
            "CapMVRVCur": "1.23456789",
        }
        for asset in ("btc", "eth")
    ]

    async def request(self, _session, parameters):
        self.request_attempts += 1
        body = json.dumps({"data": native}).encode()
        await self.response_received(parameters, 200, body, RECEIPT)
        return {"data": native}, RECEIPT

    monkeypatch.setattr(producer.RecordingCollector, "request", request)
    report = await producer.verify(output, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "MOCK_DAILY_VALUATION_POLL_VERIFIED"
    assert report["real_public_requests_attempted"] == 0
    return output


async def test_labelled_fixture_native_archive_restore_and_pit_audit(fixture_proof):
    before = auditor.tree(fixture_proof)
    result = await auditor.audit(fixture_proof, transport_provenance="explicit_mock_fixture")
    assert result["status"] == "MOCK_FIXTURE_AUDITED" and not result["public_requests_observed"]
    assert result["native"]["native_rows"] == 2 and result["committed_rows"] == 4
    assert result["pending_delivery"] == 4 and len(result["boundaries"]) == 4
    assert result["artifact_sha256"] == before == auditor.tree(fixture_proof)
    assert result["new_public_requests"] == 0 and not result["trading_enabled"]


async def test_fixture_cannot_be_audited_as_public_collection(fixture_proof):
    with pytest.raises(IntegrityError, match="TRANSPORT_PROVENANCE"):
        await auditor.audit(fixture_proof)


@pytest.mark.parametrize(
    "mutation",
    [
        "native_value",
        "native_unit",
        "source_receipt",
        "quality_value",
        "readiness",
        "public_count",
        "boundary",
    ],
)
async def test_rehashed_mutations_refused(fixture_proof, mutation):
    report_path = fixture_proof / "report.json"
    report = json.loads(report_path.read_text())
    if mutation == "native_value":
        path = fixture_proof / "native-entities.json"
        entities = json.loads(path.read_text())
        entity = entities["entities"][0]
        body = json.loads(bytes.fromhex(entity["body_hex"]))
        body["data"][0]["CapMrktCurUSD"] = "987654321.001"
        raw = json.dumps(body).encode()
        entity.update(body_hex=raw.hex(), bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        path.write_text(json.dumps(entities))
        report["native_entities_sha256"] = auditor.digest(path)
    elif mutation == "native_unit":
        report["quality"]["BTC"]["native_units"]["CapMVRVCur"] = "USD"
    elif mutation == "source_receipt":
        with duckdb.connect(str(fixture_proof / "events.duckdb")) as connection:
            connection.execute(
                "UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE kind='on_chain' AND asset='BTC'"
            )
        report["source_sha256"] = auditor.digest(fixture_proof / "events.duckdb")
    elif mutation == "quality_value":
        report["quality"]["BTC"]["native_row"]["CapMVRVCur"] = "99"
    elif mutation == "readiness":
        report["ready_for_signals"] = True
    elif mutation == "public_count":
        report["real_public_requests_attempted"] = 1
    else:
        report["boundary_cutoffs_ms"].pop()
    report_path.write_text(json.dumps(report))
    with pytest.raises(IntegrityError):
        await auditor.audit(fixture_proof, transport_provenance="explicit_mock_fixture")


async def test_acknowledged_restore_refused_even_with_updated_database_digest(fixture_proof):
    restored = fixture_proof / "restored/events.duckdb"
    with duckdb.connect(str(restored)) as connection:
        connection.execute("UPDATE delivery_outbox SET delivered=TRUE")
    path = fixture_proof / "report.json"
    report = json.loads(path.read_text())
    report["restored_sha256"] = auditor.digest(restored)
    path.write_text(json.dumps(report))
    with pytest.raises(IntegrityError, match="OUTBOX_EXACT_ALL_PENDING"):
        await auditor.audit(fixture_proof, transport_provenance="explicit_mock_fixture")


async def test_changed_current_source_binding_refused(fixture_proof):
    path = fixture_proof / "report.json"
    report = json.loads(path.read_text())
    report["code_hashes"]["src/selective_options/coinmetrics.py"] = "0" * 64
    path.write_text(json.dumps(report))
    with pytest.raises(IntegrityError, match="CURRENT_SOURCE_HASH"):
        await auditor.audit(fixture_proof, transport_provenance="explicit_mock_fixture")


async def test_repeated_cancel_drains_read_only_worker(monkeypatch, tmp_path):
    entered, release, done = asyncio.Event(), threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def worker(*_args):
        loop.call_soon_threadsafe(entered.set)
        release.wait()
        done.set()
        return {"status": "EXPLICIT_TEST_ONLY"}

    monkeypatch.setattr(auditor, "_audit", worker)
    task = asyncio.create_task(auditor.audit(tmp_path))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0.01)
    task.cancel()
    assert not task.done() and not done.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert done.is_set()


async def test_actual_module_cli_refuses_mock_and_does_not_create_output(fixture_proof, tmp_path):
    process = await asyncio.create_subprocess_exec(
        str(producer.ROOT.parents[1] / "work/.venv/Scripts/python.exe"),
        "-m",
        "scripts.audit_coinmetrics",
        "--proof",
        str(fixture_proof),
        "--output",
        str(tmp_path / "audit.json"),
        cwd=producer.ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    assert process.returncode != 0 and b"TRANSPORT_PROVENANCE" in stderr
    assert not (tmp_path / "audit.json").exists()
