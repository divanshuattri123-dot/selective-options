"""Finite verifier tests use explicit mocked HTTP; no public availability claim."""

import importlib
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import aiohttp
import pytest

from selective_options.errors import IntegrityError
from selective_options.tips_yield import TipsYieldCollector
from tests.test_tips_yield import CSV, RECEIVED, Session, snapshot

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_tips_yield")


@pytest.fixture
def output():
    scratch = ROOT.parents[1] / "work/tips-yield-0339"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="verifier-", dir=scratch) as temporary:
        yield Path(temporary) / "probe"


@pytest.mark.parametrize("days", [True, 0, -1, 367, 2.5, "90"])
def test_preflight_strict_policy_before_files_or_http(verifier, output, days):
    with pytest.raises(ValueError):
        verifier.preflight(output, days)
    assert not output.exists()


def test_preflight_restricts_workspace_scope_and_existing_output(verifier, output, tmp_path):
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(tmp_path / "probe", 90)
    output.mkdir()
    sentinel = output / "sentinel"
    sentinel.write_bytes(b"keep")
    with pytest.raises(FileExistsError, match="overwrite"):
        verifier.preflight(output, 90)
    assert sentinel.read_bytes() == b"keep"


@pytest.mark.parametrize("mode", ["success", "http403", "invalid_csv", "transport"])
async def test_mock_http_full_finite_archive_pit_reopen_bound_view(
    verifier, output, monkeypatch, mode
):
    session = Session(
        body=CSV if mode != "invalid_csv" else b"<html>fixture</html>",
        status=403 if mode == "http403" else 200,
        error=aiohttp.ClientConnectionError("Explicit mock fixture")
        if mode == "transport"
        else None,
    )

    class ManagedSession:
        def __init__(self, **kwargs):
            assert kwargs["timeout"].total <= 30

        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(verifier.aiohttp, "ClientSession", ManagedSession)
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)
    result = await verifier.verify(
        output, history_days=90, transport_provenance="explicit_mock_fixture"
    )
    assert result == json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert result["real_public_requests_attempted"] == 0
    assert result["request_attempts"] == len(session.calls) == 1
    assert result["retries"] == result["orders_submitted"] == 0
    assert result["status"] == (
        "FIXTURE_DFII10_SNAPSHOT_VERIFIED" if mode == "success" else "SOURCE_UNAVAILABLE_OR_INVALID"
    )
    assert (
        result["persisted_rows"]
        == result["outbox_pending_after_reopen"]
        == (2 if mode == "success" else 1)
    )
    assert result["pit_boundary_checks"] == 2 * result["persisted_rows"]
    assert result["parquet_exact_match"] and result["closed_reload_exact_match"]
    assert result["source_code_unchanged"] and result["bounded_store_quality_matches"]
    assert result["trading_approval"] is result["milestone_2_complete"] is False
    if mode != "transport":
        assert (output / "response.csv").read_bytes() == session.body
    else:
        assert result["entities"] == [] and not (output / "response.csv").exists()
    if mode == "success":
        assert result["native_rows"][1]["yield_percent_text"] == "-0.50"
        assert result["native_rows"][2]["missing"] == "dot"
        assert result["native_rows"][3]["missing"] == "empty"
    original = (output / "events.duckdb").read_bytes()
    with pytest.raises(FileExistsError):
        await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert len(session.calls) == 1 and (output / "events.duckdb").read_bytes() == original


def test_independent_comparison_detects_native_payload_corruption(verifier):
    row = snapshot()
    changed = row.model_copy(update={"payload_json": json.dumps({**row.payload(), "rows": []})})
    with pytest.raises(IntegrityError, match="NATIVE_VALUES_MISMATCH"):
        verifier.independent_entity(CSV, changed)
    with pytest.raises(IntegrityError, match="HEADER"):
        verifier.independent_entity(b"DATE,DFII10\n", row)
    with pytest.raises(IntegrityError, match="ROW_SHAPE"):
        verifier.independent_entity(b"observation_date,DFII10\n1,2,3\n", row)


async def test_request_budget_refuses_second_http_call(verifier, output, monkeypatch):
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: RECEIVED)
    store = await verifier.DuckStore.open(output.parent / "budget.duckdb")
    try:
        collector = verifier.RecordingCollector(verifier.AppConfig(), store)
        session = Session()
        await collector.request(session, date(2026, 9, 28), date(2026, 10, 2))
        with pytest.raises(IntegrityError, match="REQUEST_BUDGET"):
            await collector.request(session, date(2026, 9, 28), date(2026, 10, 2))
        assert len(session.calls) == 1
    finally:
        await store.close()


async def test_failed_closed_reload_cannot_emit_success_report(verifier, output, monkeypatch):
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)

    async def unavailable(self, session, start_date, end_date):
        raise aiohttp.ClientConnectionError("Explicit mock fixture")

    monkeypatch.setattr(TipsYieldCollector, "request", unavailable)

    def failed_reload(*args):
        raise IntegrityError("Fixture closed reload failed")

    monkeypatch.setattr(verifier, "closed_rows", failed_reload)
    with pytest.raises(IntegrityError, match="closed reload failed"):
        await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert (output / "events.duckdb").exists()
    assert not (output / "report.json").exists()


async def test_invalid_provenance_no_output(verifier, output):
    with pytest.raises(ValueError, match="provenance"):
        await verifier.verify(output, transport_provenance="invented")
    assert not output.exists()
