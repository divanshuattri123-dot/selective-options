"""Finite verifier outcomes use synthetic captures and real closed storage; no network."""

import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore
from tests.test_option_coverage import control, fixture

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("outcome", ["observed", "inactive", "failure", "cancelled"])
async def test_finite_outcomes_with_closed_database(
    monkeypatch, tmp_path, instrument, ticker, outcome
):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_option_coverage")

    async def capture(cfg, output, duration):
        assert cfg.system.assets == ("BTC",)
        assert cfg.ingestion.max_stream_instruments_per_asset is None
        assert cfg.ingestion.subscription_batch_size == 100
        assert duration == 75
        rows = fixture(instrument, ticker)
        if outcome == "inactive":
            rows.append(control("STREAM_DISCONNECTED", 1500, error_type="TimeoutError"))
        output.mkdir()
        store = await DuckStore.open(output / "events.duckdb")
        try:
            await store.append(rows)
            await store.append(rows)
        finally:
            await store.close()
        result = {
            "ended_at_ms": 1500,
            "option_stream_coverage": [coverage_at(rows, "BTC", 1500, 30000)],
        }
        (output / "report.json").write_text(json.dumps(result), encoding="utf-8")
        if outcome == "failure":
            raise OSError("synthetic post-commit failure")
        if outcome == "cancelled":
            import asyncio

            raise asyncio.CancelledError
        return result

    monkeypatch.setattr(verifier, "capture", capture)
    output = tmp_path / "capture"
    if outcome == "cancelled":
        import asyncio

        with pytest.raises(asyncio.CancelledError):
            await verifier.verify(output)
        assert not (output / "verification.json").exists()
        return
    report = await verifier.verify(output)
    assert report["status"] == (
        "UNCAPPED_SUBSCRIPTIONS_OBSERVED"
        if outcome == "observed"
        else "UNCAPPED_COVERAGE_NOT_VERIFIED"
    )
    assert report["failure_type"] == ("OSError" if outcome == "failure" else None)
    assert report["saved_rows"] == (7 if outcome == "inactive" else 6)
    assert report["closed_reload_coverage_matches"] is True
    assert report["sustained_capacity_verified"] is False
    assert report["all_required_assets_or_feeds_verified"] is False
    assert report["milestone_2_complete"] is False
    assert report["trading_enabled"] is False


@pytest.mark.parametrize("duration", [39, 181, float("nan"), float("inf")])
async def test_invalid_duration_no_output_or_network(monkeypatch, tmp_path, duration):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_option_coverage")
    mocked = AsyncMock()
    monkeypatch.setattr(verifier, "capture", mocked)
    with pytest.raises(ValueError):
        await verifier.verify(tmp_path / "capture", duration)
    mocked.assert_not_awaited()
    assert not (tmp_path / "capture").exists()


async def test_existing_directory_no_mutation_or_network(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_option_coverage")
    mocked = AsyncMock()
    monkeypatch.setattr(verifier, "capture", mocked)
    anchor = tmp_path / "anchor.txt"
    anchor.write_bytes(b"preserved")
    with pytest.raises(FileExistsError):
        await verifier.verify(tmp_path)
    mocked.assert_not_awaited()
    assert anchor.read_bytes() == b"preserved"
