"""Synthetic RPC and capture fixtures; these tests do not certify public availability."""

import asyncio
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.config import AppConfig
from selective_options.ingestion import capture
from selective_options.solana_network import SolanaNetworkCollector
from tests.test_solana_network import health, responses

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("available", [True, False])
async def test_finite_verifier_saved_evidence_reload_and_refusal(monkeypatch, tmp_path, available):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_solana_network")
    monkeypatch.setattr("selective_options.solana_network.now_ms", lambda: 1000500)
    monkeypatch.setattr("selective_options.solana_network.asyncio.sleep", AsyncMock())
    mocked = AsyncMock(side_effect=responses() if available else TimeoutError())
    monkeypatch.setattr(SolanaNetworkCollector, "request", mocked)
    output = tmp_path / "probe"
    result = await verifier.verify(output)
    assert result == json.loads((output / "report.json").read_text())
    assert result["public_requests_attempted"] == (4 if available else 1)
    assert result["accepted_rpc_results"] == (4 if available else 0)
    assert result["rows"] == result["pending_outbox_rows"] == (3 if available else 1)
    assert result["pit_checks"] == (6 if available else 2)
    assert result["closed_reload_verified"] and result["archive_exact_match"]
    assert result["status"] == (
        "PUBLIC_NETWORK_SAMPLE_VERIFIED" if available else "SOURCE_UNAVAILABLE"
    )
    previous = (output / "events.duckdb").read_bytes()
    mocked.reset_mock()
    with pytest.raises(FileExistsError):
        await verifier.verify(output)
    mocked.assert_not_awaited()
    assert (output / "events.duckdb").read_bytes() == previous


async def test_verifier_failed_closed_reload_is_not_success(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_solana_network")
    monkeypatch.setattr("selective_options.solana_network.now_ms", lambda: 1000500)
    monkeypatch.setattr("selective_options.solana_network.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(SolanaNetworkCollector, "request", AsyncMock(side_effect=responses()))
    monkeypatch.setattr(
        verifier, "read_saved", lambda *args: (_ for _ in ()).throw(OSError("reload failed"))
    )
    with pytest.raises(OSError, match="reload failed"):
        await verifier.verify(tmp_path / "probe")
    report = json.loads((tmp_path / "probe/report.json").read_text())
    assert report["status"] == "FAILED"
    assert (tmp_path / "probe/events.duckdb").exists()


@pytest.mark.parametrize(
    "enabled,asset,failed",
    [(False, "SOL", False), (True, "BTC", False), (True, "SOL", False), (True, "SOL", True)],
)
async def test_capture_asset_opt_in_and_committed_quality(
    monkeypatch, tmp_path, enabled, asset, failed
):
    async def idle(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    calls = []

    async def run(self):
        calls.append(True)
        from selective_options.collectors import now_ms
        from selective_options.solana_network import network_events

        current = now_ms()
        rows = network_events(
            [responses()[1][0][0]],
            (current - 1000) // 1000,
            responses()[3][0],
            (current - 300, current - 200, current - 100, current),
            180000,
        )
        if not failed:
            for row in rows:
                await self.sink(row)
        await self.sink(health(rows if not failed else (), at=current, ok=not failed))
        await idle()

    monkeypatch.setattr(SolanaNetworkCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": [asset]},
            "ingestion": {
                "solana_network_enabled": enabled,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    active = enabled and asset == "SOL"
    assert bool(calls) == active
    assert report["rows"] == (1 if failed else 3) * int(active)
    assert (
        report["solana_network_quality"]["state"]
        == ("BLOCKED" if failed else "NETWORK_SAMPLE_VERIFIED")
        if active
        else report["solana_network_quality"]["state"] == "NOT_CONFIGURED"
    )
    assert report["trading_enabled"] is False and report["milestone_2_complete"] is False
