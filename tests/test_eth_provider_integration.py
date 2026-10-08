"""Explicit provider selection never silently mixes restart provenance."""

import asyncio
import hashlib
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.eth_checkpoint import load_eth_checkpoint, validate_checkpoint
from selective_options.eth_gas import PUBLIC_PROVIDERS, EthereumGasCollector, gas_event
from selective_options.ingestion import capture
from selective_options.store import DuckStore
from tests.test_eth_checkpoint import raw


@pytest.mark.parametrize("provider", ["publicnode", "cloudflare", "drpc"])
async def test_provider_scoped_checkpoint_exact_rows_and_original_bytes(tmp_path, provider):
    row = gas_event(raw(), 1100000, 1800000, provider=provider)
    validate_checkpoint(row, 2000000, provider=provider)
    assert row.source == PUBLIC_PROVIDERS[provider][1]
    path = tmp_path / "source.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append([row], exact=True)
    finally:
        await store.close()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    assert await load_eth_checkpoint(path, 2000000, provider=provider) == row
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    other = "cloudflare" if provider == "publicnode" else "publicnode"
    with pytest.raises(IntegrityError, match="SCOPE"):
        validate_checkpoint(row, 2000000, provider=other)
    with pytest.raises(IntegrityError, match="EMPTY"):
        await load_eth_checkpoint(path, 2000000, provider=other)


@pytest.mark.parametrize(
    "changes",
    [
        {"provider": "https://example.invalid"},
        {"provider": True},
        {"max_rows": True},
        {"max_rows": 0},
        {"now": True},
        {"now": 0},
    ],
)
async def test_invalid_checkpoint_policy_precedes_file_access(tmp_path, changes):
    kwargs = {"now": 2000000, "max_rows": 10000, "provider": "publicnode", **changes}
    with pytest.raises(ValueError, match="policy"):
        await load_eth_checkpoint(tmp_path / "absent.duckdb", **kwargs)


@pytest.mark.parametrize("provider", [True, "unknown", "https://example.invalid"])
def test_config_rejects_unknown_provider_and_urls(provider):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {"eth_gas_provider": provider}})


async def test_provider_mismatch_resume_rejected_before_output_and_collectors(
    tmp_path, monkeypatch
):
    row = gas_event(raw(), 1100000, 1800000)
    source = tmp_path / "source.duckdb"
    store = await DuckStore.open(source)
    try:
        await store.append([row])
    finally:
        await store.close()
    run = AsyncMock()
    monkeypatch.setattr(EthereumGasCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["ETH"]},
            "ingestion": {"eth_gas_enabled": True, "eth_gas_provider": "drpc"},
        }
    )
    with pytest.raises(IntegrityError, match="EMPTY"):
        await capture(config, tmp_path / "capture", 0.1, resume_eth_gas_from=source)
    assert not (tmp_path / "capture").exists()
    run.assert_not_awaited()


async def test_capture_provider_seed_copy_reload_successor_and_freshness_scope(
    tmp_path, monkeypatch
):
    seed = gas_event(raw(), 1100000, 1800000, provider="drpc")
    source = tmp_path / "source.duckdb"
    store = await DuckStore.open(source)
    try:
        await store.append([seed], exact=True)
    finally:
        await store.close()

    async def idle(*args):
        await asyncio.Event().wait()

    seen = []

    async def run(self):
        assert self.provider == "drpc" and self.source == "ethereum_drpc"
        assert self.previous == seed
        self.request = AsyncMock(side_effect=[("0x1", 1109999), (raw(101), 1110000)])
        assert await self.collect(None)
        seen.append(True)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(EthereumGasCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["ETH"]},
            "ingestion": {
                "eth_gas_enabled": True,
                "eth_gas_provider": "drpc",
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.2, resume_eth_gas_from=source)
    assert seen and report["eth_gas_checkpoint_handoff"] == "COMMITTED_AND_RELOADED"
    assert report["eth_gas_checkpoint_event_id"] == seed.event_id
    assert report["eth_gas_provider"] == "drpc" and report["eth_gas_source"] == "ethereum_drpc"
    health = [item for item in report["freshness"] if item["kind"] in {"on_chain", "feed_health"}]
    assert health and {item["source"] for item in health} == {"ethereum_drpc"}
    latest = await load_eth_checkpoint(tmp_path / "capture/events.duckdb", 2000000, provider="drpc")
    assert latest.payload()["block_number"] == 101
    assert report["trading_enabled"] is False and report["ready_for_signals"] is False
