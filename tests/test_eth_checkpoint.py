import asyncio
import hashlib
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from selective_options.cli import parser
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.eth_checkpoint import load_eth_checkpoint, validate_checkpoint
from selective_options.eth_gas import EthereumGasCollector, gas_event
from selective_options.store import DuckStore


def raw(number=100, **changes):
    return {
        "number": hex(number),
        "hash": "0x" + f"{number:064x}",
        "parentHash": "0x" + f"{number - 1:064x}",
        "timestamp": hex(number * 10),
        "gasLimit": "0x100",
        "gasUsed": "0x10",
        "baseFeePerGas": "0x20",
        "transactions": [],
        **changes,
    }


def event(number=100):
    return gas_event(raw(number), number * 10000 + 100000, 1800000)


async def database(path, rows):
    store = await DuckStore.open(path)
    await store.append(rows)
    await store.close()
    return path


async def test_readonly_latest_validated_anchor(tmp_path):
    path = await database(tmp_path / "source.duckdb", [event(), event(101)])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert await load_eth_checkpoint(path, 2000000) == event(101)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load_eth_checkpoint(path, 2000000, 1)
    for limit in [0, 100001]:
        with pytest.raises(ValueError):
            await load_eth_checkpoint(path, 2000000, limit)
    with pytest.raises(FileNotFoundError):
        await load_eth_checkpoint(tmp_path / "absent", 2000000)


async def test_empty_and_regressive_history(tmp_path):
    empty = await database(tmp_path / "empty.duckdb", [])
    with pytest.raises(IntegrityError):
        await load_eth_checkpoint(empty, 2000000)
    later_old = gas_event(raw(), 1200000, 1800000)
    path = await database(tmp_path / "regression.duckdb", [event(101), later_old])
    with pytest.raises(IntegrityError, match="REGRESSION"):
        await load_eth_checkpoint(path, 2000000)


@pytest.mark.parametrize(
    "change",
    [
        {"asset": "BTC"},
        {"symbol": "OTHER"},
        {"timestamp_kind": "receipt"},
        {"event_time_ms": 1000001},
        {"observed_at_ms": 2000001},
        {"observed_at_ms": 900000},
        {"event_id": "tampered"},
    ],
)
def test_invalid_scope_clock_or_identity(change):
    with pytest.raises(IntegrityError):
        validate_checkpoint(event().model_copy(update=change), 2000000)


@pytest.mark.parametrize(
    "change",
    [
        {"gas_limit": "0"},
        {"gas_used": "257"},
        {"base_fee_per_gas_wei": str(2**256)},
        {"gas_limit": str(2**256)},
        {"gas_used": "01"},
        {"block_hash": "bad"},
        {"chain_id": 2},
        {"execution_quote": True},
        {"unexpected": 1},
    ],
)
def test_invalid_stored_payload(change):
    row = event()
    row = row.model_copy(update={"payload_json": json.dumps({**row.payload(), **change})})
    with pytest.raises((IntegrityError, ValidationError)):
        validate_checkpoint(row, 2000000)


def test_cli_option():
    args = parser().parse_args(
        ["capture", "--output", "new", "--resume-eth-gas-from", "old.duckdb"]
    )
    assert str(args.resume_eth_gas_from) == "old.duckdb"


def test_cli_forwards_eth_checkpoint(monkeypatch, tmp_path):
    from selective_options.cli import main

    capture = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", capture)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path),
            "--resume-eth-gas-from",
            "old.duckdb",
        ],
    )
    assert main() == 0
    assert str(capture.await_args.kwargs["resume_eth_gas_from"]) == "old.duckdb"
    assert capture.await_args.kwargs["recovery_marker"] is None


@pytest.mark.parametrize("cancel", [False, True])
async def test_failed_reload_never_starts_collectors(monkeypatch, tmp_path, cancel):
    from selective_options.ingestion import capture

    seed = event()
    path = await database(tmp_path / "source.duckdb", [seed])
    replacement = asyncio.CancelledError() if cancel else event(101)
    monkeypatch.setattr(
        "selective_options.ingestion.load_eth_checkpoint",
        AsyncMock(side_effect=[seed, replacement]),
    )
    run = AsyncMock()
    monkeypatch.setattr(EthereumGasCollector, "run", run)
    config = AppConfig.model_validate({"ingestion": {"eth_gas_enabled": True}})
    with pytest.raises(asyncio.CancelledError if cancel else IntegrityError):
        await capture(config, tmp_path / "capture", 0.1, resume_eth_gas_from=path)
    run.assert_not_awaited()
    # The successfully copied anchor remains readable after failure/cancellation.
    assert await load_eth_checkpoint(tmp_path / "capture/events.duckdb", 2000000) == seed


async def test_preflight_rejects_disabled_source_without_creating_output(tmp_path):
    from selective_options.ingestion import capture

    with pytest.raises(ValueError):
        await capture(AppConfig(), tmp_path / "new", 0.1, resume_eth_gas_from=tmp_path / "missing")
    assert not (tmp_path / "new").exists()


async def test_capture_copies_reloads_and_enforces_anchor(monkeypatch, tmp_path):
    from selective_options.ingestion import capture

    seed = event()
    path = await database(tmp_path / "source.duckdb", [seed])

    async def idle(*args):
        await asyncio.Event().wait()

    checked = []

    async def run(self):
        assert self.previous == seed
        # Same block is deduplicated, regression rejected, valid successor accepted.
        for number, accepted in [(100, True), (99, False), (101, True)]:
            self.request = AsyncMock(side_effect=[("0x1", 1109999), (raw(number), 1110000)])
            assert await self.collect(None) is accepted
        checked.append(True)
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr(EthereumGasCollector, "run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["ETH"]},
            "ingestion": {
                "eth_gas_enabled": True,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.2, resume_eth_gas_from=path)
    assert checked and report["eth_gas_checkpoint_handoff"] == "COMMITTED_AND_RELOADED"
    assert report["eth_gas_checkpoint_event_id"] == seed.event_id
    latest = await load_eth_checkpoint(tmp_path / "capture/events.duckdb", 2000000)
    assert latest.payload()["block_number"] == 101
    store = await DuckStore.open(tmp_path / "capture/events.duckdb")
    try:
        original = (await store.snapshot("ETH", 1100000, "public_reference")).rows()
        assert original == (seed,)
    finally:
        await store.close()
