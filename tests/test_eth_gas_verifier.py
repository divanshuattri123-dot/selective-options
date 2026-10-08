"""Verifier regression checks use synthetic RPC fixtures, never public-success evidence."""

import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.eth_gas import EthereumGasCollector

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("available", [True, False])
async def test_verifier_attempts_and_nonvacuous_dedup(monkeypatch, tmp_path, available):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_eth_gas")
    block = {
        "number": "0x64",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": "0x3e8",
        "gasLimit": "0x100",
        "gasUsed": "0x10",
        "baseFeePerGas": "0x20",
        "transactions": [],
    }
    mocked = AsyncMock(
        side_effect=[("0x1", 1099999), (block, 1100000)] if available else TimeoutError()
    )
    monkeypatch.setattr(EthereumGasCollector, "request", mocked)
    reference = AsyncMock(
        side_effect=[
            (block, 1100001),
            (
                {
                    "oldestBlock": "0x64",
                    "baseFeePerGas": ["0x20", "0x21"],
                    "gasUsedRatio": [0.0625],
                },
                1100002,
            ),
        ]
    )
    monkeypatch.setattr(verifier.RecordingCollector, "reference_request", reference)
    await verifier.verify(tmp_path / "probe")
    report = json.loads((tmp_path / "probe/report.json").read_text())
    assert report["public_requests_attempted"] == (2 if available else 1)
    assert report["accepted_rpc_results"] == (2 if available else 0)
    assert report["pit_checks"] == int(available)
    assert report["repeat_deduplication_verified"] is available
    assert report["status"] == (
        "PUBLIC_FINALIZED_GAS_VERIFIED" if available else "SOURCE_UNAVAILABLE"
    )
    assert report["archive_exact_match"]
    assert report["closed_reload_exact_match"] and report["outbox_exact_match"]
    assert reference.await_count == (2 if available else 0)


@pytest.mark.parametrize(
    "change",
    [
        "block",
        "fee",
        "ratio",
        "oldest",
        "clock",
        "boolean",
        "string",
        "nan",
        "inf",
        "negative",
        "above",
        "extra",
    ],
)
async def test_referenced_block_and_fee_history_refusal(monkeypatch, tmp_path, change):
    from pydantic import ValidationError

    from selective_options.errors import IntegrityError
    from selective_options.eth_gas import gas_event
    from selective_options.store import DuckStore

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_eth_gas")
    block = {
        "number": "0x64",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": "0x3e8",
        "gasLimit": "0x100",
        "gasUsed": "0x10",
        "baseFeePerGas": "0x20",
        "transactions": [],
    }
    fees = {"oldestBlock": "0x64", "baseFeePerGas": ["0x20", "0x21"], "gasUsedRatio": [0.0625]}
    store = await DuckStore.open(tmp_path / "fixture.duckdb")
    collector = verifier.RecordingCollector(store, "drpc")
    collector.previous = gas_event(block, 1100000, 1800000, provider="drpc")
    collector.results = [{"validated_result": "0x1"}, {"validated_result": dict(block)}]
    if change == "block":
        block["hash"] = "0x" + "33" * 32
    if change == "fee":
        fees["baseFeePerGas"] = ["0x22", "0x21"]
    if change == "ratio":
        fees["gasUsedRatio"] = [0.2]
    if change == "oldest":
        fees["oldestBlock"] = "0x65"
    if change == "boolean":
        fees["gasUsedRatio"] = [False]
    if change == "string":
        fees["gasUsedRatio"] = ["0.0625"]
    if change == "nan":
        fees["gasUsedRatio"] = [float("nan")]
    if change == "inf":
        fees["gasUsedRatio"] = [float("inf")]
    if change == "negative":
        fees["gasUsedRatio"] = [-1]
    if change == "above":
        fees["gasUsedRatio"] = [2]
    if change == "extra":
        block["extra_provider_field"] = "unmatched raw value"
    collector.reference_request = AsyncMock(
        side_effect=[(block, 1099999 if change == "clock" else 1100001), (fees, 1100002)]
    )
    try:
        with pytest.raises((IntegrityError, ValidationError)):
            await verifier.crosscheck(collector, None)
    finally:
        await store.close()


@pytest.mark.parametrize("case", ["valid", "wrong-id", "bool-id", "null", "error", "http"])
async def test_reference_rpc_envelopes_and_raw_capture(monkeypatch, tmp_path, case):
    import hashlib

    from selective_options.errors import IntegrityError
    from selective_options.store import DuckStore

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_eth_gas")
    raw = {"jsonrpc": "2.0", "id": 3, "result": {"number": "0x64"}}
    if case == "wrong-id":
        raw["id"] = 2
    if case == "bool-id":
        raw["id"] = True
    if case == "null":
        raw["result"] = None
    if case == "error":
        raw["error"] = {"code": -1}
    body = json.dumps(raw).encode()

    class Response:
        status = 403 if case == "http" else 200

        def __init__(self):
            self.content = self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def iter_chunked(self, size):
            yield body

    class Session:
        def post(self, url, **kwargs):
            assert url == "https://eth.drpc.org/"
            assert kwargs == {
                "json": {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "eth_getBlockByNumber",
                    "params": ["0x64", False],
                },
                "allow_redirects": False,
            }
            return Response()

    store = await DuckStore.open(tmp_path / "fixture.duckdb")
    collector = verifier.RecordingCollector(store, "drpc")
    try:
        if case == "valid":
            assert (
                await collector.reference_request(
                    Session(), "eth_getBlockByNumber", ["0x64", False], 3
                )
            )[0] == raw["result"]
        else:
            with pytest.raises(IntegrityError):
                await collector.reference_request(
                    Session(), "eth_getBlockByNumber", ["0x64", False], 3
                )
        assert collector.requests_attempted == 1
        recorded = collector.raw_responses[0]
        assert recorded["raw_body_hex"] == body.hex()
        assert recorded["body_sha256"] == hashlib.sha256(body).hexdigest()
        assert recorded["observed_at_ms"] > 0 and recorded["http_status"] == Response.status
        assert len(collector.results) == int(case == "valid")
    finally:
        await store.close()


@pytest.mark.parametrize(
    "method,params,rpc_id",
    [
        ("eth_sendRawTransaction", [], 3),
        ("eth_getBlockByNumber", ["latest", False], 3),
        ("eth_getBlockByNumber", ["0x1", True], 3),
        ("eth_feeHistory", ["0xff", "0x1", []], 4),
        ("eth_feeHistory", ["0x1", "0x1", [50]], 4),
    ],
)
async def test_reference_call_restrictions(monkeypatch, tmp_path, method, params, rpc_id):
    from pydantic import ValidationError

    from selective_options.store import DuckStore

    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_eth_gas")
    store = await DuckStore.open(tmp_path / "fixture.duckdb")
    collector = verifier.RecordingCollector(store, "drpc")
    try:
        with pytest.raises((ValueError, ValidationError)):
            await collector.reference_request(None, method, params, rpc_id)
        assert collector.requests_attempted == 0
    finally:
        await store.close()
