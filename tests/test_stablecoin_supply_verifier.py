"""Finite verifier transport fixtures are explicitly labelled and make no public proof claim."""

import copy
import hashlib
import json

import pytest

from scripts import verify_stablecoin_supply as proof
from selective_options.errors import IntegrityError
from selective_options.stablecoin_supply import STEPS, rpc_request, supply_event


def native_values():
    header = {
        "number": "0x64",
        "hash": "0x" + "11" * 32,
        "parentHash": "0x" + "22" * 32,
        "timestamp": "0x3e8",
    }

    def word(value):
        return "0x" + f"{value:064x}"

    return ["0x1", header, word(2**256 - 1), word(6), word(9007199254740993), word(6), header]


def entities():
    result = []
    for index, (step, value) in enumerate(zip(STEPS, native_values(), strict=True), 1):
        raw = json.dumps({"jsonrpc": "2.0", "id": index, "result": value}).encode()
        result.append(
            {
                "request": rpc_request(step, None if index < 3 else 100),
                "body_hex": raw.hex(),
                "received_at_ms": 1_100_000,
                "http_status": 200,
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    return result


def native_row():
    return supply_event([(value, 1_100_000) for value in native_values()], 1_800_000)


def test_independent_native_exact_uint256_pair():
    proof.independent_native(entities(), native_row())


@pytest.mark.parametrize(
    "mutation",
    ["count", "request", "body", "status", "checksum", "receipt", "supply", "header", "bool_id"],
)
def test_independent_native_corruption_refused(mutation):
    rows = copy.deepcopy(entities())
    row = native_row()
    if mutation == "count":
        rows.pop()
    elif mutation == "request":
        rows[2]["request"]["params"][0]["to"] = "0x" + "00" * 20
    elif mutation == "body":
        rows[0]["body_hex"] = False
    elif mutation == "status":
        rows[0]["http_status"] = 403
    elif mutation == "checksum":
        rows[0]["sha256"] = "0" * 64
    elif mutation == "receipt":
        rows[3]["received_at_ms"] += 1
    elif mutation in {"header", "bool_id"}:
        raw = json.loads(bytes.fromhex(rows[6]["body_hex"]))
        if mutation == "header":
            raw["result"]["hash"] = "0x" + "33" * 32
        else:
            raw["id"] = True
        body = json.dumps(raw).encode()
        rows[6]["body_hex"], rows[6]["sha256"] = body.hex(), hashlib.sha256(body).hexdigest()
    else:
        payload = row.payload()
        payload["usdc"]["total_supply_base_units"] = "1"
        row = row.model_copy(update={"payload_json": json.dumps(payload)})
    with pytest.raises(IntegrityError):
        proof.independent_native(rows, row)


class Response:
    def __init__(self, request, mode):
        self.status = 403 if mode == "denied" else 200
        raw = {"jsonrpc": "2.0", "id": request["id"], "result": native_values()[request["id"] - 1]}
        if mode == "invalid":
            raw["id"] = 42
        self.body = json.dumps(raw).encode()
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def iter_chunked(self, size):
        assert size == 65536
        yield self.body


class Session:
    def __init__(self, mode="success"):
        self.mode = mode
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def post(self, url, **kwargs):
        assert url == "https://eth.drpc.org/" and kwargs["allow_redirects"] is False
        request = kwargs["json"]
        self.requests.append(request)
        if self.mode == "timeout":
            raise TimeoutError()
        return Response(request, self.mode)


@pytest.fixture
def fixture_output(tmp_path, monkeypatch):
    root = tmp_path / "outputs/selective-options"
    monkeypatch.setattr(proof, "ROOT", root)
    monkeypatch.setattr(proof, "now_ms", lambda: 1_100_100)
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: 1_100_000)
    return root / "evidence/stablecoin-supply-public-01"


@pytest.mark.parametrize(
    "mode,attempts,rows",
    [("success", 7, 2), ("denied", 1, 1), ("invalid", 1, 1), ("timeout", 1, 1)],
)
async def test_verifier_real_collector_store_pit_archive_reopen(
    fixture_output, monkeypatch, mode, attempts, rows
):
    session = Session(mode)
    monkeypatch.setattr(proof.aiohttp, "ClientSession", lambda **kwargs: session)
    report = await proof.verify(fixture_output, transport_provenance="explicit_mock_fixture")
    assert report["transport_provenance"] == "explicit_mock_fixture"
    assert report["real_public_requests_attempted"] == 0
    assert report["request_attempts"] == len(session.requests) == attempts
    assert report["persisted_rows"] == rows
    assert report["pit_boundary_checks"] == rows * 2
    assert report["parquet_exact_match"] and report["closed_reload_exact_match"]
    assert report["outbox_pending_after_reopen"] == rows
    assert report["source_code_unchanged"]
    assert report["trading_approval"] is False
    assert report["status"] == (
        "FIXTURE_ETHEREUM_SUPPLY_PAIR_VERIFIED"
        if mode == "success"
        else "SOURCE_UNAVAILABLE_OR_INVALID"
    )
    with pytest.raises(FileExistsError):
        await proof.verify(fixture_output, transport_provenance="explicit_mock_fixture")
    assert len(session.requests) == attempts


async def test_verifier_preflight_before_network(fixture_output, monkeypatch):
    def never(**kwargs):
        raise AssertionError("preflight allowed network creation")

    monkeypatch.setattr(proof.aiohttp, "ClientSession", never)
    with pytest.raises(ValueError, match="provenance"):
        await proof.verify(fixture_output, transport_provenance="invented")
    with pytest.raises(ValueError, match="output"):
        await proof.verify(fixture_output.parent / "other")


async def test_verifier_request_budget_before_eighth_rpc(monkeypatch):
    collector = proof.RecordingCollector(None, None)
    collector.requests_attempted = 7
    with pytest.raises(IntegrityError, match="REQUEST_BUDGET"):
        await collector.request(None, "chain")
