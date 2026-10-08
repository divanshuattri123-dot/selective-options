"""Finite restart verifier tests use labelled mocked RPC, never public availability claims."""

import json

import pytest

from scripts import verify_eth_runtime_restart as proof
from selective_options.eth_gas import EthereumGasCollector
from tests.test_eth_checkpoint import raw


def transport(monkeypatch, *, successor=False, fail_poll=None):
    clock = [2_000_000]
    requests = []

    def now():
        clock[0] += 1
        return clock[0]

    async def request(self, session, method):
        poll = len(requests) // 2
        requests.append(method)
        if poll == fail_poll:
            raise TimeoutError("SYNTHETIC_TRANSPORT_FAILURE")
        number = 101 if successor and poll == 1 else 100
        result = (
            "0x1" if method == "eth_chainId" else raw(number, timestamp=hex(1900 + number - 100))
        )
        received = now()
        await self.response_received(
            method,
            200,
            json.dumps(
                {"jsonrpc": "2.0", "id": 1 if method == "eth_chainId" else 2, "result": result}
            ).encode(),
            received,
        )
        return result, received

    monkeypatch.setattr(proof, "now_ms", now)
    monkeypatch.setattr("selective_options.eth_gas.now_ms", now)
    monkeypatch.setattr(EthereumGasCollector, "request", request)
    return requests


@pytest.mark.parametrize("successor", [True, False])
async def test_two_mocked_production_polls_closed_handoff_exact_first_receipt(
    tmp_path, monkeypatch, successor
):
    requests = transport(monkeypatch, successor=successor)
    result = await proof.verify(tmp_path / "proof", "drpc")
    assert (
        result["status"] == "PUBLIC_GAS_RESTART_VERIFIED"
    )  # Fixture transport, not saved public evidence.
    assert requests == ["eth_chainId", "eth_getBlockByNumber"] * 2
    assert result["source_hashes_unchanged"] is True
    assert result["first_database_unchanged_after_handoff"] is True
    first = proof.saved_rows(tmp_path / "proof/first/events.duckdb")
    second = proof.saved_rows(tmp_path / "proof/restarted/events.duckdb")
    seed = next(row for row in first if row.kind == "on_chain")
    assert seed in second
    assert result["first"]["rows"] == 2
    assert result["restarted"]["rows"] == (3 if successor else 2)
    assert result["restarted"]["quality"]["state"] == "POLL_OBSERVED_VERIFIED"
    assert result["restarted"]["quality"]["trading_approval"] is False
    if not successor:
        health = next(row for row in second if row.kind == "feed_health")
        assert health.payload()["accepted_observed_at_ms"] == seed.observed_at_ms
        assert health.payload()["response_received_at_ms"] > seed.observed_at_ms


@pytest.mark.parametrize("provider", [True, "unknown", "https://example.invalid"])
async def test_invalid_provider_refuses_before_output_or_transport(tmp_path, monkeypatch, provider):
    requests = transport(monkeypatch)
    with pytest.raises(ValueError):
        await proof.verify(tmp_path / "proof", provider)
    assert requests == [] and not (tmp_path / "proof").exists()


async def test_existing_output_is_immutable(tmp_path, monkeypatch):
    requests = transport(monkeypatch)
    (tmp_path / "sentinel").write_text("unchanged")
    with pytest.raises(FileExistsError):
        await proof.verify(tmp_path)
    assert requests == [] and (tmp_path / "sentinel").read_text() == "unchanged"


@pytest.mark.parametrize("failed_poll", [0, 1])
async def test_transport_failure_records_health_and_does_not_claim_restart(
    tmp_path, monkeypatch, failed_poll
):
    requests = transport(monkeypatch, fail_poll=failed_poll)
    result = await proof.verify(tmp_path / "proof")
    assert result["status"] == (
        "INITIAL_POLL_UNAVAILABLE" if failed_poll == 0 else "RESTART_FAILED"
    )
    assert len(requests) == (1 if failed_poll == 0 else 3)
    assert result["trading_enabled"] is False
    phase = "first" if failed_poll == 0 else "restarted"
    health = next(
        row
        for row in proof.saved_rows(tmp_path / f"proof/{phase}/events.duckdb")
        if row.kind == "feed_health"
    )
    assert health.payload()["ok"] is False
