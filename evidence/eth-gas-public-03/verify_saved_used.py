"""Independent saved-public replay; reads original bytes/receipts and never calls a network."""

import asyncio
import hashlib
import json
import math
from pathlib import Path

import duckdb

from selective_options.collectors import now_ms
from selective_options.errors import IntegrityError
from selective_options.eth_checkpoint import load_eth_checkpoint
from selective_options.events import MarketEvent

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "evidence/eth-gas-public-03/drpc"


def read_rows(db, query, params):
    result = db.execute(query, params)
    columns = [c[0] for c in result.description]
    return [
        MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
        for row in result.fetchall()
    ]


async def verify():
    raw = json.loads((EVIDENCE / "raw-responses.json").read_text())
    accepted = json.loads((EVIDENCE / "results.json").read_text())
    assert len(raw) == len(accepted) == 4
    hashes = []
    responses = []
    receipts = []
    for rpc_id, (record, result) in enumerate(zip(raw, accepted, strict=True), 1):
        body = bytes.fromhex(record["raw_body_hex"])
        digest = hashlib.sha256(body).hexdigest()
        assert digest == record["body_sha256"]
        assert body.decode("utf-8") == record["raw_body"]
        response = json.loads(body)
        assert (
            response["jsonrpc"] == "2.0"
            and type(response["id"]) is int
            and response["id"] == rpc_id
        )
        assert record["http_status"] == 200 and "error" not in response
        assert response["result"] == result["validated_result"]
        assert record["observed_at_ms"] == result["observed_at_ms"]
        hashes.append(digest)
        responses.append(response["result"])
        receipts.append(record["observed_at_ms"])
    assert receipts == sorted(receipts)
    chain, block, reference, fees = responses
    assert chain == "0x1" and block == reference
    number = int(block["number"], 16)
    assert raw[2]["params"] == [hex(number), False]
    assert raw[3]["params"] == ["0x1", hex(number), []]
    assert int(fees["oldestBlock"], 16) == number
    assert len(fees["baseFeePerGas"]) == 2 and len(fees["gasUsedRatio"]) == 1
    assert int(fees["baseFeePerGas"][0], 16) == int(block["baseFeePerGas"], 16)
    assert math.isclose(
        fees["gasUsedRatio"][0],
        int(block["gasUsed"], 16) / int(block["gasLimit"], 16),
        rel_tol=1e-12,
        abs_tol=1e-15,
    )
    with duckdb.connect(str(EVIDENCE / "events.duckdb"), read_only=True) as db:
        stored = read_rows(db, "SELECT * FROM events ORDER BY event_id", [])
        archived = read_rows(
            db,
            "SELECT * FROM read_parquet(?) ORDER BY event_id",
            [str(EVIDENCE / "events.parquet")],
        )
        assert stored == archived and len(stored) == 2
        gas = next(event for event in stored if event.kind == "on_chain")
        assert (
            gas.source == "ethereum_drpc"
            and gas.event_time_ms == int(block["timestamp"], 16) * 1000
        )
        assert gas.observed_at_ms == receipts[1]
        payload = gas.payload()
        assert payload["block_number"] == number and payload["block_hash"] == block["hash"].lower()
        assert payload["parent_hash"] == block["parentHash"].lower()
        for output, field in [
            ("gas_limit", "gasLimit"),
            ("gas_used", "gasUsed"),
            ("base_fee_per_gas_wei", "baseFeePerGas"),
        ]:
            assert payload[output] == str(int(block[field], 16))
        assert payload["transaction_count"] == len(block["transactions"])
        assert payload["gas_units"] == "gas" and payload["execution_quote"] is False
        assert (
            db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [gas.event_id, gas.observed_at_ms - 1, gas.observed_at_ms - 1],
            ).fetchone()[0]
            == 0
        )
        assert (
            db.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [gas.event_id, gas.observed_at_ms, gas.observed_at_ms],
            ).fetchone()[0]
            == 1
        )
        assert (
            db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[0]
            == 2
        )
    checkpoint = await load_eth_checkpoint(EVIDENCE / "events.duckdb", now_ms(), provider="drpc")
    assert checkpoint == gas
    try:
        await load_eth_checkpoint(EVIDENCE / "events.duckdb", now_ms(), provider="publicnode")
    except IntegrityError as exc:
        assert str(exc) == "ETH_CHECKPOINT_EMPTY_OR_OVER_BUDGET"
    else:
        raise AssertionError("wrong-provider checkpoint accepted")
    report = {
        "status": "SAVED_PUBLIC_BYTES_AND_CHECKPOINT_VERIFIED",
        "verification_at_ms": now_ms(),
        "network_requests": 0,
        "source": "ethereum_drpc",
        "raw_response_sha256": hashes,
        "rpc_identity_and_results_exact": True,
        "original_receipts_preserved": True,
        "referenced_block_exact_match": True,
        "referenced_block_full_raw_result_exact_match": True,
        "fee_history_native_fee_and_ratio_match": True,
        "stored_native_fields_exact_match": True,
        "duckdb_parquet_exact_match": True,
        "receipt_pit_checks": 1,
        "outbox_pending_rows": 2,
        "closed_checkpoint_equal": True,
        "wrong_provider_checkpoint_rejected": True,
        "limitations": "One finalized block from one public provider; RPC consistency is not independent consensus validation, continuous coverage, staking/netflows/L2 feeds, or live execution evidence. M2 partial; no orders.",
    }
    (EVIDENCE / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(verify())
