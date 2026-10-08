"""One bounded public finalized ETH gas probe; no account, wallet or transaction methods."""

import argparse
import asyncio
import hashlib
import json
import math
from pathlib import Path
from typing import Annotated

import aiohttp
import duckdb
from pydantic import Field, JsonValue, TypeAdapter

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.eth_gas import (
    PUBLIC_PROVIDERS,
    Block,
    EthereumGasCollector,
    Method,
    Provider,
    Quantity,
)
from selective_options.events import MarketEvent
from selective_options.models import ExternalModel
from selective_options.store import DuckStore


class FeeHistory(ExternalModel):
    oldestBlock: Quantity
    baseFeePerGas: tuple[Quantity, ...] = Field(min_length=2, max_length=2)
    gasUsedRatio: tuple[Annotated[float, Field(strict=True, ge=0, le=1)], ...] = Field(
        min_length=1, max_length=1
    )


class RecordingCollector(EthereumGasCollector):
    def __init__(self, store: DuckStore, provider: Provider = "publicnode") -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.results: list[dict[str, object]] = []
        self.raw_responses: list[dict[str, object]] = []
        self.requests_attempted = 0
        super().__init__(AppConfig(), self.record, provider=provider)

    async def record(self, event: MarketEvent) -> None:
        await self.store.append([event])
        self.rows.append(event)

    async def response_received(
        self, method: Method, status: int, body: bytes, received: int
    ) -> None:
        self.raw_responses.append(
            {
                "method": method,
                "http_status": status,
                "observed_at_ms": received,
                "rpc_id": 1 if method == "eth_chainId" else 2,
                "params": [] if method == "eth_chainId" else ["finalized", False],
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "raw_body": body.decode("utf-8", errors="replace"),
                "raw_body_hex": body.hex(),
            }
        )

    async def request(
        self, session: aiohttp.ClientSession, method: Method
    ) -> tuple[JsonValue, int]:
        self.requests_attempted += 1
        started = now_ms()
        try:
            result, received = await super().request(session, method)
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            self.raw_responses.append(
                {
                    "method": method,
                    "started_at_ms": started,
                    "failed_at_ms": now_ms(),
                    "failure_type": type(exc).__name__,
                    "failure": str(exc),
                    "raw_body": None,
                }
            )
            raise
        self.results.append(
            {"method": method, "observed_at_ms": received, "validated_result": result}
        )
        return result, received

    async def reference_request(
        self, session: aiohttp.ClientSession, method: str, params: list[JsonValue], rpc_id: int
    ) -> tuple[JsonValue, int]:
        # This verification-only path never broadens the live collector's method allowlist.
        if (method, rpc_id) not in {("eth_getBlockByNumber", 3), ("eth_feeHistory", 4)}:
            raise ValueError("unsupported Ethereum verification method")
        if rpc_id == 3:
            if len(params) != 2 or params[1] is not False:
                raise ValueError(
                    "verification requires one numeric block without full transactions"
                )
            TypeAdapter(Quantity).validate_python(params[0])
        else:
            if len(params) != 3 or params[0] != "0x1" or params[2] != []:
                raise ValueError("verification requires one fee-history block without rewards")
            TypeAdapter(Quantity).validate_python(params[1])
        self.requests_attempted += 1
        started = now_ms()
        try:
            return await self._reference_request(session, method, params, rpc_id)
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            self.raw_responses.append(
                {
                    "method": method,
                    "params": params,
                    "rpc_id": rpc_id,
                    "started_at_ms": started,
                    "failed_at_ms": now_ms(),
                    "failure_type": type(exc).__name__,
                    "failure": str(exc),
                    "raw_body": None,
                }
            )
            raise

    async def _reference_request(
        self, session: aiohttp.ClientSession, method: str, params: list[JsonValue], rpc_id: int
    ) -> tuple[JsonValue, int]:
        request: dict[str, JsonValue] = {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": method,
            "params": params,
        }
        async with session.post(self.url, json=request, allow_redirects=False) as response:
            body = bytearray()
            async for chunk in response.content.iter_chunked(65536):
                body.extend(chunk)
                if len(body) > 4_000_000:
                    raise IntegrityError("ETH_RESPONSE_TOO_LARGE")
            received = now_ms()
            self.raw_responses.append(
                {
                    "method": method,
                    "params": params,
                    "rpc_id": rpc_id,
                    "http_status": response.status,
                    "observed_at_ms": received,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                    "raw_body": body.decode("utf-8", errors="replace"),
                    "raw_body_hex": body.hex(),
                }
            )
            if response.status != 200:
                raise IntegrityError("ETH_REFERENCE_HTTP_UNAVAILABLE")
        raw = TypeAdapter(dict[str, JsonValue]).validate_json(body)
        if (
            raw.get("jsonrpc") != "2.0"
            or type(raw.get("id")) is not int
            or raw["id"] != rpc_id
            or "error" in raw
            or raw.get("result") is None
        ):
            raise IntegrityError("ETH_REFERENCE_RPC_INVALID")
        self.results.append(
            {
                "method": method,
                "params": params,
                "observed_at_ms": received,
                "validated_result": raw["result"],
            }
        )
        return raw["result"], received


async def crosscheck(
    collector: RecordingCollector, session: aiohttp.ClientSession
) -> dict[str, object]:
    event = collector.previous
    assert event is not None
    payload = event.payload()
    number = hex(int(str(payload["block_number"])))
    reference, reference_received = await collector.reference_request(
        session, "eth_getBlockByNumber", [number, False], 3
    )
    block = Block.model_validate(reference)
    original = Block.model_validate(collector.results[1]["validated_result"])
    if block != original or int(block.number, 16) != int(str(payload["block_number"])):
        raise IntegrityError("ETH_REFERENCED_BLOCK_MISMATCH")
    if reference != collector.results[1]["validated_result"]:
        raise IntegrityError("ETH_REFERENCED_BLOCK_RAW_MISMATCH")
    fees, fee_received = await collector.reference_request(
        session, "eth_feeHistory", ["0x1", number, []], 4
    )
    history = FeeHistory.model_validate(fees)
    if (
        int(history.oldestBlock, 16) != int(block.number, 16)
        or int(history.baseFeePerGas[0], 16) != int(block.baseFeePerGas, 16)
        or not 0 <= history.gasUsedRatio[0] <= 1
        or not math.isclose(
            history.gasUsedRatio[0],
            int(block.gasUsed, 16) / int(block.gasLimit, 16),
            rel_tol=1e-12,
            abs_tol=1e-15,
        )
    ):
        raise IntegrityError("ETH_FEE_HISTORY_MISMATCH")
    if not event.observed_at_ms <= reference_received <= fee_received:
        raise IntegrityError("ETH_REFERENCE_RECEIPT_REGRESSION")
    return {
        "referenced_block_exact_match": True,
        "referenced_block_typed_fields_exact_match": True,
        "referenced_block_raw_result_exact_match": True,
        "fee_history_base_fee_exact_match": True,
        "fee_history_gas_ratio_match": True,
        "referenced_block_number": int(block.number, 16),
        "reference_receipt_order_verified": True,
        "verification_basis": "same public provider RPC consistency; not independent chain consensus",
    }


async def verify(output: Path, provider: Provider = "publicnode") -> None:
    if provider not in PUBLIC_PROVIDERS:
        raise ValueError("unsupported public Ethereum provider")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(store, provider)
    report: dict[str, object] = {
        "version": __version__,
        "started_at_ms": now_ms(),
        "provider": provider,
        "endpoint": collector.url,
        "source": collector.source,
        "tls_verification": "default certificate and hostname verification; no override",
        "orders_submitted": 0,
        "trading_enabled": False,
        "paper_trading_started": False,
        "max_requests": 4,
        "network_deadline_s": 45,
        "response_byte_limit": 4_000_000,
    }
    try:
        async with (
            asyncio.timeout(45),
            aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session,
        ):
            accepted = await collector.collect(session)
            if accepted:
                report.update(await crosscheck(collector, session))
        gas = [e for e in collector.rows if e.kind == "on_chain"]
        for event in gas:
            at = max(event.event_time_ms, event.observed_at_ms)
            assert event.event_id not in {
                e.event_id for e in (await store.snapshot("ETH", at - 1, "public_reference")).rows()
            }
            assert event.event_id in {
                e.event_id for e in (await store.snapshot("ETH", at, "public_reference")).rows()
            }
            await store.append([event])
        count = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", count)
        with duckdb.connect() as db:
            result = db.execute("SELECT * FROM read_parquet(?)", [str(output / "events.parquet")])
            columns = [c[0] for c in result.description]
            restored = [
                MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
                for row in result.fetchall()
            ]
        expected = {e.event_id: e for e in collector.rows}
        assert {e.event_id: e for e in restored} == expected
        assert {e.event_id: e for e in await store.pending_delivery()} == expected
        await store.close()
        reloaded = await DuckStore.open(output / "events.duckdb")
        try:
            assert {e.event_id: e for e in await reloaded.pending_delivery()} == expected
            assert await reloaded.pending_delivery_count() == count
            assert {
                e.event_id: e
                for e in (
                    await reloaded.snapshot(
                        "ETH",
                        max(max(e.event_time_ms, e.observed_at_ms) for e in collector.rows),
                        "public_reference",
                    )
                ).rows()
            } == expected
        finally:
            await reloaded.close()
        report.update(
            status="PUBLIC_FINALIZED_GAS_VERIFIED" if accepted else "SOURCE_UNAVAILABLE",
            rows=count,
            pit_checks=len(gas),
            archive_exact_match=True,
            closed_reload_exact_match=True,
            outbox_exact_match=True,
            repeat_deduplication_verified=bool(gas),
            parquet_sha256=archive.sha256,
            gas_observations=[e.payload() for e in gas],
            health=[e.payload() for e in collector.rows if e.kind == "feed_health"],
        )
    except BaseException as exc:
        report.update(status="FAILED", failure_type=type(exc).__name__, failure=str(exc))
        raise
    finally:
        report.update(
            ended_at_ms=now_ms(),
            public_requests_attempted=collector.requests_attempted,
            accepted_rpc_results=len(collector.results),
            raw_response_records=len(collector.raw_responses),
        )
        await store.close()
        for name, value in [
            ("results.json", collector.results),
            ("raw-responses.json", collector.raw_responses),
            ("report.json", report),
        ]:
            await asyncio.to_thread(
                (output / name).write_text, json.dumps(value, indent=2) + "\n", encoding="utf-8"
            )
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider", choices=tuple(PUBLIC_PROVIDERS), default="publicnode")
    args = parser.parse_args()
    asyncio.run(verify(args.output, args.provider))
