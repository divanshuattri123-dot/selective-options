"""One finite seven-request public Ethereum USDC/USDT supply proof; no retries."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Literal

import aiohttp
import duckdb
from pydantic import JsonValue

from selective_options.archive import file_digest, seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.stablecoin_supply import (
    STEPS,
    StablecoinSupplyCollector,
    Step,
    stablecoin_supply_quality,
)
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILE = ROOT / "src/selective_options/stablecoin_supply.py"
REFERENCES = (
    "https://developers.circle.com/stablecoins/usdc-contract-addresses",
    "https://tether.to/en/supported-protocols/",
    "https://ethereum.org/en/developers/docs/apis/json-rpc/",
    "https://eips.ethereum.org/EIPS/eip-20",
)


def preflight(output: Path) -> tuple[Path, str]:
    output = output.resolve()
    scratch = (ROOT.parents[1] / "work/stablecoin-supply-0339").resolve()
    if output != (
        ROOT / "evidence/stablecoin-supply-public-01"
    ).resolve() and not output.is_relative_to(scratch):
        raise ValueError("output must be stablecoin-supply-public-01 or in workspace scratch")
    if output.exists():
        raise FileExistsError("refusing to overwrite stablecoin proof evidence")
    return output, file_digest(SOURCE_FILE)


def independent_native(entities: list[dict[str, object]], row: MarketEvent) -> None:
    """Compare original JSON entities, selectors, block references and native integer words."""
    if len(entities) != 7:
        raise IntegrityError("SUPPLY_PROBE_INCOMPLETE_NATIVE_ENTITIES")
    requests = [item["request"] for item in entities]
    expected = [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_getBlockByNumber",
            "params": ["finalized", False],
        },
    ]
    payload = row.payload()
    number = payload["call_block_quantity"]
    for index, address, selector in (
        (3, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "0x18160ddd"),
        (4, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "0x313ce567"),
        (5, "0xdac17f958d2ee523a2206206994597c13d831ec7", "0x18160ddd"),
        (6, "0xdac17f958d2ee523a2206206994597c13d831ec7", "0x313ce567"),
    ):
        expected.append(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "eth_call",
                "params": [{"to": address, "data": selector}, number],
            }
        )
    expected.append(
        {"jsonrpc": "2.0", "id": 7, "method": "eth_getBlockByNumber", "params": [number, False]}
    )
    if requests != expected:
        raise IntegrityError("SUPPLY_PROBE_REQUEST_ALLOWLIST_MISMATCH")
    values = []
    for index, entity in enumerate(entities, 1):
        raw_hex = entity["body_hex"]
        if not isinstance(raw_hex, str):
            raise IntegrityError("SUPPLY_PROBE_ENTITY_BYTES_MISSING")
        raw = bytes.fromhex(raw_hex)
        decoded = json.loads(raw)
        if (
            entity["http_status"] != 200
            or decoded.get("jsonrpc") != "2.0"
            or type(decoded.get("id")) is not int
            or decoded["id"] != index
            or "error" in decoded
            or hashlib.sha256(raw).hexdigest() != entity["sha256"]
        ):
            raise IntegrityError("SUPPLY_PROBE_NATIVE_RPC_IDENTITY")
        values.append(decoded["result"])
    header = values[1]
    if (
        values[0] != payload["chain_id_quantity"]
        or header != values[6]
        or not isinstance(header, dict)
    ):
        raise IntegrityError("SUPPLY_PROBE_NATIVE_CHAIN_REFERENCE")
    if any(
        header[native] != payload[field]
        for native, field in (
            ("number", "block_number_quantity"),
            ("hash", "block_hash"),
            ("parentHash", "parent_hash"),
            ("timestamp", "block_timestamp_quantity"),
        )
    ):
        raise IntegrityError("SUPPLY_PROBE_NATIVE_HEADER_MISMATCH")
    if (
        int(header["number"], 16) != payload["block_number"]
        or int(header["timestamp"], 16) * 1000 != row.event_time_ms
    ):
        raise IntegrityError("SUPPLY_PROBE_NATIVE_HEADER_INTEGER")
    for symbol, index in (("usdc", 2), ("usdt", 4)):
        token = payload[symbol]
        if not isinstance(token, dict):
            raise IntegrityError("SUPPLY_PROBE_NATIVE_TOKEN_SHAPE")
        if (
            token["total_supply_abi_word"] != values[index]
            or token["decimals_abi_word"] != values[index + 1]
            or token["total_supply_base_units"] != str(int(values[index], 16))
            or token["decimals"] != int(values[index + 1], 16)
            or token["total_supply_received_at_ms"] != entities[index]["received_at_ms"]
            or token["decimals_received_at_ms"] != entities[index + 1]["received_at_ms"]
        ):
            raise IntegrityError("SUPPLY_PROBE_NATIVE_TOKEN_MISMATCH")
    receipts = [
        payload["chain_received_at_ms"],
        payload["header_received_at_ms"],
        payload["reference_received_at_ms"],
    ]
    if (
        receipts != [entities[index]["received_at_ms"] for index in (0, 1, 6)]
        or row.observed_at_ms != entities[6]["received_at_ms"]
    ):
        raise IntegrityError("SUPPLY_PROBE_NATIVE_RECEIPT_MISMATCH")


def closed_rows(database: Path, archive: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in cursor.description]
        rows = cursor.fetchall()
        if (
            rows
            != connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(archive)]
            ).fetchall()
        ):
            raise IntegrityError("SUPPLY_PROBE_DATABASE_PARQUET_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in rows
        )


class RecordingCollector(StablecoinSupplyCollector):
    def __init__(self, config: AppConfig, store: DuckStore) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.entities: list[dict[str, object]] = []
        self.requests_attempted = 0
        super().__init__(config, self.record)

    async def record(self, row: MarketEvent) -> None:
        await self.store.append([row], exact=True)
        self.rows.append(row)

    async def request(
        self, session: aiohttp.ClientSession, step: Step, block_number: int | None = None
    ) -> tuple[JsonValue, int]:
        if self.requests_attempted >= 7 or step != STEPS[self.requests_attempted]:
            raise IntegrityError("SUPPLY_PROBE_REQUEST_BUDGET_OR_ORDER")
        self.requests_attempted += 1
        return await super().request(session, step, block_number)

    async def response_received(
        self, request: dict[str, JsonValue], status: int, body: bytes, received: int
    ) -> None:
        self.entities.append(
            {
                "request": request,
                "http_status": status,
                "body_hex": body.hex(),
                "received_at_ms": received,
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "basis": "exact decoded HTTP entity bytes passed to production parser",
            }
        )


async def verify(
    output: Path,
    *,
    transport_provenance: Literal["public_http", "explicit_mock_fixture"] = "public_http",
) -> dict[str, object]:
    if transport_provenance not in ("public_http", "explicit_mock_fixture"):
        raise ValueError("explicit public or mocked fixture provenance required")
    output, source_before = await complete_thread(preflight, output)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig(ingestion={"stablecoin_supply_enabled": True})
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(config, store)
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            accepted = await collector.collect(session)
        cutoff = now_ms()
        snapshot = await store.snapshot("GLOBAL", cutoff, "public_reference")
        if set(snapshot.rows()) != set(collector.rows):
            raise IntegrityError("SUPPLY_PROBE_CURRENT_PIT_ROWS_MISMATCH")
        view = await store.stablecoin_supply_quality(as_of_ms=cutoff)
        if view != stablecoin_supply_quality(collector.rows, as_of_ms=cutoff):
            raise IntegrityError("SUPPLY_PROBE_EXACT_QUALITY_MISMATCH")
        if accepted:
            if (
                collector.requests_attempted != 7
                or len(collector.rows) != 2
                or view["state"] != "POLL_OBSERVED_VERIFIED"
            ):
                raise IntegrityError("SUPPLY_PROBE_SUCCESS_WITHOUT_NATIVE_PAIR")
            independent_native(collector.entities, collector.rows[0])
        for row in collector.rows:
            at = max(row.observed_at_ms, row.event_time_ms)
            for clock, included in ((at - 1, False), (at, True)):
                rows = (await store.snapshot("GLOBAL", clock, "public_reference")).rows()
                if (row.event_id in {item.event_id for item in rows}) != included:
                    raise IntegrityError("SUPPLY_PROBE_EXACT_PIT_BOUNDARY")
        for index, entity in enumerate(collector.entities, 1):
            raw_hex = entity["body_hex"]
            assert isinstance(raw_hex, str)
            target = output / f"response-{index:02d}.json"
            await asyncio.to_thread(target.write_bytes, bytes.fromhex(raw_hex))
            if await complete_thread(file_digest, target) != entity["sha256"]:
                raise IntegrityError("SUPPLY_PROBE_SAVED_ENTITY_CHECKSUM")
        await store.append(collector.rows, exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        if count != len(collector.rows) or await store.pending_delivery_count() != count:
            raise IntegrityError("SUPPLY_PROBE_OUTBOX_OR_ARCHIVE_COUNT")
    finally:
        await store.close()
    saved = await complete_thread(closed_rows, output / "events.duckdb", output / "events.parquet")
    if set(saved) != set(collector.rows):
        raise IntegrityError("SUPPLY_PROBE_CLOSED_RELOAD_EXACT")
    reopened = await DuckStore.open(output / "events.duckdb")
    try:
        if (
            await reopened.stablecoin_supply_quality(as_of_ms=cutoff) != view
            or await reopened.pending_delivery_count() != count
        ):
            raise IntegrityError("SUPPLY_PROBE_REOPEN_VIEW_OR_OUTBOX")
    finally:
        await reopened.close()
    source_after = await complete_thread(file_digest, SOURCE_FILE)
    if source_before != source_after:
        raise IntegrityError("SUPPLY_PROBE_SOURCE_CHANGED")
    report: dict[str, object] = {
        "status": (
            "PUBLIC_ETHEREUM_SUPPLY_PAIR_VERIFIED"
            if transport_provenance == "public_http"
            else "FIXTURE_ETHEREUM_SUPPLY_PAIR_VERIFIED"
        )
        if accepted
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "transport_provenance": transport_provenance,
        "real_public_requests_attempted": collector.requests_attempted
        if transport_provenance == "public_http"
        else 0,
        "request_attempts": collector.requests_attempted,
        "retries": 0,
        "entities": collector.entities,
        "quality": view,
        "persisted_rows": count,
        "independent_native_entity_comparison": accepted,
        "pit_boundary_checks": 2 * count,
        "parquet_exact_match": True,
        "parquet_sha256": manifest.sha256,
        "closed_reload_exact_match": True,
        "outbox_pending_after_reopen": count,
        "redis_delivery_claim": False,
        "source_code_sha256_before": source_before,
        "source_code_sha256_after": source_after,
        "source_code_unchanged": True,
        "source_references": list(REFERENCES),
        "redirects_allowed": False,
        "tls_verification": "default; no override",
        "network_timeout_s_per_request": 20,
        "poll_timeout_s": 90,
        "finality_independently_verified": False,
        "all_chain_supply_complete": False,
        "continuous_history_verified": False,
        "complete_feed_available": False,
        "milestone_2_complete": False,
        "trading_approval": False,
        "orders_submitted": 0,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "evidence/stablecoin-supply-public-01"
    )
    report = asyncio.run(verify(parser.parse_args().output))
    print(
        json.dumps(
            {
                field: report[field]
                for field in (
                    "status",
                    "real_public_requests_attempted",
                    "request_attempts",
                    "persisted_rows",
                    "parquet_exact_match",
                    "closed_reload_exact_match",
                    "source_code_unchanged",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
