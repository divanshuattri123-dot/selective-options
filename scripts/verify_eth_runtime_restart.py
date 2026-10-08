"""Two finite public gas polls across a closed checkpoint handoff; no retries or orders."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import cast

import aiohttp
import duckdb
from pydantic import JsonValue

from selective_options import __version__
from selective_options.archive import file_digest, seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.eth_checkpoint import load_eth_checkpoint
from selective_options.eth_gas import (
    PUBLIC_PROVIDERS,
    EthereumGasCollector,
    Method,
    Provider,
    gas_event,
)
from selective_options.eth_gas_quality import eth_gas_quality
from selective_options.events import MarketEvent
from selective_options.store import DuckStore

MAX_POLL_AGE_MS = 600_000
MAX_BLOCK_AGE_MS = 1_800_000


class RecordingCollector(EthereumGasCollector):
    def __init__(self, store: DuckStore, provider: Provider) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.results: list[dict[str, object]] = []
        self.raw_responses: list[dict[str, object]] = []
        self.requests_attempted = 0
        super().__init__(AppConfig(), self.record, provider=provider)

    async def record(self, row: MarketEvent) -> None:
        await self.store.append([row], exact=True)
        self.rows.append(row)

    async def response_received(
        self, method: Method, status: int, body: bytes, received: int
    ) -> None:
        self.raw_responses.append(
            {
                "method": method,
                "http_status": status,
                "observed_at_ms": received,
                "params": [] if method == "eth_chainId" else ["finalized", False],
                "rpc_id": 1 if method == "eth_chainId" else 2,
                "raw_body_hex": body.hex(),
                "body_sha256": hashlib.sha256(body).hexdigest(),
            }
        )

    async def request(
        self, session: aiohttp.ClientSession, method: Method
    ) -> tuple[JsonValue, int]:
        self.requests_attempted += 1
        result, received = await super().request(session, method)
        self.results.append({"method": method, "observed_at_ms": received, "result": result})
        return result, received


def saved_rows(path: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(path), read_only=True) as db:
        cursor = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [column[0] for column in cursor.description]
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        )


async def verify_store(
    store: DuckStore, output: Path, collector: RecordingCollector, provider: Provider
) -> dict[str, object]:
    cutoff = now_ms()
    rows = (await store.snapshot("ETH", cutoff, "public_reference")).rows()
    pure = eth_gas_quality(
        rows,
        provider=provider,
        as_of_ms=cutoff,
        max_poll_age_ms=MAX_POLL_AGE_MS,
        max_block_age_ms=MAX_BLOCK_AGE_MS,
    )
    queried = await store.eth_gas_quality(provider, cutoff, MAX_POLL_AGE_MS, MAX_BLOCK_AGE_MS)
    if queried != pure or pure["state"] != "POLL_OBSERVED_VERIFIED":
        raise AssertionError("public poll quality did not match canonical verified evidence")
    checks = 0
    for row in rows:
        visible_at = max(row.event_time_ms, row.observed_at_ms)
        for clock in (visible_at - 1, visible_at):
            actual = (await store.snapshot("ETH", clock, "public_reference")).rows()
            expected = tuple(
                item
                for item in rows
                if item.event_time_ms <= clock and item.observed_at_ms <= clock
            )
            if {item.event_id: item for item in actual} != {
                item.event_id: item for item in expected
            }:
                raise AssertionError("source/receipt PIT mismatch")
            checks += 1
    block_responses = [
        record for record in collector.raw_responses if record["method"] == "eth_getBlockByNumber"
    ]
    if len(block_responses) != 1:
        raise AssertionError("exactly one native block response required")
    record = block_responses[0]
    body = bytes.fromhex(str(record["raw_body_hex"]))
    if hashlib.sha256(body).hexdigest() != record["body_sha256"]:
        raise AssertionError("native body checksum mismatch")
    received = int(str(record["observed_at_ms"]))
    native = gas_event(json.loads(body)["result"], received, MAX_BLOCK_AGE_MS, provider=provider)
    accepted = next(row for row in rows if row.event_id == native.event_id)
    if accepted.model_copy(update={"observed_at_ms": received}) != native:
        raise AssertionError("stored gas is not the unchanged native observation")
    if accepted.observed_at_ms > received:
        raise AssertionError("accepted first observation occurs after this response")
    health = next(row for row in rows if row.event_id == pure["health_event_id"])
    if health.payload()["response_received_at_ms"] != received:
        raise AssertionError("successful poll lost native response receipt")
    await store.append(list(rows), exact=True)
    if await store.pending_delivery_count() != len(rows):
        raise AssertionError("exact replay changed outbox")
    count = await store.export_parquet(output / "events.parquet")
    manifest = await seal_archive(output / "events.parquet", count)
    with duckdb.connect() as db:
        archived = db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(output / "events.parquet")]
        ).fetchall()
    expected_rows = sorted((tuple(row.model_dump().values()) for row in rows), key=lambda r: r[0])
    if archived != expected_rows:
        raise AssertionError("archive changed native receipts or payloads")
    return {
        "quality": pure,
        "rows": len(rows),
        "pit_checks": checks,
        "native_block_response_exact": True,
        "repeated_block_keeps_first_receipt": accepted.observed_at_ms <= received,
        "exact_duplicate_replay": True,
        "archive_exact_match": True,
        "outbox_pending": len(rows),
        "parquet_sha256": manifest.sha256,
    }


async def phase(
    output: Path, provider: Provider, seed: MarketEvent | None
) -> tuple[dict[str, object], MarketEvent | None]:
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    path = output / "events.duckdb"
    if seed is not None:
        copied = await DuckStore.open(path)
        try:
            await copied.append([seed], exact=True)
        finally:
            await copied.close()
        if await load_eth_checkpoint(path, now_ms(), provider=provider) != seed:
            raise AssertionError("closed seed reload changed original checkpoint")
    store = await DuckStore.open(path)
    collector = RecordingCollector(store, provider)
    collector.previous = seed
    result: dict[str, object] = {
        "status": "STARTED",
        "seed_event_id": seed.event_id if seed else None,
    }
    try:
        async with asyncio.timeout(50):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                success = await collector.collect(session)
        result["public_poll_succeeded"] = success
        if success:
            result.update(await verify_store(store, output, collector, provider))
            result["status"] = "PUBLIC_POLL_VERIFIED"
        else:
            result["status"] = "PUBLIC_FEED_UNAVAILABLE"
            result["health"] = [
                row.payload() for row in collector.rows if row.kind == "feed_health"
            ]
            count = await store.export_parquet(output / "events.parquet")
            await seal_archive(output / "events.parquet", count)
    except Exception as exc:
        result.update(status="FAILED", failure_type=type(exc).__name__, failure=str(exc))
    finally:
        result.update(
            public_requests_attempted=collector.requests_attempted,
            accepted_rpc_results=len(collector.results),
        )
        (output / "raw-responses.json").write_text(
            json.dumps(collector.raw_responses, indent=2) + "\n", encoding="utf-8"
        )
        (output / "results.json").write_text(
            json.dumps(collector.results, indent=2) + "\n", encoding="utf-8"
        )
        await store.close()
    if result["status"] == "PUBLIC_POLL_VERIFIED":
        closed = saved_rows(path)
        with duckdb.connect(str(path), read_only=True) as db:
            pending = db.execute(
                "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
            ).fetchone()
        if pending != (len(closed),) or len(closed) != result["rows"]:
            raise AssertionError("closed store/outbox mismatch")
        loaded = await load_eth_checkpoint(path, now_ms(), provider=provider)
        if loaded != collector.previous:
            raise AssertionError("closed checkpoint changed accepted observation")
        result["closed_checkpoint_exact_match"] = True
        result["closed_outbox_exact_match"] = True
    else:
        loaded = None
    (output / "report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result, loaded


async def verify(output: Path, provider: Provider = "drpc") -> dict[str, object]:
    if type(provider) is not str or provider not in PUBLIC_PROVIDERS:
        raise ValueError("unsupported public Ethereum provider")
    if await asyncio.to_thread(output.exists):
        raise FileExistsError("restart proof output already exists")
    script_path = await asyncio.to_thread(Path(__file__).resolve)
    root = script_path.parents[1]
    paths = [script_path, *(root / "src/selective_options").glob("*.py")]
    hashes = {str(path.relative_to(root)).replace("\\", "/"): file_digest(path) for path in paths}
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    report: dict[str, object] = {
        "version": __version__,
        "provider": provider,
        "endpoint": PUBLIC_PROVIDERS[provider][0],
        "started_at_ms": now_ms(),
        "max_public_requests": 4,
        "tls_verification": "default certificate and hostname validation",
        "source_hashes_before": hashes,
        "orders_submitted": 0,
        "trading_enabled": False,
        "ready_for_signals": False,
        "scope": "Two finite production-collector polls with a closed provider-scoped anchor copy/reload between them. Same-provider finality assertion only; no whole-source restart, all-feed operation, continuous history, outage recovery or Redis deployment proof.",
    }
    first, seed = await phase(output / "first", provider, None)
    report["first"] = first
    if seed is not None:
        source_digest = file_digest(output / "first/events.duckdb")
        second, successor = await phase(output / "restarted", provider, seed)
        report["restarted"] = second
        report["first_database_unchanged_after_handoff"] = source_digest == file_digest(
            output / "first/events.duckdb"
        )
        report["status"] = (
            "PUBLIC_GAS_RESTART_VERIFIED" if successor is not None else "RESTART_FAILED"
        )
    else:
        report["status"] = "INITIAL_POLL_UNAVAILABLE"
    report["public_requests_attempted"] = sum(
        int(str(cast(dict[str, object], report[key])["public_requests_attempted"]))
        for key in ("first", "restarted")
        if key in report
    )
    report["source_hashes_unchanged"] = all(
        file_digest(path) == hashes[str(path.relative_to(root)).replace("\\", "/")]
        for path in paths
    )
    report["ended_at_ms"] = now_ms()
    if report["source_hashes_unchanged"] is not True:
        report["status"] = "SOURCE_CHANGED_DURING_PROOF"
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider", choices=tuple(PUBLIC_PROVIDERS), default="drpc")
    args = parser.parse_args()
    report = asyncio.run(verify(args.output, args.provider))
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("status", "provider", "public_requests_attempted", "trading_enabled")
            }
        )
    )


if __name__ == "__main__":
    main()
