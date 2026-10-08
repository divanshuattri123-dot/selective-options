"""Offline native Ethereum supply replay, bounded checkpoint and closed-store restart proof."""

import argparse
import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self, cast
from unittest.mock import patch

import aiohttp
import duckdb
from pydantic import JsonValue

from selective_options import stablecoin_supply
from selective_options.archive import _verify_archive, file_digest, seal_archive
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.stablecoin_checkpoint import load_stablecoin_checkpoint
from selective_options.stablecoin_supply import (
    STEPS,
    URL,
    StablecoinSupplyCollector,
    rpc_request,
    stablecoin_supply_quality,
    supply_event,
)
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
VERIFIER_PATH = Path(__file__).resolve()
SOURCE_NAMES = (
    "events.duckdb",
    "events.parquet",
    "events.parquet.manifest.json",
    "report.json",
    *(f"response-{index:02}.json" for index in range(1, 8)),
)
MAX_SOURCE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class Entity:
    request: dict[str, JsonValue]
    body: bytes
    received_at_ms: int


@dataclass(frozen=True)
class SavedEvidence:
    entities: tuple[Entity, ...]
    snapshot: MarketEvent
    health: MarketEvent
    hashes: dict[str, str]
    historical_code_hash: str


def hashes(source: Path) -> dict[str, str]:
    result = {}
    for name in SOURCE_NAMES:
        path = source / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_SOURCE_BYTES:
            raise ValueError("missing, linked or over-budget stablecoin source evidence")
        result[name] = file_digest(path)
    return result


def read_rows(database: Path, archive: Path | None = None) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id LIMIT 33")
        columns = [entry[0] for entry in cursor.description]
        values = cursor.fetchall()
        if len(values) > 32:
            raise IntegrityError("STABLECOIN_RESTART_SOURCE_ROW_BUDGET")
        if archive is not None:
            manifest = _verify_archive(archive)
            archived = connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id LIMIT 33", [str(archive)]
            ).fetchall()
            if values != archived or manifest.rows != len(values):
                raise IntegrityError("STABLECOIN_RESTART_DATABASE_ARCHIVE_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, value, strict=True))) for value in values
        )


def preflight(source: Path, output: Path) -> SavedEvidence:
    source, output = source.resolve(), output.resolve()
    scratch = (ROOT.parents[1] / "work/stablecoin-restart-0340").resolve()
    if output != (ROOT / "evidence/stablecoin-restart-01").resolve() and not output.is_relative_to(
        scratch
    ):
        raise ValueError("output must be stablecoin-restart-01 or within workspace scratch")
    if output.exists() or output == source or source.is_relative_to(output):
        raise FileExistsError("refusing to overwrite restart or source evidence")
    before = hashes(source)
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if (
        report.get("status") != "PUBLIC_ETHEREUM_SUPPLY_PAIR_VERIFIED"
        or report.get("real_public_requests_attempted") != 7
    ):
        raise IntegrityError("STABLECOIN_RESTART_SAVED_PUBLIC_PROVENANCE")
    rows = read_rows(source / "events.duckdb", source / "events.parquet")
    snapshots = [row for row in rows if row.kind == "on_chain"]
    health = [row for row in rows if row.kind == "feed_health"]
    if len(rows) != 2 or len(snapshots) != 1 or len(health) != 1:
        raise IntegrityError("STABLECOIN_RESTART_SOURCE_PAIR_MISSING")
    snapshot, bound = snapshots[0], health[0]
    quality = stablecoin_supply_quality(rows, as_of_ms=bound.observed_at_ms)
    if (
        quality["state"] != "POLL_OBSERVED_VERIFIED"
        or quality["accepted_event_id"] != snapshot.event_id
    ):
        raise IntegrityError("STABLECOIN_RESTART_SOURCE_QUALITY_BLOCKED")
    entities = []
    values = []
    metadata = report.get("entities")
    if not isinstance(metadata, list) or len(metadata) != 7:
        raise IntegrityError("STABLECOIN_RESTART_NATIVE_ENTITIES_MISSING")
    for index, step in enumerate(STEPS):
        entry = metadata[index]
        path = source / f"response-{index + 1:02}.json"
        body = path.read_bytes()
        raw = json.loads(body)
        expected = rpc_request(step, None if index < 2 else cast(int, quality["block_number"]))
        if (
            not isinstance(entry, dict)
            or entry.get("request") != expected
            or entry.get("http_status") != 200
            or entry.get("sha256") != before[path.name]
            or entry.get("bytes") != len(body)
            or type(entry.get("received_at_ms")) is not int
            or entry["received_at_ms"] <= 0
            or raw.get("jsonrpc") != "2.0"
            or type(raw.get("id")) is not int
            or raw.get("id") != index + 1
            or "error" in raw
            or raw.get("result") is None
            or entry.get("body_hex") != body.hex()
        ):
            raise IntegrityError("STABLECOIN_RESTART_NATIVE_ENTITY_MISMATCH")
        received = entry["received_at_ms"]
        values.append((raw["result"], received))
        entities.append(Entity(expected, body, received))
    if supply_event(values, 1800000) != snapshot:
        raise IntegrityError("STABLECOIN_RESTART_ORIGINAL_SNAPSHOT_MISMATCH")
    historical = report.get("source_code_sha256_before")
    if not isinstance(historical, str) or len(historical) != 64:
        raise IntegrityError("STABLECOIN_RESTART_HISTORICAL_CODE_HASH_MISSING")
    return SavedEvidence(tuple(entities), snapshot, bound, before, historical)


@dataclass
class SavedResponse:
    body: bytes
    status: int = 200

    @property
    def content(self) -> Self:
        return self

    async def iter_chunked(self, size: int) -> AsyncIterator[bytes]:
        for start in range(0, len(self.body), size):
            yield self.body[start : start + size]

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class ReplaySession:
    def __init__(
        self, saved: SavedEvidence, health_clock: int, offset: int = 0, failure: bool = False
    ) -> None:
        self.saved, self.offset, self.failure = saved, offset, failure
        self.health_clock = self.receipt = health_clock
        self.attempts = 0

    def post(self, url: str, *, json: dict[str, JsonValue], allow_redirects: bool) -> SavedResponse:
        if url != URL or allow_redirects or self.attempts >= (1 if self.failure else 7):
            raise IntegrityError("STABLECOIN_RESTART_HTTP_ALLOWLIST")
        entity = self.saved.entities[self.attempts]
        if json != entity.request:
            raise IntegrityError("STABLECOIN_RESTART_REQUEST_MISMATCH")
        self.attempts += 1
        if self.failure:
            self.receipt = self.health_clock
            return SavedResponse(
                b'{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"synthetic offline RPC failure"}}'
            )
        self.receipt = entity.received_at_ms + self.offset
        return SavedResponse(entity.body)


class ReplayCollector(StablecoinSupplyCollector):
    def __init__(
        self,
        store: DuckStore,
        session: ReplaySession,
        initial: MarketEvent | None,
        emitted: list[MarketEvent],
    ) -> None:
        async def sink(row: MarketEvent) -> None:
            await store.append([row], exact=True)
            emitted.append(row)

        super().__init__(AppConfig(), sink, initial_snapshot=initial)
        self.session = session

    async def response_received(
        self, request: dict[str, JsonValue], status: int, body: bytes, received: int
    ) -> None:
        if status != 200:
            raise IntegrityError("STABLECOIN_RESTART_UNEXPECTED_HTTP_STATUS")
        self.session.receipt = self.session.health_clock


async def replay(
    store: DuckStore,
    saved: SavedEvidence,
    health_clock: int,
    emitted: list[MarketEvent],
    *,
    initial: MarketEvent | None = None,
    offset: int = 0,
    failure: bool = False,
) -> int:
    session = ReplaySession(saved, health_clock, offset, failure)
    with patch.object(stablecoin_supply, "now_ms", lambda: session.receipt):
        collector = ReplayCollector(store, session, initial, emitted)
        accepted = await collector.collect(cast(aiohttp.ClientSession, session))
    if accepted == failure or session.attempts != (1 if failure else 7):
        raise IntegrityError("STABLECOIN_RESTART_REPLAY_OUTCOME")
    return session.attempts


async def archive_close(
    store: DuckStore, database: Path, output: Path, name: str, expected: list[MarketEvent]
) -> dict[str, object]:
    archive = output / f"{name}.parquet"
    count = await store.export_parquet(archive)
    await seal_archive(archive, count)
    await store.close()
    rows = await complete_thread(read_rows, database, archive)
    if sorted(rows, key=lambda row: row.event_id) != sorted(expected, key=lambda row: row.event_id):
        raise IntegrityError("STABLECOIN_RESTART_CLOSED_ROWS_MISMATCH")
    return {
        "rows": count,
        "archive": archive.name,
        "sha256": await complete_thread(file_digest, archive),
        "exact_closed_reload": True,
    }


async def verify(source: Path, output: Path) -> dict[str, object]:
    source, output = await complete_thread(lambda: (source.resolve(), output.resolve()))
    saved = await complete_thread(preflight, source, output)
    code_paths = [
        ROOT / "src/selective_options/stablecoin_supply.py",
        ROOT / "src/selective_options/stablecoin_checkpoint.py",
        VERIFIER_PATH,
    ]
    code_hashes = await complete_thread(
        lambda: {path.name: file_digest(path) for path in code_paths}
    )
    await complete_thread(output.mkdir, parents=True, exist_ok=False)
    database = output / "events.duckdb"
    expected: list[MarketEvent] = []
    original_clock = saved.health.observed_at_ms
    stage_clocks = {
        "original": original_clock,
        "repeat": original_clock + 10000,
        "failure": original_clock + 20000,
        "recovery": original_clock + 30000,
    }
    scenarios = {}
    phases = {}
    attempts = 0
    store = await DuckStore.open(database)
    try:
        attempts += await replay(store, saved, original_clock, expected)
        if expected != [saved.snapshot, saved.health]:
            raise IntegrityError("STABLECOIN_RESTART_ORIGINAL_ROWS_REWRITTEN")
        scenarios["original"] = await store.stablecoin_supply_quality(as_of_ms=original_clock)
        phases["original"] = await archive_close(store, database, output, "original", expected)
    finally:
        await store.close()
    anchor = await load_stablecoin_checkpoint(database, original_clock, 2)
    if anchor != saved.snapshot:
        raise IntegrityError("STABLECOIN_RESTART_ORIGINAL_CHECKPOINT_MISMATCH")
    store = await DuckStore.open(database)
    try:
        attempts += await replay(
            store, saved, stage_clocks["repeat"], expected, initial=anchor, offset=10000
        )
        repeated = expected[-2]
        attempts += await replay(
            store, saved, stage_clocks["failure"], expected, initial=repeated, failure=True
        )
        for name, clock in (
            ("before_failure", stage_clocks["failure"] - 1),
            ("at_failure", stage_clocks["failure"]),
        ):
            scenarios[name] = await store.stablecoin_supply_quality(as_of_ms=clock)
        phases["failed"] = await archive_close(store, database, output, "failed", expected)
    finally:
        await store.close()
    try:
        await load_stablecoin_checkpoint(database, stage_clocks["failure"])
    except IntegrityError as exc:
        if str(exc) != "STABLECOIN_FEED_UNAVAILABLE":
            raise
    else:
        raise IntegrityError("STABLECOIN_RESTART_FAILURE_CHECKPOINT_ACCEPTED")
    # This explicit historical anchor never claims that the later failed poll is healthy.
    historical = await load_stablecoin_checkpoint(database, stage_clocks["failure"] - 1)
    if historical != repeated:
        raise IntegrityError("STABLECOIN_RESTART_REPEAT_CHECKPOINT_MISMATCH")
    store = await DuckStore.open(database)
    try:
        scenarios["reopened_failure"] = await store.stablecoin_supply_quality(
            as_of_ms=stage_clocks["failure"]
        )
        attempts += await replay(
            store, saved, stage_clocks["recovery"], expected, initial=historical, offset=30000
        )
        recovery = expected[-2]
        for name, clock in (
            ("before_recovery_response", recovery.observed_at_ms - 1),
            ("at_recovery_response", recovery.observed_at_ms),
            ("before_recovery_health", stage_clocks["recovery"] - 1),
            ("at_recovery_health", stage_clocks["recovery"]),
        ):
            scenarios[name] = await store.stablecoin_supply_quality(as_of_ms=clock)
        phases["recovered"] = await archive_close(store, database, output, "recovered", expected)
    finally:
        await store.close()
    if await load_stablecoin_checkpoint(database, stage_clocks["recovery"]) != recovery:
        raise IntegrityError("STABLECOIN_RESTART_RECOVERY_CHECKPOINT_MISMATCH")
    if (
        scenarios["before_failure"]["state"] != "POLL_OBSERVED_VERIFIED"
        or any(
            scenarios[name]["blocker"] != "STABLECOIN_FEED_UNAVAILABLE"
            for name in (
                "at_failure",
                "reopened_failure",
                "before_recovery_response",
                "at_recovery_response",
                "before_recovery_health",
            )
        )
        or scenarios["at_recovery_health"]["state"] != "POLL_OBSERVED_VERIFIED"
    ):
        raise IntegrityError("STABLECOIN_RESTART_QUALITY_BOUNDARY")
    store = await DuckStore.open(database)
    try:
        pending = await store.pending_delivery(32)
        if sorted(pending, key=lambda row: row.event_id) != sorted(
            expected, key=lambda row: row.event_id
        ):
            raise IntegrityError("STABLECOIN_RESTART_OUTBOX_MISMATCH")
        pit_checks = 0
        for row in expected:
            for as_of in (
                max(row.event_time_ms, row.observed_at_ms) - 1,
                max(row.event_time_ms, row.observed_at_ms),
            ):
                snapshot = await store.snapshot("GLOBAL", as_of, "public_reference")
                visible = row.event_id in {event.event_id for event in snapshot.rows()}
                if visible != (as_of >= max(row.event_time_ms, row.observed_at_ms)):
                    raise IntegrityError("STABLECOIN_RESTART_PIT_BOUNDARY")
                pit_checks += 1
        scenarios["reopened_recovery"] = await store.stablecoin_supply_quality(
            as_of_ms=stage_clocks["recovery"]
        )
        originals = await store.read_events([saved.snapshot.event_id, saved.health.event_id])
        if {row.model_dump_json() for row in originals} != {
            saved.snapshot.model_dump_json(),
            saved.health.model_dump_json(),
        }:
            raise IntegrityError("STABLECOIN_RESTART_ORIGINAL_METADATA_REWRITTEN")
    finally:
        await store.close()
    after = await complete_thread(hashes, source)
    after_code = await complete_thread(
        lambda: {path.name: file_digest(path) for path in code_paths}
    )
    if after != saved.hashes or code_hashes != after_code:
        raise IntegrityError("STABLECOIN_RESTART_SOURCE_OR_CODE_CHANGED")
    report: dict[str, object] = {
        "status": "OFFLINE_SAVED_PUBLIC_SUPPLY_RESTART_VERIFIED",
        "source": str(source),
        "provenance": "Original seven public HTTP entities replayed at original receipts, reproducing two original records exactly. Later repeated poll, RPC failure and recovery clocks/outcomes are explicitly synthetic fixtures. No new public request, actual outage/recovery, first publication or independent finality claim.",
        "source_hashes_before": saved.hashes,
        "source_hashes_after": after,
        "source_files_unchanged": True,
        "current_verification_code_hashes": code_hashes,
        "historical_capture_code_hash": saved.historical_code_hash,
        "native_records": 2,
        "persisted_records": len(expected),
        "mocked_http_attempts": attempts,
        "real_public_requests": 0,
        "exact_closed_reloads": 3,
        "three_archives_exact": True,
        "original_receipts_preserved": True,
        "synthetic_receipt_offsets_ms": {"repeat": 10000, "recovery": 30000},
        "simulated_health_clocks": stage_clocks,
        "synthetic_records": [row.model_dump() for row in expected[2:]],
        "failure_checkpoint_refused": True,
        "historical_anchor_policy": "Explicit as-of before failed poll; never a latest healthy fallback or current health approval",
        "quality_scenarios": scenarios,
        "phases": phases,
        "pit_checks": pit_checks,
        "outbox_pending_after_final_reopen": len(pending),
        "redis_delivery_claim": False,
        "continuous_history_complete": False,
        "actual_outage_recovery_verified": False,
        "milestone_2_complete": False,
        "trading_approval": False,
        "orders_submitted": 0,
    }
    await complete_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=ROOT / "evidence/stablecoin-supply-public-01"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/stablecoin-restart-01")
    args = parser.parse_args()
    report = asyncio.run(verify(args.source, args.output))
    print(
        json.dumps(
            {
                field: report[field]
                for field in (
                    "status",
                    "native_records",
                    "persisted_records",
                    "real_public_requests",
                    "exact_closed_reloads",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
