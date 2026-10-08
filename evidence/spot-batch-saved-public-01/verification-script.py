"""Offline saved Kraken canonical-event replay and staged-batch refusal proof.

Original websocket frames were not archived. Batch containers, ordering and
boundaries are reconstructed and explicitly labelled; no original-frame claim.
"""

import argparse
import asyncio
import hashlib
import json
import math
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast
from unittest.mock import patch

import duckdb
from pydantic import JsonValue

from selective_options import collectors
from selective_options.archive import _verify_archive, file_digest, seal_archive
from selective_options.bus import EventBus, HotBuffer
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.rotation_runtime import drain_shutdown
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
VERIFIER = Path(__file__).resolve()
MAX_FILES = 32
MAX_BYTES = 32 * 1024 * 1024
MAX_ROWS = 256
CODE = (
    "spot_trades.py",
    "bars.py",
    "events.py",
    "bus.py",
    "store.py",
    "ingestion.py",
    "rotation_runtime.py",
    "collectors.py",
    "config.py",
)


@dataclass(frozen=True)
class Saved:
    native: tuple[MarketEvent, ...]
    cohorts: tuple[tuple[MarketEvent, ...], ...]
    hashes: dict[str, str]
    omitted_id: int


def source_hashes(source: Path) -> dict[str, str]:
    if source.is_symlink() or not source.is_dir():
        raise ValueError("source must be an existing regular evidence directory")
    paths = sorted(source.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError("linked source evidence is refused")
    files = [path for path in paths if path.is_file()]
    if len(files) > MAX_FILES:
        raise IntegrityError("SPOT_BATCH_PROOF_SOURCE_FILE_BUDGET")
    if sum(path.stat().st_size for path in files) > MAX_BYTES:
        raise IntegrityError("SPOT_BATCH_PROOF_SOURCE_BYTE_BUDGET")
    return {path.relative_to(source).as_posix(): file_digest(path) for path in files}


def read_rows(database: Path, archive: Path | None = None) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [MAX_ROWS + 1]
        )
        columns = [row[0] for row in cursor.description]
        values = cursor.fetchall()
        if len(values) > MAX_ROWS:
            raise IntegrityError("SPOT_BATCH_PROOF_ROW_BUDGET")
        if archive is not None:
            manifest = _verify_archive(archive)
            archived = connection.execute(
                "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id LIMIT ?",
                [str(archive), MAX_ROWS + 1],
            ).fetchall()
            if archived != values or manifest.rows != len(values):
                raise IntegrityError("SPOT_BATCH_PROOF_DATABASE_ARCHIVE_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, value, strict=True)), strict=True)
            for value in values
        )


def reconstructed_trade(row: MarketEvent) -> dict[str, JsonValue]:
    payload = row.payload()
    return {
        "symbol": row.symbol,
        "price": payload["price"],
        "qty": payload["qty"],
        "side": payload["side"],
        "trade_id": payload["trade_id"],
        "timestamp": payload["source_timestamp"],
    }


def preflight(source: Path, output: Path) -> Saved:
    scratch = (ROOT.parents[1] / "work/spot-batch-0344").resolve()
    destination = output.resolve()
    if destination != (
        ROOT / "evidence/spot-batch-saved-public-01"
    ).resolve() and not destination.is_relative_to(scratch):
        raise ValueError("output must be spot-batch-saved-public-01 or within workspace scratch")
    if (
        destination.exists()
        or destination == source.resolve()
        or source.resolve().is_relative_to(destination)
        or destination.is_relative_to(source.resolve())
    ):
        raise FileExistsError("refusing to overwrite proof or original evidence")
    before = source_hashes(source)
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if (
        report.get("status") != "PUBLIC_GAP_TEST_PASSED"
        or report.get("evidence_type")
        != "live public Kraken BTC stream plus REST; one trade intentionally omitted from collector input"
        or report.get("orders_submitted") != 0
        or report.get("trading_enabled") is not False
    ):
        raise IntegrityError("SPOT_BATCH_PROOF_SOURCE_PROVENANCE")
    all_rows = read_rows(source / "events.duckdb", source / "events.parquet")
    native = tuple(
        sorted(
            (
                row
                for row in all_rows
                if row.source == "kraken"
                and row.environment == "public_reference"
                and row.asset == "BTC"
                and row.kind == "trade"
                and row.symbol == "BTC/USD"
            ),
            key=lambda row: cast(int, row.payload()["trade_id"]),
        )
    )
    if len(native) != 55 or len({row.event_id for row in native}) != 55:
        raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_COUNT")
    groups: list[list[MarketEvent]] = []
    previous: int | None = None
    for row in native:
        payload = row.payload()
        trade_id = payload.get("trade_id")
        if (
            type(trade_id) is not int
            or trade_id < 0
            or not isinstance(payload.get("source_timestamp"), str)
        ):
            raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_FIELDS")
        stamp = datetime.fromisoformat(cast(str, payload["source_timestamp"]))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_TIMEZONE")
        body: dict[str, JsonValue] = {
            "trade_id": trade_id,
            "price": payload["price"],
            "qty": payload["qty"],
            "side": payload["side"],
            "quote_currency": "USD",
            "source_timestamp": stamp.isoformat(),
        }
        for field in ("price", "qty"):
            value = body[field]
            if (
                type(value) not in (int, float)
                or not math.isfinite(cast(float, value))
                or cast(float, value) <= 0
            ):
                raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_FIELDS")
        expected = make_event(
            source="kraken",
            environment="public_reference",
            asset="BTC",
            symbol=row.symbol,
            kind="trade",
            event_time_ms=int(stamp.timestamp() * 1000),
            observed_at_ms=row.observed_at_ms,
            native_id=str(trade_id),
            payload=body,
        )
        if expected != row:
            raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_ID_PAYLOAD_CLOCK")
        if previous is None or trade_id != previous + 1:
            groups.append([])
        groups[-1].append(row)
        previous = trade_id
    if (
        [len(group) for group in groups] != [50, 5]
        or report.get("omitted_trade_id") != 109230677
        or any(cast(int, row.payload()["trade_id"]) == 109230677 for row in native)
    ):
        raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_COHORT_GAP")
    return Saved(native, tuple(tuple(group) for group in groups), before, 109230677)


def _json_default(value: object) -> object:
    if isinstance(value, MarketEvent):
        return value.model_dump()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def collector_state(collector: SpotTradeCollector) -> dict[str, object]:
    return {
        "symbols": dict(collector.symbols),
        "sequences": {
            symbol: {
                "last": sequence.last,
                "capacity": sequence.capacity,
                "seen": list(sequence.seen.items()),
            }
            for symbol, sequence in collector.sequences.items()
        },
        "last_events": {symbol: row.model_dump() for symbol, row in collector.last_events.items()},
        "bars": {
            symbol: {
                "timeframes": list(builder.timeframes),
                "coverage_start_ms": builder.coverage_start_ms,
                "lateness_ms": builder.lateness_ms,
                "watermark_ms": builder.watermark_ms,
                "identity": builder.identity,
                "buckets": [
                    {"key": list(key), "accumulator": asdict(bar)}
                    for key, bar in sorted(builder.buckets.items())
                ],
            }
            for symbol, builder in collector.bars.items()
        },
        "awaiting_snapshot": sorted(collector.awaiting_snapshot),
        "repairing": sorted(collector.repairing),
        "checkpoint_assets": dict(collector.checkpoint_assets),
        "pending_frames": list(collector.pending_frames),
        "emission_failed": collector._emission_failed,
    }


def state_digest(collector: SpotTradeCollector) -> str:
    raw = json.dumps(
        collector_state(collector),
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
        allow_nan=False,
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def expected_bars(
    trades: tuple[MarketEvent, ...], coverage_start: int, received: int
) -> list[MarketEvent]:
    groups: dict[int, list[MarketEvent]] = {}
    for row in trades:
        opening = row.event_time_ms // 300000 * 300000
        groups.setdefault(opening, []).append(row)
    output = []
    for opening, group in sorted(groups.items()):
        if opening + 300000 > received:
            continue
        ordered = sorted(
            group, key=lambda row: (row.event_time_ms, cast(int, row.payload()["trade_id"]))
        )
        payloads = [row.payload() for row in ordered]
        prices = [cast(float, payload["price"]) for payload in payloads]
        body: dict[str, JsonValue] = {
            "open_time_ms": opening,
            "close_time_ms": opening + 300000,
            "timeframe_s": 300,
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": float(
                sum((Decimal(str(payload["qty"])) for payload in payloads), Decimal(0))
            ),
            "volume_unit": "base_asset",
            "trade_count": len(group),
            "closed": True,
            "complete": opening >= coverage_start,
            "aggregation_version": 1,
            "lateness_ms": 0,
            "coverage_start_ms": coverage_start,
        }
        output.append(
            make_event(
                source="kraken",
                environment="public_reference",
                asset="BTC",
                symbol="BTC/USD",
                kind="bar",
                event_time_ms=opening + 300000,
                observed_at_ms=received,
                payload=body,
            )
        )
    return output


async def verify(source: Path, output: Path) -> dict[str, object]:
    saved = await complete_thread(preflight, source, output)
    destination = await complete_thread(output.resolve)
    code_before = {name: file_digest(ROOT / "src/selective_options" / name) for name in CODE}
    verifier_hash = file_digest(VERIFIER)
    destination.mkdir(parents=True, exist_ok=False)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "bar_timeframes_s": [300],
                "bar_lateness_s": 0,
                "flush_interval_s": 0.005,
                "storage_batch_size": 3,
                "queue_capacity": 128,
            },
        }
    )
    bus = EventBus(config.ingestion.queue_capacity, config.ingestion.queue_timeout_s)
    store = await DuckStore.open(destination / "events.duckdb")
    stopped = asyncio.Event()
    writer = asyncio.create_task(
        consume(config, bus, store, HotBuffer(256), FreshnessMonitor(5000), stopped, None)
    )
    emitted: list[MarketEvent] = []
    containers: list[dict[str, object]] = []
    cohort_reports: list[dict[str, object]] = []
    derived: dict[str, dict[str, object]] = {}
    rejected: dict[str, object] = {}
    clock = [saved.native[0].observed_at_ms]

    async def sink(row: MarketEvent) -> None:
        emitted.append(row)
        await bus.publish(row)

    async def replay(
        collector: SpotTradeCollector,
        rows: tuple[MarketEvent, ...],
        kind: Literal["snapshot", "update"],
        receipt: int,
        cohort: int,
    ) -> None:
        body: dict[str, JsonValue] = {
            "channel": "trade",
            "type": kind,
            "data": [reconstructed_trade(row) for row in reversed(rows)]
            if kind == "snapshot"
            else [reconstructed_trade(row) for row in rows],
        }
        containers.append(
            {
                "provenance": "reconstructed_container_from_saved_canonical_events",
                "cohort": cohort,
                "received_at_ms": receipt,
                "original_frame_bytes_available": False,
                "original_frame_boundaries_verified": False,
                "event_ids": [row.event_id for row in rows],
                "body": body,
            }
        )
        clock[0] = receipt
        before = len(emitted)
        await collector._trades(body, receipt)
        actual = tuple(
            sorted(
                (row for row in emitted[before:] if row.kind == "trade"),
                key=lambda row: row.event_id,
            )
        )
        if actual != tuple(sorted(rows, key=lambda row: row.event_id)):
            raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_REPLAY_MISMATCH")
        for row in emitted[before:]:
            if row.kind != "trade":
                derived[row.event_id] = {
                    "provenance": "offline_reconstructed_batch_derived_status",
                    "cohort": cohort,
                }
        await bus.flush()

    collection_failed = False
    try:
        with patch.object(collectors, "now_ms", lambda: clock[0]):
            for index, cohort in enumerate(saved.cohorts):
                collector = SpotTradeCollector(config, sink)
                collector.symbols = {"BTC/USD": "BTC"}
                collector.awaiting_snapshot = {"BTC/USD"}
                receipts: dict[int, list[MarketEvent]] = {}
                for row in cohort:
                    receipts.setdefault(row.observed_at_ms, []).append(row)
                for part, (receipt, rows) in enumerate(sorted(receipts.items())):
                    await replay(
                        collector,
                        tuple(rows),
                        "snapshot" if part == 0 else "update",
                        receipt,
                        index,
                    )
                coverage = min(row.observed_at_ms for row in cohort)
                flush = max(
                    max(row.observed_at_ms for row in cohort),
                    (max(row.event_time_ms for row in cohort) // 300000 + 1) * 300000 + 1,
                )
                before_flush = len(emitted)
                clock[0] = flush
                await collector._flush(flush)
                bars = [row for row in emitted[before_flush:] if row.kind == "bar"]
                independent = expected_bars(cohort, coverage, flush)
                if bars != independent:
                    raise IntegrityError("SPOT_BATCH_PROOF_NATIVE_BAR_OHLC_VOLUME")
                for row in bars:
                    derived[row.event_id] = {
                        "provenance": "offline_saved_native_input_derived_bar",
                        "cohort": index,
                        "flush_observed_at_ms": flush,
                    }
                await bus.flush()
                snapshot = {
                    "channel": "trade",
                    "type": "snapshot",
                    "data": [reconstructed_trade(row) for row in cohort],
                }
                before_overlap, digest = len(emitted), state_digest(collector)
                await collector._trades(cast(dict[str, JsonValue], snapshot), flush + 1)
                if len(emitted) != before_overlap or state_digest(collector) != digest:
                    raise IntegrityError("SPOT_BATCH_PROOF_OVERLAP_NOT_DEDUPED")
                cohort_reports.append(
                    {
                        "cohort": index,
                        "native_rows": len(cohort),
                        "first_trade_id": cohort[0].payload()["trade_id"],
                        "last_trade_id": cohort[-1].payload()["trade_id"],
                        "accepted_input_event_ids": [row.event_id for row in cohort],
                        "derived_bar_event_ids": [row.event_id for row in bars],
                        "coverage_start_ms": coverage,
                        "flush_observed_at_ms": flush,
                        "timeframe_s": 300,
                        "overlap_reemitted_rows": 0,
                        "native_successor_within_cohort_verified": len(receipts) > 1,
                        "continuity_across_cohorts_claimed": False,
                    }
                )
                if index == 0:
                    last_id = cast(int, cohort[-1].payload()["trade_id"])
                    receipt = flush + 200
                    good = {
                        "symbol": "BTC/USD",
                        "trade_id": last_id + 1,
                        "price": 123.45,
                        "qty": 0.25,
                        "side": "buy",
                        "timestamp": datetime.fromtimestamp((flush + 100) / 1000, UTC).isoformat(),
                        "fixture_provenance": "synthetic_offline_fixture",
                    }
                    late = good | {
                        "trade_id": last_id + 2,
                        "timestamp": datetime.fromtimestamp((flush - 1) / 1000, UTC).isoformat(),
                    }
                    bad_body: dict[str, JsonValue] = {
                        "channel": "trade",
                        "type": "update",
                        "data": [
                            cast(dict[str, JsonValue], good),
                            cast(dict[str, JsonValue], late),
                        ],
                    }
                    before_invalid, before_state = len(emitted), state_digest(collector)
                    before_rows = await store.read_events([row.event_id for row in emitted])
                    try:
                        await collector._trades(bad_body, receipt)
                    except IntegrityError as exc:
                        if "LATE_TRADE" not in str(exc):
                            raise
                        refusal = str(exc)
                    else:
                        raise IntegrityError("SPOT_BATCH_PROOF_LATE_BATCH_ACCEPTED")
                    await bus.flush()
                    after_rows = await store.read_events([row.event_id for row in emitted])
                    if (
                        len(emitted) != before_invalid
                        or state_digest(collector) != before_state
                        or before_rows != after_rows
                    ):
                        raise IntegrityError("SPOT_BATCH_PROOF_INVALID_BATCH_PARTIALLY_APPLIED")
                    candidate_ids = []
                    for raw in (good, late):
                        stamp = datetime.fromisoformat(cast(str, raw["timestamp"]))
                        body = {
                            key: cast(JsonValue, raw[key])
                            for key in ("trade_id", "price", "qty", "side")
                        }
                        body |= {"quote_currency": "USD", "source_timestamp": stamp.isoformat()}
                        candidate_ids.append(
                            make_event(
                                source="kraken",
                                environment="public_reference",
                                asset="BTC",
                                symbol="BTC/USD",
                                kind="trade",
                                event_time_ms=int(stamp.timestamp() * 1000),
                                observed_at_ms=receipt,
                                native_id=str(raw["trade_id"]),
                                payload=body,
                            ).event_id
                        )
                    rejected = {
                        "provenance": "synthetic_offline_later_invalid_batch",
                        "cohort": index,
                        "body": bad_body,
                        "received_at_ms": receipt,
                        "candidate_event_ids": candidate_ids,
                        "error": refusal,
                        "emitted_rows": 0,
                        "state_sha256_before": before_state,
                        "state_sha256_after": state_digest(collector),
                        "database_unchanged": True,
                    }
                    good_body: dict[str, JsonValue] = {
                        "channel": "trade",
                        "type": "update",
                        "data": [cast(dict[str, JsonValue], good)],
                    }
                    clock[0] = receipt
                    before_successor = len(emitted)
                    await collector._trades(good_body, receipt)
                    successor = emitted[before_successor:]
                    if len(successor) != 1 or successor[0].event_id != candidate_ids[0]:
                        raise IntegrityError("SPOT_BATCH_PROOF_SUCCESSOR_AFTER_REFUSAL")
                    derived[successor[0].event_id] = {
                        "provenance": "synthetic_offline_accepted_trade_fixture",
                        "cohort": index,
                        "body": good_body,
                        "received_at_ms": receipt,
                    }
                    synthetic_flush = (successor[0].event_time_ms // 300000 + 1) * 300000 + 1
                    clock[0] = synthetic_flush
                    start = len(emitted)
                    await collector._flush(synthetic_flush)
                    if emitted[start:] != expected_bars(
                        tuple(successor), coverage, synthetic_flush
                    ):
                        raise IntegrityError("SPOT_BATCH_PROOF_SYNTHETIC_SUCCESSOR_BAR")
                    for row in emitted[start:]:
                        derived[row.event_id] = {
                            "provenance": "synthetic_offline_input_derived_bar",
                            "cohort": index,
                            "accepted_input_event_ids": [event.event_id for event in successor],
                            "flush_observed_at_ms": synthetic_flush,
                        }
                    await bus.flush()
                    cohort_reports[-1]["synthetic_accepted_trade_event_ids"] = [
                        row.event_id for row in successor
                    ]
                    cohort_reports[-1]["synthetic_derived_bar_event_ids"] = [
                        row.event_id for row in emitted[start:]
                    ]
                    cohort_reports[-1]["synthetic_flush_observed_at_ms"] = synthetic_flush
    except BaseException:
        collection_failed = True
        raise
    finally:

        async def finish_writer() -> None:
            stopped.set()
            await writer

        try:
            cancelled = await drain_shutdown(finish_writer())
        except BaseException:
            await drain_shutdown(store.close())
            raise
        if collection_failed or cancelled is not None:
            await drain_shutdown(store.close())
        if cancelled is not None and not collection_failed:
            raise cancelled
    try:
        all_rows = await store.read_events([row.event_id for row in emitted])
        native_ids = {row.event_id for row in saved.native}
        if tuple(
            sorted(
                (row for row in all_rows if row.event_id in native_ids),
                key=lambda row: row.event_id,
            )
        ) != tuple(sorted(saved.native, key=lambda row: row.event_id)) or len(all_rows) != len(
            {row.event_id for row in emitted}
        ):
            raise IntegrityError("SPOT_BATCH_PROOF_PERSISTED_NATIVE_MISMATCH")
        count = await store.export_parquet(destination / "events.parquet")
        await seal_archive(destination / "events.parquet", count)
        if await store.pending_delivery_count() != count:
            raise IntegrityError("SPOT_BATCH_PROOF_OUTBOX_NOT_PENDING")
    finally:
        close_cancelled = await drain_shutdown(store.close())
        if close_cancelled is not None:
            raise close_cancelled
    persisted = read_rows(destination / "events.duckdb", destination / "events.parquet")
    pit = []
    with duckdb.connect(str(destination / "events.duckdb"), read_only=True) as connection:
        cutoffs = sorted(
            {
                value
                for row in persisted
                for value in (
                    row.event_time_ms - 1,
                    row.event_time_ms,
                    row.observed_at_ms - 1,
                    row.observed_at_ms,
                )
                if value > 0
            }
        )
        for cutoff in cutoffs:
            actual = connection.execute(
                "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                [cutoff, cutoff],
            ).fetchall()
            expected = [
                (row.event_id,)
                for row in persisted
                if row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff
            ]
            if actual != expected:
                raise IntegrityError("SPOT_BATCH_PROOF_PIT_BOTH_CLOCKS")
            pit.append(
                {
                    "as_of_ms": cutoff,
                    "available_rows": len(actual),
                    "event_ids": [identity[0] for identity in actual],
                }
            )
        pending = connection.execute(
            "SELECT event_id FROM delivery_outbox WHERE NOT delivered ORDER BY event_id"
        ).fetchall()
        if pending != [(row.event_id,) for row in persisted]:
            raise IntegrityError("SPOT_BATCH_PROOF_OUTBOX_MEMBERSHIP")
    code_after = {name: file_digest(ROOT / "src/selective_options" / name) for name in CODE}
    if (
        code_before != code_after
        or verifier_hash != file_digest(VERIFIER)
        or saved.hashes != source_hashes(source)
    ):
        raise IntegrityError("SPOT_BATCH_PROOF_SOURCE_OR_CODE_CHANGED")
    report: dict[str, object] = {
        "status": "OFFLINE_SAVED_SPOT_BATCH_STAGING_VERIFIED",
        "real_public_requests_attempted": 0,
        "source_evidence": source.name,
        "saved_native_trade_rows": len(saved.native),
        "replayed_native_trade_rows": 55,
        "persisted_rows": len(persisted),
        "pending_delivery_rows": len(persisted),
        "original_websocket_frames_available": False,
        "original_frame_boundaries_verified": False,
        "container_provenance": "reconstructed from saved canonical native events; ordering/boundaries are offline choices",
        "cohorts": cohort_reports,
        "source_native_gap_trade_id": saved.omitted_id,
        "continuity_across_native_gap_claimed": False,
        "reconstructed_containers": containers,
        "synthetic_rejected_batch": rejected,
        "derived_event_provenance": derived,
        "persisted_event_ids": [row.event_id for row in persisted],
        "pit_checks": pit,
        "source_hashes_before": saved.hashes,
        "source_hashes_after": source_hashes(source),
        "code_sha256_before": code_before,
        "code_sha256_after": code_after,
        "verifier_sha256": verifier_hash,
        "consumer_queue_drained": bus.queue.empty(),
        "redis_delivery_claimed": False,
        "current_feed_health_verified": False,
        "complete_history_verified": False,
        "seven_day_validation_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
        "limitation": "Native event fields, IDs and clocks are preserved; websocket bytes/boundaries were not saved. Two fresh collector cohorts are separated across missing WS trade677 without REST-to-WS identity conversion. Synthetic invalid/accepted updates and flush clocks are labelled. Staging refuses invalid batches before emission; sink publication is not an atomic database commit. No live continuity or full history is established.",
    }
    (destination / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(VERIFIER, destination / "verification-script.py")
    (destination / "README.md").write_text(
        "# Offline saved Kraken batch staging\n\nZero public requests. Real SpotTradeCollector, EventBus, production consumer and DuckStore replay55 saved Kraken BTC native trade events from live-gap-02. Original frame bytes and boundaries were not archived, so input containers are explicitly reconstructed. Two fresh collectors process contiguous cohorts50and5 separated across missing WS trade109230677; no REST row is converted to WS and no cross-gap continuity is claimed. Native fields, IDs, source clocks and original receipts remain exact. Derived status/bar rows use explicitly synthetic offline clocks and are mapped by event ID. A two-trade synthetic update whose later trade is below a finalized watermark emits zero rows and preserves collector state and storage; its valid successor is then accepted and independently aggregated. OHLC, Decimal volume/count, closed bars, both-clock PIT, sealed archive and exact pending outbox membership are checked. This is not original-frame replay, atomic sink commit, public collection, Redis delivery, complete history or seven-day validation.\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "evidence/live-gap-02")
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/spot-batch-saved-public-01")
    args = parser.parse_args()
    report = asyncio.run(verify(args.source, args.output))
    print(
        json.dumps(
            {
                "status": report["status"],
                "native_rows": report["replayed_native_trade_rows"],
                "persisted_rows": report["persisted_rows"],
                "public_requests": 0,
            }
        )
    )


if __name__ == "__main__":
    main()
