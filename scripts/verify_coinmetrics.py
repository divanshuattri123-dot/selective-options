"""One bounded production daily valuation poll through the actual committed bus.

Preserves native bytes, both clocks, pending outbox, archive and restore. A finite
poll cannot establish complete history, provider publication/finality or readiness.
"""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Literal

import aiohttp
import duckdb

from selective_options.archive import file_digest, restore_archive, seal_archive
from selective_options.bus import EventBus, HotBuffer
from selective_options.coinmetrics import CoinMetricsCollector, canonical_coinmetrics
from selective_options.coinmetrics_quality import coinmetrics_quality
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.research import _path
from selective_options.rotation_runtime import drain_shutdown
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
POLICY = {
    "assets": ("BTC", "ETH"),
    "poll_interval_s": 3600,
    "lookback_days": 7,
    "max_age_ms": 259200000,
    "max_pages": 4,
    "max_rows": 128,
    "max_response_bytes": 2000000,
    "timeout_s": 30,
}


def code_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): file_digest(_path(path))
        for path in sorted((ROOT / "src/selective_options").glob("*.py"))
    }


def preflight(output: Path) -> Path:
    target = _path(output, must_exist=False)
    allowed = (ROOT / "evidence/coinmetrics-public-01").resolve()
    scratch = (ROOT.parents[1] / "work/coinmetrics-0347").resolve()
    if target != allowed and not target.is_relative_to(scratch):
        raise ValueError(
            "output must be new coinmetrics-public-01 or workspace Coin Metrics scratch"
        )
    if target.exists():
        raise FileExistsError("refusing to overwrite poll evidence")
    return target


def closed_rows(database: Path, archive: Path | None = None) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(_path(database)), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id")
        columns = [item[0] for item in cursor.description]
        rows = cursor.fetchall()
        identities = {row[0] for row in rows}
        pending = connection.execute(
            "SELECT event_id, delivered FROM delivery_outbox ORDER BY event_id"
        ).fetchall()
        if len(identities) != len(rows) or pending != [
            (identity, False) for identity in sorted(identities)
        ]:
            raise IntegrityError("COINMETRICS_PROOF_OUTBOX_MEMBERSHIP")
        if (
            archive is not None
            and rows
            != connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY event_id",
                [str(_path(archive))],
            ).fetchall()
        ):
            raise IntegrityError("COINMETRICS_PROOF_ARCHIVE_ROWS")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
            for row in rows
        )


class RecordingCollector(CoinMetricsCollector):
    def __init__(self, bus: EventBus, transport_provenance: str) -> None:
        self.rows: list[MarketEvent] = []
        self.entities: list[dict[str, object]] = []
        self.request_attempts = 0
        self.bus = bus
        self.transport_provenance = transport_provenance
        super().__init__(
            self.record,
            assets=("BTC", "ETH"),
            poll_interval_s=3600,
            lookback_days=7,
            max_age_ms=259200000,
            max_pages=4,
            max_rows=128,
            max_response_bytes=2000000,
            timeout_s=30,
        )

    async def record(self, event: MarketEvent) -> None:
        await self.bus.publish(event)
        self.rows.append(event)

    async def request(
        self, session: aiohttp.ClientSession, parameters: dict[str, str]
    ) -> tuple[object, int]:
        if self.request_attempts >= self.max_pages:
            raise IntegrityError("COINMETRICS_PROOF_REQUEST_BUDGET")
        self.request_attempts += 1
        return await super().request(session, parameters)

    async def response_received(
        self, parameters: dict[str, str], status: int, body: bytes, received_at_ms: int
    ) -> None:
        self.entities.append(
            {
                "request": parameters,
                "http_status": status,
                "body_hex": body.hex(),
                "received_at_ms": received_at_ms,
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "basis": "actual_public_response"
                if self.transport_provenance == "public_http"
                else "explicit_mock_fixture",
            }
        )


async def verify(
    output: Path,
    *,
    transport_provenance: Literal["public_http", "explicit_mock_fixture"] = "public_http",
) -> dict[str, object]:
    if transport_provenance not in ("public_http", "explicit_mock_fixture"):
        raise ValueError("explicit transport provenance required")
    output = await complete_thread(preflight, output)
    frozen = await complete_thread(code_hashes)
    verifier_sha = await complete_thread(file_digest, Path(__file__))
    await complete_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig(
        ingestion={"coinmetrics_enabled": True, "storage_batch_size": 25, "flush_interval_s": 0.1}
    )
    store = await DuckStore.open(output / "events.duckdb")
    stopped = asyncio.Event()
    bus = EventBus(256, 10)
    hot = HotBuffer(256)
    monitor = FreshnessMonitor(int(config.data.max_clock_skew_s * 1000))
    writer = asyncio.create_task(consume(config, bus, store, hot, monitor, stopped, None))
    collector = RecordingCollector(bus, transport_provenance)

    async def drain_writer() -> None:
        await writer

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            accepted = await collector.collect(session)
    finally:
        stopped.set()
        try:
            await drain_shutdown(drain_writer())
        finally:
            await drain_shutdown(store.close())
        # Preserve native entities even if later proof checks fail.
        with (output / "native-entities.json").open("x", encoding="utf-8") as stream:
            json.dump(
                {"transport_provenance": transport_provenance, "entities": collector.entities},
                stream,
                indent=2,
                allow_nan=False,
            )
            stream.write("\n")
    cutoff = now_ms()
    rows = await complete_thread(closed_rows, output / "events.duckdb")
    if set(rows) != set(collector.rows):
        raise IntegrityError("COINMETRICS_PROOF_COMMITTED_ROWS")
    for asset in ("BTC", "ETH"):
        for kind in ("on_chain", "feed_health"):
            if set(hot.recent("coinmetrics_community", asset, kind)) != {
                row for row in rows if row.asset == asset and row.kind == kind
            }:
                raise IntegrityError("COINMETRICS_PROOF_HOT_BUFFER")
    store = await DuckStore.open(output / "events.duckdb")
    quality: dict[str, object] = {}
    boundaries = sorted(
        {
            point
            for row in rows
            for point in (
                max(row.event_time_ms, row.observed_at_ms) - 1,
                max(row.event_time_ms, row.observed_at_ms),
            )
        }
    )
    try:
        for asset in ("BTC", "ETH"):
            view = await store.coinmetrics_quality(asset, cutoff)
            if view != coinmetrics_quality(rows, asset=asset, as_of_ms=cutoff):
                raise IntegrityError("COINMETRICS_PROOF_QUALITY_SCOPE")
            quality[asset] = view
            for boundary in boundaries:
                actual = (await store.snapshot(asset, boundary, "public_reference")).rows()
                expected = {
                    row
                    for row in rows
                    if row.asset == asset
                    and row.event_time_ms <= boundary
                    and row.observed_at_ms <= boundary
                }
                if set(actual) != expected:
                    raise IntegrityError("COINMETRICS_PROOF_BOTH_CLOCK_BOUNDARY")
        await store.append(list(rows), exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        if count != len(rows) or await store.pending_delivery_count() != count:
            raise IntegrityError("COINMETRICS_PROOF_REPEAT_OUTBOX")
    finally:
        await drain_shutdown(store.close())
    if (
        await complete_thread(closed_rows, output / "events.duckdb", output / "events.parquet")
        != rows
    ):
        raise IntegrityError("COINMETRICS_PROOF_REOPEN_ARCHIVE")
    restore = await restore_archive(output / "events.parquet", output / "restored")
    if await complete_thread(closed_rows, output / "restored/events.duckdb") != rows:
        raise IntegrityError("COINMETRICS_PROOF_RESTORE_NATIVE_ROWS")
    if (
        await complete_thread(code_hashes) != frozen
        or await complete_thread(file_digest, Path(__file__)) != verifier_sha
    ):
        raise IntegrityError("COINMETRICS_PROOF_EXECUTED_CODE_CHANGED")
    native_count = sum(row.kind == "on_chain" for row in rows)
    if accepted and (
        not native_count
        or any(
            not isinstance(view, dict) or view["state"] != "DAILY_REGIME_OBSERVED"
            for view in quality.values()
        )
    ):
        raise IntegrityError("COINMETRICS_PROOF_SUCCESS_WITHOUT_NATIVE_COHORT")
    for row in rows:
        if row.kind == "on_chain":
            canonical_coinmetrics(row)
    report: dict[str, object] = {
        "status": (
            "PUBLIC_DAILY_VALUATION_POLL_VERIFIED"
            if transport_provenance == "public_http"
            else "MOCK_DAILY_VALUATION_POLL_VERIFIED"
        )
        if accepted
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "transport_provenance": transport_provenance,
        "policy": POLICY,
        "as_of_ms": cutoff,
        "quality": quality,
        "rows": count,
        "persisted_rows": count,
        "native_daily_rows": native_count,
        "request_attempts": collector.request_attempts,
        "real_public_requests_attempted": collector.request_attempts
        if transport_provenance == "public_http"
        else 0,
        "retries": 0,
        "boundary_cutoffs_ms": boundaries,
        "pit_boundary_checks": 2 * len(boundaries),
        "source_sha256": await complete_thread(file_digest, output / "events.duckdb"),
        "archive_sha256": manifest.sha256,
        "native_entities_sha256": await complete_thread(
            file_digest, output / "native-entities.json"
        ),
        "restored_sha256": await complete_thread(file_digest, output / "restored/events.duckdb"),
        "code_hashes": frozen,
        "verifier_sha256": verifier_sha,
        "parquet_exact_match": True,
        "closed_reload_exact_match": True,
        "outbox_pending_after_reopen": count,
        "restored_outbox_pending": restore["redis_delivery_pending"],
        "redis_delivery_claim": False,
        "redirects_allowed": False,
        "tls_verification": "default; no override",
        "publication_time_available": False,
        "provider_finality_independently_verified": False,
        "exchange_attribution_available": False,
        "history_complete": False,
        "continuous_history_verified": False,
        "milestone_2_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
        "orders_submitted": 0,
        "historical_strategy_trials": 0,
        "paper_days": 0,
        "paper_trades": 0,
    }
    with (output / "report.json").open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
        stream.write("\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/coinmetrics-public-01")
    result = asyncio.run(verify(parser.parse_args().output))
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "persisted_rows",
                    "native_daily_rows",
                    "real_public_requests_attempted",
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
