"""One bounded official FRED DFII10 CSV request, exact receipt/PIT/archive evidence."""

import argparse
import asyncio
import csv
import hashlib
import io
import json
from datetime import date
from pathlib import Path
from typing import Literal

import aiohttp
import duckdb

from selective_options import __version__
from selective_options.archive import file_digest, seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.store import DuckStore, complete_thread
from selective_options.tips_yield import REFERENCES, TipsYieldCollector, tips_yield_quality

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILE = ROOT / "src/selective_options/tips_yield.py"


def preflight(output: Path, history_days: int) -> tuple[Path, str]:
    if type(history_days) is not int or not 1 <= history_days <= 366:
        raise ValueError("bounded inclusive history days of 1..366 required")
    output = output.resolve()
    scratch = (ROOT.parents[1] / "work/tips-yield-0339").resolve()
    if output != (ROOT / "evidence/tips-yield-public-01").resolve() and not output.is_relative_to(
        scratch
    ):
        raise ValueError(
            "output must be tips-yield-public-01 or within workspace work/tips-yield-0339"
        )
    if output.exists():
        raise FileExistsError("refusing to overwrite public probe evidence")
    return output, file_digest(SOURCE_FILE)


def independent_entity(body: bytes, snapshot: MarketEvent) -> list[dict[str, str]]:
    """Independently read exact native CSV fields, without the production parser."""
    table = list(csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""), strict=True))
    if not table or table[0] != ["observation_date", "DFII10"]:
        raise IntegrityError("TIPS_PROBE_INDEPENDENT_HEADER")
    rows = []
    for fields in table[1:]:
        if len(fields) != 2:
            raise IntegrityError("TIPS_PROBE_INDEPENDENT_ROW_SHAPE")
        dated, value = fields
        rows.append(
            {
                "observation_date": dated,
                "yield_percent_text": value,
                "missing": "dot" if value == "." else "empty" if value == "" else "not_missing",
            }
        )
    payload = snapshot.payload()
    if (
        rows != payload["rows"]
        or hashlib.sha256(body).hexdigest() != payload["raw_csv_sha256"]
        or body.hex() != payload["raw_csv_hex"]
    ):
        raise IntegrityError("TIPS_PROBE_INDEPENDENT_NATIVE_VALUES_MISMATCH")
    return rows


def closed_rows(database: Path, archive: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id")
        columns = [entry[0] for entry in cursor.description]
        rows = cursor.fetchall()
        if (
            rows
            != connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(archive)]
            ).fetchall()
        ):
            raise IntegrityError("TIPS_PROBE_DATABASE_PARQUET_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in rows
        )


class RecordingCollector(TipsYieldCollector):
    def __init__(self, config: AppConfig, store: DuckStore) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.entities: list[dict[str, object]] = []
        self.body: bytes | None = None
        self.requests_attempted = 0
        super().__init__(config, self.record)

    async def record(self, row: MarketEvent) -> None:
        await self.store.append([row], exact=True)
        self.rows.append(row)

    async def response_received(
        self, body: bytes, status: int, received: int, start_date: date, end_date: date
    ) -> None:
        if self.entities:
            raise IntegrityError("TIPS_PROBE_MORE_THAN_ONE_ENTITY")
        self.body = body
        self.entities.append(
            {
                "http_status": status,
                "received_at_ms": received,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "requested_start_date": start_date.isoformat(),
                "requested_end_date": end_date.isoformat(),
                "basis": "exact decoded HTTP entity bytes passed to production parser",
            }
        )

    async def request(
        self, session: aiohttp.ClientSession, start_date: date, end_date: date
    ) -> tuple[bytes, int]:
        self.requests_attempted += 1
        if self.requests_attempted != 1:
            raise IntegrityError("TIPS_PROBE_REQUEST_BUDGET")
        return await super().request(session, start_date, end_date)


async def verify(
    output: Path,
    *,
    history_days: int = 90,
    transport_provenance: Literal["public_http", "explicit_mock_fixture"] = "public_http",
) -> dict[str, object]:
    if transport_provenance not in ("public_http", "explicit_mock_fixture"):
        raise ValueError("explicit public or mocked fixture provenance required")
    output, source_before = await complete_thread(preflight, output, history_days)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig(
        ingestion={"tips_yield_enabled": True, "tips_yield_history_days": history_days}
    )
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(config, store)
    native_rows: list[dict[str, str]] = []
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=min(config.venue.request_timeout_s, 30.0)),
            headers={"User-Agent": "SelectiveOptionsResearch/0.3 (finite public DFII10 CSV probe)"},
        ) as session:
            await collector.poll(session)
        if collector.requests_attempted != 1:
            raise IntegrityError("TIPS_PROBE_REQUEST_COUNT")
        cutoff = now_ms()
        snapshot = await store.snapshot("GLOBAL", cutoff, "public_reference")
        if set(snapshot.rows()) != set(collector.rows):
            raise IntegrityError("TIPS_PROBE_CURRENT_PIT_ROWS_MISMATCH")
        view = tips_yield_quality(snapshot.rows(), as_of_ms=cutoff)
        if await store.tips_yield_quality(as_of_ms=cutoff) != view:
            raise IntegrityError("TIPS_PROBE_BOUNDED_STORE_QUALITY_MISMATCH")
        if view != tips_yield_quality(collector.rows, as_of_ms=cutoff):
            raise IntegrityError("TIPS_PROBE_EXACT_QUALITY_MISMATCH")
        snapshots = [row for row in collector.rows if row.kind == "index"]
        if view["state"] == "POLL_SNAPSHOT_VERIFIED":
            if collector.body is None or len(snapshots) != 1:
                raise IntegrityError("TIPS_PROBE_ACCEPTED_ENTITY_MISSING")
            native_rows = independent_entity(collector.body, snapshots[0])
        for row in collector.rows:
            at = max(row.observed_at_ms, row.event_time_ms)
            before = await store.snapshot("GLOBAL", at - 1, "public_reference")
            available = await store.snapshot("GLOBAL", at, "public_reference")
            if row.event_id in {item.event_id for item in before.rows()} or row.event_id not in {
                item.event_id for item in available.rows()
            }:
                raise IntegrityError("TIPS_PROBE_EXACT_PIT_BOUNDARY")
        if collector.body is not None:
            await asyncio.to_thread((output / "response.csv").write_bytes, collector.body)
            if (
                await complete_thread(file_digest, output / "response.csv")
                != collector.entities[0]["sha256"]
            ):
                raise IntegrityError("TIPS_PROBE_SAVED_ENTITY_CHECKSUM")
        await store.append(collector.rows, exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        if count != len(collector.rows) or await store.pending_delivery_count() != count:
            raise IntegrityError("TIPS_PROBE_OUTBOX_OR_ARCHIVE_COUNT")
    finally:
        await store.close()
    saved = await complete_thread(closed_rows, output / "events.duckdb", output / "events.parquet")
    if set(saved) != set(collector.rows):
        raise IntegrityError("TIPS_PROBE_CLOSED_RELOAD_EXACT")
    reopened = await DuckStore.open(output / "events.duckdb")
    try:
        snapshot = await reopened.snapshot("GLOBAL", cutoff, "public_reference")
        if (
            tips_yield_quality(snapshot.rows(), as_of_ms=cutoff) != view
            or await reopened.pending_delivery_count() != count
        ):
            raise IntegrityError("TIPS_PROBE_REOPEN_VIEW_OR_OUTBOX")
        if await reopened.tips_yield_quality(as_of_ms=cutoff) != view:
            raise IntegrityError("TIPS_PROBE_REOPEN_BOUNDED_STORE_QUALITY_MISMATCH")
    finally:
        await reopened.close()
    source_after = await complete_thread(file_digest, SOURCE_FILE)
    if source_before != source_after:
        raise IntegrityError("TIPS_PROBE_SOURCE_CHANGED")
    report: dict[str, object] = {
        "version": __version__,
        "status": (
            "PUBLIC_DFII10_SNAPSHOT_VERIFIED"
            if transport_provenance == "public_http"
            else "FIXTURE_DFII10_SNAPSHOT_VERIFIED"
        )
        if view["state"] == "POLL_SNAPSHOT_VERIFIED"
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "transport_provenance": transport_provenance,
        "real_public_requests_attempted": collector.requests_attempted
        if transport_provenance == "public_http"
        else 0,
        "request_attempts": collector.requests_attempted,
        "retries": 0,
        "source": "fred_tips",
        "series": "DFII10",
        "units": "percent",
        "frequency": "daily",
        "logical_date_is_publication_instant": False,
        "entities": collector.entities,
        "native_rows": native_rows,
        "quality": view,
        "persisted_rows": count,
        "independent_native_entity_comparison": view["state"] == "POLL_SNAPSHOT_VERIFIED",
        "pit_boundary_checks": 2 * len(collector.rows),
        "parquet_exact_match": True,
        "parquet_sha256": manifest.sha256,
        "closed_reload_exact_match": True,
        "outbox_pending_after_reopen": count,
        "redis_delivery_claim": False,
        "bounded_store_quality_matches": True,
        "source_code_sha256_before": source_before,
        "source_code_sha256_after": source_after,
        "source_code_unchanged": True,
        "source_references": list(REFERENCES),
        "redirects_allowed": False,
        "tls_verification": "default; no override",
        "network_timeout_s": min(config.venue.request_timeout_s, 30.0),
        "native_data_backdated": False,
        "historical_vintage_available": False,
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
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/tips-yield-public-01")
    parser.add_argument("--history-days", type=int, default=90)
    args = parser.parse_args()
    report = asyncio.run(verify(args.output, history_days=args.history_days))
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
