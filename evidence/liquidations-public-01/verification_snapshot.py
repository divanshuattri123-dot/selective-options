"""One finite public forceOrder socket subscription; quiet markets remain unobserved."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
from typing import Literal

import duckdb

from selective_options import __version__
from selective_options.archive import file_digest, seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.liquidations import SOURCE, URL, LiquidationCollector, liquidations_quality
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILE = ROOT / "src/selective_options/liquidations.py"
REFERENCES = [
    "https://developers.binance.info/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream"
]


def preflight(output: Path, duration_s: float) -> tuple[Path, str]:
    if type(duration_s) not in (int, float) or not 0 < duration_s <= 20:
        raise ValueError("finite duration of at most 20 seconds required")
    output = output.resolve()
    scratch = (ROOT.parents[1] / "work/liquidations-0339").resolve()
    if output != (ROOT / "evidence/liquidations-public-01").resolve() and not output.is_relative_to(
        scratch
    ):
        raise ValueError("output must be liquidation public evidence or workspace scratch")
    if output.exists():
        raise FileExistsError("refusing to overwrite liquidation evidence")
    return output, file_digest(SOURCE_FILE)


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
            raise IntegrityError("LIQUIDATION_PROBE_PARQUET_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in rows
        )


class RecordingCollector(LiquidationCollector):
    def __init__(self, config: AppConfig, store: DuckStore) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.frames: list[dict[str, object]] = []
        self.acknowledged = asyncio.Event()
        self.connection_attempts = 0
        super().__init__(config, self.record)

    async def record(self, row: MarketEvent) -> None:
        writing = asyncio.create_task(self.store.append([row], exact=True))
        cancelled: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(writing)
                break
            except asyncio.CancelledError as exc:
                cancelled = cancelled or exc
                if writing.done():
                    writing.result()
                    break
        self.rows.append(row)
        if row.kind == "feed_health" and row.payload()["reason"] == "SUBSCRIPTION_ACKNOWLEDGED":
            self.acknowledged.set()
        if cancelled is not None:
            raise cancelled

    async def frame_received(self, frame: str | bytes, received: int) -> None:
        if len(self.frames) >= 10000:
            raise IntegrityError("LIQUIDATION_PROBE_FRAME_BUDGET")
        body = frame.encode("utf-8") if isinstance(frame, str) else frame
        self.frames.append(
            {
                "body_hex": body.hex(),
                "sha256": hashlib.sha256(body).hexdigest(),
                "received_at_ms": received,
                "bytes": len(body),
            }
        )

    async def run(self) -> None:
        self.connection_attempts += 1
        if self.connection_attempts != 1:
            raise IntegrityError("LIQUIDATION_PROBE_CONNECTION_BUDGET")
        await super().run()


def independent_native(frames: list[dict[str, object]], rows: list[MarketEvent]) -> int:
    """Check raw transport payload and actual first receipt without using the native parser."""
    native = []
    for frame in frames:
        encoded = bytes.fromhex(str(frame["body_hex"]))
        if hashlib.sha256(encoded).hexdigest() != frame["sha256"] or len(encoded) != frame["bytes"]:
            raise IntegrityError("LIQUIDATION_PROBE_RAW_FRAME_DIGEST")
        parsed = json.loads(encoded)
        if isinstance(parsed, dict) and set(parsed) == {"stream", "data"}:
            native.append((parsed["data"], frame["received_at_ms"]))
    observations = [row for row in rows if row.kind == "liquidation"]
    for row in observations:
        matches = [receipt for body, receipt in native if body == row.payload()["native"]]
        if not matches or min(int(str(receipt)) for receipt in matches) != row.observed_at_ms:
            raise IntegrityError("LIQUIDATION_PROBE_RAW_NATIVE_OR_FIRST_RECEIPT")
    return len(observations)


async def verify(
    output: Path,
    *,
    duration_s: float = 20,
    transport_provenance: Literal["public_websocket", "explicit_mock_fixture"] = "public_websocket",
) -> dict[str, object]:
    if transport_provenance not in ("public_websocket", "explicit_mock_fixture"):
        raise ValueError("explicit transport provenance required")
    output, before_hash = await complete_thread(preflight, output, duration_s)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig(ingestion={"liquidations_enabled": True})
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(config, store)
    task = asyncio.create_task(collector.run())
    error_type: str | None = None
    pre_stop_cutoff = 0
    pre_stop_rows: tuple[MarketEvent, ...] = ()
    try:
        done, _pending = await asyncio.wait({task}, timeout=max(0.01, duration_s - 2))
        if done:
            try:
                await task
            except (OSError, ValueError, IntegrityError, TimeoutError) as exc:
                error_type = type(exc).__name__
            except Exception as exc:
                # Record transport exception class only; no credentials or unsolicited detail.
                error_type = type(exc).__name__
        stop_requested_at_ms = now_ms()
        # A historical cutoff, not an event clock rewrite: stop health may share
        # the request's millisecond, so preserve the preceding instant explicitly.
        pre_stop_cutoff = stop_requested_at_ms - 1
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        final_cutoff = now_ms()
        pre_stop_rows = tuple(
            row
            for row in collector.rows
            if max(row.event_time_ms, row.observed_at_ms) <= pre_stop_cutoff
        )
        native_count = independent_native(collector.frames, collector.rows)
        views = {}
        stopped_views = {}
        pit_checks = 0
        for asset in config.system.assets:
            current = await store.snapshot(asset, final_cutoff, "public_reference")
            expected = {row for row in collector.rows if row.asset == asset}
            if set(current.rows()) != expected:
                raise IntegrityError("LIQUIDATION_PROBE_PIT_ROWS_MISMATCH")
            views[asset] = liquidations_quality(
                pre_stop_rows, asset=asset, as_of_ms=pre_stop_cutoff
            )
            if views[asset] != await store.liquidations_quality(asset, as_of_ms=pre_stop_cutoff):
                raise IntegrityError("LIQUIDATION_PROBE_PRE_STOP_BOUNDED_QUALITY")
            stopped_views[asset] = await store.liquidations_quality(asset, as_of_ms=final_cutoff)
            if stopped_views[asset] != liquidations_quality(
                collector.rows, asset=asset, as_of_ms=final_cutoff
            ):
                raise IntegrityError("LIQUIDATION_PROBE_BOUNDED_QUALITY_MISMATCH")
        for row in collector.rows:
            boundary = max(row.event_time_ms, row.observed_at_ms)
            for cutoff, visible in ((boundary - 1, False), (boundary, True)):
                snapshot = await store.snapshot(row.asset, cutoff, "public_reference")
                if (row.event_id in {item.event_id for item in snapshot.rows()}) != visible:
                    raise IntegrityError("LIQUIDATION_PROBE_PIT_BOUNDARY")
                pit_checks += 1
        await store.append(collector.rows, exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        if (
            count != len({row.event_id for row in collector.rows})
            or await store.pending_delivery_count() != count
        ):
            raise IntegrityError("LIQUIDATION_PROBE_ARCHIVE_OUTBOX_COUNT")
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await store.close()
    saved = await complete_thread(closed_rows, output / "events.duckdb", output / "events.parquet")
    if set(saved) != set(collector.rows):
        raise IntegrityError("LIQUIDATION_PROBE_CLOSED_RELOAD_MISMATCH")
    reopened = await DuckStore.open(output / "events.duckdb")
    try:
        for asset in config.system.assets:
            if await reopened.liquidations_quality(asset, as_of_ms=pre_stop_cutoff) != views[asset]:
                raise IntegrityError("LIQUIDATION_PROBE_REOPEN_PRE_STOP_QUALITY")
            if (
                await reopened.liquidations_quality(asset, as_of_ms=final_cutoff)
                != stopped_views[asset]
            ):
                raise IntegrityError("LIQUIDATION_PROBE_REOPEN_QUALITY")
        if await reopened.pending_delivery_count() != count:
            raise IntegrityError("LIQUIDATION_PROBE_REOPEN_OUTBOX")
    finally:
        await reopened.close()
    after_hash = await complete_thread(file_digest, SOURCE_FILE)
    if before_hash != after_hash:
        raise IntegrityError("LIQUIDATION_PROBE_SOURCE_CHANGED")
    observed = all(view["state"] == "LOSSY_STREAM_SESSION_OBSERVED" for view in views.values())
    report: dict[str, object] = {
        "version": __version__,
        "status": (
            "PUBLIC_LOSSY_STREAM_SESSION_OBSERVED"
            if transport_provenance == "public_websocket"
            else "FIXTURE_LOSSY_STREAM_SESSION_OBSERVED"
        )
        if observed
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "source": SOURCE,
        "source_url": URL,
        "transport_provenance": transport_provenance,
        "real_public_connection_attempts": collector.connection_attempts
        if transport_provenance == "public_websocket"
        else 0,
        "connection_attempts": collector.connection_attempts,
        "subscription_attempts": collector.subscription_attempts,
        "retries": 0,
        "duration_s": duration_s,
        "subscription_ack_observed": collector.acknowledged.is_set(),
        "transport_failure_class": error_type,
        "pre_stop_as_of_ms": pre_stop_cutoff,
        "stop_requested_at_ms": stop_requested_at_ms,
        "pre_stop_lookback_ms": 1,
        "final_as_of_ms": final_cutoff,
        "quality_before_stop": views,
        "quality_after_stop": stopped_views,
        "raw_frames": len(collector.frames),
        "native_observations": native_count,
        "native_entity_comparisons": native_count,
        "persisted_rows": count,
        "pit_boundary_checks": pit_checks,
        "parquet_exact_match": True,
        "parquet_sha256": manifest.sha256,
        "closed_reload_exact_match": True,
        "outbox_pending_after_reopen": count,
        "redis_delivery_claim": False,
        "source_code_sha256_before": before_hash,
        "source_code_sha256_after": after_hash,
        "source_code_unchanged": True,
        "source_references": REFERENCES,
        "tls_verification": "default; no override",
        "no_snapshot_means_zero_liquidations": False,
        "continuous_history_verified": False,
        "complete_feed_available": False,
        "milestone_2_complete": False,
        "trading_approval": False,
        "orders_submitted": 0,
    }
    await asyncio.to_thread(
        (output / "frames.json").write_text,
        json.dumps(collector.frames, indent=2) + "\n",
        encoding="utf-8",
    )
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/liquidations-public-01")
    parser.add_argument("--duration-s", type=float, default=20)
    args = parser.parse_args()
    report = asyncio.run(verify(args.output, duration_s=args.duration_s))
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "real_public_connection_attempts",
                    "subscription_attempts",
                    "native_observations",
                    "persisted_rows",
                    "source_code_unchanged",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
