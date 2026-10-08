"""Finite public BTC stream test with one deliberately omitted trade; cannot place orders."""

import argparse
import asyncio
import json
import math
from pathlib import Path

from pydantic import JsonValue
from websockets.asyncio.client import ClientConnection

from selective_options import __version__
from selective_options.archive import seal_archive
from selective_options.bus import EventBus, HotBuffer
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError, TradeGap
from selective_options.events import MarketEvent
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.socket_recovery import socket_gap_handler
from selective_options.spot_trades import Frame, KrakenTradeMessage, SpotTradeCollector, trade_event
from selective_options.store import DuckStore


class ControlledCollector(SpotTradeCollector):
    omitted: MarketEvent | None = None
    recovered_through: int | None = None

    def __init__(self, config: AppConfig, bus: EventBus) -> None:
        super().__init__(config, bus.publish)
        self.verified = asyncio.Event()

    async def _read(self, socket: ClientConnection) -> dict[str, JsonValue]:
        message = await super()._read(socket)
        if (
            self.omitted is None
            and not self.repairing
            and message.get("channel") == "trade"
            and message.get("type") == "update"
        ):
            parsed = KrakenTradeMessage.model_validate(message)
            for index, trade in enumerate(parsed.data):
                sequence = self.sequences.get(trade.symbol)
                if (
                    sequence is not None
                    and sequence.last is not None
                    and trade.trade_id == sequence.last + 1
                ):
                    self.omitted = trade_event(trade, self.symbols[trade.symbol], now_ms())
                    rows: list[JsonValue] = [
                        row.model_dump(mode="json")
                        for i, row in enumerate(parsed.data)
                        if i != index
                    ]
                    return (
                        {"channel": "trade", "type": "update", "data": rows}
                        if rows
                        else {"channel": "heartbeat"}
                    )
        return message

    async def _trades(self, message: dict[str, JsonValue], received: int) -> None:
        await super()._trades(message, received)
        sequence = self.sequences.get("BTC/USD")
        if (
            self.recovered_through is not None
            and sequence is not None
            and sequence.last is not None
            and sequence.last > self.recovered_through
        ):
            self.verified.set()


async def verify(output: Path, duration: float) -> None:
    if not math.isfinite(duration) or not 1 <= duration <= 300:
        raise ValueError("duration must be between one and 300 seconds")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {"kraken_auto_recovery_enabled": True, "flush_interval_s": 0.05},
        }
    )
    bus = EventBus(config.ingestion.queue_capacity, config.ingestion.queue_timeout_s)
    store = await DuckStore.open(output / "events.duckdb")
    stopped = asyncio.Event()
    collector = ControlledCollector(config, bus)
    handler = socket_gap_handler(collector, bus, store, output / "recovery")

    async def repair(
        socket: ClientConnection, message: dict[str, JsonValue], received: int, gap: TradeGap
    ) -> list[Frame]:
        frames = await handler(socket, message, received, gap)
        collector.recovered_through = collector.sequences[gap.symbol].last
        return frames

    collector.gap_handler = repair
    writer = asyncio.create_task(
        consume(config, bus, store, HotBuffer(1000), FreshnessMonitor(5000), stopped, None)
    )
    stream = asyncio.create_task(collector.run())
    complete = asyncio.create_task(collector.verified.wait())
    started = now_ms()
    failure: BaseException | None = None
    try:
        done, _ = await asyncio.wait(
            [writer, stream, complete], timeout=duration, return_when=asyncio.FIRST_COMPLETED
        )
        for task in (writer, stream):
            if task in done:
                task.result()
                raise IntegrityError("LIVE_TEST_WORKER_EXITED")
        if complete not in done:
            raise IntegrityError("LIVE_TEST_DEADLINE_WITHOUT_VERIFIED_SUCCESSOR")
    except BaseException as exc:
        failure = exc
    finally:
        stream.cancel()
        complete.cancel()
        await asyncio.gather(stream, complete, return_exceptions=True)
        stopped.set()
        try:
            await writer
        except BaseException as exc:
            failure = failure or exc
    try:
        ended = now_ms()
        snapshot = await store.snapshot("BTC", ended, "public_reference")
        omitted_id = collector.omitted.payload()["trade_id"] if collector.omitted else None
        recovered = [
            row
            for row in snapshot.rows(source="kraken_rest", kind="trade")
            if row.payload()["trade_id"] == omitted_id
        ]
        if failure is None and len(recovered) != 1:
            failure = IntegrityError("LIVE_TEST_OMISSION_NOT_RECOVERED_FROM_REST")
        if collector.omitted:
            await asyncio.to_thread(
                (output / "intentionally-omitted.json").write_text,
                collector.omitted.model_dump_json(indent=2),
                encoding="utf-8",
            )
        rows = await store.export_parquet(output / "events.parquet")
        archive = await seal_archive(output / "events.parquet", rows)
        report = {
            "build_version": __version__,
            "status": "FAILED" if failure else "PUBLIC_GAP_TEST_PASSED",
            "evidence_type": "live public Kraken BTC stream plus REST; one trade intentionally omitted from collector input",
            "started_at_ms": started,
            "ended_at_ms": ended,
            "deadline_s": duration,
            "rows": rows,
            "counts": await store.counts(),
            "omitted_trade_id": omitted_id,
            "omission_recovered_from_rest": len(recovered) == 1,
            "recovered_through_id": collector.recovered_through,
            "post_repair_live_successor_verified": collector.verified.is_set(),
            "pending_repair_books": sorted(collector.repairing),
            "failure_type": type(failure).__name__ if failure else None,
            "failure_reason": str(failure) if isinstance(failure, IntegrityError) else None,
            "redis": "NOT_CONFIGURED",
            "orders_submitted": 0,
            "trading_enabled": False,
            "paper_trading_started": False,
            "parquet_sha256": archive.sha256,
        }
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2))
    finally:
        await store.close()
    if failure:
        raise failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=90)
    args = parser.parse_args()
    asyncio.run(verify(args.output, args.duration))
