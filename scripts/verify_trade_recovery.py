"""Finite public Kraken stop/restart experiment; no private API or orders."""

import argparse
import asyncio
import json
from pathlib import Path

from selective_options.archive import seal_archive
from selective_options.checkpoints import load_trade_checkpoints
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.spot_trades import SpotTradeCollector
from selective_options.store import DuckStore


async def collect(path: Path, seeds: tuple[MarketEvent, ...]) -> list[MarketEvent]:
    await asyncio.to_thread(path.mkdir)
    store = await DuckStore.open(path / "events.duckdb")
    rows: list[MarketEvent] = []
    ready = asyncio.Event()
    initialized: set[str] = set()

    async def sink(event: MarketEvent) -> None:
        rows.append(event)
        payload = event.payload()
        if payload.get("reason") in {"TRADE_SNAPSHOT_INITIALIZED", "TRADE_SEQUENCE_RECOVERED"}:
            symbol = payload["symbol"]
            assert isinstance(symbol, str)
            initialized.add(symbol)
            if initialized == set(collector.symbols):
                ready.set()

    collector = SpotTradeCollector(AppConfig(), sink, seeds)
    task = asyncio.create_task(collector.run())
    waiter = asyncio.create_task(ready.wait())
    try:
        done, _ = await asyncio.wait(
            [task, waiter], timeout=30, return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            task.result()
        if waiter not in done:
            raise IntegrityError("RECOVERY_EXPERIMENT_SNAPSHOT_TIMEOUT")
    finally:
        task.cancel()
        waiter.cancel()
        await asyncio.gather(task, waiter, return_exceptions=True)
        try:
            # This experiment commits each finite session as a batch. Abrupt-exit
            # checkpoint behavior is tested separately against the real store.
            await store.append([*seeds, *rows])
            count = await store.export_parquet(path / "events.parquet")
            await seal_archive(path / "events.parquet", count)
        finally:
            await store.close()
    return rows


async def verify(output: Path) -> None:
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    started = now_ms()
    report: dict[str, object] = {"started_at_ms": started, "trading_enabled": False}
    try:
        await collect(output / "before", ())
        seeds = await load_trade_checkpoints(
            output / "before/events.duckdb", AppConfig().system.assets
        )
        await asyncio.sleep(1)
        resumed_at = now_ms()
        rows = await collect(output / "after", seeds)
        recovered = [row for row in rows if row.kind == "trade"]
        assert all(row.observed_at_ms >= resumed_at for row in recovered)
        store = await DuckStore.open(output / "after/events.duckdb")
        try:
            for asset in AppConfig().system.assets:
                before = (await store.snapshot(asset, resumed_at - 1, "public_reference")).rows(
                    kind="trade"
                )
                assert {row.event_id for row in before} == {
                    row.event_id for row in seeds if row.asset == asset
                }
        finally:
            await store.close()
        report.update(
            status="PASSED",
            checkpoint_rows=len(seeds),
            recovered_trades=len(recovered),
            resumed_at_ms=resumed_at,
            observation_times_preserved=True,
            recovered_trades_hidden_before_receipt=True,
            recovery_status=[
                row.payload()
                for row in rows
                if row.payload().get("reason") == "TRADE_SEQUENCE_RECOVERED"
            ],
        )
    except BaseException as exc:
        report.update(status="FAILED", failure_type=type(exc).__name__)
        raise
    finally:
        report["ended_at_ms"] = now_ms()
        await asyncio.to_thread(
            (output / "verification.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(verify(parser.parse_args().output))
