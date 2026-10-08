import asyncio
import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from selective_options.checkpoints import load_trade_checkpoints
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture
from selective_options.spot_trades import KrakenTrade, SpotTradeCollector, trade_event
from selective_options.store import DuckStore


def wire(identifier, **changes):
    return (
        dict(
            symbol="BTC/USD",
            side="buy",
            price=100.0,
            qty=0.1,
            trade_id=identifier,
            timestamp=datetime.fromtimestamp((300000 + identifier * 1000) / 1000, UTC).isoformat(),
        )
        | changes
    )


def event(identifier):
    return trade_event(KrakenTrade.model_validate(wire(identifier)), "BTC", 500000)


def message(*identifiers, kind="snapshot"):
    return {"channel": "trade", "type": kind, "data": [wire(i) for i in identifiers]}


async def save(path, rows):
    store = await DuckStore.open(path)
    await store.append(rows)
    await store.close()


async def test_bounded_committed_tail_read_only_and_exact_observation(tmp_path):
    path = tmp_path / "prior.duckdb"
    rows = [event(i) for i in range(1, 101)]
    await save(path, rows)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    tail = await load_trade_checkpoints(path, ("BTC",))
    assert tail == tuple(rows[-50:])
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert all(row.observed_at_ms == 500000 for row in tail)


@pytest.mark.parametrize(
    "rows,error",
    [
        ([], "NO_KRAKEN"),
        ([event(1), event(3)], "SEQUENCE_GAP"),
        ([event(1).model_copy(update={"event_id": "corrupt"})], "IDENTITY_MISMATCH"),
    ],
)
async def test_checkpoint_rejects_empty_gap_or_corruption(tmp_path, rows, error):
    path = tmp_path / "prior.duckdb"
    await save(path, rows)
    with pytest.raises(IntegrityError, match=error):
        await load_trade_checkpoints(path, ("BTC",))


async def test_checkpoint_missing_file_does_not_create_it(tmp_path):
    path = tmp_path / "absent.duckdb"
    with pytest.raises(FileNotFoundError):
        await load_trade_checkpoints(path, ("BTC",))
    assert not path.exists()


async def test_restart_overlap_new_trades_and_point_in_time(tmp_path):
    path = tmp_path / "prior.duckdb"
    await save(path, [event(2), event(3)])
    tail = await load_trade_checkpoints(path, ("BTC",))
    store = await DuckStore.open(tmp_path / "next.duckdb")
    await store.append(list(tail))

    async def sink(row):
        await store.append([row])

    collector = SpotTradeCollector(AppConfig(), sink, tail)
    collector.symbols = {"BTC/USD": "BTC"}
    # Earlier unknown prefix is skipped, known overlap deduplicated, gap 4..5 recovered.
    await collector._trades(message(1, 2, 3, 4, 5), 600000)
    await collector._flush(605000)
    before = await store.snapshot("BTC", 599999, "public_reference")
    after = await store.snapshot("BTC", 600000, "public_reference")
    assert [r.payload()["trade_id"] for r in before.rows(kind="trade")] == [2, 3]
    assert [r.payload()["trade_id"] for r in after.rows(kind="trade")] == [2, 3, 4, 5]
    assert [r.observed_at_ms for r in after.rows(kind="trade")] == [500000, 500000, 600000, 600000]
    assert all(
        not row.payload()["complete"]
        for row in (await store.snapshot("BTC", 605000, "public_reference")).rows(kind="bar")
    )
    assert not collector.awaiting_snapshot
    await store.close()


@pytest.mark.parametrize(
    "data,error",
    [
        (message(4, 6), "SEQUENCE_GAP"),
        (message(54, 55), "SEQUENCE_GAP"),
        (message(1, 2), "STALE_RECOVERY"),
        (message(4, kind="update"), "UPDATE_BEFORE"),
    ],
)
async def test_bad_recovery_emits_no_partial_batch_and_keeps_checkpoint(data, error):
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink, (event(3),))
    collector.symbols = {"BTC/USD": "BTC"}
    with pytest.raises(IntegrityError, match=error):
        await collector._trades(data, 600000)
    sink.assert_not_awaited()
    assert collector.sequences["BTC/USD"].last == 3
    assert collector.awaiting_snapshot == {"BTC/USD"}


async def test_conflicting_overlap_is_not_silently_deduplicated():
    collector = SpotTradeCollector(AppConfig(), AsyncMock(), (event(3),))
    collector.symbols = {"BTC/USD": "BTC"}
    data = message(3, 4)
    data["data"][0]["price"] = 999
    with pytest.raises(IntegrityError, match="CONFLICTING"):
        await collector._trades(data, 600000)


async def test_no_overlap_but_exact_next_trade_is_recoverable():
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink, (event(3),))
    collector.symbols = {"BTC/USD": "BTC"}
    await collector._trades(message(*range(4, 54)), 600000)
    assert collector.sequences["BTC/USD"].last == 53
    assert sum(call.args[0].kind == "trade" for call in sink.await_args_list) == 50


async def test_checkpoint_survives_abrupt_exit_but_not_uncommitted_tail(tmp_path):
    path = tmp_path / "crashed.duckdb"
    script = """
import asyncio,os,sys,json
from pathlib import Path
from selective_options.store import DuckStore
from selective_options.events import MarketEvent
async def main():
 s=await DuckStore.open(Path(sys.argv[1]))
 await s.append([MarketEvent.model_validate_json(sys.argv[2])])
 s._connection.execute('BEGIN TRANSACTION')
 s._connection.execute("DELETE FROM events")
 os._exit(23)
asyncio.run(main())
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script, str(path), event(3).model_dump_json()],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 23, result.stderr.decode()
    assert await load_trade_checkpoints(path, ("BTC",)) == (event(3),)


async def test_capture_seeds_checkpoints_and_reports_partial_scope(tmp_path, monkeypatch):
    async def idle(*args):
        await asyncio.Event().wait()

    for name in ("DeribitCollector.run", "poll_chains", "SpotTradeCollector.run"):
        monkeypatch.setattr("selective_options.ingestion." + name, idle)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "derivatives_enabled": False,
                "reference_sources": [],
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    source = tmp_path / "prior.duckdb"
    await save(source, [event(3)])
    report = await capture(config, tmp_path / "new", 0.01, source)
    assert report["trade_checkpoint_seed_rows"] == report["rows"] == 1
    assert report["trade_checkpoint_event_ids"] == [event(3).event_id]
    assert report["trade_recovery_pending_symbols"] == ["BTC/USD"]
    assert report["milestone_2_complete"] is report["trading_enabled"] is False
    assert await load_trade_checkpoints(tmp_path / "new/events.duckdb", ("BTC",)) == (event(3),)
    disabled = config.model_copy(
        update={"ingestion": config.ingestion.model_copy(update={"spot_trades_enabled": False})}
    )
    with pytest.raises(ValueError, match="requires spot trades"):
        await capture(disabled, tmp_path / "disabled", 0.01, source)
    assert not (tmp_path / "disabled").exists()


def test_cli_threads_checkpoint_path_to_capture(monkeypatch, tmp_path):
    from selective_options.cli import main

    checkpoint = tmp_path / "old.duckdb"
    output = tmp_path / "new"
    mocked = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", mocked)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(output),
            "--duration",
            "1",
            "--resume-trades-from",
            str(checkpoint),
        ],
    )
    assert main() == 0
    assert mocked.await_args.args[1:] == (output, 1.0, checkpoint)


async def test_halted_recovery_records_exact_reason(monkeypatch):
    sink = AsyncMock()

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("selective_options.spot_trades.connect", lambda *a, **k: Socket())
    collector = SpotTradeCollector(AppConfig(), sink)
    monkeypatch.setattr(
        collector,
        "_discover",
        AsyncMock(side_effect=IntegrityError("CHECKPOINT_MARKET_UNAVAILABLE")),
    )
    with pytest.raises(IntegrityError, match="CHECKPOINT_MARKET_UNAVAILABLE"):
        await collector.run()
    assert sink.await_args.args[0].payload() == {
        "reason": "SPOT_TRADE_HALTED",
        "detail": "CHECKPOINT_MARKET_UNAVAILABLE",
    }


async def test_disconnect_blocks_bar_flush_until_snapshot_recovery(monkeypatch):
    sink, ready = AsyncMock(), asyncio.Event()
    sockets = []

    class Socket:
        def __init__(self):
            sockets.append(self)
            ack = {
                "method": "subscribe",
                "req_id": 2,
                "success": True,
                "result": {"channel": "trade", "symbol": "BTC/USD"},
            }
            self.messages = (
                [ack, message(3)]
                if len(sockets) == 1
                else [ack, {"channel": "heartbeat"}, message(3, 4, 5)]
            )

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            if self.messages:
                return json.dumps(self.messages.pop(0))
            if len(sockets) == 1:
                raise OSError("forced interruption")
            ready.set()
            await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.spot_trades.connect", lambda *a, **k: Socket())
    monkeypatch.setattr(
        "selective_options.spot_trades.now_ms", lambda: 303000 if len(sockets) == 1 else 605000
    )
    config = AppConfig.model_validate({"ingestion": {"reconnect_delay_s": 0.001}})
    collector = SpotTradeCollector(config, sink)

    async def discover(socket):
        collector.symbols = {"BTC/USD": "BTC"}

    monkeypatch.setattr(collector, "_discover", discover)
    task = asyncio.create_task(collector.run())
    try:
        await asyncio.wait_for(ready.wait(), 3)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    rows = [call.args[0] for call in sink.await_args_list]
    assert [row.payload()["trade_id"] for row in rows if row.kind == "trade"] == [3, 4, 5]
    bar = next(row for row in rows if row.kind == "bar")
    assert bar.payload()["trade_count"] == 3 and bar.payload()["complete"] is False
    assert any(row.payload().get("reason") == "TRADE_SEQUENCE_RECOVERED" for row in rows)
