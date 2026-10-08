import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from test_recovery_runtime import running
from test_trade_backfill import body
from test_trade_checkpoints import message, wire
from test_trade_reconciliation import ws

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError, TradeGap
from selective_options.ingestion import capture
from selective_options.socket_recovery import FrameRouter, socket_gap_handler
from selective_options.spot_trades import SpotTradeCollector


def router(**limits):
    collector = SpotTradeCollector(AppConfig(), AsyncMock(), (ws(),))
    collector.symbols = {"BTC/USD": "BTC", "ETH/USD": "ETH"}
    return FrameRouter(collector, ws(), **limits)


def test_multi_book_frames_preserve_unconsumed_rows_and_original_receipts():
    route = router()
    route.add(
        {
            "channel": "trade",
            "type": "update",
            "data": [wire(5), wire(20, symbol="ETH/USD"), wire(6)],
        },
        600000,
    )
    assert route.take().payload()["trade_id"] == 5
    route.add(message(7, 8, kind="update"), 610000)
    assert route.take().payload()["trade_id"] == 6
    assert route.take().payload()["trade_id"] == 7
    deferred = route.deferred()
    assert [frame[1] for frame in deferred] == [600000, 610000]
    assert [frame[0]["data"][0]["trade_id"] for frame in deferred] == [20, 8]


@pytest.mark.parametrize("limits", [{"max_frames": 0}, {"max_bytes": 1}, {"max_trades": 1}])
def test_buffer_limits_fail_without_silent_drop(limits):
    with pytest.raises(IntegrityError, match="BUFFER_OVERFLOW"):
        router(**limits).add(message(4, 5), 600000)


@pytest.mark.parametrize(
    "frame,error",
    [
        ({"method": "subscribe", "success": True}, "unexpected"),
        (
            {
                "channel": "instrument",
                "data": {
                    "pairs": [
                        {"symbol": "BTC/USD", "base": "BTC", "quote": "USD", "status": "offline"}
                    ]
                },
            },
            "no longer online",
        ),
        ({"channel": "trade", "type": "update", "data": [wire(3, price=101.0)]}, "CONFLICTING"),
        (
            {"channel": "trade", "type": "update", "data": [wire(4, symbol="UNKNOWN/USD")]},
            "INVALID_TRADE",
        ),
        (
            {
                "channel": "trade",
                "type": "update",
                "data": [wire(4, timestamp="2099-01-01T00:00:00Z")],
            },
            "CLOCK_SKEW",
        ),
    ],
)
def test_invalid_controls_and_trades_fail(frame, error):
    with pytest.raises(IntegrityError, match=error):
        router().add(frame, 600000)


def test_known_snapshot_prefix_and_control_frames_do_not_become_new_trades():
    route = router()
    for frame in (
        {"channel": "heartbeat"},
        {"channel": "status"},
        {"channel": "instrument", "data": {"pairs": []}},
        message(1, 2, 3, 5),
    ):
        route.add(frame, 600000)
    assert route.take().payload()["trade_id"] == 5
    assert not route.ready and not route.deferred()


async def test_handler_buffers_other_book_and_remaining_target_batch(tmp_path, monkeypatch):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        entered = asyncio.Event()

        class Socket:
            async def recv(self):
                entered.set()
                return json.dumps(
                    {
                        "channel": "trade",
                        "type": "update",
                        "data": [wire(6), wire(20, symbol="ETH/USD"), wire(7)],
                    }
                )

        calls = 0
        drained = asyncio.Event()

        async def read(socket):
            nonlocal calls
            calls += 1
            if calls == 1:
                return json.loads(await socket.recv())
            drained.set()
            await asyncio.Event().wait()

        async def fetch(*args):
            await entered.wait()
            # Wait until both target records have actually reached the recovery buffer.
            await drained.wait()
            return body([3, 4, 5]), 600000

        monkeypatch.setattr(collector, "_read", read)
        monkeypatch.setattr("selective_options.socket_recovery.now_ms", lambda: 560000)
        monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
        repair = socket_gap_handler(collector, bus, store, tmp_path / "repairs")
        deferred = await repair(
            Socket(), message(5, kind="update"), 550000, TradeGap("BTC/USD", 4, 5)
        )
        assert collector.sequences["BTC/USD"].last == 7 and not collector.repairing
        assert len(deferred) == 1 and deferred[0][1] == 560000
        assert deferred[0][0]["data"][0]["symbol"] == "ETH/USD"
        await collector._trades(*deferred[0])
        assert collector.sequences["ETH/USD"].last == 20


@pytest.mark.parametrize("failure", ["disconnect", "cancel", "offline"])
async def test_handler_failure_does_not_activate_and_joins_fetch(tmp_path, monkeypatch, failure):
    async with running(tmp_path) as (collector, bus, store, _, _):
        await collector._trades(message(3), 500000)
        fetch_started, fetch_stopped = asyncio.Event(), asyncio.Event()

        async def fetch(*args):
            fetch_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                fetch_stopped.set()

        async def read(socket):
            await fetch_started.wait()
            if failure == "disconnect":
                raise OSError("forced disconnect")
            if failure == "offline":
                return {
                    "channel": "instrument",
                    "data": {
                        "pairs": [
                            {
                                "symbol": "BTC/USD",
                                "base": "BTC",
                                "quote": "USD",
                                "status": "offline",
                            }
                        ]
                    },
                }
            await asyncio.Event().wait()

        monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
        monkeypatch.setattr(collector, "_read", read)
        repair = socket_gap_handler(collector, bus, store, tmp_path / "repairs")
        task = asyncio.create_task(repair(None, message(5), 550000, TradeGap("BTC/USD", 4, 5)))
        await fetch_started.wait()
        if failure == "cancel":
            task.cancel()
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else IntegrityError):
            await task
        assert fetch_stopped.is_set()
        assert collector.sequences["BTC/USD"].last == 3 and collector.repairing == {"BTC/USD"}


async def test_actual_run_loop_invokes_repair_and_processes_deferred_frames(tmp_path, monkeypatch):
    async with running(tmp_path) as (collector, bus, store, _, _):
        done = asyncio.Event()

        class Socket:
            def __init__(self):
                self.frames = [
                    {
                        "method": "subscribe",
                        "req_id": 2,
                        "success": True,
                        "result": {"channel": "trade", "symbol": "BTC/USD"},
                    },
                    message(3),
                    message(5, kind="update"),
                    message(6, kind="update"),
                ]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, raw):
                pass

            async def recv(self):
                if self.frames:
                    return json.dumps(self.frames.pop(0))
                done.set()
                await asyncio.Event().wait()

        async def discover(socket):
            collector.symbols = {"BTC/USD": "BTC"}

        async def fetch(*args):
            await done.wait()
            return body([3, 4, 5]), 600000

        monkeypatch.setattr("selective_options.spot_trades.connect", lambda *a, **k: Socket())
        monkeypatch.setattr("selective_options.spot_trades.now_ms", lambda: 500000)
        monkeypatch.setattr("selective_options.socket_recovery.now_ms", lambda: 550000)
        monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
        monkeypatch.setattr(collector, "_discover", discover)
        activated = asyncio.Event()
        handler = socket_gap_handler(collector, bus, store, tmp_path / "repairs")

        async def repair(*args):
            result = await handler(*args)
            activated.set()
            return result

        collector.gap_handler = repair
        task = asyncio.create_task(collector.run())
        try:
            await asyncio.wait_for(activated.wait(), 5)
            assert collector.sequences["BTC/USD"].last == 6
            assert not collector.repairing
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_capture_opt_in_installs_socket_handler(tmp_path, monkeypatch):
    installed = asyncio.Event()

    async def idle(*args):
        await asyncio.Event().wait()

    async def run(collector):
        assert collector.gap_handler is not None
        installed.set()
        await idle()

    for name in ("DeribitCollector.run", "poll_chains"):
        monkeypatch.setattr("selective_options.ingestion." + name, idle)
    monkeypatch.setattr("selective_options.ingestion.SpotTradeCollector.run", run)
    config = AppConfig.model_validate(
        {
            "ingestion": {
                "kraken_auto_recovery_enabled": True,
                "derivatives_enabled": False,
                "reference_sources": [],
                "policy_news_sources": [],
                "flush_interval_s": 0.01,
            }
        }
    )
    report = await capture(config, tmp_path / "capture", 0.05)
    assert installed.is_set() and report["automatic_kraken_recovery_enabled"] is True
    assert report["trading_enabled"] is False
