"""Finite lifecycle integration with gated workers and mocked transport/signals."""

import asyncio
import json
import signal
import threading
from datetime import UTC, datetime

import duckdb
import pytest

from selective_options import active_rotation, capture_lifecycle, cli, ingestion
from selective_options.bar_checkpoint import load_bar_checkpoint
from selective_options.bus import EventBus
from selective_options.capture_lifecycle import CaptureStopRequest, scoped_sigterm_stop
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.store import DuckStore


def config(**changes):
    return AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
            **changes,
        },
    )


async def idle(*args):
    await asyncio.Event().wait()


def fixture_event():
    return make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=1000,
        observed_at_ms=2000,
        payload={"price": 100},
    )


def disable_transport(monkeypatch, producer=idle):
    monkeypatch.setattr(ingestion.DeribitCollector, "run", producer)
    monkeypatch.setattr(ingestion, "poll_chains", idle)
    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", idle)


@pytest.fixture
def signal_contract(monkeypatch):
    """Use controlled SIGTERM registration on all platforms, never send OS signals."""
    previous = object()
    state = {"handler": previous, "calls": []}
    original_get = signal.getsignal
    original_set = signal.signal

    def get(signum):
        return state["handler"] if signum == signal.SIGTERM else original_get(signum)

    def install(signum, handler):
        if signum != signal.SIGTERM:
            return original_set(signum, handler)
        old = state["handler"]
        state["calls"].append((signum, handler))
        state["handler"] = handler
        return old

    monkeypatch.setattr(capture_lifecycle, "_supports_sigterm", lambda: True)
    monkeypatch.setattr(signal, "getsignal", get)
    monkeypatch.setattr(signal, "signal", install)
    state["previous"] = previous
    return state


async def test_request_first_reason_wins_and_reuse_refuses_without_output(tmp_path, monkeypatch):
    request = CaptureStopRequest()

    async def produce(self):
        await self.sink(fixture_event())
        request.request()
        request.request("SIGTERM")
        await idle()

    disable_transport(monkeypatch, produce)
    report = await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 1
    assert report["capture_stop"] == {
        "reason": "STOP_REQUESTED",
        "trigger": "STOP_REQUESTED",
        "request_reason": "OPERATOR",
        "request_enabled": True,
        "signal_handling": "NOT_REQUESTED",
        "periodic_crash_recovery": False,
    }
    assert report["redis_delivery_pending"] == 1
    assert report["milestone_2_complete"] is report["ready_for_signals"] is False
    assert report["orders_submitted"] == 0 and report["trading_enabled"] is False
    assert json.loads((tmp_path / "capture/report.json").read_text()) == report
    with duckdb.connect(str(tmp_path / "capture/events.duckdb"), read_only=True) as database:
        assert database.execute("SELECT count(*) FROM events").fetchone() == (1,)
    with pytest.raises(ValueError, match="cannot be reused"):
        await ingestion.capture(config(), tmp_path / "reuse", 10, stop_request=request)
    assert not (tmp_path / "reuse").exists()


@pytest.mark.parametrize("reason", ["OPERATOR", "SIGTERM"])
async def test_pre_requested_stop_never_creates_output_or_starts_transport(
    tmp_path, monkeypatch, reason
):
    started = []

    async def forbidden(*args):
        started.append(True)
        raise AssertionError("pre-requested transport started")

    disable_transport(monkeypatch, forbidden)
    request = CaptureStopRequest()
    request.request(reason)
    with pytest.raises(ValueError, match="already requested"):
        await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    assert not (tmp_path / "capture").exists() and not started


@pytest.mark.parametrize("bad", [True, asyncio.Event(), object()])
async def test_wrong_request_type_refuses_before_output(tmp_path, bad):
    with pytest.raises(ValueError, match="CaptureStopRequest"):
        await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=bad)
    assert not (tmp_path / "capture").exists()


@pytest.mark.parametrize("bad", ["UNKNOWN", "", None, True])
def test_request_reason_refuses_arbitrary_input(bad):
    request = CaptureStopRequest()
    with pytest.raises(ValueError, match="unsupported"):
        request.request(bad)
    assert not request.requested and request.reason is None


def test_bound_request_refuses_another_event_loop():
    request = CaptureStopRequest()

    async def bind():
        request.bind()

    async def request_elsewhere():
        with pytest.raises(RuntimeError, match="another event loop"):
            request.request()

    asyncio.run(bind())
    asyncio.run(request_elsewhere())
    assert not request.requested


async def test_duration_remains_finite_when_no_stop_arrives(tmp_path, monkeypatch):
    disable_transport(monkeypatch)
    report = await ingestion.capture(
        config(), tmp_path / "capture", 0.01, stop_request=CaptureStopRequest()
    )
    assert report["status"] == "CAPTURE_FINISHED"
    assert report["capture_stop"]["reason"] == "DURATION_ELAPSED"
    assert report["capture_stop"]["request_reason"] is None
    assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]


async def test_request_during_store_setup_starts_no_producers(tmp_path, monkeypatch):
    request = CaptureStopRequest()
    original_open = DuckStore.open
    started = []

    async def opening(*args, **kwargs):
        store = await original_open(*args, **kwargs)
        request.request()
        return store

    async def forbidden(*args):
        started.append(True)
        raise AssertionError("transport started after startup stop")

    monkeypatch.setattr(DuckStore, "open", opening)
    disable_transport(monkeypatch, forbidden)
    report = await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 0
    assert report["capture_stop"]["reason"] == "STOP_REQUESTED" and not started
    assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]


@pytest.mark.parametrize("successful_exit", [False, True])
async def test_tied_producer_failure_or_early_exit_overrides_stop(
    tmp_path, monkeypatch, successful_exit
):
    request = CaptureStopRequest()

    async def produce(self):
        request.request()
        if not successful_exit:
            raise IntegrityError("fixture producer failed with stop")

    disable_transport(monkeypatch, produce)
    with pytest.raises(IntegrityError):
        await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["failure_type"] == "IntegrityError"


@pytest.mark.parametrize("stop", [False, True])
async def test_producer_cleanup_failure_overrides_stop_or_deadline(tmp_path, monkeypatch, stop):
    request = CaptureStopRequest()

    async def produce(self):
        try:
            await self.sink(fixture_event())
            if stop:
                request.request()
            await idle()
        finally:
            raise IntegrityError("fixture cleanup failed")

    disable_transport(monkeypatch, produce)
    with pytest.raises(IntegrityError):
        await ingestion.capture(
            config(), tmp_path / "capture", 10 if stop else 0.01, stop_request=request
        )
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["rows"] == 1
    assert report["capture_stop"]["reason"] == "FAILED"
    assert report["capture_stop"]["trigger"] == ("STOP_REQUESTED" if stop else "DURATION_ELAPSED")


async def test_writer_drain_keeps_capture_open_until_gated_commit_finishes(tmp_path, monkeypatch):
    request = CaptureStopRequest()
    entered, released = asyncio.Event(), asyncio.Event()
    original_append = DuckStore.append
    original_close = DuckStore.close
    closed = []

    async def append(self, events, **kwargs):
        if events:
            entered.set()
            await released.wait()
        return await original_append(self, events, **kwargs)

    async def close(self):
        assert released.is_set()
        closed.append(True)
        await original_close(self)

    async def produce(self):
        await self.sink(fixture_event())
        await entered.wait()
        request.request()
        await idle()

    disable_transport(monkeypatch, produce)
    monkeypatch.setattr(DuckStore, "append", append)
    monkeypatch.setattr(DuckStore, "close", close)
    output = tmp_path / "capture"
    task = asyncio.create_task(ingestion.capture(config(), output, 10, stop_request=request))
    await asyncio.wait_for(entered.wait(), 5)
    await asyncio.sleep(0)
    assert not task.done() and not closed and not (output / "report.json").exists()
    released.set()
    report = await asyncio.wait_for(task, 5)
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 1 and closed


async def test_hard_cancel_during_cooperative_drain_remains_failed(tmp_path, monkeypatch):
    request = CaptureStopRequest()
    stopping, released = asyncio.Event(), asyncio.Event()

    async def produce(self):
        try:
            request.request()
            await idle()
        finally:
            stopping.set()
            await released.wait()

    disable_transport(monkeypatch, produce)
    output = tmp_path / "capture"
    task = asyncio.create_task(ingestion.capture(config(), output, 10, stop_request=request))
    await asyncio.wait_for(stopping.wait(), 5)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 5)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["failure_type"] == "CancelledError"
    assert report["capture_stop"]["reason"] == "FAILED"
    assert report["capture_stop"]["trigger"] == "STOP_REQUESTED"


async def test_real_handler_contract_stops_actual_capture_and_restores_prior_handler(
    tmp_path, monkeypatch, signal_contract
):
    request = CaptureStopRequest()

    async def produce(self):
        await self.sink(fixture_event())
        handler = signal_contract["handler"]
        handler(signal.SIGTERM, None)
        handler(signal.SIGTERM, None)
        assert not request.requested  # The handler schedules into the event loop.
        await idle()

    disable_transport(monkeypatch, produce)
    with scoped_sigterm_stop(request):
        report = await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
        assert signal_contract["handler"] is not signal_contract["previous"]
    assert signal_contract["handler"] is signal_contract["previous"]
    assert len(signal_contract["calls"]) == 2
    assert report["capture_stop"]["request_reason"] == "SIGTERM"
    assert report["capture_stop"]["signal_handling"] == "SCOPED_POSIX_SIGTERM"
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 1


@pytest.mark.parametrize("stage", ["startup", "producer", "report", "close"])
async def test_scoped_handler_restores_on_every_capture_failure(
    tmp_path, monkeypatch, signal_contract, stage
):
    request = CaptureStopRequest()
    disable_transport(monkeypatch)

    async def produce(self):
        request.request()
        if stage == "producer":
            raise IntegrityError("fixture producer failure")
        await idle()

    async def fail(*args, **kwargs):
        raise IntegrityError("fixture boundary failure")

    original_close = DuckStore.close

    async def fail_close(self):
        await original_close(self)
        raise IntegrityError("fixture close failure")

    monkeypatch.setattr(ingestion.DeribitCollector, "run", produce)
    if stage == "startup":
        monkeypatch.setattr(DuckStore, "open", fail)
    elif stage == "report":
        monkeypatch.setattr(ingestion, "seal_archive", fail)
    elif stage == "close":
        monkeypatch.setattr(DuckStore, "close", fail_close)
    with pytest.raises(IntegrityError), scoped_sigterm_stop(request):
        await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    assert signal_contract["handler"] is signal_contract["previous"]
    if stage == "close":
        report = json.loads((tmp_path / "capture/report.json").read_text())
        assert report["status"] == "FAILED" and report["capture_stop"]["reason"] == "FAILED"


async def test_mirror_close_failure_after_store_close_publishes_failed_report(
    tmp_path, monkeypatch
):
    request = CaptureStopRequest()
    store_closed = []
    original_close = DuckStore.close

    class Mirror:
        def __init__(self, *args):
            pass

        async def close(self):
            assert store_closed == [True]
            raise IntegrityError("fixture mirror close failure")

    async def close(self):
        await original_close(self)
        store_closed.append(self._closed)

    async def produce(self):
        request.request()
        await idle()

    disable_transport(monkeypatch, produce)
    monkeypatch.setattr(ingestion, "RedisMirror", Mirror)
    monkeypatch.setattr(DuckStore, "close", close)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="mirror close failure"):
        await ingestion.capture(
            config(redis_url="redis://localhost:6379"), output, 10, stop_request=request
        )
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["capture_stop"]["reason"] == "FAILED"
    assert report["capture_stop"]["trigger"] == "STOP_REQUESTED"


def test_cli_signal_opt_in_defaults_disabled_and_unsupported_platform_refuses_before_capture(
    tmp_path, monkeypatch
):
    called = []

    async def forbidden(*args, **kwargs):
        called.append(True)
        raise AssertionError("unsupported capture started")

    assert not cli.parser().parse_args(["capture", "--output", str(tmp_path)]).graceful_stop_signals
    monkeypatch.setattr(cli, "load_config", lambda *_: config())
    monkeypatch.setattr(cli, "capture", forbidden)
    monkeypatch.setattr(capture_lifecycle, "_supports_sigterm", lambda: False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--graceful-stop-signals",
        ],
    )
    assert cli.main() == 2 and not called and not (tmp_path / "capture").exists()


async def test_signal_opt_in_refuses_non_main_thread_before_registration(
    monkeypatch, signal_contract
):
    monkeypatch.setattr(threading, "current_thread", lambda: object())
    with pytest.raises(ValueError, match="main thread"), scoped_sigterm_stop(CaptureStopRequest()):
        raise AssertionError("unsupported context entered")
    assert signal_contract["calls"] == []


def test_cli_installs_scope_and_routes_request_through_actual_capture(
    tmp_path, monkeypatch, signal_contract, capsys
):
    async def produce(self):
        await self.sink(fixture_event())
        signal_contract["handler"](signal.SIGTERM, None)
        await idle()

    disable_transport(monkeypatch, produce)
    monkeypatch.setattr(cli, "load_config", lambda *_: config())
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--duration",
            "10",
            "--graceful-stop-signals",
        ],
    )
    assert cli.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 1
    assert report["capture_stop"]["request_reason"] == "SIGTERM"
    assert signal_contract["handler"] is signal_contract["previous"]


async def test_registration_failure_refuses_before_entering_capture(monkeypatch, signal_contract):
    original_install = signal.signal

    def fail(signum, handler):
        if signum == signal.SIGTERM:
            raise OSError("fixture handler registration refused")
        return original_install(signum, handler)

    monkeypatch.setattr(signal, "signal", fail)
    request = CaptureStopRequest()
    with pytest.raises(OSError), scoped_sigterm_stop(request):
        raise AssertionError("registration failure entered capture scope")
    assert request.signal_handling == "NOT_REQUESTED"
    assert signal_contract["handler"] is signal_contract["previous"]


async def test_sigint_handler_is_unchanged_inside_and_after_sigterm_scope(signal_contract):
    previous = signal.getsignal(signal.SIGINT)
    with scoped_sigterm_stop(CaptureStopRequest()):
        assert signal.getsignal(signal.SIGINT) is previous
    assert signal.getsignal(signal.SIGINT) is previous


async def test_writer_failure_tied_with_request_remains_failed(tmp_path, monkeypatch):
    request = CaptureStopRequest()

    async def fail_writer(*args):
        request.request()
        raise IntegrityError("fixture writer failed with stop")

    disable_transport(monkeypatch)
    monkeypatch.setattr(ingestion, "consume", fail_writer)
    with pytest.raises(IntegrityError):
        await ingestion.capture(config(), tmp_path / "capture", 10, stop_request=request)
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["capture_stop"]["reason"] == "FAILED"


async def test_archive_worker_failure_tied_with_request_remains_failed(tmp_path, monkeypatch):
    request = CaptureStopRequest()

    async def fail_rotation(*args):
        request.request()
        raise IntegrityError("fixture archive worker failed with stop")

    disable_transport(monkeypatch)
    monkeypatch.setattr(ingestion.RotationRuntime, "run", fail_rotation)
    with pytest.raises(IntegrityError):
        await ingestion.capture(
            config(archive_rotation_interval_s=1), tmp_path / "capture", 10, stop_request=request
        )
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["capture_stop"]["reason"] == "FAILED"


@pytest.mark.parametrize("poisoned", [False, True])
async def test_cooperative_stop_real_bar_checkpoint_or_poisoned_save_refusal(
    tmp_path, monkeypatch, poisoned
):
    request = CaptureStopRequest()
    clock = 1002000

    async def produce(self):
        self.symbols = {"BTC/USD": "BTC"}  # Explicitly mocked discovery.
        self.awaiting_snapshot.add("BTC/USD")
        await self._trades(
            {
                "channel": "trade",
                "type": "snapshot",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "side": "buy",
                        "price": 100.0,
                        "qty": 1.0,
                        "trade_id": 1,
                        "timestamp": datetime.fromtimestamp(1001000 / 1000, UTC).isoformat(),
                    }
                ],
            },
            clock,
        )
        if poisoned:
            self._emission_failed = True
        request.request()
        await idle()

    disable_transport(monkeypatch)
    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", produce)
    monkeypatch.setattr(ingestion, "now_ms", lambda: clock)
    cfg = config(spot_trades_enabled=True, bar_timeframes_s=[300, 900], bar_lateness_s=0)
    output = tmp_path / "capture"
    if poisoned:
        with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
            await ingestion.capture(cfg, output, 10, save_bars_on_stop=True, stop_request=request)
        report = json.loads((output / "report.json").read_text())
        assert report["status"] == "FAILED"
        assert report["bar_checkpoint"]["status"] == "BAR_CHECKPOINT_FAILED"
        assert not (output / "bar-checkpoint/manifest.json").exists()
    else:
        report = await ingestion.capture(
            cfg, output, 10, save_bars_on_stop=True, stop_request=request
        )
        assert report["status"] == "CAPTURE_FINISHED"
        resume = await load_bar_checkpoint(
            output / "bar-checkpoint", clock, ("BTC",), (300, 900), 0
        )
        assert len(resume.books) == 1 and len(resume.events) == 1
        assert resume.events[0].payload()["trade_id"] == 1
        assert resume.events[0].observed_at_ms == clock


async def test_optional_constructor_failure_drains_existing_tasks_and_writer(tmp_path, monkeypatch):
    disable_transport(monkeypatch)

    def fail_constructor(*args):
        raise IntegrityError("fixture optional constructor failure")

    monkeypatch.setattr(ingestion.BybitCollector, "__init__", fail_constructor)
    with pytest.raises(IntegrityError):
        await ingestion.capture(
            config(bybit_enabled=True), tmp_path / "capture", 10, stop_request=CaptureStopRequest()
        )
    assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED"


async def test_cooperative_stop_drains_real_rotation_snapshot_before_store_close(
    tmp_path, monkeypatch
):
    request = CaptureStopRequest()
    entered, release = threading.Event(), threading.Event()
    original_snapshot = active_rotation._snapshot
    original_close = DuckStore.close
    closed = []

    def snapshot(*args, **kwargs):
        result = original_snapshot(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    async def close(self):
        assert release.is_set()
        closed.append(True)
        await original_close(self)

    async def due(self):
        return True

    async def produce(self):
        await self.sink(fixture_event())
        await idle()

    disable_transport(monkeypatch, produce)
    monkeypatch.setattr(active_rotation, "_snapshot", snapshot)
    monkeypatch.setattr(ingestion.RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(DuckStore, "close", close)
    output = tmp_path / "capture"
    task = asyncio.create_task(
        ingestion.capture(config(archive_rotation_interval_s=1), output, 10, stop_request=request)
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        request.request()
        await asyncio.sleep(0)
        assert not task.done() and not closed
    finally:
        release.set()
    report = await asyncio.wait_for(task, 5)
    assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 1 and closed
    assert report["archive_rotation"]["status"] == "STOPPED"
    assert report["archive_rotation"]["last_attempt"]["status"] == "CANCELLED"
    assert report["archive_rotation"]["completed_snapshots"] == 0
    snapshot_directory = output / "archive-rotations/snapshot-000000"
    assert not (snapshot_directory / "active-rotation.json").exists()
    assert (snapshot_directory / "active-rotation-failed.json").exists()


async def test_cooperative_stop_mid_emission_retains_prefix_and_refuses_bar_checkpoint(
    tmp_path, monkeypatch
):
    request = CaptureStopRequest()
    original_publish = EventBus.publish
    seen = []
    collectors = []

    async def publish(self, event):
        if event.kind == "trade":
            seen.append(event)
            if len(seen) == 2:
                request.request()
                await idle()
        await original_publish(self, event)

    async def produce(self):
        collectors.append(self)
        self.symbols = {"BTC/USD": "BTC"}
        self.awaiting_snapshot.add("BTC/USD")
        await self._trades(
            {
                "channel": "trade",
                "type": "snapshot",
                "data": [
                    {
                        "symbol": "BTC/USD",
                        "side": "buy",
                        "price": 100.0,
                        "qty": 1.0,
                        "trade_id": identifier,
                        "timestamp": datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(),
                    }
                    for identifier, timestamp in [(1, 1001000), (2, 1001500)]
                ],
            },
            1002000,
        )
        raise AssertionError("ambiguous batch installed after stop")

    disable_transport(monkeypatch)
    monkeypatch.setattr(EventBus, "publish", publish)
    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", produce)
    monkeypatch.setattr(ingestion, "now_ms", lambda: 1002000)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="SPOT_EMISSION_OUTCOME_UNKNOWN"):
        await ingestion.capture(
            config(spot_trades_enabled=True),
            output,
            10,
            save_bars_on_stop=True,
            stop_request=request,
        )
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["rows"] == 1
    assert (
        report["capture_stop"]["reason"] == "FAILED"
        and report["capture_stop"]["trigger"] == "STOP_REQUESTED"
    )
    assert len(collectors) == 1 and collectors[0]._emission_failed
    assert collectors[0].bar_inputs == {} and collectors[0].bars == {}
    assert not (output / "bar-checkpoint/checkpoint.json").exists()
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as database:
        assert database.execute("SELECT event_id FROM events").fetchall() == [(seen[0].event_id,)]
        assert database.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered"
        ).fetchone() == (0,)
