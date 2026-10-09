"""Periodic worker supervision in the actual finite capture; no network transport."""

import asyncio
import json
import threading

import duckdb
import pytest
from test_capture_lifecycle import config, disable_transport, idle
from test_periodic_bar_runtime import CUTOFF, message

from selective_options import bar_runtime, ingestion, periodic_bar_runtime
from selective_options.capture_lifecycle import CaptureStopRequest
from selective_options.errors import IntegrityError
from selective_options.periodic_bar_runtime import PeriodicBarRuntime


async def test_default_never_starts_periodic_worker_or_creates_generations(tmp_path, monkeypatch):
    disable_transport(monkeypatch)

    async def forbidden(self):
        pytest.fail("default disabled periodic worker started")

    monkeypatch.setattr(PeriodicBarRuntime, "run", forbidden)
    output = tmp_path / "capture"
    report = await ingestion.capture(config(), output, 0.02)
    assert report["periodic_bar_checkpoints"]["status"] == "DISABLED"
    assert report["periodic_bar_checkpoints"]["completed_generations"] == 0
    assert not (output / "bar-generations").exists()


async def test_periodic_opt_in_tracks_native_inputs_and_drains_worker_before_store_close(
    tmp_path, monkeypatch
):
    disable_transport(monkeypatch)
    request = CaptureStopRequest()
    evidence = []

    async def managed(self):
        assert self.collector is not None and self.collector.track_bar_inputs
        assert not self.store._closed
        request.request()
        try:
            await idle()
        finally:
            assert not self.store._closed
            await self.bus.flush()
            evidence.append("periodic worker drained with live writer")

    monkeypatch.setattr(PeriodicBarRuntime, "run", managed)
    report = await ingestion.capture(
        config(spot_trades_enabled=True, bar_checkpoint_interval_s=1),
        tmp_path / "capture",
        5,
        stop_request=request,
    )
    assert evidence == ["periodic worker drained with live writer"]
    assert report["capture_stop"]["reason"] == "STOP_REQUESTED"
    assert report["trading_enabled"] is report["milestone_2_complete"] is False


async def test_periodic_worker_failure_records_failed_report_and_closes_database(
    tmp_path, monkeypatch
):
    disable_transport(monkeypatch)

    async def failure(self):
        assert not self.store._closed
        raise IntegrityError("EXPLICIT_SYNTHETIC_PERIODIC_FAILURE")

    monkeypatch.setattr(PeriodicBarRuntime, "run", failure)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="EXPLICIT_SYNTHETIC_PERIODIC_FAILURE"):
        await ingestion.capture(
            config(spot_trades_enabled=True, bar_checkpoint_interval_s=1), output, 5
        )
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and report["trading_enabled"] is False
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as database:
        assert database.execute("SELECT count(*) FROM events").fetchone() == (0,)
    assert not (output / "events.duckdb.wal").exists()


async def test_unexpected_normal_periodic_exit_fails_capture(tmp_path, monkeypatch):
    disable_transport(monkeypatch)

    async def early(self):
        return

    monkeypatch.setattr(PeriodicBarRuntime, "run", early)
    output = tmp_path / "capture"
    with pytest.raises(IntegrityError, match="worker exited before capture deadline"):
        await ingestion.capture(
            config(spot_trades_enabled=True, bar_checkpoint_interval_s=1), output, 5
        )
    assert json.loads((output / "report.json").read_text())["status"] == "FAILED"


async def test_pre_requested_stop_does_not_start_opted_in_periodic_worker(tmp_path, monkeypatch):
    request = CaptureStopRequest()
    request.request()

    async def forbidden(*args):
        pytest.fail("a pre-requested stop started a producer or periodic worker")

    monkeypatch.setattr(ingestion.DeribitCollector, "run", forbidden)
    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", forbidden)
    monkeypatch.setattr(PeriodicBarRuntime, "run", forbidden)
    with pytest.raises(ValueError, match="already requested before startup"):
        await ingestion.capture(
            config(spot_trades_enabled=True, bar_checkpoint_interval_s=1),
            tmp_path / "capture",
            5,
            stop_request=request,
        )
    assert not (tmp_path / "capture").exists()


async def test_stop_during_late_failed_seal_cannot_report_capture_success(tmp_path, monkeypatch):
    disable_transport(monkeypatch)
    seeded = asyncio.Event()
    entered, release = threading.Event(), threading.Event()
    request = CaptureStopRequest()
    runtime_tasks = []
    original_run = PeriodicBarRuntime.run
    original_seal = periodic_bar_runtime._seal

    async def produce(self):
        self.symbols = {"BTC/USD": "BTC"}
        await self._trades(message(1, kind="snapshot"), 1002000)
        seeded.set()
        await idle()

    async def due(self):
        await seeded.wait()
        return True

    async def managed(self):
        runtime_tasks.append(asyncio.current_task())
        await original_run(self)

    def failed_seal(*args):
        original_seal(*args)
        entered.set()
        assert release.wait(5)
        raise OSError("EXPLICIT_WORKER_FAILURE_AFTER_CAPTURE_STOP")

    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", produce)
    monkeypatch.setattr(PeriodicBarRuntime, "_wait_due", due)
    monkeypatch.setattr(PeriodicBarRuntime, "run", managed)
    monkeypatch.setattr(periodic_bar_runtime, "_seal", failed_seal)
    monkeypatch.setattr(bar_runtime, "now_ms", lambda: CUTOFF)
    output = tmp_path / "capture"
    task = asyncio.create_task(
        ingestion.capture(
            config(
                spot_trades_enabled=True,
                bar_checkpoint_interval_s=1,
                bar_timeframes_s=[300, 900],
                bar_lateness_s=0,
            ),
            output,
            5,
            stop_request=request,
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        request.request()
        async with asyncio.timeout(2):
            for _ in range(10000):
                if runtime_tasks[0].cancelling():
                    break
                await asyncio.sleep(0)
            else:
                pytest.fail("requested stop never cancelled the periodic task")
    finally:
        release.set()
    with pytest.raises(IntegrityError, match="BAR_GENERATION_CANCELLATION_DRAIN_FAILURE") as caught:
        await task
    assert isinstance(caught.value.__cause__, OSError)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    periodic = report["periodic_bar_checkpoints"]
    assert periodic["status"] == "FAILED" and periodic["failure_type"] == "OSError"
    assert periodic["completed_generations"] == 0
    assert periodic["last_attempt"]["status"] == "FAILED"
    generation = output / "bar-generations/generation-000000"
    assert not (generation / "generation.json").exists()
    assert not (generation / "checkpoint/checkpoint.json").exists()
    assert json.loads((generation / "failure.json").read_text())["failure_type"] == "OSError"
    assert not (output / "events.duckdb.wal").exists()
