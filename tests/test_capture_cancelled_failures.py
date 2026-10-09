"""Cancellation cannot hide a drained worker error or report capture success."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from test_capture_lifecycle import config, disable_transport, idle

from selective_options import ingestion, rotation_runtime
from selective_options.bus import EventBus
from selective_options.capture_lifecycle import CaptureStopRequest, cancelled_worker_failure
from selective_options.errors import IntegrityError
from selective_options.periodic_bar_runtime import PeriodicBarRuntime
from selective_options.rotation_runtime import RotationRuntime
from selective_options.store import DuckStore


def test_explicit_cancel_cause_classification_preserves_failure_and_pure_cancel():
    pure = asyncio.CancelledError()
    assert cancelled_worker_failure(pure) is None
    failure = OSError("EXPLICIT_SYNTHETIC_WORKER_FAILURE")
    wrapped = asyncio.CancelledError()
    wrapped.__cause__ = failure
    nested = asyncio.CancelledError()
    nested.__cause__ = wrapped
    assert cancelled_worker_failure(wrapped) is failure
    assert cancelled_worker_failure(nested) is failure
    wrapped.__cause__ = pure
    assert cancelled_worker_failure(wrapped) is None


def test_cancel_cause_cycle_refuses_success():
    first = asyncio.CancelledError()
    second = asyncio.CancelledError()
    first.__cause__ = second
    second.__cause__ = first
    result = cancelled_worker_failure(first)
    assert isinstance(result, IntegrityError)
    assert str(result) == "CAPTURE_CANCELLATION_CAUSE_CYCLE"


@pytest.mark.parametrize("worker", ["producer", "archive", "periodic"])
async def test_supervisor_preserves_worker_cause_during_requested_stop(
    tmp_path, monkeypatch, worker
):
    disable_transport(monkeypatch)
    request = CaptureStopRequest()
    failure = OSError("EXPLICIT_SYNTHETIC_FAILURE_DURING_REQUESTED_STOP")

    async def running(self):
        request.request()
        try:
            await idle()
        except asyncio.CancelledError as exc:
            raise exc from failure

    if worker == "producer":
        monkeypatch.setattr(ingestion.DeribitCollector, "run", running)
        changes = {}
    elif worker == "archive":
        monkeypatch.setattr(RotationRuntime, "run", running)
        changes = {"archive_rotation_interval_s": 1}
    else:
        monkeypatch.setattr(PeriodicBarRuntime, "run", running)
        changes = {"spot_trades_enabled": True, "bar_checkpoint_interval_s": 1}
    output = tmp_path / worker
    with pytest.raises(OSError, match="FAILURE_DURING_REQUESTED_STOP") as captured:
        await ingestion.capture(config(**changes), output, 5, stop_request=request)
    assert captured.value is failure
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED"
    assert report["trading_enabled"] is False
    assert not (output / "events.duckdb.wal").exists()


async def test_archive_runtime_records_failed_attempt_when_cancel_wraps_worker_error(
    tmp_path, monkeypatch
):
    stopped = asyncio.Event()
    store = await DuckStore.open(tmp_path / "events.duckdb")
    bus = EventBus(10, 1)
    monkeypatch.setattr(bus, "flush", AsyncMock())
    failure = OSError("EXPLICIT_SYNTHETIC_ARCHIVE_DRAIN_FAILURE")

    async def due(self):
        return True

    async def failed(*args, **kwargs):
        stopped.set()
        raise asyncio.CancelledError() from failure

    monkeypatch.setattr(RotationRuntime, "_wait_due", due)
    monkeypatch.setattr(rotation_runtime, "rotate_active_store", failed)
    runtime = RotationRuntime(config(archive_rotation_interval_s=1), bus, store, tmp_path, stopped)
    try:
        with pytest.raises(
            IntegrityError, match="ARCHIVE_ROTATION_CANCELLATION_DRAIN_FAILURE"
        ) as caught:
            await runtime.run()
        assert caught.value.__cause__ is failure
        result = runtime.summary()
        assert result["status"] == "FAILED" and result["failure_type"] == "OSError"
        assert result["last_attempt"]["status"] == "FAILED"
        assert result["last_attempt"]["failure_type"] == "OSError"
        assert result["completed_snapshots"] == 0
    finally:
        await store.close()
