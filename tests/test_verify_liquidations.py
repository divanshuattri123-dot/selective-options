"""Finite verifier tests use explicitly labeled mock transport and exact saved evidence."""

import asyncio
import importlib.util
import json
from pathlib import Path

import pytest

from selective_options import liquidations as mod
from selective_options.collectors import now_ms
from selective_options.errors import IntegrityError

SPEC = importlib.util.spec_from_file_location(
    "verify_liquidations", Path(__file__).parents[1] / "scripts/verify_liquidations.py"
)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def workspace(monkeypatch, tmp_path):
    monkeypatch.setattr(probe, "ROOT", tmp_path / "outputs/selective-options")
    return tmp_path / "work/liquidations-0339/proof"


class FixtureSocket:
    def __init__(self, observe):
        self.observe = observe
        self.sent = []
        self.receives = 0

    async def send(self, body):
        self.sent.append(json.loads(body))

    async def recv(self):
        self.receives += 1
        if self.receives == 1:
            return json.dumps({"result": None, "id": mod.REQUEST_ID})
        if self.observe and self.receives == 2:
            clock = now_ms()
            return json.dumps(
                {
                    "stream": "btcusdt@forceOrder",
                    "data": {
                        "e": "forceOrder",
                        "E": clock,
                        "o": {
                            "s": "BTCUSDT",
                            "S": "SELL",
                            "o": "LIMIT",
                            "f": "IOC",
                            "q": "1",
                            "p": "60000",
                            "ap": "60000",
                            "X": "FILLED",
                            "l": "1",
                            "z": "1",
                            "T": clock,
                        },
                    },
                }
            )
        await asyncio.Event().wait()


@pytest.mark.parametrize("observe", [False, True])
async def test_finite_mock_saved_pit_stop_and_archive(monkeypatch, tmp_path, observe):
    output = workspace(monkeypatch, tmp_path)
    socket = FixtureSocket(observe)
    calls = []

    class Context:
        async def __aenter__(self):
            return socket

        async def __aexit__(self, *_args):
            return False

    def connect(url, **options):
        calls.append((url, options))
        return Context()

    monkeypatch.setattr(mod, "connect", connect)
    report = await probe.verify(output, duration_s=3, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "FIXTURE_LOSSY_STREAM_SESSION_OBSERVED"
    assert report["real_public_connection_attempts"] == 0
    assert report["connection_attempts"] == report["subscription_attempts"] == len(calls) == 1
    assert report["parquet_exact_match"] and report["closed_reload_exact_match"]
    assert report["native_observations"] == int(observe)
    assert report["quality_before_stop"]["BTC"]["observation_state"] == (
        "NATIVE_SNAPSHOT_OBSERVED" if observe else "UNOBSERVED"
    )
    assert all(view["state"] == "BLOCKED" for view in report["quality_after_stop"].values())
    assert report["outbox_pending_after_reopen"] == report["persisted_rows"]
    assert report["pit_boundary_checks"] == 2 * report["persisted_rows"]
    assert json.loads((output / "report.json").read_text()) == report
    assert len(json.loads((output / "frames.json").read_text())) == 1 + int(observe)
    assert calls[0][0] == mod.URL and "ssl" not in calls[0][1]


async def test_unavailable_transport_is_saved_once(monkeypatch, tmp_path):
    output = workspace(monkeypatch, tmp_path)
    calls = []

    class Context:
        async def __aenter__(self):
            raise OSError("mock unavailable")

        async def __aexit__(self, *_args):
            return False

    def connect(*args, **options):
        calls.append(args)
        return Context()

    monkeypatch.setattr(mod, "connect", connect)
    report = await probe.verify(output, duration_s=3, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "SOURCE_UNAVAILABLE_OR_INVALID"
    assert report["transport_failure_class"] == "OSError"
    assert len(calls) == 1 and report["subscription_attempts"] == 0
    assert report["persisted_rows"] == 4 and report["raw_frames"] == 0
    assert report["native_entity_comparisons"] == 0


@pytest.mark.parametrize("duration", [0, True, float("nan"), float("inf"), 20.1])
def test_preflight_before_output_or_network(tmp_path, monkeypatch, duration):
    output = workspace(monkeypatch, tmp_path)
    with pytest.raises(ValueError):
        probe.preflight(output, duration)
    assert not output.exists()


def test_preflight_refuses_unowned_or_existing_output(tmp_path, monkeypatch):
    output = workspace(monkeypatch, tmp_path)
    with pytest.raises(ValueError):
        probe.preflight(tmp_path / "unexpected", 3)
    output.mkdir(parents=True)
    with pytest.raises(FileExistsError):
        probe.preflight(output, 3)


def test_independent_native_comparison_rejects_raw_or_receipt_corruption():
    with pytest.raises(IntegrityError, match="DIGEST"):
        probe.independent_native(
            [{"body_hex": "7b7d", "sha256": "wrong", "bytes": 2, "received_at_ms": 1000}], []
        )


async def test_verifier_drains_inflight_committed_sink_on_cancellation():
    writing_started = asyncio.Event()
    release = asyncio.Event()
    committed = []

    class Store:
        async def append(self, rows, *, exact):
            assert exact
            writing_started.set()
            await release.wait()
            committed.extend(rows)

    feed = probe.RecordingCollector(probe.AppConfig(), Store())
    row = mod.health_event(
        "BTC",
        1100,
        state="CONNECTED",
        ok=True,
        reason="SUBSCRIPTION_ACKNOWLEDGED",
        session_id="a" * 32,
        session_started_at_ms=1000,
        streams=("btcusdt@forceOrder",),
        ignored_unrequested_frames=0,
        accepted_event_id=None,
        accepted_observed_at_ms=None,
    )
    task = asyncio.create_task(feed.record(row))
    await writing_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert feed.rows == committed == [row]
    assert feed.acknowledged.is_set()
