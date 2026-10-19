"""Finite saved-native active bar generations and explicit capture resume.

Original 50/5 Kraken cohorts stay separate across the documented missing trade.
Transport containers, periodic scheduling and lifecycle clocks are mocked. Each
initial capture publishes two real immutable generations while its writer stays
open, then its producer advances. No network request, OS crash or soak is tested.
"""

import argparse
import asyncio
import json
import socket
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from pydantic import JsonValue

from selective_options import __version__, bar_runtime, collectors, ingestion, spot_trades
from selective_options.archive import file_digest
from selective_options.bar_checkpoint import BarCheckpointResume
from selective_options.capture_lifecycle import CaptureStopRequest
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.periodic_bar_runtime import PeriodicBarRuntime, load_bar_generation
from selective_options.research import _path
from selective_options.spot_trades import SpotTradeCollector, stored_trade, trade_event

if TYPE_CHECKING or not __package__:
    import verify_bar_checkpoint as saved
else:
    from scripts import verify_bar_checkpoint as saved

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/live-gap-02"
SOURCE_SHA256 = "08b81a0c2ced7cc288b253a6b5f6a9ec8690cfad3a7bc5583c9dde2552ce1ce7"
EXPECTED_VERSION = "0.3.55"
SAFETY_TIMEOUT_S = 60.0


def native_cohorts() -> tuple[tuple[MarketEvent, ...], ...]:
    """Read only the fixed original source; never manufacture the omitted ID."""
    if file_digest(SOURCE / "events.duckdb") != SOURCE_SHA256:
        raise IntegrityError("PERIODIC_PROOF_NATIVE_SOURCE_DIGEST_MISMATCH")
    source_report = json.loads((SOURCE / "report.json").read_text(encoding="utf-8"))
    if (
        source_report["status"] != "PUBLIC_GAP_TEST_PASSED"
        or source_report["omitted_trade_id"] != 109230677
    ):
        raise IntegrityError("PERIODIC_PROOF_NATIVE_SOURCE_SCOPE_MISMATCH")
    native = sorted(
        (
            row
            for row in saved.rows(SOURCE / "events.duckdb")
            if row.source == "kraken" and row.kind == "trade"
        ),
        key=lambda row: cast(int, row.payload()["trade_id"]),
    )
    cohorts: list[list[MarketEvent]] = []
    previous: int | None = None
    for row in native:
        if (row.environment, row.asset, row.symbol) != (
            "public_reference",
            "BTC",
            "BTC/USD",
        ) or row != trade_event(stored_trade(row), row.asset, row.observed_at_ms):
            raise IntegrityError("PERIODIC_PROOF_NATIVE_RECORD_CHANGED")
        identifier = cast(int, row.payload()["trade_id"])
        if previous is None or identifier != previous + 1:
            cohorts.append([])
        cohorts[-1].append(row)
        previous = identifier
    if len(native) != 55 or [len(group) for group in cohorts] != [50, 5]:
        raise IntegrityError("PERIODIC_PROOF_NATIVE_COHORT_MEMBERSHIP")
    return tuple(tuple(group) for group in cohorts)


async def _resume_capture(
    config: AppConfig,
    output: Path,
    duration_s: float | None = None,
    *,
    resume_bars_from: Path | None = None,
    save_bars_on_stop: bool | None = None,
) -> dict[str, object]:
    """The reused native verifier stops after its actual restored-bar flush."""
    request = CaptureStopRequest()
    original_flush = SpotTradeCollector._flush

    async def flushed(collector: SpotTradeCollector, cutoff: int) -> None:
        await original_flush(collector, cutoff)
        request.request("OPERATOR")

    with patch.object(SpotTradeCollector, "_flush", flushed):
        report = await capture(
            config,
            output,
            SAFETY_TIMEOUT_S,
            resume_bars_from=resume_bars_from,
            save_bars_on_stop=save_bars_on_stop,
            stop_request=request,
        )
    stop = cast(dict[str, object], report["capture_stop"])
    assert stop["reason"] == "STOP_REQUESTED" and stop["request_reason"] == "OPERATOR"
    assert stop["signal_handling"] == "NOT_REQUESTED"
    assert report["status"] == "CAPTURE_FINISHED"
    return report


def _members(resume: BarCheckpointResume) -> tuple[MarketEvent, ...]:
    assert len(resume.books) == 1
    book = resume.books[0]
    assert book.tail_event == book.input_events[-1]
    assert set(resume.events) == set(book.input_events)
    return book.input_events


async def initial_capture(cohort: tuple[MarketEvent, ...], output: Path) -> dict[str, object]:
    """Exercise the actual supervised writer and periodic worker on two batches."""
    midpoint = len(cohort) // 2
    prefix = cohort[:midpoint]
    assert prefix and len(prefix) < len(cohort)
    first_cutoff = max(max(row.event_time_ms, row.observed_at_ms) for row in prefix) + 1
    final_cutoff = max(max(row.event_time_ms, row.observed_at_ms) for row in cohort) + 1
    clock = {"now": first_cutoff}
    prefix_ready, first_saved, suffix_ready = asyncio.Event(), asyncio.Event(), asyncio.Event()
    request = CaptureStopRequest()
    timeline: list[str] = []
    observed: list[dict[str, object]] = []
    first_hashes: dict[str, str] = {}
    config = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "policy_news_sources": [],
            "derivatives_enabled": False,
            "bar_timeframes_s": [300, 900],
            "bar_lateness_s": 5,
            "flush_interval_s": 0.001,
            "bar_checkpoint_interval_s": 1,
            "bar_checkpoint_max_generations": 2,
        },
    )

    class ReplayCollector(SpotTradeCollector):
        async def run(self) -> None:
            self.symbols = {"BTC/USD": "BTC"}
            self.awaiting_snapshot.add("BTC/USD")
            for index, event in enumerate(cohort):
                message: dict[str, JsonValue] = {
                    "channel": "trade",
                    "type": "snapshot" if index == 0 else "update",
                    "data": [stored_trade(event).model_dump(mode="json")],
                }
                await self._trades(message, event.observed_at_ms)
                if index + 1 == midpoint:
                    timeline.append("PREFIX_FRONTIER_INSTALLED")
                    prefix_ready.set()
                    await first_saved.wait()
                    # No early bar flush: source records have original delayed
                    # receipts, and advancing a watermark could reject their tail.
                    clock["now"] = final_cutoff
                    timeline.append("PRODUCER_ADVANCED_AFTER_FIRST_GENERATION")
            await self._flush(final_cutoff)
            timeline.append("FULL_COHORT_FRONTIER_INSTALLED")
            suffix_ready.set()
            await asyncio.Event().wait()

    async def idle(*args: object) -> None:
        await asyncio.Event().wait()

    async def due(runtime: PeriodicBarRuntime) -> bool:
        index = len(runtime.generations)
        if index == 0:
            await prefix_ready.wait()
            return True
        generation = runtime.generations[-1]
        assert not runtime.store._closed
        directory = output / cast(str, generation["directory"])
        digest = cast(str, generation["generation_sha256"])
        assert file_digest(directory / "generation.json") == digest
        selected = await load_bar_generation(
            directory,
            clock["now"],
            config.system.assets,
            config.ingestion.bar_timeframes_s,
            int(config.ingestion.bar_lateness_s * 1000),
            expected_generation_sha256=digest,
        )
        expected = prefix if index == 1 else cohort
        assert _members(selected) == expected
        committed = await runtime.store.read_events([row.event_id for row in expected])
        assert sorted(committed, key=lambda row: row.event_id) == sorted(
            expected, key=lambda row: row.event_id
        )
        timeline.append(f"GENERATION_{index - 1}_VERIFIED_WHILE_WRITER_OPEN")
        observed.append(
            {
                "index": index - 1,
                "directory": generation["directory"],
                "generation_sha256": digest,
                "as_of_ms": selected.as_of_ms,
                "input_event_ids": [row.event_id for row in expected],
                "native_count": len(expected),
                "writer_open_during_selection": True,
                "committed_native_membership_exact": True,
            }
        )
        if index == 1:
            first_hashes.update(saved.hashes(directory))
            first_saved.set()
            await suffix_ready.wait()
            return True
        assert index == 2
        assert saved.hashes(output / "bar-generations/generation-000000") == first_hashes
        timeline.append("STOP_REQUESTED_AFTER_SECOND_GENERATION")
        request.request("OPERATOR")
        # A managed worker must not exit normally before capture acknowledges
        # stop; its writer remains available until the supervisor stops us.
        await runtime.stopped.wait()
        return False

    with (
        patch.object(ingestion, "now_ms", lambda: clock["now"]),
        patch.object(spot_trades, "now_ms", lambda: clock["now"]),
        patch.object(collectors, "now_ms", lambda: clock["now"]),
        patch.object(bar_runtime, "now_ms", lambda: clock["now"]),
        patch.object(ingestion, "SpotTradeCollector", ReplayCollector),
        patch.object(collectors.DeribitCollector, "run", idle),
        patch.object(ingestion, "poll_chains", idle),
        patch.object(PeriodicBarRuntime, "_wait_due", due),
    ):
        report = await capture(config, output, SAFETY_TIMEOUT_S, stop_request=request)
    assert report["status"] == "CAPTURE_FINISHED"
    assert report["orders_submitted"] == 0 and report["milestone_2_complete"] is False
    assert report["ready_for_signals"] is False and report["trading_enabled"] is False
    stop = cast(dict[str, object], report["capture_stop"])
    assert stop["reason"] == "STOP_REQUESTED" and stop["request_reason"] == "OPERATOR"
    periodic = cast(dict[str, object], report["periodic_bar_checkpoints"])
    assert periodic["status"] == "STOPPED" and periodic["completed_generations"] == 2
    assert periodic["attempts"] == 2 and periodic["failure_type"] is None
    stored = saved.rows(output / "events.duckdb")
    native = tuple(
        sorted(
            (row for row in stored if row.kind == "trade"),
            key=lambda row: cast(int, row.payload()["trade_id"]),
        )
    )
    assert native == cohort and not any(row.kind == "bar" for row in stored)
    assert len(observed) == 2
    return {
        "capture_report_sha256": file_digest(output / "report.json"),
        "native_count": len(cohort),
        "trade_event_ids": [row.event_id for row in native],
        "generations": observed,
        "first_generation_unchanged_after_new_commits": True,
        "pit_boundary_checks": saved.exact_archive(output),
        "closed_archive_exact": True,
        "pending_outbox_exact": True,
        "timeline": timeline,
        "synthetic_first_cutoff_ms": first_cutoff,
        "synthetic_final_cutoff_ms": final_cutoff,
        "stop_after_second_generation": True,
    }


async def verify(output: Path) -> dict[str, object]:
    """Create one exclusive finite proof; no automatic generation search occurs."""
    output = _path(output, must_exist=False)
    if output.exists() or output.is_relative_to(SOURCE) or SOURCE.is_relative_to(output):
        raise ValueError("new proof output must be separate from original native evidence")
    if __version__ != EXPECTED_VERSION:
        raise IntegrityError("PERIODIC_PROOF_RUNTIME_VERSION_MISMATCH")
    original = saved.hashes(SOURCE)
    code = saved.code_hashes()
    verifier_sha = file_digest(Path(__file__))
    helper_sha = file_digest(ROOT / "scripts/verify_bar_checkpoint.py")
    cohorts = native_cohorts()
    output.mkdir(parents=True, exist_ok=False)
    phases: list[dict[str, object]] = []
    attempted_connections = 0

    def forbidden_network(*args: object, **kwargs: object) -> None:
        nonlocal attempted_connections
        attempted_connections += 1
        raise IntegrityError("PERIODIC_PROOF_NETWORK_REQUEST_REFUSED")

    with (
        patch.object(socket.socket, "connect", forbidden_network),
        patch.object(socket.socket, "connect_ex", forbidden_network),
        patch.object(socket, "getaddrinfo", forbidden_network),
    ):
        for index, cohort in enumerate(cohorts):
            initial_output = output / f"cohort-{index}-initial"
            initial = await initial_capture(cohort, initial_output)
            generations = cast(list[dict[str, object]], initial["generations"])
            chosen = generations[-1]
            generation = initial_output / cast(str, chosen["directory"])
            restart_ms = cast(int, initial["synthetic_final_cutoff_ms"]) + 1000
            selected = await load_bar_generation(
                generation,
                restart_ms,
                ("BTC",),
                (300, 900),
                5000,
                expected_generation_sha256=cast(str, chosen["generation_sha256"]),
            )
            assert _members(selected) == cohort
            with patch.object(saved, "capture", _resume_capture):
                resumed = await saved.phase(
                    cohort,
                    output / f"cohort-{index}-resumed",
                    {"now": restart_ms},
                    generation / "checkpoint",
                )
            # Both generations remain immutable across explicit selection/resume.
            for earlier in generations:
                directory = initial_output / cast(str, earlier["directory"])
                assert file_digest(directory / "generation.json") == earlier["generation_sha256"]
            phases.append(
                {
                    "cohort": index,
                    "native_count": len(cohort),
                    "initial": initial,
                    "explicitly_selected_generation": chosen,
                    "resumed": resumed,
                }
            )
    assert attempted_connections == 0
    assert saved.hashes(SOURCE) == original and saved.code_hashes() == code
    assert file_digest(Path(__file__)) == verifier_sha
    assert file_digest(ROOT / "scripts/verify_bar_checkpoint.py") == helper_sha
    report: dict[str, object] = {
        "status": "SAVED_NATIVE_ACTIVE_BAR_GENERATIONS_PASSED",
        "application_version": __version__,
        "source_database_sha256": SOURCE_SHA256,
        "source_hashes": original,
        "source_mutated": False,
        "code_hashes": code,
        "code_hash_scope": "all current src/selective_options Python modules",
        "verifier_sha256": verifier_sha,
        "reused_native_verifier_sha256": helper_sha,
        "artifact_hashes": saved.hashes(output),
        "saved_native_trade_count": 55,
        "separate_contiguous_cohort_sizes": [50, 5],
        "known_missing_trade_id": 109230677,
        "missing_trade_repaired": False,
        "actual_capture_pipelines": 4,
        "active_committed_generations": 4,
        "derived_partial_bar_count": 4,
        "native_payloads_and_first_receipts_unchanged": True,
        "generation_selection": "explicit immutable directory plus expected marker SHA256",
        "automatic_latest_generation_fallback": False,
        "stop_trigger": "OPERATOR request after second verified generation",
        "finite_safety_timeout_s": SAFETY_TIMEOUT_S,
        "transport": "mocked; original native envelopes in reconstructed containers",
        "periodic_scheduling": "mocked cohort-ready gates; actual periodic run and checkpoint pipeline",
        "lifecycle_clocks": "explicit synthetic cutoff/restart/close clocks",
        "original_frame_bytes_available": False,
        "phases": phases,
        "new_public_requests": 0,
        "blocked_network_attempts": attempted_connections,
        "os_signal_delivery_tested": False,
        "os_crash_tested": False,
        "periodic_crash_recovery": False,
        "continuous_schedule_or_service_soak_tested": False,
        "seven_day_validation_completed": False,
        "history_complete": False,
        "source_frontier_complete": False,
        "current_feed_health_verified": False,
        "milestone_2_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    with (output / "verification.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2, allow_nan=False) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    report = asyncio.run(verify(parser.parse_args().output))
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "status",
                    "actual_capture_pipelines",
                    "saved_native_trade_count",
                    "active_committed_generations",
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
