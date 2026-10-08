"""Saved-native capture/stop/checkpoint proof with mocked transport and clocks.

Four real capture pipelines replay separate original 50/5 trade cohorts. A stop
request is triggered only after the collector flushes; no timer expires, network
transport starts or native gap is repaired. This is cooperative stop evidence,
not an operating-system signal test, periodic crash recovery or a service soak.
"""

import argparse
import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from selective_options.archive import file_digest
from selective_options.capture_lifecycle import CaptureStopRequest
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.research import _path
from selective_options.spot_trades import SpotTradeCollector, stored_trade, trade_event

if TYPE_CHECKING:
    import verify_bar_checkpoint as saved
else:
    from scripts import verify_bar_checkpoint as saved

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/live-gap-02"
SOURCE_SHA256 = "08b81a0c2ced7cc288b253a6b5f6a9ec8690cfad3a7bc5583c9dde2552ce1ce7"


async def requested_capture(
    config: AppConfig,
    output: Path,
    duration_s: float | None = None,
    *,
    resume_bars_from: Path | None = None,
    save_bars_on_stop: bool | None = None,
) -> dict[str, object]:
    request = CaptureStopRequest()
    original_flush = SpotTradeCollector._flush

    async def flushed(collector: SpotTradeCollector, cutoff: int) -> None:
        await original_flush(collector, cutoff)
        request.request("OPERATOR")

    with patch.object(SpotTradeCollector, "_flush", flushed):
        report = await capture(
            config,
            output,
            60.0,
            resume_bars_from=resume_bars_from,
            save_bars_on_stop=save_bars_on_stop,
            stop_request=request,
        )
    stop = cast(dict[str, object], report["capture_stop"])
    assert stop["reason"] == "STOP_REQUESTED"
    assert stop["request_reason"] == "OPERATOR"
    assert stop["signal_handling"] == "NOT_REQUESTED"
    assert report["status"] == "CAPTURE_FINISHED"
    return report


async def verify(output: Path) -> dict[str, object]:
    output = _path(output, must_exist=False)
    if output.exists() or output.is_relative_to(SOURCE) or SOURCE.is_relative_to(output):
        raise ValueError("new proof output must be separate from original native evidence")
    before = saved.hashes(SOURCE)
    code = saved.code_hashes()
    assert file_digest(SOURCE / "events.duckdb") == SOURCE_SHA256
    source_report = json.loads((SOURCE / "report.json").read_text(encoding="utf-8"))
    assert source_report["omitted_trade_id"] == 109230677
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
        assert row == trade_event(stored_trade(row), row.asset, row.observed_at_ms)
        identifier = cast(int, row.payload()["trade_id"])
        if previous is None or identifier != previous + 1:
            cohorts.append([])
        cohorts[-1].append(row)
        previous = identifier
    assert len(native) == 55 and [len(group) for group in cohorts] == [50, 5]
    output.mkdir(parents=True, exist_ok=False)
    phases = []
    with patch.object(saved, "capture", requested_capture):
        for index, group in enumerate(cohorts):
            cohort = tuple(group)
            cutoff = max(max(row.event_time_ms, row.observed_at_ms) for row in cohort) + 1
            clock = {"now": cutoff}
            initial = await saved.phase(cohort, output / f"cohort-{index}-initial", clock)
            clock["now"] = cutoff + 1000
            resumed = await saved.phase(
                cohort,
                output / f"cohort-{index}-resumed",
                clock,
                output / f"cohort-{index}-initial/bar-checkpoint",
            )
            phases.append(
                {"cohort": index, "count": len(cohort), "initial": initial, "resumed": resumed}
            )
    assert saved.hashes(SOURCE) == before
    assert saved.code_hashes() == code
    report: dict[str, object] = {
        "status": "SAVED_NATIVE_COOPERATIVE_CAPTURE_STOP_PASSED",
        "source_database_sha256": SOURCE_SHA256,
        "source_hashes": before,
        "source_mutated": False,
        "code_hashes": code,
        "verifier_sha256": file_digest(Path(__file__)),
        "saved_native_trade_count": 55,
        "separate_contiguous_cohort_sizes": [50, 5],
        "known_missing_trade_id": 109230677,
        "missing_trade_repaired": False,
        "actual_capture_pipelines": 4,
        "stop_trigger": "OPERATOR request after actual collector flush",
        "finite_safety_timeout_s": 60,
        "transport": "mocked; original native envelopes and synthetic lifecycle clocks",
        "phases": phases,
        "new_public_requests": 0,
        "os_signal_delivery_tested": False,
        "periodic_crash_recovery": False,
        "seven_day_validation_completed": False,
        "milestone_2_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    with (output / "verification.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
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
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
