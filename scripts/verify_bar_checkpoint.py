"""Finite offline actual-capture handoff using saved public trades and mocked clocks.

Wire containers and boundaries are reconstructed; original WS bytes are unavailable.
No new public connection or request occurs. Two separate cohorts never bridge the
documented missing native trade. Synthetic shutdown/restart/close clocks are explicit.
"""

import argparse
import asyncio
import json
import shutil
from decimal import Decimal
from pathlib import Path
from typing import cast
from unittest.mock import patch

import duckdb
from pydantic import JsonValue

from selective_options import collectors, ingestion, spot_trades
from selective_options.archive import _verify_archive, file_digest
from selective_options.bar_checkpoint import BarCheckpointResume, verify_bar_checkpoint_seed
from selective_options.bar_state import decode_bar_state, encode_bar_state
from selective_options.collectors import DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.research import _path, research_features
from selective_options.spot_trades import SpotTradeCollector, stored_trade, trade_event
from selective_options.store import complete_thread

ROOT = Path(__file__).resolve().parents[1]
PRIOR_ZIP_SHA = "5019f646b9b554801a0e48c9ba6e539a78e8b0336f7c77aa976dff2f0288514d"


def hashes(directory: Path, *, max_files: int = 64) -> dict[str, str]:
    paths = sorted(directory.rglob("*"))
    if len(paths) > 128 or any(
        path.is_symlink()
        or (path.exists() and getattr(path.lstat(), "st_file_attributes", 0) & 0x400)
        for path in (directory, *directory.absolute().parents, *paths)
    ):
        raise ValueError("proof tree links or entry budget")
    files = [path for path in paths if path.is_file()]
    if (
        len(files) > max_files
        or sum(path.stat().st_size for path in files) > 64 * 1024 * 1024
        or any(path.suffix in {".wal", ".lock"} for path in files)
    ):
        raise ValueError("proof source must be closed and within file/byte budgets")
    return {path.relative_to(directory).as_posix(): file_digest(path) for path in files}


def code_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): file_digest(path)
        for path in sorted((ROOT / "src/selective_options").glob("*.py"))
    }


def rows(database: Path) -> tuple[MarketEvent, ...]:
    if database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("BAR_PROOF_SOURCE_MUST_BE_CLOSED")
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id LIMIT 1001")
        names = [item[0] for item in cursor.description]
        data = cursor.fetchall()
    if len(data) > 1000:
        raise IntegrityError("BAR_PROOF_ROW_BUDGET")
    return tuple(
        MarketEvent.model_validate(dict(zip(names, value, strict=True)), strict=True)
        for value in data
    )


def exact_archive(directory: Path) -> int:
    database = directory / "events.duckdb"
    parquet = directory / "events.parquet"
    _verify_archive(parquet)
    native = rows(database)
    with duckdb.connect(str(database), read_only=True) as connection:
        for query in (
            "SELECT * FROM events EXCEPT SELECT * FROM read_parquet(?,hive_partitioning=false)",
            "SELECT * FROM read_parquet(?,hive_partitioning=false) EXCEPT SELECT * FROM events",
        ):
            assert not connection.execute(query, [str(parquet)]).fetchall()
        assert connection.execute(
            "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id"
        ).fetchall() == [(row.event_id, False) for row in native]
        for row in native:
            boundary = max(row.event_time_ms, row.observed_at_ms)
            assert connection.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row.event_id, boundary - 1, boundary - 1],
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                [row.event_id, boundary, boundary],
            ).fetchone() == (1,)
    return len(native) * 2


def check_bar(bar: MarketEvent, native: tuple[MarketEvent, ...]) -> None:
    payload = bar.payload()
    selected = sorted(
        (
            row
            for row in native
            if cast(int, payload["open_time_ms"])
            <= row.event_time_ms
            < cast(int, payload["close_time_ms"])
        ),
        key=lambda row: (row.event_time_ms, cast(int, row.payload()["trade_id"])),
    )
    prices = [cast(float, row.payload()["price"]) for row in selected]
    volume = sum((Decimal(str(row.payload()["qty"])) for row in selected), Decimal("0"))
    assert prices
    assert (payload["open"], payload["high"], payload["low"], payload["close"]) == (
        prices[0],
        max(prices),
        min(prices),
        prices[-1],
    )
    assert payload["trade_count"] == len(native) == len(selected)
    assert payload["volume"] == float(volume) and payload["complete"] is False


async def phase(
    cohort: tuple[MarketEvent, ...],
    output: Path,
    clock: dict[str, int],
    checkpoint: Path | None = None,
) -> dict[str, object]:
    config = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "policy_news_sources": [],
            "derivatives_enabled": False,
            "bar_timeframes_s": [300, 900],
            "bar_lateness_s": 5,
            "flush_interval_s": 0.001,
        },
    )
    restore: dict[str, object] = {}

    class ReplayCollector(SpotTradeCollector):
        async def run(self) -> None:
            self.symbols = {"BTC/USD": "BTC"}
            if checkpoint is None:
                self.awaiting_snapshot.add("BTC/USD")
                for index, event in enumerate(cohort):
                    message: dict[str, JsonValue] = {
                        "channel": "trade",
                        "type": "snapshot" if index == 0 else "update",
                        "data": [stored_trade(event).model_dump(mode="json")],
                    }
                    await self._trades(message, event.observed_at_ms)
                await self._flush(clock["now"])
            else:
                assert self.awaiting_snapshot == {"BTC/USD"}
                state = decode_bar_state(encode_bar_state(self.bars["BTC/USD"], clock["now"]))
                assert all(bucket.complete is False for bucket in state.buckets)
                assert state.coverage_start_ms == clock["now"]
                assert self.bar_inputs["BTC/USD"] == cohort
                restore.update(
                    {
                        "restart_ms": clock["now"],
                        "coverage_start_ms": state.coverage_start_ms,
                        "watermark_ms": state.watermark_ms,
                        "all_retained_partial": True,
                        "state_json": json.loads(
                            encode_bar_state(self.bars["BTC/USD"], clock["now"])
                        ),
                        "input_event_ids": [row.event_id for row in self.bar_inputs["BTC/USD"]],
                    }
                )
                # Same native tail, synthetic new delivery. Sequence deduplication
                # preserves original receipts in both ledger and copied database.
                await self._trades(
                    {
                        "channel": "trade",
                        "type": "snapshot",
                        "data": [stored_trade(cohort[-1]).model_dump(mode="json")],
                    },
                    clock["now"],
                )
                assert not self.awaiting_snapshot
                close_at = (
                    max(bucket.opening_ms + bucket.interval_ms for bucket in state.buckets)
                    + state.lateness_ms
                )
                clock["now"] = close_at
                restore["synthetic_close_request_ms"] = close_at
                await self._flush(close_at)
                assert self.bar_inputs["BTC/USD"] == ()
                assert not self.bars["BTC/USD"].buckets
            await asyncio.Event().wait()

    async def idle(*args: object) -> None:
        await asyncio.Event().wait()

    async def observed_seed(
        database: Path, resume: BarCheckpointResume, now_ms: int, **kwargs: int
    ) -> dict[str, object]:
        verified = await verify_bar_checkpoint_seed(database, resume, now_ms, **kwargs)
        # Proof observation only: capture's already verified, closed destination
        # bytes are retained before reopen. Equal records need not imply equal
        # DuckDB file bytes across independently constructed databases.
        copied = output / "seed-verified.duckdb"
        await complete_thread(shutil.copyfile, database, copied)
        assert file_digest(copied) == verified["database_sha256"]
        return verified

    with (
        patch.object(ingestion, "now_ms", lambda: clock["now"]),
        patch.object(spot_trades, "now_ms", lambda: clock["now"]),
        patch.object(collectors, "now_ms", lambda: clock["now"]),
        patch.object(ingestion, "SpotTradeCollector", ReplayCollector),
        patch.object(ingestion, "verify_bar_checkpoint_seed", observed_seed),
        patch.object(DeribitCollector, "run", idle),
        patch.object(ingestion, "poll_chains", idle),
    ):
        report = await capture(
            config,
            output,
            0.15,
            resume_bars_from=checkpoint,
            save_bars_on_stop=True,
        )
    assert report["status"] == "CAPTURE_FINISHED"
    assert report["milestone_2_complete"] is False and report["orders_submitted"] == 0
    assert report["ready_for_signals"] is False and report["trading_enabled"] is False
    if checkpoint is not None:
        (output / "restored.json").write_text(
            json.dumps(restore, indent=2) + "\n", encoding="utf-8"
        )
    stored = rows(output / "events.duckdb")
    trades = tuple(
        sorted(
            (row for row in stored if row.kind == "trade"),
            key=lambda row: cast(int, row.payload()["trade_id"]),
        )
    )
    assert trades == cohort
    bars = tuple(row for row in stored if row.kind == "bar")
    assert len(bars) == (0 if checkpoint is None else 2)
    for bar in bars:
        check_bar(bar, cohort)
    return {
        "capture_report_sha256": file_digest(output / "report.json"),
        "checkpoint_manifest_sha256": file_digest(output / "bar-checkpoint/checkpoint.json"),
        "trade_event_ids": [row.event_id for row in trades],
        "partial_bar_ids": [row.event_id for row in bars],
        "pit_boundary_checks": exact_archive(output),
        "closed_archive_exact": True,
        "pending_outbox_exact": True,
        "restored": restore if checkpoint is not None else None,
        "closed_seed_observation_sha256": file_digest(output / "seed-verified.duckdb")
        if checkpoint is not None
        else None,
        "seed_observation_scope": "proof copy of verified closed destination before reopen/producers"
        if checkpoint is not None
        else None,
    }


async def verify(source: Path, features: Path, output: Path) -> dict[str, object]:
    source, features, output = await complete_thread(
        lambda: (
            _path(source / "report.json").parent,
            _path(features),
            _path(output, must_exist=False),
        )
    )
    scratch = ROOT.parents[1] / "work/bar-checkpoint-0346"
    if output != ROOT / "evidence/bar-checkpoint-resume-01" and not output.is_relative_to(scratch):
        raise ValueError("proof output must be final evidence or named scratch directory")
    if output.exists() or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError("proof output must be new and separate")
    original = hashes(source)
    prior = hashes(ROOT / "evidence/parallel-foundations-01")
    feature_sha = file_digest(features)
    code = code_hashes()
    verifier_sha = file_digest(Path(__file__))
    assert file_digest(ROOT / "../selective-options-0.3.45.zip") == PRIOR_ZIP_SHA
    source_report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    assert source_report["status"] == "PUBLIC_GAP_TEST_PASSED"
    assert source_report["omitted_trade_id"] == 109230677
    _verify_archive(source / "events.parquet")
    native = tuple(
        sorted(
            (
                row
                for row in rows(source / "events.duckdb")
                if row.source == "kraken" and row.kind == "trade"
            ),
            key=lambda row: cast(int, row.payload()["trade_id"]),
        )
    )
    cohorts: list[list[MarketEvent]] = []
    previous: int | None = None
    for row in native:
        assert (row.environment, row.asset, row.symbol) == ("public_reference", "BTC", "BTC/USD")
        assert trade_event(stored_trade(row), row.asset, row.observed_at_ms) == row
        identifier = cast(int, row.payload()["trade_id"])
        if previous is None or identifier != previous + 1:
            cohorts.append([])
        cohorts[-1].append(row)
        previous = identifier
    assert len(native) == 55 and [len(group) for group in cohorts] == [50, 5]
    output.mkdir(parents=True, exist_ok=False)
    phases: list[dict[str, object]] = []
    for index, group in enumerate(cohorts):
        cohort = tuple(group)
        cutoff = max(max(row.event_time_ms, row.observed_at_ms) for row in cohort) + 1
        clock = {"now": cutoff}
        initial = await phase(cohort, output / f"cohort-{index}-initial", clock)
        clock["now"] = cutoff + 1000
        resumed = await phase(
            cohort,
            output / f"cohort-{index}-resumed",
            clock,
            output / f"cohort-{index}-initial/bar-checkpoint",
        )
        phases.append(
            {
                "cohort": index,
                "native_count": len(cohort),
                "synthetic_checkpoint_cutoff_ms": cutoff,
                "initial": initial,
                "resumed": resumed,
            }
        )
    fixture = rows(features)
    assert len(fixture) == 240 and all(row.source == "synthetic_fixture" for row in fixture)
    feature_result = research_features(
        features,
        output / "trend-result",
        asset="BTC",
        environment="public_reference",
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        as_of_ms=max(row.observed_at_ms for row in fixture),
        annualization_periods_per_year=365 * 24 * 12,
        include_trend=True,
    )
    assert feature_result["source_database_sha256"] == feature_sha
    assert hashes(source) == original and hashes(ROOT / "evidence/parallel-foundations-01") == prior
    assert code_hashes() == code and file_digest(Path(__file__)) == verifier_sha
    assert file_digest(ROOT / "../selective-options-0.3.45.zip") == PRIOR_ZIP_SHA
    report: dict[str, object] = {
        "status": "SAVED_NATIVE_COMMITTED_BAR_RESUME_PASSED",
        "source_hashes": original,
        "prior_foundation_hashes": prior,
        "prior_zip_sha256": PRIOR_ZIP_SHA,
        "code_hashes": code,
        "verifier_sha256": verifier_sha,
        "feature_source_sha256": feature_sha,
        "feature_result_sha256": file_digest(output / "trend-result/features.json"),
        "saved_native_trade_count": 55,
        "cohort_sizes": [50, 5],
        "missing_trade_id_not_repaired": 109230677,
        "native_payloads_and_first_receipts_unchanged": True,
        "wire_containers_reconstructed": True,
        "original_frame_bytes_available": False,
        "capture_transport_and_lifecycle_clocks": "explicit mocked fixtures, not current public operation",
        "captures": phases,
        "actual_supervised_captures": 4,
        "derived_partial_bar_count": 4,
        "committed_stopped_checkpoint_resume_implemented": True,
        "periodic_crash_recovery": False,
        "feature_bar_count": 240,
        "feature_data_kind": "synthetic_fixture",
        "new_public_requests": 0,
        "historical_strategy_trials": 0,
        "full_ingestion_complete": False,
        "full_feature_layer_complete": False,
        "full_backtester_complete": False,
        "seven_day_validation_completed": False,
        "paper_days": 0,
        "paper_trades": 0,
        "ready_for_signals": False,
        "trading_enabled": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "evidence/live-gap-02")
    parser.add_argument(
        "--features",
        type=Path,
        default=ROOT / "evidence/parallel-foundations-01/features-fixture.duckdb",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/bar-checkpoint-resume-01")
    args = parser.parse_args()
    report = asyncio.run(verify(args.source, args.features, args.output))
    print(
        json.dumps(
            {
                name: report[name]
                for name in (
                    "status",
                    "actual_supervised_captures",
                    "saved_native_trade_count",
                    "derived_partial_bar_count",
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
