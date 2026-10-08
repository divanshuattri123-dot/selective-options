"""Offline saved-native bar state plus labelled feature/backtest fixture proof.

This is not a public collection, real trading, all-feed validation, or a committed
capture restart. A single actual registered research run is deliberately losing.
"""

import argparse
import asyncio
import json
import math
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb
from pydantic import JsonValue

from selective_options.archive import _verify_archive, file_digest, seal_archive
from selective_options.bar_state import decode_bar_state, encode_bar_state, restore_bar_state
from selective_options.bars import TradeBars
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.features import compute_features
from selective_options.research import _path, research_backtest, research_features
from selective_options.spot_trades import stored_trade, trade_event
from selective_options.store import DuckStore, complete_thread
from selective_options.trials import read_trials, trial_counts

ROOT = Path(__file__).resolve().parents[1]
MAX_SOURCE_ROWS = 256


def code_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): file_digest(path)
        for path in sorted((ROOT / "src/selective_options").glob("*.py"))
    }


def read_events(path: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(path), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id LIMIT ?", [1001])
        columns = [value[0] for value in cursor.description]
        values = cursor.fetchall()
    if len(values) > 1000:
        raise IntegrityError("PARALLEL_PROOF_ROW_BUDGET")
    return tuple(
        MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
        for row in values
    )


def source_hashes(source: Path) -> dict[str, str]:
    if not source.is_dir():
        raise ValueError("proof source must be an existing directory")
    files: list[Path] = []
    pending = [source]
    entries = 0
    while pending:
        directory = pending.pop()
        if directory.is_symlink() or getattr(directory.lstat(), "st_file_attributes", 0) & 0x400:
            raise ValueError("proof source symlink/reparse/junction paths are refused")
        for path in directory.iterdir():
            entries += 1
            if entries > 64:
                raise IntegrityError("PARALLEL_PROOF_SOURCE_BUDGET")
            if path.is_symlink() or getattr(path.lstat(), "st_file_attributes", 0) & 0x400:
                raise ValueError("proof source symlink/reparse/junction paths are refused")
            if path.is_dir():
                pending.append(path)
            elif path.is_file():
                files.append(path)
            else:
                raise ValueError("proof source contains a nonregular entity")
            if len(files) > 32:
                raise IntegrityError("PARALLEL_PROOF_SOURCE_BUDGET")
    if sum(path.stat().st_size for path in files) > 32 * 1024 * 1024:
        raise IntegrityError("PARALLEL_PROOF_SOURCE_BUDGET")
    files.sort()
    return {path.relative_to(source).as_posix(): file_digest(path) for path in files}


def _fixture_bar(index: int) -> MarketEvent:
    opening = 300000 + index * 300000
    closing = opening + 300000
    return make_event(
        source="synthetic_fixture",
        environment="public_reference",
        asset="BTC",
        symbol="FIXTURE-BTC/USD",
        kind="bar",
        event_time_ms=closing,
        observed_at_ms=closing + 10,
        payload={
            "open_time_ms": opening,
            "close_time_ms": closing,
            "timeframe_s": 300,
            "open": 100.0 + index,
            "close": 101.0 + index,
            "high": 102.0 + index,
            "low": 99.0 + index,
            "volume": 1.0,
            "trade_count": 2,
            "closed": True,
            "complete": True,
            "volume_unit": "base_asset",
            "aggregation_version": 1,
            "lateness_ms": 10,
            "coverage_start_ms": 300000,
        },
    )


def _check_ohlcv(bar: MarketEvent, native: tuple[MarketEvent, ...]) -> None:
    payload = bar.payload()
    selected = sorted(
        (
            event
            for event in native
            if cast(int, payload["open_time_ms"])
            <= event.event_time_ms
            < cast(int, payload["close_time_ms"])
        ),
        key=lambda event: (event.event_time_ms, cast(int, event.payload()["trade_id"])),
    )
    assert selected
    prices = [cast(float, row.payload()["price"]) for row in selected]
    volume = sum((Decimal(str(row.payload()["qty"])) for row in selected), Decimal("0"))
    assert (payload["open"], payload["high"], payload["low"], payload["close"]) == (
        prices[0],
        max(prices),
        min(prices),
        prices[-1],
    )
    assert payload["volume"] == float(volume) and payload["trade_count"] == len(selected)
    assert payload["complete"] is False


async def verify(source: Path, output: Path) -> dict[str, object]:
    source, output = await complete_thread(
        lambda: (_path(source / "report.json").parent, _path(output, must_exist=False))
    )
    if (
        output.exists()
        or source == output
        or source.is_relative_to(output)
        or output.is_relative_to(source)
    ):
        raise ValueError("proof output must be new and separate from its source")
    scratch = ROOT.parents[1] / "work/parallel-foundations-0345"
    if output != ROOT / "evidence/parallel-foundations-01" and not output.is_relative_to(scratch):
        raise ValueError("proof output must be final evidence or the named scratch directory")
    before = source_hashes(source)
    code = code_hashes()
    verifier_hash = file_digest(Path(__file__))
    preserved_paths = (
        "../selective-options-0.3.44.zip",
        "evidence/spot-batch-saved-public-01/report.json",
        "evidence/spot-batch-parent-validation-0344-02.json",
    )
    preserved = {name: file_digest(ROOT / name) for name in preserved_paths}
    assert (
        preserved["../selective-options-0.3.44.zip"]
        == "211521b5c2e2c3b22ab677df3e4a3f9cbc1952552cfaab31c60f4e50d8a26d82"
    )
    source_report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    assert source_report["status"] == "PUBLIC_GAP_TEST_PASSED"
    assert source_report["orders_submitted"] == 0 and source_report["trading_enabled"] is False
    rows = read_events(source / "events.duckdb")
    assert len(rows) <= MAX_SOURCE_ROWS
    _verify_archive(source / "events.parquet")
    with duckdb.connect(str(source / "events.duckdb"), read_only=True) as connection:
        assert not connection.execute(
            "SELECT * FROM events EXCEPT SELECT * FROM read_parquet(?,hive_partitioning=false)",
            [str(source / "events.parquet")],
        ).fetchall()
        assert not connection.execute(
            "SELECT * FROM read_parquet(?,hive_partitioning=false) EXCEPT SELECT * FROM events",
            [str(source / "events.parquet")],
        ).fetchall()
    native = tuple(
        sorted(
            (row for row in rows if row.source == "kraken" and row.kind == "trade"),
            key=lambda row: cast(int, row.payload()["trade_id"]),
        )
    )
    assert len(native) == 55
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
    assert [len(cohort) for cohort in cohorts] == [50, 5]
    assert source_report["omitted_trade_id"] == 109230677
    output.mkdir(parents=True, exist_ok=False)
    store = await DuckStore.open(output / "native-and-bars.duckdb")
    derived: list[MarketEvent] = []
    states: list[dict[str, object]] = []
    try:
        await store.append(list(native), exact=True)
        for index, values in enumerate(cohorts):
            cohort = tuple(values)
            cutoff = max(max(event.event_time_ms, event.observed_at_ms) for event in cohort) + 1
            builder = TradeBars((300, 900), min(event.observed_at_ms for event in cohort), 5000)
            for event in cohort:
                builder.add(event)
            already_closed = builder.close_ready(cutoff)
            # This preserved sample has not reached a complete bar boundary.
            assert not already_closed
            encoded = encode_bar_state(builder, cutoff)
            assert encode_bar_state(builder, cutoff) == encoded
            state = decode_bar_state(encoded)
            for bucket in state.buckets:
                assert bucket.prototype.event() in native
                selected = tuple(
                    event
                    for event in cohort
                    if bucket.opening_ms
                    <= event.event_time_ms
                    < bucket.opening_ms + bucket.interval_ms
                )
                assert bucket.count == len(selected)
                assert bucket.volume() == sum(
                    (Decimal(str(row.payload()["qty"])) for row in selected), Decimal("0")
                )
            restart = cutoff + 1000
            restored = restore_bar_state(
                encoded, restart, timeframes_s=(300, 900), lateness_ms=5000
            )
            assert restored.watermark_ms == builder.watermark_ms
            assert all(not bucket.complete for bucket in restored.buckets.values())
            closing = (
                max(bucket.opening_ms + bucket.interval_ms for bucket in restored.buckets.values())
                + 5000
            )
            resumed = restored.close_ready(closing)
            for bar in resumed:
                _check_ohlcv(bar, cohort)
                assert bar.payload()["coverage_start_ms"] == restart
                PointInTimeSnapshot(bar.observed_at_ms, bar.asset, bar.environment, (bar,))
                try:
                    PointInTimeSnapshot(bar.observed_at_ms - 1, bar.asset, bar.environment, (bar,))
                except IntegrityError:
                    pass
                else:
                    raise AssertionError("derived bar leaked before synthetic availability")
            assert not restored.close_ready(closing + 900000)
            path = output / f"bar-state-cohort-{index}.json"
            path.write_bytes(encoded + b"\n")
            states.append(
                {
                    "cohort": index,
                    "native_inputs": len(cohort),
                    "native_input_ids": [row.event_id for row in cohort],
                    "as_of_ms": cutoff,
                    "synthetic_restart_ms": restart,
                    "derived_bar_ids": [row.event_id for row in resumed],
                    "state_file": path.name,
                    "state_sha256": file_digest(path),
                    "retained_buckets": len(state.buckets),
                    "watermark_ms": state.watermark_ms,
                }
            )
            derived.extend(resumed)
        await store.append(derived, exact=True)
        archive_rows = await store.export_parquet(output / "native-and-bars.parquet")
        await seal_archive(output / "native-and-bars.parquet", archive_rows)
        assert await store.pending_delivery_count() == len(native) + len(derived)
    finally:
        await store.close()
    saved = read_events(output / "native-and-bars.duckdb")
    assert tuple(row for row in saved if row.kind == "trade") == tuple(
        sorted(native, key=lambda row: row.event_id)
    )
    assert len(saved) == len(native) + len(derived)
    assert saved == tuple(sorted((*native, *derived), key=lambda row: row.event_id))
    _verify_archive(output / "native-and-bars.parquet")
    with duckdb.connect(str(output / "native-and-bars.duckdb"), read_only=True) as connection:
        assert (
            connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == connection.execute(
                "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                [str(output / "native-and-bars.parquet")],
            ).fetchall()
        )
    fixture = tuple(_fixture_bar(index) for index in range(240))
    fixture_store = await DuckStore.open(output / "features-fixture.duckdb")
    try:
        await fixture_store.append(list(fixture), exact=True)
    finally:
        await fixture_store.close()
    feature_report = research_features(
        output / "features-fixture.duckdb",
        output / "feature-result",
        asset="BTC",
        environment="public_reference",
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        as_of_ms=fixture[-1].observed_at_ms,
        annualization_periods_per_year=105120,
    )
    feature_body = cast(dict[str, JsonValue], feature_report["feature_report"])
    assert feature_body["state"] == "FEATURES_COMPUTED" and not feature_body["blockers"]
    assert feature_body["bars_available"] == feature_body["bars_used"] == 240
    computed = {
        cast(dict[str, JsonValue], row)["name"]: cast(dict[str, JsonValue], row)
        for row in cast(list[JsonValue], feature_body["features"])
    }
    latest = 340.0
    assert all(
        value["state"] == "AVAILABLE" and value["value"] is not None for value in computed.values()
    )
    for period in (8, 21, 50, 200):
        assert math.isclose(
            cast(float, computed[f"ema_{period}"]["value"]),
            latest - (period - 1) / 2,
            abs_tol=1e-10,
        )
        assert math.isclose(
            cast(float, computed[f"ema_{period}_slope"]["value"]), 1.0, abs_tol=1e-10
        )
    assert computed["rsi_14"]["value"] == 100 and computed["atr_14"]["value"] == 3
    assert math.isclose(cast(float, computed["macd_12_26"]["value"]), 7, abs_tol=1e-10)
    assert math.isclose(cast(float, computed["macd_signal_9"]["value"]), 7, abs_tol=1e-10)
    assert math.isclose(cast(float, computed["macd_histogram"]["value"]), 0, abs_tol=1e-10)
    assert math.isclose(
        cast(float, computed["bollinger_width_20"]["value"]),
        4 * math.sqrt(399 / 12) / (latest - 9.5),
        abs_tol=1e-10,
    )
    for window in (5, 20):
        assert math.isclose(
            cast(float, computed[f"roc_{window}"]["value"]),
            latest / (latest - window) - 1,
            abs_tol=1e-10,
        )
    for window in (20, 60):
        returns = [
            math.log((latest - window + index + 1) / (latest - window + index))
            for index in range(window)
        ]
        mean = sum(returns) / window
        expected = (sum((value - mean) ** 2 for value in returns) / (window - 1) * 105120) ** 0.5
        assert math.isclose(
            cast(float, computed[f"realized_vol_{window}"]["value"]),
            expected,
            rel_tol=1e-9,
            abs_tol=1e-12,
        )
    # Fabricated snapshot deliberately bypasses constructor: feature boundary must still refuse.
    malformed = PointInTimeSnapshot(fixture[-1].observed_at_ms, "BTC", "public_reference", fixture)
    object.__setattr__(malformed, "as_of_ms", fixture[-1].observed_at_ms - 1)
    try:
        compute_features(
            malformed, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
        )
    except IntegrityError as exc:
        leaked = False
        leakage_reason = str(exc)
    else:
        raise AssertionError("feature future receipt was accepted")
    input_path = ROOT / "examples/backtest-known-bad.json"
    input_digest = file_digest(input_path)
    registry = output / "trials.jsonl"
    backtest = research_backtest(input_path, output / "backtest-result", registry)
    simulation = cast(dict[str, JsonValue], backtest["simulation"])
    assert Decimal(cast(str, simulation["closed_roundtrip_net_pnl"])) == Decimal("-44")
    assert Decimal(cast(str, simulation["total_fees"])) == Decimal("4")
    assert trial_counts(registry) == {
        "started": 1,
        "completed": 1,
        "failed": 0,
        "unresolved": 0,
        "trial_count_N": 1,
    }
    recorded = read_trials(registry)
    assert (
        recorded[0].parameters is not None
        and recorded[0].parameters["input_sha256"] == input_digest
    )
    assert (
        source_hashes(source) == before
        and code_hashes() == code
        and file_digest(input_path) == input_digest
        and file_digest(Path(__file__)) == verifier_hash
        and {name: file_digest(ROOT / name) for name in preserved_paths} == preserved
    )
    report: dict[str, object] = {
        "status": "PARALLEL_FOUNDATIONS_VERIFIED",
        "source_evidence": str(source),
        "source_sha256": before,
        "tested_source_sha256": code,
        "verifier_sha256": verifier_hash,
        "preserved_artifact_sha256": preserved,
        "saved_native_trade_count": 55,
        "native_cohort_lengths": [50, 5],
        "missing_ws_trade_id": 109230677,
        "original_frame_replay_claimed": False,
        "cross_gap_continuity_claimed": False,
        "native_clocks_payloads_ids_preserved": True,
        "derived_bar_count": len(derived),
        "bar_states": states,
        "bar_state_committed_capture_resume_implemented": False,
        "bar_restart_clocks_are_synthetic": True,
        "feature_bar_count": 240,
        "feature_data_kind": "synthetic_fixture",
        "future_receipt_leaked": leaked,
        "future_receipt_refusal": leakage_reason,
        "fabricated_leakage_cutoff_ms": fixture[-1].observed_at_ms - 1,
        "offending_fixture_bar_id": fixture[-1].event_id,
        "offending_fixture_receipt_ms": fixture[-1].observed_at_ms,
        "manual_ema_rsi_atr_macd_bollinger_checks": True,
        "known_bad_fixture_net_pnl": "-44",
        "fee_policy_is_synthetic": True,
        "backtest_trials_registered": 1,
        "historical_strategy_trials": 0,
        "registry_counts": trial_counts(registry),
        "input_sha256": input_digest,
        "new_public_requests": 0,
        "full_ingestion_complete": False,
        "full_feature_layer_complete": False,
        "full_backtester_complete": False,
        "seven_day_validation_completed": False,
        "paper_days": 0,
        "paper_trades": 0,
        "trading_enabled": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "evidence/live-gap-02")
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/parallel-foundations-01")
    args = parser.parse_args()
    result = asyncio.run(verify(args.source, args.output))
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "status",
                    "saved_native_trade_count",
                    "derived_bar_count",
                    "known_bad_fixture_net_pnl",
                    "backtest_trials_registered",
                    "trading_enabled",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
