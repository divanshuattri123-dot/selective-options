"""Independent read-only audit of finite committed stopped-bar capture handoffs.

No producer, checkpoint save/load/install, network, or strategy run is executed.
Fixed proof paths and exact saved membership are audited from immutable bytes;
synthetic lifecycle clocks do not establish a sustained live capture.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
import statistics
from copy import deepcopy
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Any, cast

import duckdb

if TYPE_CHECKING:
    # The project checks scripts as standalone modules. Keep the static name
    # consistent with the other verifiers, while -m execution uses its package.
    from audit_parallel_foundations import (
        check_ohlc,
        database_audit,
        digest,
        load,
        require,
        rows,
        sha,
        tree,
    )
else:
    from .audit_parallel_foundations import (
        check_ohlc,
        database_audit,
        digest,
        load,
        require,
        rows,
        sha,
        tree,
    )
from selective_options.events import MarketEvent
from selective_options.rotation_runtime import drain_shutdown
from selective_options.store import complete_thread

ROOT = Path(__file__).resolve().parents[1]
PRIOR_ZIP_SHA = "5019f646b9b554801a0e48c9ba6e539a78e8b0336f7c77aa976dff2f0288514d"
PRIOR_REPORT_SHA = "ddee97d01e2095473e5c6286848433e4e0e6d25e714321d38264642ef84f3b67"
SCOPE = {"source": "kraken", "environment": "public_reference", "asset": "BTC", "symbol": "BTC/USD"}
CONTROLS = (
    "history_complete",
    "source_frontier_complete",
    "current_feed_health_verified",
    "delivery_acknowledgements_copied",
    "source_database_hash_claimed",
    "periodic_crash_recovery",
    "ready_for_signals",
    "trading_enabled",
)


def _integer(value: object) -> int:
    require(type(value) is int, "STRICT_INTEGER_REQUIRED")
    return cast(int, value)


def _native_id(row: MarketEvent) -> int:
    return _integer(row.payload()["trade_id"])


def _native(values: tuple[MarketEvent, ...]) -> tuple[MarketEvent, ...]:
    return tuple(sorted((r for r in values if r.kind == "trade"), key=_native_id))


def _schema(database: Path) -> None:
    expected = [
        (
            name,
            "BIGINT"
            if name in {"event_time_ms", "observed_at_ms"}
            else "INTEGER"
            if name == "schema_version"
            else "VARCHAR",
        )
        for name in MarketEvent.model_fields
    ]
    with duckdb.connect(str(database), read_only=True) as connection:
        require(
            connection.execute("SHOW TABLES").fetchall() == [("delivery_outbox",), ("events",)],
            "MINIMAL_DATABASE_TABLE_SET",
        )
        for table, columns in (
            ("events", expected),
            ("delivery_outbox", [("event_id", "VARCHAR"), ("delivered", "BOOLEAN")]),
        ):
            actual = connection.execute("DESCRIBE " + table).fetchall()
            require(
                [(column[0], column[1]) for column in actual] == columns,
                "DATABASE_TYPED_COLUMN_ORDER",
            )


def checkpoint_audit(
    directory: Path,
    cohort: tuple[MarketEvent, ...],
    cutoff: int,
    *,
    closed: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = load(directory / "checkpoint.json")
    expected_keys = {
        "format_version",
        "status",
        "as_of_ms",
        "events_file",
        "events_sha256",
        "events_bytes",
        "event_ids",
        "books",
        "source_scope",
        *CONTROLS,
    }
    require(set(manifest) == expected_keys, "CHECKPOINT_EXACT_FIELDS")
    require(
        type(manifest["format_version"]) is int and manifest["format_version"] == 1,
        "CHECKPOINT_VERSION",
    )
    require(
        manifest["status"] == "COMMITTED_BAR_CHECKPOINT" and manifest["as_of_ms"] == cutoff,
        "CHECKPOINT_CUTOFF_STATUS",
    )
    require(all(manifest[name] is False for name in CONTROLS), "CHECKPOINT_FALSE_CONTROLS")
    require(
        manifest["source_scope"]
        == "exact supplied consumed WS inputs and book tails committed at cutoff",
        "CHECKPOINT_SCOPE_CLAIM",
    )
    require(manifest["events_file"] == "events.duckdb", "CHECKPOINT_FIXED_DATABASE")
    database = directory / "events.duckdb"
    require(
        manifest["events_sha256"] == digest(database)
        and type(manifest["events_bytes"]) is int
        and manifest["events_bytes"] == database.stat().st_size,
        "CHECKPOINT_DATABASE_SEAL",
    )
    _schema(database)
    saved, pit = database_audit(database)
    expected = tuple(sorted((cohort[-1],) if closed else cohort, key=lambda r: r.event_id))
    require(
        saved == expected and manifest["event_ids"] == [r.event_id for r in expected],
        "CHECKPOINT_NATIVE_MEMBERSHIP",
    )
    require(
        all(max(r.event_time_ms, r.observed_at_ms) <= cutoff for r in saved),
        "CHECKPOINT_BOTH_CLOCKS",
    )
    require(
        type(manifest["books"]) is list and len(manifest["books"]) == 1, "CHECKPOINT_BOOK_COUNT"
    )
    part = manifest["books"][0]
    require(
        set(part) == {"state_file", "state_sha256", "state_bytes", "scope", "input_ids", "tail_id"},
        "CHECKPOINT_PART_FIELDS",
    )
    require(
        part["state_file"] == "state-000000.json" and part["scope"] == SCOPE,
        "CHECKPOINT_PART_FIXED_SCOPE",
    )
    state_path = directory / "state-000000.json"
    require(
        part["state_sha256"] == digest(state_path)
        and type(part["state_bytes"]) is int
        and part["state_bytes"] == state_path.stat().st_size,
        "CHECKPOINT_STATE_SEAL",
    )
    require(
        part["input_ids"] == ([] if closed else [r.event_id for r in cohort])
        and part["tail_id"] == cohort[-1].event_id,
        "CHECKPOINT_INPUT_AND_TAIL_IDS",
    )
    require(
        set(tree(directory)) == {"checkpoint.json", "events.duckdb", "state-000000.json"},
        "CHECKPOINT_FIXED_ARTIFACTS",
    )
    state = load(state_path)
    require(
        set(state)
        == {
            "format_version",
            "as_of_ms",
            "timeframes_ms",
            "lateness_ms",
            "coverage_start_ms",
            "watermark_ms",
            "scope",
            "buckets",
        },
        "STATE_EXACT_FIELDS",
    )
    require(
        type(state["format_version"]) is int
        and state["format_version"] == 1
        and state["as_of_ms"] == cutoff
        and state["scope"] == SCOPE,
        "STATE_CUTOFF_SCOPE",
    )
    require(
        state["timeframes_ms"] == [300000, 900000]
        and state["lateness_ms"] == 5000
        and state["watermark_ms"] == cutoff - 5000,
        "STATE_TIME_POLICY",
    )
    require(
        type(state["buckets"]) is list and len(state["buckets"]) == (0 if closed else 2),
        "STATE_RETAINED_BUCKET_COUNT",
    )
    memberships = []
    for bucket, interval in zip(state["buckets"], (300000, 900000), strict=False):
        opening = cohort[0].event_time_ms // interval * interval
        require(
            bucket["interval_ms"] == interval
            and bucket["opening_ms"] == opening
            and opening + interval > state["watermark_ms"],
            "STATE_OPEN_WINDOW",
        )
        require(
            bucket["prototype"] == cohort[0].model_dump(mode="json"), "STATE_EXACT_NATIVE_PROTOTYPE"
        )
        members = check_ohlc(bucket, cohort, opening, interval, bucket=True)
        require(members == tuple(r.event_id for r in cohort), "STATE_EXACT_INPUT_WINDOW")
        memberships.append(
            {"interval_ms": interval, "opening_ms": opening, "input_event_ids": list(members)}
        )
    return state, [
        {
            "checkpoint": directory.name,
            "both_clock_barriers": pit,
            "bucket_memberships": memberships,
        }
    ]


def phase_audit(
    directory: Path,
    metadata: dict[str, Any],
    cohort: tuple[MarketEvent, ...],
    cutoff: int,
    initial_state: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resumed = initial_state is not None
    require(
        metadata["capture_report_sha256"] == digest(directory / "report.json")
        and metadata["checkpoint_manifest_sha256"]
        == digest(directory / "bar-checkpoint/checkpoint.json"),
        "PHASE_REPORT_SEALS",
    )
    require(
        metadata["closed_archive_exact"] is True and metadata["pending_outbox_exact"] is True,
        "PHASE_ARCHIVE_DECLARATIONS",
    )
    capture = load(directory / "report.json")
    require(
        capture["status"] == "CAPTURE_FINISHED" and capture["failure_type"] is None,
        "CAPTURE_SUCCESS",
    )
    require(
        capture["milestone_2_complete"] is False
        and capture["ready_for_signals"] is False
        and capture["trading_enabled"] is False
        and capture["orders_submitted"] == 0,
        "CAPTURE_NO_PROMOTION",
    )
    _schema(directory / "events.duckdb")
    saved, pit = database_audit(directory / "events.duckdb", directory / "events.parquet")
    require(
        _native(saved) == cohort and metadata["trade_event_ids"] == [r.event_id for r in cohort],
        "PHASE_EXACT_NATIVE_ROWS",
    )
    derived = tuple(row for row in saved if row.kind == "bar")
    require(
        len(derived) == (2 if resumed else 0)
        and metadata["partial_bar_ids"] == [r.event_id for r in derived],
        "PHASE_DERIVED_IDS",
    )
    require(metadata["pit_boundary_checks"] == len(saved) * 2, "PHASE_PIT_DECLARATION")
    if resumed:
        require(
            {_integer(row.payload()["timeframe_s"]) for row in derived} == {300, 900},
            "DERIVED_EXACT_TIMEFRAME_SET",
        )
    if resumed:
        assert initial_state is not None
        restored = load(directory / "restored.json")
        require(restored == metadata["restored"], "RESTORED_REPORT_EQUALITY")
        restart = cutoff + 1000
        expected_state = deepcopy(initial_state)
        expected_state["as_of_ms"] = restart
        expected_state["coverage_start_ms"] = restart
        for bucket in expected_state["buckets"]:
            bucket["complete"] = False
        require(restored["state_json"] == expected_state, "RESTORED_EXACT_STATE_AND_PARTIAL")
        require(
            restored["restart_ms"] == restored["coverage_start_ms"] == restart
            and restored["watermark_ms"] == initial_state["watermark_ms"]
            and restored["all_retained_partial"] is True
            and restored["input_event_ids"] == [r.event_id for r in cohort],
            "RESTORED_FRONTIER",
        )
        close_at = max(b["opening_ms"] + b["interval_ms"] for b in initial_state["buckets"]) + 5000
        require(restored["synthetic_close_request_ms"] == close_at, "SYNTHETIC_CLOSE_CLOCK")
        resume = capture["bar_resume"]
        require(
            resume["status"] == "COMMITTED_SEED_VERIFIED_BEFORE_PRODUCERS"
            and resume["checkpoint_as_of_ms"] == cutoff
            and resume["restart_ms"] == restart
            and resume["books"] == 1
            and resume["events"] == len(cohort),
            "CAPTURE_RESUME_FRONTIER",
        )
        require(
            resume["original_receipts_preserved"] is True
            and resume["retained_buckets_partial"] is True
            and all(
                resume[name] is False
                for name in (
                    "history_complete",
                    "current_feed_health_verified",
                    "periodic_crash_recovery",
                )
            ),
            "CAPTURE_RESUME_DECLARATIONS",
        )
        seed = resume["closed_seed_verification"]
        require(
            seed["status"] == "BAR_CHECKPOINT_SEED_VERIFIED"
            and seed["events"] == len(cohort)
            and seed["books"] == 1
            and all(
                seed[name] is False
                for name in (
                    "source_mutated",
                    "delivery_acknowledgements_copied",
                    "history_complete",
                    "trading_enabled",
                )
            ),
            "CLOSED_SEED_VERIFICATION",
        )
        seed_path = directory / "seed-verified.duckdb"
        require(
            seed["database_sha256"]
            == metadata["closed_seed_observation_sha256"]
            == digest(seed_path)
            and metadata["seed_observation_scope"]
            == "proof copy of verified closed destination before reopen/producers",
            "CLOSED_SEED_OBSERVATION_SEAL",
        )
        _schema(seed_path)
        seed_rows, seed_barriers = database_audit(seed_path)
        require(
            seed_rows == tuple(sorted(cohort, key=lambda row: row.event_id)),
            "CLOSED_SEED_EXACT_MEMBERSHIP",
        )
        for bar in derived:
            payload = bar.payload()
            interval = _integer(payload["timeframe_s"]) * 1000
            opening = cohort[0].event_time_ms // interval * interval
            require(
                interval in (300000, 900000)
                and bar.event_time_ms == opening + interval
                and bar.observed_at_ms == close_at,
                "DERIVED_BOTH_CLOCKS",
            )
            require(
                (bar.source, bar.environment, bar.asset, bar.symbol) == tuple(SCOPE.values())
                and payload["closed"] is True
                and payload["aggregation_version"] == 1
                and payload["coverage_start_ms"] == restart
                and payload["lateness_ms"] == 5000
                and payload["volume_unit"] == "base_asset",
                "DERIVED_SCOPE_POLICY",
            )
            require(
                payload["open_time_ms"] == opening
                and payload["close_time_ms"] == opening + interval,
                "DERIVED_WINDOW",
            )
            require(
                check_ohlc(payload, cohort, opening, interval, bucket=False)
                == tuple(r.event_id for r in cohort),
                "DERIVED_INPUT_MEMBERSHIP",
            )
        state, checkpoint_details = checkpoint_audit(
            directory / "bar-checkpoint", cohort, close_at, closed=True
        )
        require(state["coverage_start_ms"] == restart, "CLOSED_STATE_COVERAGE")
    else:
        require(metadata["restored"] is None and "bar_resume" not in capture, "INITIAL_FRESH_SCOPE")
        require(
            all(max(row.event_time_ms, row.observed_at_ms) <= cutoff for row in saved),
            "INITIAL_FINITE_LIFECYCLE_CLOCKS",
        )
        state, checkpoint_details = checkpoint_audit(
            directory / "bar-checkpoint", cohort, cutoff, closed=False
        )
        require(state["coverage_start_ms"] == cohort[0].observed_at_ms, "INITIAL_COVERAGE_RECEIPT")
    require(
        capture["bar_checkpoint"] == load(directory / "bar-checkpoint/checkpoint.json"),
        "CAPTURE_MANIFEST_EQUALITY",
    )
    return state, {
        "directory": directory.name,
        "rows": len(saved),
        "partial_bars": len(derived),
        "pit": pit,
        "checkpoint": checkpoint_details,
        "closed_seed_barriers": seed_barriers if resumed else None,
    }


def trend_audit(proof: Path, root: Path, report: dict[str, Any]) -> dict[str, Any]:
    database = root / "evidence/parallel-foundations-01/features-fixture.duckdb"
    require(report["feature_source_sha256"] == digest(database), "FEATURE_SOURCE_SEAL")
    fixture = tuple(sorted(rows(database), key=lambda r: r.event_time_ms))
    require(
        len(fixture) == 240 and all(r.source == "synthetic_fixture" for r in fixture),
        "EXPLICIT_SYNTHETIC_FEATURE_INPUTS",
    )
    for index, row in enumerate(fixture):
        payload = row.payload()
        require(
            (payload["open"], payload["high"], payload["low"], payload["close"])
            == (100.0 + index, 102.0 + index, 99.0 + index, 101.0 + index)
            and payload["complete"] is True
            and payload["closed"] is True,
            "LINEAR_FEATURE_FIXTURE",
        )
    wrapper = load(proof / "trend-result/features.json")
    require(
        report["feature_result_sha256"] == digest(proof / "trend-result/features.json")
        and wrapper["source_database_sha256"] == digest(database)
        and wrapper["source_mutated"] is False,
        "FEATURE_RESULT_SEAL",
    )
    base = wrapper["feature_report"]
    trend = wrapper["trend_report"]
    require(
        base
        == load(root / "evidence/parallel-foundations-01/feature-result/features.json")[
            "feature_report"
        ],
        "PRESERVED_ANALYTICAL_BASE_RESULTS",
    )
    require(
        trend["base"] == base
        and base["bars_available"] == 240
        and base["blockers"] == []
        and base["as_of_ms"] == fixture[-1].observed_at_ms,
        "TREND_CAUSAL_BASE",
    )
    require(
        base["history_sha256"]
        == sha([[r.event_id, r.event_time_ms, r.observed_at_ms] for r in fixture]),
        "TREND_INPUT_DIGEST",
    )
    require(
        all(
            row.event_time_ms <= base["as_of_ms"] and row.observed_at_ms <= base["as_of_ms"]
            for row in fixture
        ),
        "TREND_BOTH_CLOCKS",
    )
    values = {v["name"]: v for v in trend["features"]}
    expected = {
        "plus_di_14": 100 / 3,
        "minus_di_14": 0.0,
        "adx_14": 100.0,
        "bollinger_width_20_percentile": 0.0,
        "realized_vol_20_percentile": 0.0,
    }
    require(set(values) == set(expected), "TREND_EXACT_FEATURE_SET")
    for name, value in expected.items():
        require(
            values[name]["state"] == "AVAILABLE"
            and type(values[name]["value"]) in (int, float)
            and math.isclose(values[name]["value"], value, rel_tol=0, abs_tol=1e-10),
            "TREND_ANALYTICAL_" + name,
        )
    # Linear prices imply strictly declining width and sample log-return RV.
    closes = [101.0 + index for index in range(240)]
    for initial, name in (
        (20, "bollinger_width_20_percentile"),
        (21, "realized_vol_20_percentile"),
    ):

        def measure(end: int, count: int = initial) -> float:
            window = closes[end - count + 1 : end + 1]
            if count == 20:
                return 4 * statistics.pstdev(window) / statistics.mean(window)
            returns = [
                math.log(right / left) for left, right in zip(window, window[1:], strict=False)
            ]
            return statistics.stdev(returns) * math.sqrt(105120)

        prior = [measure(index) for index in range(139, 239)]
        require(all(value > measure(239) for value in prior), "TREND_RANK_ANALYTICAL_PRIOR")
        reference = next(v for v in trend["percentile_references"] if v["name"] == name)
        require(
            reference
            == {
                "name": name,
                "window": 100,
                "minimum_history": 20,
                "candidate_observations": 100,
                "observations": 100,
                "first_close_ms": fixture[139].event_time_ms,
                "latest_close_ms": fixture[238].event_time_ms,
                "current_excluded": True,
                "tie_policy": "exact_midrank",
            },
            "TREND_EXACT_PRIOR_REFERENCE",
        )
    require(
        len(trend["percentile_references"]) == 2
        and all(
            component["ready_for_signals"] is False and component["trading_enabled"] is False
            for component in (wrapper, base, trend)
        )
        and trend["regime_classification_complete"] is False,
        "TREND_NO_PROMOTION",
    )
    return {
        "synthetic_bars": 240,
        "analytical_values": expected,
        "prior_references": trend["percentile_references"],
    }


def _audit(proof: Path, root: Path, require_current_code: bool) -> dict[str, Any]:
    auditor_sha = digest(Path(__file__))
    helper_sha = digest(root / "scripts/audit_parallel_foundations.py")
    require(
        helper_sha == "959b2e938dabb3bc6058f6f2a50ba660cfb76a5bcf748ed60079ca7941f23769",
        "FROZEN_SHARED_AUDIT_HELPER",
    )
    before = tree(proof)
    source = root / "evidence/live-gap-02"
    prior = root / "evidence/parallel-foundations-01"
    source_before, prior_before = tree(source), tree(prior)
    report = load(proof / "report.json")
    require(report["status"] == "SAVED_NATIVE_COMMITTED_BAR_RESUME_PASSED", "PROOF_STATUS")
    require(
        digest(prior / "report.json") == PRIOR_REPORT_SHA
        and report["prior_foundation_hashes"] == prior_before,
        "PRESERVED_FOUNDATION_SEAL",
    )
    require(
        report["source_hashes"] == source_before == load(prior / "report.json")["source_sha256"],
        "PRESERVED_NATIVE_SOURCE",
    )
    zip_path = root.parent / "selective-options-0.3.45.zip"
    require(
        digest(zip_path) == report["prior_zip_sha256"] == PRIOR_ZIP_SHA, "PRESERVED_RELEASE_SEAL"
    )
    recorded_code = report["code_hashes"]
    # Historical mode binds the producer's recorded identifiers without claiming
    # those old modules equal the installed release. Validate that map before
    # choosing either policy; it must never contain paths for arbitrary files.
    require(
        type(recorded_code) is dict
        and 1 <= len(recorded_code) <= 256
        and all(
            type(name) is str
            and re.fullmatch(r"src/selective_options/[A-Za-z_][A-Za-z0-9_]*\.py", name)
            and type(hashed) is str
            and re.fullmatch(r"[0-9a-f]{64}", hashed)
            for name, hashed in recorded_code.items()
        ),
        "STRICT_RECORDED_CODE_MAP",
    )
    require(
        type(report["verifier_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", report["verifier_sha256"]) is not None,
        "STRICT_RECORDED_PRODUCER_HASH",
    )
    current_code = {
        p.relative_to(root).as_posix(): digest(p)
        for p in sorted((root / "src/selective_options").glob("*.py"))
    }
    if require_current_code:
        require(set(recorded_code) == set(current_code), "EXACT_CODE_FILE_SET")
        require(
            recorded_code == current_code
            and report["verifier_sha256"] == digest(root / "scripts/verify_bar_checkpoint.py"),
            "FROZEN_CODE_OR_PRODUCER_CHANGED",
        )
    require(
        report["saved_native_trade_count"] == 55
        and report["cohort_sizes"] == [50, 5]
        and report["missing_trade_id_not_repaired"] == 109230677
        and report["actual_supervised_captures"] == 4
        and report["derived_partial_bar_count"] == 4,
        "EXACT_FINITE_PROOF_COUNTS",
    )
    for name in (
        "native_payloads_and_first_receipts_unchanged",
        "wire_containers_reconstructed",
        "committed_stopped_checkpoint_resume_implemented",
    ):
        require(report[name] is True, "PROOF_DECLARATION_" + name)
    for name in (
        "original_frame_bytes_available",
        "periodic_crash_recovery",
        "full_ingestion_complete",
        "full_feature_layer_complete",
        "full_backtester_complete",
        "seven_day_validation_completed",
        "ready_for_signals",
        "trading_enabled",
    ):
        require(report[name] is False, "PROOF_NO_PROMOTION_" + name)
    require(
        all(
            type(report[name]) is int and report[name] == 0
            for name in (
                "new_public_requests",
                "historical_strategy_trials",
                "paper_days",
                "paper_trades",
            )
        ),
        "NO_OPERATIONS_OR_TRIALS",
    )
    native = _native(tuple(r for r in rows(source / "events.duckdb") if r.source == "kraken"))
    require(
        len(native) == 55
        and all(
            (r.source, r.environment, r.asset, r.symbol) == tuple(SCOPE.values()) for r in native
        ),
        "NATIVE_EXACT_SCOPE",
    )
    cohorts = (native[:50], native[50:])
    require(
        all(
            all(
                _native_id(right) == _native_id(left) + 1
                for left, right in zip(group, group[1:], strict=False)
            )
            for group in cohorts
        )
        and _native_id(native[49]) + 2 == _native_id(native[50])
        and _native_id(native[49]) + 1 == 109230677,
        "SEPARATE_NATIVE_COHORTS_NO_GAP_REPAIR",
    )
    require(
        report["capture_transport_and_lifecycle_clocks"]
        == "explicit mocked fixtures, not current public operation"
        and report["feature_data_kind"] == "synthetic_fixture"
        and type(report["feature_bar_count"]) is int
        and report["feature_bar_count"] == 240,
        "EXPLICIT_FIXTURE_LABELS",
    )
    require(len(report["captures"]) == 2, "COHORT_PHASES")
    details = []
    for index, cohort in enumerate(cohorts):
        entry = report["captures"][index]
        cutoff = max(max(row.event_time_ms, row.observed_at_ms) for row in cohort) + 1
        require(
            entry["cohort"] == index
            and entry["native_count"] == len(cohort)
            and entry["synthetic_checkpoint_cutoff_ms"] == cutoff,
            "COHORT_NATIVE_CLOCKS",
        )
        initial_state, initial = phase_audit(
            proof / f"cohort-{index}-initial", entry["initial"], cohort, cutoff, None
        )
        _, resumed = phase_audit(
            proof / f"cohort-{index}-resumed", entry["resumed"], cohort, cutoff, initial_state
        )
        details.append(
            {"cohort": index, "native_count": len(cohort), "initial": initial, "resumed": resumed}
        )
    trend = trend_audit(proof, root, report)
    require(
        tree(proof) == before
        and tree(source) == source_before
        and tree(prior) == prior_before
        and digest(zip_path) == PRIOR_ZIP_SHA
        and digest(Path(__file__)) == auditor_sha
        and digest(root / "scripts/audit_parallel_foundations.py") == helper_sha,
        "AUDIT_INPUT_CHANGED",
    )
    if require_current_code:
        require(
            current_code
            == {
                p.relative_to(root).as_posix(): digest(p)
                for p in sorted((root / "src/selective_options").glob("*.py"))
            },
            "CODE_CHANGED_DURING_AUDIT",
        )
    return {
        "status": "COMMITTED_BAR_HANDOFF_INDEPENDENTLY_AUDITED",
        "proof_sha256": before,
        "source_sha256": source_before,
        "prior_foundation_sha256": prior_before,
        "prior_zip_sha256": PRIOR_ZIP_SHA,
        "recorded_code_sha256": recorded_code,
        "current_code_binding_required": require_current_code,
        "auditor_sha256": auditor_sha,
        "shared_readonly_audit_helper_sha256": helper_sha,
        "native_trades": 55,
        "cohort_sizes": [50, 5],
        "missing_trade_id_not_repaired": 109230677,
        "supervised_capture_artifacts": 4,
        "partial_bar_count": 4,
        "cohorts": details,
        "trend": trend,
        "audit_mutated_inputs": False,
        "new_public_requests": 0,
        "historical_strategy_trials": 0,
        "full_ingestion_complete": False,
        "periodic_crash_recovery": False,
        "seven_day_validation_completed": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }


def _prepare(
    proof: Path, output: Path | None, source_root: Path, require_current_code: bool
) -> tuple[Path, Path, Path | None]:
    require(type(require_current_code) is bool, "EXPLICIT_CODE_BINDING_POLICY")
    proof, root = proof.absolute(), source_root.absolute()
    tree(proof)
    tree(root / "evidence/live-gap-02")
    if output is not None:
        output = output.absolute()
        require(
            not output.exists()
            and output.parent.is_dir()
            and not output.is_relative_to(proof)
            and not output.is_relative_to(root / "evidence")
            and not output.is_relative_to(root / "src")
            and not output.is_relative_to(root / "scripts"),
            "AUDIT_OUTPUT_NEW_AND_SEPARATE",
        )
        require(
            all(
                not part.is_symlink()
                and not (part.exists() and getattr(part.lstat(), "st_file_attributes", 0) & 0x400)
                for part in (output, *output.parents)
            ),
            "OUTPUT_LINK_OR_REPARSE_REFUSED",
        )
    return proof, root, output


def _publish(output: Path, result: dict[str, Any], created: Event) -> None:
    with output.open("x", encoding="utf-8") as stream:
        created.set()
        stream.write(json.dumps(result, indent=2) + "\n")


def _revoke_publication(output: Path) -> None:
    if output.exists():
        prior_sha = digest(output)
        output.write_text(
            json.dumps(
                {
                    "status": "AUDIT_PUBLICATION_CANCELLED",
                    "revoked_report_sha256": prior_sha,
                    "audit_completed_for_caller": False,
                    "trading_enabled": False,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


async def audit(
    proof: Path,
    output: Path | None = None,
    *,
    source_root: Path = ROOT,
    require_current_code: bool = True,
) -> dict[str, Any]:
    """Read-only bounded audit; drains workers before propagating cancellation.

    Cancelled validation never publishes. Cancelled output publication drains its
    writer and revokes any success report before returning cancellation to caller.
    """
    proof, root, output = await complete_thread(
        _prepare, proof, output, source_root, require_current_code
    )
    result = await complete_thread(_audit, proof, root, require_current_code)
    if output is not None:
        created = Event()
        try:
            await complete_thread(_publish, output, result, created)
        except asyncio.CancelledError:
            if created.is_set():
                await drain_shutdown(complete_thread(_revoke_publication, output))
            raise
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("proof", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(audit(args.proof, args.output))
    print(
        json.dumps(
            {
                key: result[key]
                for key in ("status", "native_trades", "partial_bar_count", "trading_enabled")
            }
        )
    )


if __name__ == "__main__":
    main()
