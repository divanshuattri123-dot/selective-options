"""Read-only independent audit of the bounded offline parallel foundation proof.

No collection, checkpoint restart, strategy trial, or backtest is run here. The
only production computation exercised is deliberate rejection of a future row.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import statistics
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb

from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot
from selective_options.features import compute_features

ROOT = Path(__file__).resolve().parents[1]
OLD_ZIP_HASH = "211521b5c2e2c3b22ab677df3e4a3f9cbc1952552cfaab31c60f4e50d8a26d82"
MAX_FILES = 64
MAX_BYTES = 64 * 1024 * 1024


class AuditError(ValueError):
    """A missing, inconsistent or changed proof component refuses publication."""


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise AuditError(reason)


def digest(path: Path) -> str:
    require(
        path.is_file()
        and all(
            not p.is_symlink()
            and not (p.exists() and getattr(p.lstat(), "st_file_attributes", 0) & 0x400)
            for p in (path, *path.absolute().parents)
        ),
        "HASH_PATH_REGULAR_NO_LINKS",
    )
    with path.open("rb") as stream:
        value = hashlib.file_digest(stream, "sha256").hexdigest()
    return value


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def sha(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in values:
        require(key not in result, "DUPLICATE_JSON_FIELD")
        result[key] = value
    return result


def load(path: Path) -> dict[str, Any]:
    require(path.is_file() and path.stat().st_size <= 8 * 1024 * 1024, "JSON_MISSING_OR_BUDGET")
    value = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(AuditError("NONFINITE_JSON")),
    )
    require(isinstance(value, dict), "JSON_OBJECT_REQUIRED")
    return cast(dict[str, Any], value)


def tree(path: Path) -> dict[str, str]:
    require(path.is_dir(), "DIRECTORY_REQUIRED")
    discovered: list[Path] = []
    for entry in path.rglob("*"):
        require(len(discovered) < 128, "TREE_ENTRY_BUDGET")
        discovered.append(entry)
    entries = (path, *discovered, *path.absolute().parents)
    require(
        all(
            not p.is_symlink()
            and not (p.exists() and getattr(p.lstat(), "st_file_attributes", 0) & 0x400)
            for p in entries
        ),
        "LINK_OR_REPARSE_REFUSED",
    )
    files = sorted(p for p in discovered if p.is_file())
    require(
        len(files) <= MAX_FILES and sum(p.stat().st_size for p in files) <= MAX_BYTES, "TREE_BUDGET"
    )
    require(not any(p.suffix in {".wal", ".lock"} for p in files), "UNCLOSED_PROOF_OR_JOURNAL")
    return {p.relative_to(path).as_posix(): digest(p) for p in files}


def rows(database: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id LIMIT 1001")
        names = [item[0] for item in cursor.description]
        data = cursor.fetchall()
    require(len(data) <= 1000, "ROW_BUDGET")
    result = tuple(
        MarketEvent.model_validate(dict(zip(names, row, strict=True)), strict=True) for row in data
    )
    require(len({row.event_id for row in result}) == len(result), "DUPLICATE_EVENT_ID")
    for row in result:
        body = json.loads(row.payload_json, object_pairs_hook=pairs)
        require(canonical(body).decode() == row.payload_json, "NONCANONICAL_EVENT_PAYLOAD")
        native_id = str(body["trade_id"]) if row.kind == "trade" else None
        require(
            row.event_id
            == sha(
                [
                    row.source,
                    row.environment,
                    row.asset,
                    row.symbol,
                    row.kind,
                    native_id,
                    row.event_time_ms,
                    row.payload_json,
                ]
            ),
            "EVENT_ID_MISMATCH",
        )
    return result


def _payload(row: MarketEvent) -> dict[str, Any]:
    value = json.loads(row.payload_json, object_pairs_hook=pairs)
    require(isinstance(value, dict), "EVENT_PAYLOAD_OBJECT")
    return cast(dict[str, Any], value)


def database_audit(
    database: Path, parquet: Path | None = None
) -> tuple[tuple[MarketEvent, ...], list[dict[str, Any]]]:
    saved = rows(database)
    barriers: list[dict[str, Any]] = []
    with duckdb.connect(str(database), read_only=True) as connection:
        outbox = connection.execute(
            "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id"
        ).fetchall()
        require(outbox == [(row.event_id, False) for row in saved], "OUTBOX_EXACT_ALL_PENDING")
        if parquet is not None:
            manifest = load(parquet.with_name(parquet.name + ".manifest.json"))
            require(type(manifest.get("format_version")) is int, "MANIFEST_STRICT_VERSION")
            require(
                manifest
                == {
                    "format_version": 1,
                    "sha256": digest(parquet),
                    "rows": len(saved),
                    "bytes": parquet.stat().st_size,
                },
                "SEALED_MANIFEST_MISMATCH",
            )
            db_schema = connection.execute("DESCRIBE SELECT * FROM events").fetchall()
            pq_schema = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?,hive_partitioning=false)", [str(parquet)]
            ).fetchall()
            require(
                [(r[0], r[1]) for r in db_schema] == [(r[0], r[1]) for r in pq_schema],
                "PARQUET_TYPED_SCHEMA",
            )
            require(
                connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
                == connection.execute(
                    "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                    [str(parquet)],
                ).fetchall(),
                "PARQUET_TYPED_ROWS",
            )
        cutoffs = sorted(
            {
                clock + delta
                for row in saved
                for clock in (
                    row.event_time_ms,
                    row.observed_at_ms,
                    max(row.event_time_ms, row.observed_at_ms),
                )
                for delta in (-1, 0)
            }
        )
        for cutoff in cutoffs:
            actual = [
                r[0]
                for r in connection.execute(
                    "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                    [cutoff, cutoff],
                ).fetchall()
            ]
            expected = [
                row.event_id
                for row in saved
                if row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff
            ]
            require(actual == expected, "SQL_BOTH_CLOCK_PIT")
            barriers.append({"as_of_ms": cutoff, "rows": len(actual), "ids_sha256": sha(actual)})
    return saved, barriers


def check_ohlc(
    payload: dict[str, Any],
    inputs: tuple[MarketEvent, ...],
    opening: int,
    interval: int,
    *,
    bucket: bool,
) -> tuple[str, ...]:
    selected = sorted(
        (r for r in inputs if opening <= r.event_time_ms < opening + interval),
        key=lambda r: (r.event_time_ms, _payload(r)["trade_id"]),
    )
    require(bool(selected), "EMPTY_BAR_MEMBERSHIP")
    prices = [_payload(r)["price"] for r in selected]
    volume = sum((Decimal(str(_payload(r)["qty"])) for r in selected), Decimal(0))
    require(
        (payload["open"], payload["high"], payload["low"], payload["close"])
        == (prices[0], max(prices), min(prices), prices[-1]),
        "BAR_OHLC",
    )
    require(payload["count" if bucket else "trade_count"] == len(selected), "BAR_COUNT")
    require(
        Decimal(payload["volume_decimal"]) == volume
        if bucket
        else payload["volume"] == float(volume),
        "BAR_DECIMAL_VOLUME",
    )
    require(payload["complete"] is False, "RESTORED_BUCKET_COMPLETENESS")
    if bucket:
        require(
            payload["first"] == [selected[0].event_time_ms, selected[0].payload()["trade_id"]]
            and payload["last"] == [selected[-1].event_time_ms, selected[-1].payload()["trade_id"]],
            "BUCKET_ORDER_KEYS",
        )
    return tuple(r.event_id for r in selected)


def bars_audit(
    proof: Path,
    report: dict[str, Any],
    native: tuple[MarketEvent, ...],
    derived: tuple[MarketEvent, ...],
) -> list[dict[str, Any]]:
    require(len(report["bar_states"]) == 2 and len(derived) == 4, "COHORT_BAR_COUNTS")
    audited: list[dict[str, Any]] = []
    native_map = {row.event_id: row for row in native}
    derived_map = {row.event_id: row for row in derived}
    seen: set[str] = set()
    for index, metadata in enumerate(report["bar_states"]):
        cohort = native[:50] if index == 0 else native[50:]
        require(
            metadata["cohort"] == index
            and metadata["native_input_ids"] == [r.event_id for r in cohort]
            and metadata["native_inputs"] == len(cohort),
            "COHORT_EXACT_INPUTS",
        )
        filename = f"bar-state-cohort-{index}.json"
        require(
            metadata["state_file"] == filename
            and metadata["state_sha256"] == digest(proof / filename),
            "CODEC_FILE_HASH",
        )
        state = load(proof / filename)
        cutoff = max(max(r.event_time_ms, r.observed_at_ms) for r in cohort) + 1
        restart = cutoff + 1000
        require(
            state["as_of_ms"] == metadata["as_of_ms"] == cutoff
            and metadata["synthetic_restart_ms"] == restart,
            "SYNTHETIC_RESTART_CLOCK",
        )
        require(
            type(state["format_version"]) is int
            and state["format_version"] == 1
            and state["timeframes_ms"] == [300000, 900000]
            and state["lateness_ms"] == 5000,
            "CODEC_POLICY",
        )
        require(
            state["coverage_start_ms"] == min(r.observed_at_ms for r in cohort)
            and state["watermark_ms"] == cutoff - 5000 == metadata["watermark_ms"],
            "CODEC_FRONTIER",
        )
        require(
            state["scope"]
            == {
                "asset": "BTC",
                "environment": "public_reference",
                "source": "kraken",
                "symbol": "BTC/USD",
            },
            "CODEC_SCOPE",
        )
        require(len(state["buckets"]) == metadata["retained_buckets"] == 2, "CODEC_BUCKET_COUNT")
        closing = max(b["opening_ms"] + b["interval_ms"] for b in state["buckets"]) + 5000
        by_bucket: dict[tuple[int, int], tuple[str, ...]] = {}
        for bucket in state["buckets"]:
            prototype = bucket["prototype"]
            require(
                prototype["event_id"] in native_map
                and canonical(native_map[prototype["event_id"]].model_dump())
                == canonical(prototype)
                and prototype["event_id"] in metadata["native_input_ids"],
                "NATIVE_PROTOTYPE_PRESERVED",
            )
            opening, interval = bucket["opening_ms"], bucket["interval_ms"]
            require(
                interval in (300000, 900000)
                and opening % interval == 0
                and opening + interval > state["watermark_ms"],
                "BUCKET_ALIGNMENT",
            )
            key = (opening, interval)
            require(key not in by_bucket, "DUPLICATE_BUCKET")
            by_bucket[key] = check_ohlc(bucket, cohort, opening, interval, bucket=True)
        require(len(metadata["derived_bar_ids"]) == 2, "DERIVED_MEMBERSHIP")
        for event_id in metadata["derived_bar_ids"]:
            require(event_id in derived_map and event_id not in seen, "DERIVED_ID_MEMBERSHIP")
            row = derived_map[event_id]
            body = _payload(row)
            key = (int(body["open_time_ms"]), int(body["timeframe_s"]) * 1000)
            require(key in by_bucket, "DERIVED_BUCKET_SCOPE")
            memberships = check_ohlc(body, cohort, *key, bucket=False)
            require(
                body["coverage_start_ms"] == restart
                and body["lateness_ms"] == 5000
                and body["closed"] is True
                and row.event_time_ms == key[0] + key[1]
                and row.observed_at_ms == closing,
                "DERIVED_SYNTHETIC_AVAILABILITY",
            )
            require(memberships == by_bucket[key], "BUCKET_DERIVED_INPUT_MEMBERSHIP")
            seen.add(event_id)
            audited.append(
                {
                    "cohort": index,
                    "bar_id": event_id,
                    "input_ids": list(memberships),
                    "source_time_ms": row.event_time_ms,
                    "synthetic_receipt_ms": row.observed_at_ms,
                    "complete": False,
                }
            )
    require(seen == set(derived_map), "ALL_DERIVED_BARS_ACCOUNTED")
    return audited


def feature_audit(proof: Path, report: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    fixture, barriers = database_audit(proof / "features-fixture.duckdb")
    ordered = sorted(fixture, key=lambda r: r.event_time_ms)
    require(len(ordered) == 240, "FEATURE_FIXTURE_COUNT")
    for index, row in enumerate(ordered):
        body = _payload(row)
        opening = 300000 + index * 300000
        require(
            (row.source, row.symbol, row.kind, row.environment, row.asset)
            == ("synthetic_fixture", "FIXTURE-BTC/USD", "bar", "public_reference", "BTC"),
            "FEATURE_SYNTHETIC_SCOPE",
        )
        expected = {
            "open_time_ms": opening,
            "close_time_ms": opening + 300000,
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
        }
        require(
            canonical(body) == canonical(expected)
            and row.event_time_ms == opening + 300000
            and row.observed_at_ms == opening + 300010,
            "EXACT_LINEAR_FIXTURE_ROWS",
        )
    wrapper = load(proof / "feature-result/features.json")
    feature = wrapper["feature_report"]
    require(
        wrapper["source_database_sha256"] == digest(proof / "features-fixture.duckdb")
        and wrapper["source_mutated"] is False,
        "FEATURE_SOURCE_HASH",
    )
    require(
        feature["history_sha256"]
        == sha([[r.event_id, r.event_time_ms, r.observed_at_ms] for r in ordered]),
        "FEATURE_HISTORY_INPUT_HASH",
    )
    require(
        feature["bars_available"] == feature["bars_used"] == 240
        and feature["formula_version"] == "closed-bars-v1"
        and feature["state"] == "FEATURES_COMPUTED"
        and feature["blockers"] == [],
        "FEATURE_STATE",
    )
    require(
        (
            feature["asset"],
            feature["environment"],
            feature["source"],
            feature["symbol"],
            feature["timeframe_s"],
        )
        == ("BTC", "public_reference", "synthetic_fixture", "FIXTURE-BTC/USD", 300)
        and feature["history_truncated"] is False
        and feature["source_frontier_complete"] is False
        and feature["first_close_ms"] == ordered[0].event_time_ms
        and feature["latest_close_ms"] == ordered[-1].event_time_ms
        and feature["latest_bar_age_ms"] == 10,
        "FEATURE_EXACT_SCOPE_FRONTIER",
    )
    require(
        feature["as_of_ms"] == ordered[-1].observed_at_ms
        and feature["annualization_periods_per_year"] == 105120,
        "FEATURE_CUTOFF_ANNUALIZATION",
    )
    values = {v["name"]: v for v in feature["features"]}
    require(len(values) == len(feature["features"]), "DUPLICATE_FEATURE_NAME")
    expected_values = {f"ema_{p}": 340 - (p - 1) / 2 for p in (8, 21, 50, 200)}
    expected_values.update({f"ema_{p}_slope": 1.0 for p in (8, 21, 50, 200)})
    expected_values.update(
        {
            "rsi_14": 100.0,
            "atr_14": 3.0,
            "macd_12_26": 7.0,
            "macd_signal_9": 7.0,
            "macd_histogram": 0.0,
            "bollinger_width_20": 4 * math.sqrt(399 / 12) / 330.5,
            "roc_5": 340 / 335 - 1,
            "roc_20": 340 / 320 - 1,
        }
    )
    for window in (20, 60):
        returns = [math.log((101 + i) / (100 + i)) for i in range(240 - window, 240)]
        expected_values[f"realized_vol_{window}"] = statistics.stdev(returns) * math.sqrt(105120)
    require(set(values) == set(expected_values), "EXACT_FEATURE_SET")
    for name, analytical_value in expected_values.items():
        require(
            name in values
            and values[name]["state"] == "AVAILABLE"
            and math.isclose(values[name]["value"], analytical_value, rel_tol=0, abs_tol=1e-10),
            "ANALYTICAL_FEATURE_" + name,
        )
    for wrapper_part in (wrapper, feature):
        require(
            wrapper_part["ready_for_signals"] is False and wrapper_part["trading_enabled"] is False,
            "FEATURE_READINESS",
        )
    malformed = PointInTimeSnapshot(
        ordered[-1].observed_at_ms, "BTC", "public_reference", tuple(ordered)
    )
    object.__setattr__(malformed, "as_of_ms", ordered[-1].observed_at_ms - 1)
    try:
        compute_features(
            malformed, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
        )
    except IntegrityError as error:
        refusal = str(error)
    else:
        raise AuditError("FUTURE_RECEIPT_ACCEPTED")
    require(
        report["future_receipt_refusal"] == refusal == "FEATURE_LOOKAHEAD_EVENT"
        and report["fabricated_leakage_cutoff_ms"] == ordered[-1].observed_at_ms - 1
        and report["offending_fixture_bar_id"] == ordered[-1].event_id
        and report["offending_fixture_receipt_ms"] == ordered[-1].observed_at_ms,
        "DURABLE_LEAKAGE_REFUSAL",
    )
    return barriers, refusal


def trial_audit(proof: Path, root: Path, report: dict[str, Any]) -> dict[str, Any]:
    request = load(root / "examples/backtest-known-bad.json")
    input_hash = digest(root / "examples/backtest-known-bad.json")
    require(
        input_hash == "c4f3591736a9a571e2f0ff89edf6378a3ee9387a66771fb3fd392774d502fe89",
        "KNOWN_BAD_REVIEWED_INPUT_HASH",
    )
    backtest = load(proof / "backtest-result/backtest.json")
    simulation = backtest["simulation"]
    require(
        report["input_sha256"] == backtest["input_sha256"] == input_hash
        and request["data_kind"] == backtest["data_kind"] == "synthetic_fixture",
        "BACKTEST_INPUT_PROVENANCE",
    )
    require(len(simulation["fills"]) == 2, "KNOWN_BAD_FILL_COUNT")
    for fill, side, price, notional, fee, cash, source, receipt in zip(
        simulation["fills"],
        ["buy", "sell"],
        ["101", "99"],
        ["2020", "1980"],
        ["2.02", "1.98"],
        ["-2022.02", "1978.02"],
        [2000, 4000],
        [2001, 4001],
        strict=True,
    ):
        require(
            fill["side"] == side
            and fill["order_id"] == side
            and fill["fee_role"] == "taker"
            and fill["quote_source_time_ms"] == source
            and fill["quote_observed_at_ms"] == fill["filled_at_ms"] == receipt,
            "KNOWN_BAD_FILL_SCOPE_CLOCK",
        )
        for key, expected in {
            "contracts": "2",
            "execution_price": price,
            "executable_touch": price,
            "premium_notional": notional,
            "fee": fee,
            "cash_delta": cash,
            "slippage_cost": "0",
        }.items():
            require(Decimal(fill[key]) == Decimal(expected), "KNOWN_BAD_FILL_" + key)
    for key, expected in {
        "gross_realized_pnl": "-40",
        "total_fees": "4",
        "net_realized_pnl": "-44",
        "net_cash_flow": "-44",
        "closed_roundtrip_net_pnl": "-44",
        "inventory_contracts": "0",
        "premium_cost_basis": "0",
        "entry_fee_cost_basis": "0",
        "total_slippage": "0",
    }.items():
        require(Decimal(simulation[key]) == Decimal(expected), "KNOWN_BAD_ACCOUNTING_" + key)
    require(
        canonical(simulation["instrument"]) == canonical(request["instrument"])
        and canonical(simulation["fee_policy"]) == canonical(request["fees"])
        and canonical(simulation["policy"]) == canonical(request["policy"])
        and simulation["funding_cashflow"] is None,
        "EXPLICIT_ECONOMIC_POLICIES",
    )
    for part in (backtest, simulation):
        require(
            part["trading_enabled"] is False and part["ready_for_signals"] is False,
            "BACKTEST_READINESS",
        )
    require(
        simulation["gate2_complete"] is False and simulation["history_complete"] is False,
        "BACKTEST_SCOPE",
    )
    data = (proof / "trials.jsonl").read_bytes()
    require(data.endswith(b"\n"), "TRIAL_INCOMPLETE")
    lines = data.splitlines()
    require(len(lines) == 2, "TRIAL_COUNT_N_ONE")
    records = [json.loads(line, object_pairs_hook=pairs) for line in lines]
    previous = "0" * 64
    for sequence, (line, record) in enumerate(zip(lines, records, strict=True), 1):
        require(
            set(record)
            == {
                "schema_version",
                "sequence",
                "trial_id",
                "phase",
                "recorded_at_ms",
                "parameters_sha256",
                "previous_sha256",
                "record_sha256",
                "parameters",
                "provenance",
                "result",
                "failure_type",
            },
            "TRIAL_EXACT_SCHEMA",
        )
        require(
            type(record.get("schema_version")) is int
            and record["schema_version"] == 1
            and type(record.get("sequence")) is int
            and type(record.get("recorded_at_ms")) is int
            and record["recorded_at_ms"] > 0,
            "TRIAL_STRICT_INTEGERS",
        )
        require(
            canonical(record) == line
            and record["sequence"] == sequence
            and record["previous_sha256"] == previous
            and record["record_sha256"]
            == sha({k: v for k, v in record.items() if k != "record_sha256"}),
            "TRIAL_CHAIN_HASH",
        )
        previous = record["record_sha256"]
    start, end = records
    require(
        start["result"] is None
        and start["failure_type"] is None
        and end["parameters"] is None
        and end["provenance"] is None
        and end["failure_type"] is None,
        "TRIAL_PHASE_FIELDS",
    )
    require(
        start["phase"] == "STARTED"
        and end["phase"] == "COMPLETED"
        and start["trial_id"] == end["trial_id"] == backtest["trial_id"]
        and start["recorded_at_ms"] <= end["recorded_at_ms"],
        "TRIAL_PHASES_ID",
    )
    parameters = {k: v for k, v in request.items() if k not in {"quotes", "provenance"}}
    parameters["orders"] = [
        dict(o, cancelled_at_ms=None, limit_price=None) for o in request["orders"]
    ]
    parameters.update(input_sha256=input_hash, quote_count=2)
    require(
        canonical(start["parameters"]) == canonical(parameters)
        and start["parameters_sha256"] == end["parameters_sha256"] == sha(parameters),
        "REGISTERED_EXACT_PARAMETERS",
    )
    require(
        start["provenance"] == backtest["provenance"]
        and start["provenance"]["historical_data_independently_verified"] is False,
        "REGISTERED_SYNTHETIC_PROVENANCE",
    )
    result_sha = hashlib.sha256(
        json.dumps(simulation, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    expected_summary = {
        "simulation_id": simulation["simulation_id"],
        "result_sha256": result_sha,
        "fills": 2,
        "closed_roundtrip_net_pnl": simulation["closed_roundtrip_net_pnl"],
        "net_cash_flow": simulation["net_cash_flow"],
        "inventory_contracts": simulation["inventory_contracts"],
        "gate2_complete": False,
        "trading_enabled": False,
    }
    require(
        canonical(end["result"]) == canonical(expected_summary), "REGISTERED_EXACT_RESULT_SUMMARY"
    )
    require(
        end["result"]["result_sha256"] == result_sha
        and end["result"]["simulation_id"] == simulation["simulation_id"],
        "REGISTERED_RESULT_HASH",
    )
    counts = {"started": 1, "completed": 1, "failed": 0, "unresolved": 0, "trial_count_N": 1}
    require(report["registry_counts"] == backtest["registry_counts"] == counts, "REGISTRY_COUNTS")
    return {
        "trial_id": start["trial_id"],
        "input_sha256": input_hash,
        "parameters_sha256": sha(parameters),
        "result_sha256": result_sha,
        "trial_count_N": 1,
        "closed_roundtrip_net_pnl": "-44",
        "historical_strategy_trials": 0,
    }


def _audit(proof: Path, source_root: Path, require_current_code: bool) -> dict[str, Any]:
    tree(proof.absolute())
    tree(source_root.absolute() / "evidence/live-gap-02")
    proof = proof.resolve()
    root = source_root.resolve()
    source = root / "evidence/live-gap-02"
    auditor_before = digest(Path(__file__))
    verifier_before = digest(root / "scripts/verify_parallel_foundations.py")
    input_before = digest(root / "examples/backtest-known-bad.json")
    before = tree(proof)
    source_before = tree(source)
    report = load(proof / "report.json")
    require(
        report["status"] == "PARALLEL_FOUNDATIONS_VERIFIED"
        and Path(report["source_evidence"]).resolve() == source.resolve()
        and report["source_sha256"] == source_before,
        "ORIGINAL_SOURCE_HASH_SCOPE",
    )
    current_code = {
        p.relative_to(root).as_posix(): digest(p)
        for p in sorted((root / "src/selective_options").glob("*.py"))
    }
    if require_current_code:
        require(report["tested_source_sha256"] == current_code, "FROZEN_CODE_HASHES")
        require(
            report["verifier_sha256"] == digest(root / "scripts/verify_parallel_foundations.py"),
            "FROZEN_VERIFIER_HASH",
        )
    require(
        report["native_cohort_lengths"] == [50, 5]
        and report["missing_ws_trade_id"] == 109230677
        and report["saved_native_trade_count"] == 55
        and report["derived_bar_count"] == 4,
        "REPORT_COUNTS",
    )
    for name in (
        "original_frame_replay_claimed",
        "cross_gap_continuity_claimed",
        "bar_state_committed_capture_resume_implemented",
        "future_receipt_leaked",
        "full_ingestion_complete",
        "full_feature_layer_complete",
        "full_backtester_complete",
        "seven_day_validation_completed",
        "trading_enabled",
    ):
        require(report[name] is False, "OVERCLAIM_" + name)
    require(
        report["bar_restart_clocks_are_synthetic"] is True
        and report["feature_data_kind"] == "synthetic_fixture"
        and report["fee_policy_is_synthetic"] is True
        and report["backtest_trials_registered"] == 1
        and report["historical_strategy_trials"]
        == report["new_public_requests"]
        == report["paper_days"]
        == report["paper_trades"]
        == 0,
        "SCOPE_LABELS",
    )
    original = rows(source / "events.duckdb")
    native = tuple(
        sorted(
            (r for r in original if r.source == "kraken" and r.kind == "trade"),
            key=lambda r: _payload(r)["trade_id"],
        )
    )
    require(
        [_payload(r)["trade_id"] for r in native]
        == list(range(109230627, 109230677)) + list(range(109230678, 109230683)),
        "NATIVE_55_CONTIGUOUS_COHORTS",
    )
    for row in native:
        body = _payload(row)
        stamp = datetime.fromisoformat(str(body["source_timestamp"]))
        require(
            stamp.tzinfo is not None
            and int(stamp.timestamp() * 1000) == row.event_time_ms
            and (row.environment, row.asset, row.symbol) == ("public_reference", "BTC", "BTC/USD"),
            "NATIVE_SOURCE_CLOCK_SCOPE",
        )
    saved, barriers = database_audit(
        proof / "native-and-bars.duckdb", proof / "native-and-bars.parquet"
    )
    require(
        tuple(r for r in saved if r.kind == "trade")
        == tuple(sorted(native, key=lambda r: r.event_id))
        and len(saved) == 59,
        "EXACT_NATIVE_FULL_ROWS",
    )
    derived = tuple(r for r in saved if r.kind == "bar")
    cohort_bars = bars_audit(proof, report, native, derived)
    feature_barriers, refusal = feature_audit(proof, report)
    trial = trial_audit(proof, root, report)
    old_zip = root.parent / "selective-options-0.3.44.zip"
    require(digest(old_zip) == OLD_ZIP_HASH, "OLD_044_ZIP_CHANGED")
    expected_preserved = {
        "../selective-options-0.3.44.zip": OLD_ZIP_HASH,
        "evidence/spot-batch-saved-public-01/report.json": "5dd31648410b91407765a69a0f80c00feeb6613b026ae458a3e7557a6e431c56",
        "evidence/spot-batch-parent-validation-0344-02.json": "3e49bf9ad901e1504d60cb8cc4a575670d27afc120247a5aaab5f21527f116fd",
    }
    preserved = report["preserved_artifact_sha256"]
    require(preserved == expected_preserved, "PUBLISHED_044_BASELINES")
    require(
        preserved.get("../selective-options-0.3.44.zip") == OLD_ZIP_HASH, "PRESERVED_ZIP_BASELINE"
    )
    require(
        set(preserved)
        == {
            "../selective-options-0.3.44.zip",
            "evidence/spot-batch-saved-public-01/report.json",
            "evidence/spot-batch-parent-validation-0344-02.json",
        },
        "PRESERVED_ARTIFACT_SET",
    )
    for relative, expected_digest in preserved.items():
        require(digest(root / relative) == expected_digest, "OLD_EVIDENCE_CHANGED")
    prior = load(root / "evidence/spot-batch-saved-public-01/report.json")
    require(
        prior["source_hashes_before"] == prior["source_hashes_after"] == source_before,
        "ORIGINAL_SOURCE_044_BASELINE",
    )
    require(
        digest(Path(__file__)) == auditor_before
        and digest(root / "scripts/verify_parallel_foundations.py") == verifier_before
        and digest(root / "examples/backtest-known-bad.json") == input_before,
        "AUDITOR_VERIFIER_OR_INPUT_CHANGED",
    )
    require(
        tree(proof) == before
        and tree(source) == source_before
        and current_code
        == {
            p.relative_to(root).as_posix(): digest(p)
            for p in sorted((root / "src/selective_options").glob("*.py"))
        },
        "AUDIT_SOURCE_OR_ARTIFACT_CHANGED",
    )
    return {
        "status": "PARALLEL_FOUNDATIONS_INDEPENDENTLY_AUDITED",
        "proof_sha256": before,
        "original_source_sha256": source_before,
        "tested_source_sha256": report["tested_source_sha256"],
        "current_source_sha256": current_code,
        "current_code_binding_required": require_current_code,
        "proof_verifier_sha256": report["verifier_sha256"],
        "auditor_sha256": auditor_before,
        "current_proof_verifier_sha256": verifier_before,
        "old_044_zip_sha256": OLD_ZIP_HASH,
        "saved_native_trade_count": 55,
        "native_cohort_lengths": [50, 5],
        "missing_ws_trade_id": 109230677,
        "derived_bar_count": 4,
        "cohort_bar_memberships": cohort_bars,
        "sql_both_clock_barriers": barriers,
        "feature_sql_both_clock_barriers": feature_barriers,
        "synthetic_feature_bars": 240,
        "independent_future_receipt_refusal": refusal,
        "registered_known_bad_fixture": trial,
        "native_and_bar_outbox_all_pending": 59,
        "feature_outbox_all_pending": 240,
        "new_public_requests": 0,
        "full_ingestion_complete": False,
        "full_capture_restart_verified": False,
        "full_feature_layer_complete": False,
        "full_backtester_complete": False,
        "historical_strategy_profitability_claimed": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }


def output_preflight(output: Path, proof: Path, root: Path) -> Path:
    output = output.absolute()
    require(
        not output.exists()
        and not output.is_relative_to(proof.resolve())
        and not output.is_relative_to(root.resolve() / "evidence/live-gap-02")
        and not output.is_relative_to(root.resolve() / "evidence/spot-batch-saved-public-01")
        and output.parent.is_dir(),
        "AUDIT_OUTPUT_MUST_BE_NEW_AND_SEPARATE",
    )
    require(
        all(
            not p.is_symlink()
            and not (p.exists() and getattr(p.lstat(), "st_file_attributes", 0) & 0x400)
            for p in (output, *output.parents)
        ),
        "AUDIT_OUTPUT_LINK",
    )
    return output


def publish(output: Path, result: dict[str, Any]) -> None:
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(result, indent=2, allow_nan=False) + "\n")


async def audit(
    proof: Path,
    output: Path | None = None,
    *,
    source_root: Path = ROOT,
    require_current_code: bool = True,
) -> dict[str, Any]:
    """Audit immutable proof; optional exclusive report is outside proof and source."""
    require(type(require_current_code) is bool, "STRICT_CODE_BINDING_CONTROL")
    if output is not None:
        output = await asyncio.to_thread(output_preflight, output, proof, source_root)
    result = await asyncio.to_thread(_audit, proof, source_root, require_current_code)
    if output is not None:
        await asyncio.to_thread(publish, output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(audit(args.proof, args.output))
    print(
        json.dumps(
            {
                "status": result["status"],
                "native": 55,
                "synthetic_bars": 240,
                "known_bad_net_pnl": "-44",
                "trading_enabled": False,
            }
        )
    )


if __name__ == "__main__":
    main()
