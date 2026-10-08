"""Finite read-only saved-option clock diagnostics; canonical coverage remains authoritative."""

import argparse
import hashlib
import json
import math
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb
from pydantic import ValidationError

from selective_options.events import MarketEvent
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore

ASSETS = {"BTC", "ETH", "SOL", "PAXG"}
LIMITATIONS = [
    "Finite preserved testnet observations; no continuity, capacity or seven-day claim.",
    "Receipt-minus-source is a signed clock difference, not wire latency; cause is unknown.",
    "Native timestamps, freshness bounds and canonical stale/blocking states are unchanged.",
    "History counts describe stored eligible events, not received frames or unseen updates.",
    "SQL scans and internal engine memory are not bounded by Python materialization limits.",
    "No fresh observations, features, gold substitutes, trading approval or M2 completion.",
]


def policy(
    asset: str,
    cutoff: int,
    leg_age: int,
    universe_age: int,
    max_rows: int,
    max_history_rows: int,
    timeout_s: float,
) -> None:
    coverage_at([], asset, cutoff, leg_age, universe_age, max_rows)
    try:
        valid_timeout = type(timeout_s) in (int, float) and math.isfinite(timeout_s)
    except OverflowError:
        valid_timeout = False
    if (
        type(max_history_rows) is not int
        or not 1 <= max_history_rows <= 2000000
        or not valid_timeout
        or not 0 < timeout_s <= 120
    ):
        raise ValueError("invalid finite freshness audit policy")


class ProjectionReader(DuckStore):
    """Use the canonical store projection without opening its schema/writer path."""

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        super().__init__(connection, 5000)
        self.projected: list[MarketEvent] = []
        self.fetch_counts: list[int] = []

    def _select(self, sql: str, params: list[object]) -> list[dict[str, object]]:
        rows = super()._select(sql, params)
        self.fetch_counts.append(len(rows))
        self.projected.extend(MarketEvent.model_validate(row) for row in rows)
        return rows


def audit_database(
    path: Path,
    asset: str,
    cutoff: int,
    leg_age: int = 30000,
    universe_age: int = 3600000,
    max_rows: int = 100000,
    max_history_rows: int = 200000,
    timeout_s: float = 30,
) -> dict[str, Any]:
    """Latest-pair projection plus grouped eligible history, never all historical Python rows."""
    policy(asset, cutoff, leg_age, universe_age, max_rows, max_history_rows, timeout_s)
    if not path.is_file() or path.stat().st_size > 1024**3:
        raise ValueError("freshness input database absent or exceeds 1 GiB budget")
    deadline = time.monotonic() + timeout_s

    def check_time() -> None:
        if time.monotonic() > deadline:
            raise TimeoutError("finite freshness audit deadline exceeded")

    with duckdb.connect(str(path), read_only=True) as db:
        timer = threading.Timer(timeout_s, db.interrupt)
        timer.daemon = True
        timer.start()
        try:
            reader = ProjectionReader(db)
            view: dict[str, Any] = reader._option_coverage(
                asset, cutoff, leg_age, universe_age, max_rows
            )
            check_time()
            result: dict[str, Any] = {
                "asset": asset,
                "as_of_ms": cutoff,
                "max_leg_age_ms": leg_age,
                "coverage": view,
                "projection_fetch_counts": reader.fetch_counts,
                "selected_session": None,
                "legs": [],
                "diagnostic_counts": {},
                "history_rows_scanned_for_statistics": 0,
                "budgets": {
                    "projection_rows": max_rows,
                    "eligible_selected_session_history_rows": max_history_rows,
                    "deadline_s": timeout_s,
                    "database_bytes": 1024**3,
                },
                "milestone_2_complete": False,
                "trading_enabled": False,
                "new_network_requests": 0,
                "orders_submitted": 0,
                "limitations": LIMITATIONS,
            }
            context = [row for row in reader.projected if row.kind not in {"ticker", "book"}]
            context_view: dict[str, Any] = coverage_at(
                context, asset, cutoff, leg_age, universe_age, max_rows
            )
            if context_view["blocker"] is not None:
                result["status"] = "CONTEXT_BLOCKED"
                return result
            ack = next(
                row for row in context if row.event_id == context_view["stream_control_event_id"]
            )
            start = next(
                row.observed_at_ms
                for row in context
                if row.kind == "status"
                and row.payload().get("reason") == "STREAM_RESYNC"
                and type(row.payload().get("connection")) is int
                and row.payload().get("connection") == ack.payload()["connection"]
                and row.observed_at_ms <= ack.observed_at_ms
            )
            symbols = sorted({leg["symbol"] for leg in context_view["legs"]})
            result["selected_session"] = {
                "resync_at_ms": start,
                "acknowledged_at_ms": ack.observed_at_ms,
                "stream_control_event_id": ack.event_id,
                "manifest_event_id": context_view["manifest_event_id"],
                "both_clocks_at_or_before_cutoff": True,
                "receipt_at_or_after_resync_required": True,
            }
            where = """FROM events WHERE source='deribit' AND environment='testnet'
                AND asset=? AND kind IN ('ticker','book')
                AND symbol IN (SELECT value ->> '$' FROM json_each(?))
                AND event_time_ms<=? AND observed_at_ms BETWEEN ? AND ?"""
            params: list[Any] = [asset, json.dumps(symbols), cutoff, start, cutoff]
            count_row = db.execute("SELECT count(*) " + where, params).fetchone()
            assert count_row is not None
            if count_row[0] > max_history_rows:
                raise ValueError("freshness selected-session history row budget exceeded")
            result["history_rows_scanned_for_statistics"] = count_row[0]
            threshold = cutoff - leg_age
            cursor = db.execute(
                """SELECT symbol,kind,count(*) AS stored_observation_count,
                    count(DISTINCT event_time_ms) AS distinct_native_timestamp_count,
                    min(event_time_ms) AS first_source_ms,max(event_time_ms) AS last_source_ms,
                    min(observed_at_ms) AS first_receipt_ms,max(observed_at_ms) AS last_receipt_ms,
                    min(observed_at_ms-event_time_ms) AS min_receipt_minus_source_ms,
                    max(observed_at_ms-event_time_ms) AS max_receipt_minus_source_ms,
                    count(*) FILTER (WHERE observed_at_ms<event_time_ms) AS negative_difference_count,
                    count(*) FILTER (WHERE event_time_ms>=?) AS recent_source_count,
                    count(*) FILTER (WHERE observed_at_ms>=?) AS recent_receipt_count,
                    count(*) FILTER (WHERE observed_at_ms>=? AND event_time_ms<?)
                        AS recent_receipts_with_old_source_count """
                + where
                + " GROUP BY symbol,kind ORDER BY symbol,kind LIMIT ?",
                [threshold, threshold, threshold, threshold, *params, 2 * len(symbols) + 1],
            )
            columns = [column[0] for column in cursor.description]
            statistics = {
                (values[0], values[1]): dict(zip(columns, values, strict=True))
                for values in cursor.fetchall()
            }
            if len(statistics) > 2 * len(symbols):
                raise ValueError("freshness grouped statistics budget exceeded")
            quotes: dict[tuple[str, str], set[MarketEvent]] = {}
            for row in reader.projected:
                if row.kind in {"ticker", "book"}:
                    quotes.setdefault((row.symbol, row.kind), set()).add(row)
            canonical_legs = {(leg["symbol"], leg["kind"]): leg for leg in view.get("legs", [])}
            counts: Counter[str] = Counter()
            for symbol in symbols:
                for kind in ("ticker", "book"):
                    check_time()
                    candidates = quotes.get((symbol, kind), set())
                    ordered = sorted(candidates, key=lambda row: (row.event_id, row.payload_json))
                    stats = statistics.get((symbol, kind))
                    if not candidates:
                        diagnosis = "MISSING_SELECTED_SESSION_OBSERVATION"
                    elif len(candidates) > 1:
                        diagnosis = "AMBIGUOUS_LATEST_PAIR"
                    elif view["blocker"] is not None:
                        diagnosis = "CANONICAL_LEG_EVIDENCE_BLOCKED"
                    elif stats is not None and stats["recent_source_count"] == 0:
                        diagnosis = (
                            "RECENT_RECEIPTS_WITH_OLD_NATIVE_TIMESTAMPS"
                            if stats["recent_receipt_count"] > 0
                            else "NO_RECENT_NATIVE_SOURCE_UPDATES_OR_RECEIPTS"
                        )
                    else:
                        diagnosis = (
                            "SOURCE_AND_RECEIPT_WITHIN_BOUND"
                            if canonical_legs[(symbol, kind)]["state"] == "OBSERVED"
                            else "RECENT_NATIVE_SOURCE_WITH_STALE_SELECTED_RECEIPT"
                        )
                    details = []
                    for row in ordered:
                        payload_valid = True
                        try:
                            body = row.payload()
                        except ValidationError:
                            body = {}
                            payload_valid = False
                        details.append(
                            {
                                "event_id": row.event_id,
                                "event_time_ms": row.event_time_ms,
                                "observed_at_ms": row.observed_at_ms,
                                "source_age_ms": cutoff - row.event_time_ms,
                                "receipt_age_ms": cutoff - row.observed_at_ms,
                                "source_exceeds_age_bound": cutoff - row.event_time_ms > leg_age,
                                "receipt_exceeds_age_bound": cutoff - row.observed_at_ms > leg_age,
                                "receipt_minus_source_ms": row.observed_at_ms - row.event_time_ms,
                                "negative_clock_difference": row.observed_at_ms < row.event_time_ms,
                                "timestamp_kind": row.timestamp_kind,
                                "payload_json_object_valid": payload_valid,
                                "native_timestamp_key": "timestamp",
                                "native_timestamp_value": body.get("timestamp"),
                                "native_timestamp_matches_event": type(body.get("timestamp")) is int
                                and body["timestamp"] == row.event_time_ms,
                                "change_id": body.get("change_id") if kind == "book" else None,
                                "payload_sha256": hashlib.sha256(
                                    row.payload_json.encode()
                                ).hexdigest(),
                            }
                        )
                    counts[diagnosis] += 1
                    result["legs"].append(
                        {
                            "symbol": symbol,
                            "kind": kind,
                            "canonical_state": canonical_legs.get((symbol, kind), {}).get("state"),
                            "diagnosis": diagnosis,
                            "latest_pair_candidate_count": len(candidates),
                            "latest_pair_candidates": details,
                            "selected_session_statistics": stats,
                        }
                    )
            result["diagnostic_counts"] = dict(sorted(counts.items()))
            result["status"] = (
                "CANONICAL_LEG_BLOCKED" if view["blocker"] else "FINITE_CLOCK_DIAGNOSTICS"
            )
            check_time()
            return result
        except duckdb.InterruptException as exc:
            raise TimeoutError("finite freshness audit query deadline exceeded") from exc
        finally:
            timer.cancel()
            timer.join()


def file_hash(path: Path, deadline: float) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024**2):
            if time.monotonic() > deadline:
                raise TimeoutError("finite freshness capture deadline exceeded")
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size > 8 * 1024**2:
        raise ValueError("freshness JSON input absent or exceeds 8 MiB budget")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate freshness JSON key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> Any:
        raise ValueError(f"invalid freshness JSON constant: {value}")

    body = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=unique,
        parse_constant=invalid_constant,
    )
    if type(body) is not dict:
        raise ValueError("freshness JSON input must be an object")
    return body


def seconds_ms(value: Any) -> int:
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and value > 0
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("invalid saved freshness seconds policy")
    milliseconds = value * 1000
    try:
        valid_milliseconds = math.isfinite(milliseconds) and milliseconds == int(milliseconds)
    except OverflowError:
        valid_milliseconds = False
    if not valid_milliseconds:
        raise ValueError("saved freshness policy requires exact integer milliseconds")
    return int(milliseconds)


def audit_capture(capture: Path, assets: tuple[str, ...], output: Path) -> dict[str, Any]:
    deadline = time.monotonic() + 120
    if not assets or len(set(assets)) != len(assets) or not set(assets) <= ASSETS:
        raise ValueError("invalid finite freshness capture assets")
    if output.exists():
        raise FileExistsError("freshness audit requires fresh output")
    saved = read_json(capture / "report.json")
    config = read_json(capture / "config.sanitized.json")
    cutoff = saved["ended_at_ms"]
    leg_age = seconds_ms(config["data"]["max_staleness_s"])
    universe_age = seconds_ms(config["ingestion"]["option_universe_max_age_s"])
    for asset in assets:
        policy(asset, cutoff, leg_age, universe_age, 100000, 200000, 30)
    database = capture / "events.duckdb"
    if not database.is_file() or database.stat().st_size > 1024**3:
        raise ValueError("freshness input database absent or exceeds 1 GiB budget")
    files = sorted(path for path in capture.iterdir() if path.is_file())
    if len(files) > 32 or sum(path.stat().st_size for path in files) > 2 * 1024**3:
        raise ValueError("freshness source file count/byte budget exceeded")
    before = {str(path.resolve()): file_hash(path, deadline) for path in files}
    reports = []
    for asset in assets:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("finite freshness capture deadline exceeded")
        reports.append(
            audit_database(
                capture / "events.duckdb",
                asset,
                cutoff,
                leg_age,
                universe_age,
                timeout_s=min(30, remaining),
            )
        )
    saved_views = {view["asset"]: view for view in saved["option_stream_coverage"]}
    if len(saved_views) != len(saved["option_stream_coverage"]):
        raise ValueError("duplicate saved freshness coverage asset")
    for item in reports:
        if item["coverage"] != saved_views[item["asset"]]:
            raise ValueError("freshness coverage differs from preserved capture")
    after = {str(path.resolve()): file_hash(path, deadline) for path in files}
    if before != after:
        raise ValueError("source bytes changed during read-only freshness audit")
    result = {
        "capture": str(capture.resolve()),
        "source_hashes": before,
        "source_bytes_unchanged": True,
        "saved_coverage_exact": True,
        "assets": reports,
        "new_network_requests": 0,
        "orders_submitted": 0,
        "milestone_2_complete": False,
        "trading_enabled": False,
    }
    output.mkdir(parents=True)
    (output / "report.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--assets", nargs="+", choices=sorted(ASSETS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit_capture(args.capture, tuple(args.assets), args.output)
    print(
        json.dumps(
            [
                {"asset": item["asset"], "counts": item["diagnostic_counts"]}
                for item in report["assets"]
            ],
            indent=2,
        )
    )
