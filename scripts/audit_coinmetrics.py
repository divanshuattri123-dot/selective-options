"""Read-only native daily-response/commit audit; never collects or trades.

Actual HTTP entity bytes support one finite observed poll only. Daily labels are
not first publication timestamps or independently verified finality. The explicit
mock mode is solely a labelled development fixture and cannot yield public status.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import parse_qsl, urlparse

import duckdb

from selective_options.coinmetrics import DAY_MS, SOURCE, SYMBOL, URL
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.store import complete_thread

ROOT = Path(__file__).resolve().parents[1]
MAX_FILES = 128
MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 1026
EXECUTED_AUDITOR_SHA = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
CONTROLS = (
    "history_complete",
    "publication_time_available",
    "provider_finality_independently_verified",
    "exchange_attribution_available",
    "ready_for_signals",
    "trading_enabled",
    "milestone_2_complete",
    "continuous_history_verified",
    "redis_delivery_claim",
)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError("COINMETRICS_AUDIT_" + reason)


def integer(value: object) -> int:
    require(type(value) is int and 0 < value < 2**63, "STRICT_POSITIVE_INTEGER")
    return cast(int, value)


def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in values:
        require(key not in result, "DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def decode(body: bytes) -> Any:
    return json.loads(
        body,
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(
            IntegrityError("COINMETRICS_AUDIT_NONFINITE_JSON")
        ),
    )


def load(path: Path) -> dict[str, Any]:
    result = decode(path.read_bytes())
    require(isinstance(result, dict), "JSON_OBJECT_REQUIRED")
    return cast(dict[str, Any], result)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def safe(path: Path) -> Path:
    absolute = path.absolute()
    require(not any("*" in str(p) or "?" in str(p) for p in (absolute,)), "GLOB_REFUSED")
    require(
        all(
            not p.is_symlink()
            and not (p.exists() and getattr(p.lstat(), "st_file_attributes", 0) & 0x400)
            for p in (absolute, *absolute.parents)
        ),
        "LINK_OR_REPARSE_REFUSED",
    )
    return absolute.resolve()


def tree(path: Path) -> dict[str, str]:
    require(path.is_dir(), "PROOF_DIRECTORY_REQUIRED")
    entries: list[Path] = []
    for entry in path.rglob("*"):
        require(len(entries) < MAX_FILES, "TREE_ENTRY_BUDGET")
        safe(entry)
        entries.append(entry)
    files = sorted(p for p in entries if p.is_file())
    require(sum(p.stat().st_size for p in files) <= MAX_BYTES, "TREE_BYTE_BUDGET")
    require(not any(p.suffix in (".wal", ".lock") for p in files), "UNCLOSED_PROOF")
    return {p.relative_to(path).as_posix(): digest(p) for p in files}


def date_label(value: object) -> int:
    require(type(value) is str, "NATIVE_DATE_STRING")
    text = cast(str, value)
    require(len(text) == 10, "REQUEST_DATE_FORMAT")
    parsed = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=UTC)
    require(parsed.strftime("%Y-%m-%d") == text, "REQUEST_DATE_FORMAT")
    return int(parsed.timestamp()) * 1000


def identity(row: MarketEvent, native_id: str | None) -> str:
    encoded = json.dumps(
        [
            row.source,
            row.environment,
            row.asset,
            row.symbol,
            row.kind,
            native_id,
            row.event_time_ms,
            row.payload_json,
        ],
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def saved_rows(database: Path, parquet: Path | None = None) -> tuple[MarketEvent, ...]:
    expected = [
        (
            name,
            "BIGINT"
            if name in ("event_time_ms", "observed_at_ms")
            else "INTEGER"
            if name == "schema_version"
            else "VARCHAR",
        )
        for name in MarketEvent.model_fields
    ]
    with duckdb.connect(str(database), read_only=True) as connection:
        require(
            connection.execute("SHOW TABLES").fetchall() == [("delivery_outbox",), ("events",)],
            "DATABASE_TABLES",
        )
        actual = connection.execute("DESCRIBE events").fetchall()
        require([(r[0], r[1]) for r in actual] == expected, "EVENT_TYPED_SCHEMA")
        require(
            [(r[0], r[1]) for r in connection.execute("DESCRIBE delivery_outbox").fetchall()]
            == [("event_id", "VARCHAR"), ("delivered", "BOOLEAN")],
            "OUTBOX_TYPED_SCHEMA",
        )
        values = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [MAX_ROWS + 1]
        ).fetchall()
        require(len(values) <= MAX_ROWS, "ROW_BUDGET")
        result = tuple(
            MarketEvent.model_validate(
                dict(zip(MarketEvent.model_fields, row, strict=True)), strict=True
            )
            for row in values
        )
        require(len({r.event_id for r in result}) == len(result), "DUPLICATE_EVENT_ID")
        require(
            connection.execute("SELECT * FROM delivery_outbox ORDER BY event_id").fetchall()
            == [(r.event_id, False) for r in result],
            "OUTBOX_EXACT_ALL_PENDING",
        )
        if parquet is not None:
            manifest = load(parquet.with_name(parquet.name + ".manifest.json"))
            require(
                type(manifest.get("format_version")) is int
                and manifest
                == {
                    "format_version": 1,
                    "sha256": digest(parquet),
                    "rows": len(result),
                    "bytes": parquet.stat().st_size,
                },
                "SEALED_MANIFEST",
            )
            schema = connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?,hive_partitioning=false)", [str(parquet)]
            ).fetchall()
            require([(r[0], r[1]) for r in schema] == expected, "PARQUET_TYPED_SCHEMA")
            require(
                connection.execute(
                    "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                    [str(parquet)],
                ).fetchall()
                == values,
                "PARQUET_EXACT_TYPED_EQUALITY",
            )
    for row in result:
        payload = decode(row.payload_json.encode())
        require(canonical(payload) == row.payload_json, "CANONICAL_PAYLOAD_BYTES")
        require(
            (row.source, row.environment, row.symbol) == (SOURCE, "public_reference", SYMBOL)
            and row.asset in ("BTC", "ETH")
            and row.kind in ("on_chain", "feed_health"),
            "EVENT_SCOPE",
        )
        require(
            type(row.schema_version) is int and row.schema_version == 1, "STRICT_SCHEMA_VERSION"
        )
        native_id = None
        if row.kind == "on_chain":
            native = payload["native_row"]
            native_id = f"{native['asset']}:{native['time']}:{row.observed_at_ms}"
        require(row.event_id == identity(row, native_id), "CANONICAL_IDENTITY")
    return result


def native_audit(
    entities: list[dict[str, Any]],
    rows: tuple[MarketEvent, ...],
    policy: dict[str, Any],
    cutoff: int,
    transport: str,
) -> dict[str, object]:
    assets = policy["assets"]
    require(assets == ["BTC", "ETH"], "EXPLICIT_ASSET_POLICY")
    bounds = {
        "poll_interval_s": (60, 86400),
        "lookback_days": (1, 30),
        "max_age_ms": (1, 30 * DAY_MS),
        "max_pages": (1, 8),
        "max_rows": (1, 1024),
        "max_response_bytes": (1024, 4000000),
        "timeout_s": (1, 120),
    }
    require(set(policy) == {"assets", *bounds}, "EXACT_COLLECTOR_POLICY_FIELDS")
    for name, (minimum, maximum) in bounds.items():
        require(minimum <= integer(policy[name]) <= maximum, "BOUNDED_EXPLICIT_COLLECTOR_POLICY")
    require(0 < len(entities) <= integer(policy["max_pages"]) <= 8, "NATIVE_PAGE_BUDGET")
    data = [row for row in rows if row.kind == "on_chain"]
    health = [row for row in rows if row.kind == "feed_health"]
    require(
        len(health) == 2 and {r.asset for r in health} == {"BTC", "ETH"}, "EXACT_PER_ASSET_HEALTH"
    )
    bodies = {row.asset: cast(dict[str, Any], decode(row.payload_json.encode())) for row in health}
    started = {integer(body["poll_started_at_ms"]) for body in bodies.values()}
    finished = {integer(body["poll_finished_at_ms"]) for body in bodies.values()}
    require(len(started) == len(finished) == 1, "SINGLE_POLL_CLOCKS")
    start, finish = next(iter(started)), next(iter(finished))
    require(start <= finish <= cutoff, "POLL_CUTOFF")
    begin = start // DAY_MS * DAY_MS - integer(policy["lookback_days"]) * DAY_MS
    end = start // DAY_MS * DAY_MS - DAY_MS
    require(
        all(
            body["frequency"] == "1d"
            and body["window_start_ms"] == begin
            and body["window_end_ms"] == end
            and body["ok"] is True
            and body["reason"] == "DAILY_REGIME_OBSERVED"
            for body in bodies.values()
        ),
        "SUCCESS_BOUND_WINDOW",
    )
    expected = {}
    previous = start
    next_token = None
    tokens = set()
    params_base = {
        "assets": "btc,eth",
        "metrics": "CapMrktCurUSD,CapMVRVCur",
        "frequency": "1d",
        "page_size": str(min(100, integer(policy["max_rows"]))),
        "start_time": datetime.fromtimestamp(begin / 1000, UTC).strftime("%Y-%m-%d"),
        "end_time": datetime.fromtimestamp(end / 1000, UTC).strftime("%Y-%m-%d"),
        "paging_from": "start",
    }
    for index, entity in enumerate(entities):
        require(
            entity["basis"]
            == (
                "actual_public_response" if transport == "public_http" else "explicit_mock_fixture"
            ),
            "NATIVE_TRANSPORT_LABEL",
        )
        require(
            type(entity["http_status"]) is int and entity["http_status"] == 200,
            "PUBLIC_POLL_UNAVAILABLE",
        )
        params = dict(params_base)
        if next_token is not None:
            params["next_page_token"] = next_token
        require(entity["request"] == params, "EXACT_REQUEST_PARAMETERS")
        raw = bytes.fromhex(entity["body_hex"])
        require(
            len(raw) <= integer(policy["max_response_bytes"])
            and type(entity["bytes"]) is int
            and len(raw) == entity["bytes"]
            and hashlib.sha256(raw).hexdigest() == entity["sha256"],
            "NATIVE_ENTITY_HASH_OR_SIZE",
        )
        receipt = integer(entity["received_at_ms"])
        require(previous <= receipt <= finish, "ACTUAL_RESPONSE_CLOCK")
        previous = receipt
        document = decode(raw)
        require(
            isinstance(document, dict)
            and set(document) <= {"data", "next_page_token", "next_page_url"}
            and isinstance(document.get("data"), list),
            "NATIVE_RESPONSE_SCHEMA",
        )
        require(len(document["data"]) <= min(100, integer(policy["max_rows"])), "NATIVE_PAGE_ROWS")
        for native in document["data"]:
            require(
                isinstance(native, dict)
                and native.get("asset") in ("btc", "eth")
                and set(native) <= {"asset", "time", "CapMrktCurUSD", "CapMVRVCur"},
                "NATIVE_METRIC_SCOPE",
            )
            require(
                type(native.get("time")) is str and native["time"][10:] == "T00:00:00.000000000Z",
                "NATIVE_EXACT_DAILY_TIME",
            )
            label = date_label(native["time"][:10])
            require(
                begin <= label <= end and label + DAY_MS <= start, "COMPLETED_REQUESTED_DAILY_LABEL"
            )
            for metric in ("CapMrktCurUSD", "CapMVRVCur"):
                value = native.get(metric)
                if value is not None:
                    require(
                        type(value) is str
                        and len(value) <= 128
                        and re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value) is not None
                        and Decimal(value).is_finite()
                        and Decimal(value) > 0,
                        "NATIVE_DECIMAL_STRING",
                    )
            key = (native["asset"].upper(), label)
            require(key not in expected, "DUPLICATE_NATIVE_ASSET_DATE")
            expected[key] = (native, receipt)
        next_token = document.get("next_page_token")
        if next_token:
            require(
                type(next_token) is str and len(next_token) <= 4096 and next_token not in tokens,
                "DISTINCT_PAGINATION_TOKEN",
            )
            tokens.add(next_token)
            parsed = urlparse(document["next_page_url"])
            expected_url = urlparse(URL)
            query = parse_qsl(parsed.query, keep_blank_values=True)
            require(
                parsed.scheme == expected_url.scheme
                and parsed.netloc == expected_url.netloc
                and parsed.path == expected_url.path
                and not parsed.fragment
                and len(query) == len(params_base) + 1
                and dict(query) == params_base | {"next_page_token": next_token},
                "FIXED_CONTINUATION_ORIGIN_AND_PARAMETERS",
            )
        require(
            (index == len(entities) - 1) == (not next_token and not document.get("next_page_url")),
            "COMPLETE_FINITE_PAGINATION",
        )
    require(len(data) == len(expected) <= integer(policy["max_rows"]), "EXACT_NATIVE_ROW_COUNT")
    for row in data:
        body = cast(dict[str, Any], decode(row.payload_json.encode()))
        native, receipt = expected[(row.asset, row.event_time_ms)]
        require(
            body["native_row"] == native
            and row.observed_at_ms == receipt
            and body["response_received_at_ms"] == receipt
            and body["poll_started_at_ms"] == start,
            "EXACT_NATIVE_VALUE_AND_FIRST_RECEIPT",
        )
        require(
            set(body)
            == {
                "frequency",
                "native_row",
                "units",
                "period_label_ms",
                "conservative_complete_after_ms",
                "response_received_at_ms",
                "poll_started_at_ms",
                "publication_time_available",
                "revision_basis",
                "unavailable_metrics",
                "exchange_attribution_available",
            },
            "EXACT_DAILY_PAYLOAD_FIELDS",
        )
        require(row.timestamp_kind == "source", "NATIVE_SOURCE_CLOCK_KIND")
        require(
            body["units"] == {"CapMrktCurUSD": "USD", "CapMVRVCur": "Dimensionless"}
            and body["conservative_complete_after_ms"] == row.event_time_ms + DAY_MS
            and body["period_label_ms"] == row.event_time_ms
            and body["frequency"] == "1d"
            and body["revision_basis"] == "response observation; not first publication"
            and body["unavailable_metrics"] == ["CapRealUSD", "SOPR"]
            and body["publication_time_available"] is False
            and body["exchange_attribution_available"] is False,
            "NATIVE_UNITS_OR_SCOPE",
        )
    for row in health:
        body = bodies[row.asset]
        require(
            row.event_time_ms == row.observed_at_ms == finish
            and row.timestamp_kind == "receipt"
            and body["response_received_at_ms"] == previous
            and body["publication_time_available"] is False
            and body["unavailable_metrics"] == ["CapRealUSD", "SOPR"],
            "EXACT_HEALTH_CLOCK_SCOPE",
        )
        require(
            body["code"] is None and body["error_type"] is None, "HEALTH_NO_FAILED_POLL_DIAGNOSTIC"
        )
        selected = sorted((r for r in data if r.asset == row.asset), key=lambda r: r.event_time_ms)
        require(
            bool(selected)
            and body["accepted_observations"]
            == [
                {
                    "event_id": r.event_id,
                    "observed_at_ms": r.observed_at_ms,
                    "period_label_ms": r.event_time_ms,
                }
                for r in selected
            ],
            "EXACT_BOUND_NATIVE_MEMBERSHIP",
        )
        require(
            start - selected[-1].event_time_ms <= integer(policy["max_age_ms"]),
            "LATEST_PERIOD_STALE",
        )
        latest_native = cast(dict[str, Any], decode(selected[-1].payload_json.encode()))[
            "native_row"
        ]
        require(
            all(
                latest_native.get(metric) is not None for metric in ("CapMrktCurUSD", "CapMVRVCur")
            ),
            "LATEST_NATIVE_METRICS_AVAILABLE",
        )
    return {
        "native_rows": len(data),
        "health_rows": len(health),
        "poll_started_at_ms": start,
        "poll_finished_at_ms": finish,
        "native_response_receipts": [e["received_at_ms"] for e in entities],
        "requested_window_start_ms": begin,
        "requested_window_end_ms": end,
        "native_event_ids": sorted(r.event_id for r in data),
    }


def _audit(proof: Path, require_current_code: bool, transport: str) -> dict[str, object]:
    require(digest(Path(__file__)) == EXECUTED_AUDITOR_SHA, "AUDITOR_SOURCE_CHANGED_AFTER_IMPORT")
    proof = safe(proof)
    before = tree(proof)
    report = load(proof / "report.json")
    require(report.get("transport_provenance") == transport, "REPORT_TRANSPORT_PROVENANCE")
    require(all(report.get(flag) is False for flag in CONTROLS), "FALSE_READINESS_AND_SCOPE_FLAGS")
    require(
        report.get("status")
        == (
            "PUBLIC_DAILY_VALUATION_POLL_VERIFIED"
            if transport == "public_http"
            else "MOCK_DAILY_VALUATION_POLL_VERIFIED"
        ),
        "SUCCESS_REPORT_REQUIRED",
    )
    require(
        type(report.get("orders_submitted")) is int and report["orders_submitted"] == 0,
        "ZERO_ORDERS",
    )
    require(
        all(
            type(report.get(name)) is int and report[name] == 0
            for name in ("historical_strategy_trials", "paper_days", "paper_trades")
        ),
        "ZERO_STRATEGY_AND_PAPER_CLAIMS",
    )
    policy = report["policy"]
    require(isinstance(policy, dict), "EXPLICIT_POLICY")
    cutoff = integer(report["as_of_ms"])
    hashes = report["code_hashes"]
    require(isinstance(hashes, dict) and bool(hashes), "FROZEN_CODE_BINDING")
    if require_current_code:
        expected_paths = {
            p.relative_to(ROOT).as_posix() for p in (ROOT / "src/selective_options").glob("*.py")
        }
        require(set(hashes) == expected_paths, "FULL_SOURCE_HASH_SET")
        for name, hashed in hashes.items():
            path = safe(ROOT / name)
            require(path.is_relative_to(ROOT) and digest(path) == hashed, "CURRENT_SOURCE_HASH")
        require(
            digest(ROOT / "scripts/verify_coinmetrics.py") == report["verifier_sha256"],
            "FROZEN_PRODUCER_HASH",
        )
    entity_document = load(proof / "native-entities.json")
    require(entity_document["transport_provenance"] == transport, "NATIVE_DOCUMENT_PROVENANCE")
    entities = entity_document["entities"]
    require(
        isinstance(entities, list) and all(isinstance(e, dict) for e in entities),
        "NATIVE_ENTITIES_LIST",
    )
    source, archive, restored = (
        proof / "events.duckdb",
        proof / "events.parquet",
        proof / "restored/events.duckdb",
    )
    require(
        report["source_sha256"] == digest(source)
        and report["archive_sha256"] == digest(archive)
        and report["native_entities_sha256"] == digest(proof / "native-entities.json")
        and report["restored_sha256"] == digest(restored),
        "REPORT_ARTIFACT_BINDINGS",
    )
    original = saved_rows(source, archive)
    require(saved_rows(restored) == original, "RESTORED_EXACT_NATIVE_EQUALITY")
    restore = load(proof / "restored/restore.json")
    require(
        restore["status"] == "RESTORE_FINISHED"
        and restore["rows"] == restore["redis_delivery_pending"] == len(original)
        and restore["archive_sha256"] == digest(archive)
        and restore["observation_timestamps_preserved"] is True
        and restore["ready_for_signals"] is False
        and restore["trading_enabled"] is False,
        "RESTORE_REPORT",
    )
    native = native_audit(cast(list[dict[str, Any]], entities), original, policy, cutoff, transport)
    require(set(report["quality"]) == {"BTC", "ETH"}, "EXACT_QUALITY_ASSETS")
    for asset in ("BTC", "ETH"):
        asset_rows = [r for r in original if r.asset == asset and r.kind == "on_chain"]
        latest = max(asset_rows, key=lambda row: row.event_time_ms)
        body = decode(latest.payload_json.encode())
        health_row = next(r for r in original if r.asset == asset and r.kind == "feed_health")
        view = report["quality"][asset]
        require(
            view["state"] == "DAILY_REGIME_OBSERVED"
            and view["blocker"] is None
            and view["source"] == SOURCE
            and view["asset"] == asset
            and view["symbol"] == SYMBOL
            and view["as_of_ms"] == cutoff
            and view["health_event_id"] == health_row.event_id
            and view["accepted_event_id"] == latest.event_id
            and view["native_row"] == body["native_row"]
            and view["native_units"] == body["units"]
            and view["period_label_ms"] == latest.event_time_ms
            and view["accepted_observed_at_ms"] == latest.observed_at_ms
            and view["period_age_ms"] == cutoff - latest.event_time_ms
            and view["max_poll_age_ms"] == policy["poll_interval_s"] * 2000
            and view["max_data_age_ms"] == policy["max_age_ms"]
            and view["response_age_ms"] == cutoff - integer(entities[-1]["received_at_ms"])
            and view["poll_age_ms"] == cutoff - integer(native["poll_finished_at_ms"])
            and view["response_received_at_ms"] == entities[-1]["received_at_ms"]
            and view["requested_window_start_ms"] == native["requested_window_start_ms"]
            and view["requested_window_end_ms"] == native["requested_window_end_ms"]
            and view["historical_observations"]
            == view["bound_cohort_observations"]
            == len(asset_rows)
            and view["publication_time_available"] is False
            and view["provider_finality_independently_verified"] is False
            and view["history_complete"] is False
            and view["exchange_attribution_available"] is False
            and view["unavailable_metrics"] == ["CapRealUSD", "SOPR"]
            and view["ready_for_signals"] is False
            and view["trading_enabled"] is False,
            "EXACT_NATIVE_QUALITY_REPORT",
        )
    require(
        type(report["request_attempts"]) is int
        and report["request_attempts"] == len(entities)
        and type(report["real_public_requests_attempted"]) is int
        and report["real_public_requests_attempted"]
        == (len(entities) if transport == "public_http" else 0)
        and type(report["retries"]) is int
        and report["retries"] == 0,
        "FINITE_REQUEST_COUNT",
    )
    require(
        type(report["rows"]) is int and report["rows"] == report["persisted_rows"] == len(original),
        "REPORT_COMMITTED_COUNTS",
    )
    require(
        type(report["native_daily_rows"]) is int
        and report["native_daily_rows"] == native["native_rows"]
        and report["outbox_pending_after_reopen"]
        == report["restored_outbox_pending"]
        == len(original)
        and report["parquet_exact_match"] is True
        and report["closed_reload_exact_match"] is True,
        "EXACT_REOPEN_ARCHIVE_COUNT_REPORT",
    )
    boundaries = []
    with duckdb.connect(str(source), read_only=True) as connection:
        for cut in sorted(
            {
                clock
                for row in original
                for clock in (
                    max(row.event_time_ms, row.observed_at_ms) - 1,
                    max(row.event_time_ms, row.observed_at_ms),
                )
            }
        ):
            included = sorted(
                r.event_id for r in original if r.event_time_ms <= cut and r.observed_at_ms <= cut
            )
            actual = [
                r[0]
                for r in connection.execute(
                    "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                    [cut, cut],
                ).fetchall()
            ]
            require(actual == included, "SQL_BOTH_CLOCK_BOUNDARY")
            boundaries.append({"as_of_ms": cut, "event_ids": included})
    require(
        report["boundary_cutoffs_ms"] == [b["as_of_ms"] for b in boundaries],
        "REPORT_EXACT_BOUNDARIES",
    )
    if require_current_code:
        require(
            all(digest(safe(ROOT / name)) == hashed for name, hashed in hashes.items()),
            "SOURCE_CHANGED_DURING_AUDIT",
        )
    require(tree(proof) == before, "PROOF_CHANGED_DURING_AUDIT")
    require(digest(Path(__file__)) == EXECUTED_AUDITOR_SHA, "AUDITOR_SOURCE_CHANGED_DURING_AUDIT")
    return {
        "status": "COINMETRICS_PUBLIC_AUDITED"
        if transport == "public_http"
        else "MOCK_FIXTURE_AUDITED",
        "transport_provenance": transport,
        "public_requests_observed": transport == "public_http",
        "audit_source_sha256": EXECUTED_AUDITOR_SHA,
        "current_code_hashes_verified": require_current_code,
        "artifact_sha256": before,
        "audited_report_sha256": before["report.json"],
        "native": native,
        "committed_rows": len(original),
        "pending_delivery": len(original),
        "boundaries": boundaries,
        "code_hashes": hashes,
        "verifier_sha256": report["verifier_sha256"],
        **dict.fromkeys(CONTROLS, False),
        "new_public_requests": 0,
    }


async def audit(
    proof: Path,
    *,
    require_current_code: bool = True,
    transport_provenance: Literal["public_http", "explicit_mock_fixture"] = "public_http",
) -> dict[str, object]:
    require(type(require_current_code) is bool, "STRICT_CODE_POLICY")
    require(
        transport_provenance in ("public_http", "explicit_mock_fixture"),
        "EXPLICIT_TRANSPORT_POLICY",
    )
    return await complete_thread(_audit, proof, require_current_code, transport_provenance)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output, proof = safe(args.output), safe(args.proof)
    require(
        not output.exists() and output.parent.is_dir() and not output.is_relative_to(proof),
        "NEW_EXTERNAL_OUTPUT_REQUIRED",
    )
    result = asyncio.run(audit(proof))
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
