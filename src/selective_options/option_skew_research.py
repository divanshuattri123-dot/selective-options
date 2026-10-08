"""Offline DuckDB reader and exclusive report writer for native option skew."""

import json
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import PointInTimeSnapshot
from .options_research import load_options_snapshots
from .options_skew import SkewReport, compute_risk_reversal
from .research import _hash, _path

_JSON = TypeAdapter(dict[str, JsonValue])


def research_option_skew(
    database: Path,
    output: Path,
    *,
    asset: str,
    as_of_ms: int,
    target_absolute_delta: float,
    maximum_delta_error: float,
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_underlying_mismatch_fraction: float,
    max_rows: int = 100_000,
    max_expiries: int = 128,
) -> dict[str, JsonValue]:
    """Read a closed option-history cohort, publish an analytics-only JSON file."""
    output = _path(output, must_exist=False)
    policies = {
        "target_absolute_delta": target_absolute_delta,
        "maximum_delta_error": maximum_delta_error,
        "max_quote_age_ms": max_quote_age_ms,
        "max_discovery_age_ms": max_discovery_age_ms,
        "max_clock_skew_ms": max_clock_skew_ms,
        "max_underlying_mismatch_fraction": max_underlying_mismatch_fraction,
        "max_rows": max_rows,
        "max_expiries": max_expiries,
    }

    def calculate(snapshot: PointInTimeSnapshot, discovery: PointInTimeSnapshot) -> SkewReport:
        return compute_risk_reversal(
            snapshot,
            discovery,
            target_absolute_delta=target_absolute_delta,
            maximum_delta_error=maximum_delta_error,
            max_quote_age_ms=max_quote_age_ms,
            max_discovery_age_ms=max_discovery_age_ms,
            max_clock_skew_ms=max_clock_skew_ms,
            max_underlying_mismatch_fraction=max_underlying_mismatch_fraction,
            max_rows=max_rows,
            max_expiries=max_expiries,
        )

    # Validate every caller policy before opening the source file.
    calculate(
        PointInTimeSnapshot(as_of_ms, asset, "testnet", ()),
        PointInTimeSnapshot(as_of_ms, "SYSTEM", "testnet", ()),
    )
    database = _path(database)
    if output == database or output == database.parent:
        raise ValueError("risk-reversal input and output paths must be distinct")
    snapshot, discovery, digest = load_options_snapshots(
        database, asset=asset, as_of_ms=as_of_ms, max_rows=max_rows
    )
    report: SkewReport = calculate(snapshot, discovery)
    value = _JSON.validate_python(
        {
            "source_database_sha256": digest,
            "source_mutated": False,
            "source_scope_and_cutoff": {
                "asset": asset,
                "environment": "testnet",
                "source": "deribit",
                "as_of_ms": as_of_ms,
                "materialized_rows": len(snapshot.rows()) + len(discovery.rows()),
                "full_history_or_universe_verified": False,
            },
            "policy": policies,
            "risk_reversal_report": report.model_dump(mode="json"),
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("OPTION_SKEW_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    report_path = output / "risk-reversal.json"
    with report_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
