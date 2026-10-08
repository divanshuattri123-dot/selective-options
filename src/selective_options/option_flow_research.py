"""Read-only native option amount ratios with exclusive research report output."""

import json
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import PointInTimeSnapshot
from .options_flow_features import PutCallReport, compute_put_call_ratios
from .options_research import load_options_snapshots
from .research import _hash, _path

_JSON = TypeAdapter(dict[str, JsonValue])


def research_option_flow(
    database: Path,
    output: Path,
    *,
    asset: str,
    as_of_ms: int,
    amount_unit_policy: str,
    volume_field: str,
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_rows: int = 100_000,
    max_expiries: int = 128,
) -> dict[str, JsonValue]:
    """Compute native OI/rolling-volume put/call ratios, never a strategy trial."""
    if (
        type(amount_unit_policy) is not str
        or amount_unit_policy != "deribit_option_base_currency"
        or type(volume_field) is not str
        or volume_field != "stats.volume"
    ):
        raise ValueError("explicit native option base-currency and stats.volume policies required")
    if type(max_rows) is not int or not 1 <= max_rows <= 100_000:
        raise ValueError("option flow source row budget must be an integer in [1, 100000]")
    output = _path(output, must_exist=False)

    def calculate(snapshot: PointInTimeSnapshot, discovery: PointInTimeSnapshot) -> PutCallReport:
        return compute_put_call_ratios(
            snapshot,
            discovery,
            amount_unit_policy="deribit_option_base_currency",
            volume_field="stats.volume",
            max_quote_age_ms=max_quote_age_ms,
            max_discovery_age_ms=max_discovery_age_ms,
            max_clock_skew_ms=max_clock_skew_ms,
            max_rows=max_rows,
            max_expiries=max_expiries,
        )

    # Reject invalid policies before touching a database or creating output.
    calculate(
        PointInTimeSnapshot(as_of_ms, asset, "testnet", ()),
        PointInTimeSnapshot(as_of_ms, "SYSTEM", "testnet", ()),
    )
    database = _path(database)
    if output == database or output == database.parent:
        raise ValueError("option flow source and output paths must be distinct")
    snapshot, discovery, digest = load_options_snapshots(
        database, asset=asset, as_of_ms=as_of_ms, max_rows=max_rows
    )
    report = calculate(snapshot, discovery)
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
            "policy": {
                "amount_unit_policy": amount_unit_policy,
                "volume_field": volume_field,
                "max_quote_age_ms": max_quote_age_ms,
                "max_discovery_age_ms": max_discovery_age_ms,
                "max_clock_skew_ms": max_clock_skew_ms,
                "max_rows": max_rows,
                "max_expiries": max_expiries,
            },
            "put_call_report": report.model_dump(mode="json"),
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("OPTION_FLOW_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    with (output / "put-call.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
