"""Explicit read-only options analytics on a bounded saved testnet cohort.

Both availability clocks filter rows before the overflow sentinel. Every retained
metadata/ticker/control row in that scope is supplied unchanged to pure feature
validation. The application bound limits materialization, not SQL scan cost.
"""

import json
from pathlib import Path
from typing import Literal, cast

import duckdb
from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import MarketEvent, PointInTimeSnapshot
from .options_features import OptionsFeatureReport, compute_options_features
from .research import MAX_SOURCE_BYTES, _hash, _path

_JSON = TypeAdapter(dict[str, JsonValue])


def load_options_snapshots(
    database: Path, *, asset: str, as_of_ms: int, max_rows: int = 100000
) -> tuple[PointInTimeSnapshot, PointInTimeSnapshot, str]:
    """Return asset quotes/metadata and SYSTEM controls without changing the source."""
    if type(asset) is not str or asset not in ("BTC", "ETH", "SOL", "PAXG"):
        raise ValueError("options research requires one explicit supported asset")
    if type(as_of_ms) is not int or not 0 < as_of_ms <= 2**63 - 1:
        raise ValueError("invalid options research cutoff")
    if type(max_rows) is not int or not 1 <= max_rows <= 100000:
        raise ValueError("invalid options research materialized-row budget")
    database = _path(database)
    if database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RESEARCH_SOURCE_MUST_BE_CLOSED")
    if database.stat().st_size > MAX_SOURCE_BYTES:
        raise IntegrityError("RESEARCH_SOURCE_BYTE_BUDGET")
    digest = _hash(database)
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events WHERE source='deribit' AND environment='testnet' "
            "AND ((asset=? AND kind IN ('instrument','ticker')) "
            "OR (asset='SYSTEM' AND kind='status')) "
            "AND event_time_ms<=? AND observed_at_ms<=? "
            "ORDER BY observed_at_ms,event_time_ms,event_id LIMIT ?",
            [asset, as_of_ms, as_of_ms, max_rows + 1],
        )
        values = cursor.fetchall()
        if len(values) > max_rows:
            raise IntegrityError("OPTIONS_RESEARCH_HISTORY_OVER_BUDGET")
        columns = [entry[0] for entry in cursor.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
            for row in values
        )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RESEARCH_SOURCE_CHANGED")
    return (
        PointInTimeSnapshot(as_of_ms, asset, "testnet", tuple(r for r in rows if r.asset == asset)),
        PointInTimeSnapshot(
            as_of_ms, "SYSTEM", "testnet", tuple(r for r in rows if r.asset == "SYSTEM")
        ),
        digest,
    )


def research_options(
    database: Path,
    output: Path,
    *,
    asset: str,
    as_of_ms: int,
    option_type: str,
    moneyness_method: str,
    tie_policy: str,
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_atm_log_moneyness: float,
    max_underlying_mismatch_fraction: float,
    flat_threshold_iv_points: float,
    max_rows: int = 100000,
    max_expiries: int = 128,
) -> dict[str, JsonValue]:
    """Compute transparent ATM IV and term shape; never a signal or counted trial."""
    if option_type not in ("call", "put") or type(option_type) is not str:
        raise ValueError("options research requires call or put")
    if type(moneyness_method) is not str or moneyness_method != "native_underlying_log_distance":
        raise ValueError("options research requires native underlying log-distance moneyness")
    if type(tie_policy) is not str or tie_policy != "exact_distance_refuse":
        raise ValueError("options research refuses exact-distance ATM ties")
    output = _path(output, must_exist=False)
    # The pure calculation validates all policies before the source is opened.
    side = cast(Literal["call", "put"], option_type)
    policies = dict(
        option_type=side,
        moneyness_method=moneyness_method,
        tie_policy=tie_policy,
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_atm_log_moneyness=max_atm_log_moneyness,
        max_underlying_mismatch_fraction=max_underlying_mismatch_fraction,
        flat_threshold_iv_points=flat_threshold_iv_points,
        max_rows=max_rows,
        max_expiries=max_expiries,
    )
    # Explicit kwargs keep the public policy typed and visible to strict mypy.
    compute_options_features(
        PointInTimeSnapshot(as_of_ms, asset, "testnet", ()),
        PointInTimeSnapshot(as_of_ms, "SYSTEM", "testnet", ()),
        option_type=side,
        moneyness_method="native_underlying_log_distance",
        tie_policy="exact_distance_refuse",
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_atm_log_moneyness=max_atm_log_moneyness,
        max_underlying_mismatch_fraction=max_underlying_mismatch_fraction,
        flat_threshold_iv_points=flat_threshold_iv_points,
        max_rows=max_rows,
        max_expiries=max_expiries,
    )
    snapshot, discovery, digest = load_options_snapshots(
        database, asset=asset, as_of_ms=as_of_ms, max_rows=max_rows
    )
    report: OptionsFeatureReport = compute_options_features(
        snapshot,
        discovery,
        option_type=side,
        moneyness_method="native_underlying_log_distance",
        tie_policy="exact_distance_refuse",
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_atm_log_moneyness=max_atm_log_moneyness,
        max_underlying_mismatch_fraction=max_underlying_mismatch_fraction,
        flat_threshold_iv_points=flat_threshold_iv_points,
        max_rows=max_rows,
        max_expiries=max_expiries,
    )
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
            "options_report": report.model_dump(mode="json"),
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    checked_database = _path(database)
    if (
        _hash(checked_database) != digest
        or checked_database.with_suffix(checked_database.suffix + ".wal").exists()
    ):
        raise IntegrityError("RESEARCH_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    (output / "options-features.json").write_text(
        json.dumps(value, indent=2) + "\n", encoding="utf-8"
    )
    return value
