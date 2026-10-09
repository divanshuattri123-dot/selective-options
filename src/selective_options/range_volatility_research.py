"""Exclusive offline OHLC range-volatility reports from a closed source database."""

import json
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import Environment, PointInTimeSnapshot
from .range_volatility import compute_range_volatility
from .research import _hash, _path, load_bar_snapshot

_JSON = TypeAdapter(dict[str, JsonValue])


def research_range_volatility(
    database: Path,
    output: Path,
    *,
    asset: str,
    environment: Environment,
    source: str,
    symbol: str,
    timeframe_s: int,
    as_of_ms: int,
    windows: tuple[int, ...],
    max_latest_age_ms: int,
    max_bars: int = 2000,
    annualization_periods_per_year: float | None = None,
    history_start_ms: int | None = None,
) -> dict[str, JsonValue]:
    """Calculate model-based historical estimators, never signals or a trial."""
    output = _path(output, must_exist=False)

    def calculate(snapshot: PointInTimeSnapshot) -> dict[str, JsonValue]:
        result = compute_range_volatility(
            snapshot,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            windows=windows,
            max_latest_age_ms=max_latest_age_ms,
            max_bars=max_bars,
            annualization_periods_per_year=annualization_periods_per_year,
        )
        return _JSON.validate_python(result.model_dump(mode="json"))

    # Policy validation has no source or output side effects.
    calculate(PointInTimeSnapshot(as_of_ms, asset, environment, ()))
    database = _path(database)
    if output == database or output == database.parent:
        raise ValueError("range volatility source and output paths must be distinct")
    snapshot, digest = load_bar_snapshot(
        database,
        asset=asset,
        environment=environment,
        source=source,
        symbol=symbol,
        as_of_ms=as_of_ms,
        max_bars=max_bars,
        history_start_ms=history_start_ms,
    )
    report = calculate(snapshot)
    value = _JSON.validate_python(
        {
            "source_database_sha256": digest,
            "source_mutated": False,
            "source_scope_and_cutoff": {
                "asset": asset,
                "environment": environment,
                "source": source,
                "symbol": symbol,
                "as_of_ms": as_of_ms,
                "explicit_history_start_ms": history_start_ms,
                "materialized_rows": len(snapshot.rows()),
                "full_history_or_source_authenticity_verified": False,
            },
            "range_volatility_report": report,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RANGE_VOLATILITY_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    with (output / "range-volatility.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
