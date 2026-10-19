"""Exclusive offline saved-bar report for causally confirmed swing structure."""

import json
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import Environment, PointInTimeSnapshot
from .features import _integer
from .research import _hash, _path, load_bar_snapshot
from .swing_structure_features import compute_swing_structure

_JSON = TypeAdapter(dict[str, JsonValue])


def research_swing_structure(
    database: Path,
    output: Path,
    *,
    asset: str,
    environment: Environment,
    source: str,
    symbol: str,
    timeframe_s: int,
    as_of_ms: int,
    max_latest_age_ms: int,
    left_bars: int = 2,
    right_bars: int = 2,
    max_bars: int = 2000,
    history_start_ms: int | None = None,
) -> dict[str, JsonValue]:
    _integer("max_latest_age_ms", max_latest_age_ms, 0, (1 << 63) - 1)
    _integer("left_bars", left_bars, 1, 1000)
    _integer("right_bars", right_bars, 1, 1000)
    output = _path(output, must_exist=False)

    def calculate(snapshot: PointInTimeSnapshot) -> dict[str, JsonValue]:
        report = compute_swing_structure(
            snapshot,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            left_bars=left_bars,
            right_bars=right_bars,
            max_bars=max_bars,
            max_latest_age_ms=max_latest_age_ms,
        )
        return _JSON.validate_python(report.model_dump(mode="json"))

    # Validate scope and parameters before touching the saved source.
    calculate(PointInTimeSnapshot(as_of_ms, asset, environment, ()))
    database = _path(database)
    if output == database or output == database.parent:
        raise ValueError("swing structure source and output paths must be distinct")
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
                "timeframe_s": timeframe_s,
                "as_of_ms": as_of_ms,
                "max_latest_age_ms": max_latest_age_ms,
                "explicit_history_start_ms": history_start_ms,
                "materialized_rows": len(snapshot.rows()),
                "full_history_or_source_authenticity_verified": False,
            },
            "swing_structure_report": report,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("SWING_STRUCTURE_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    with (output / "swing-structure.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
