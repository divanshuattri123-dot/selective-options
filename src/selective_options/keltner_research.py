"""Exclusive offline modern Keltner reports from a closed saved-bar database."""

import json
from pathlib import Path

from pydantic import JsonValue, TypeAdapter

from .errors import IntegrityError
from .events import Environment, PointInTimeSnapshot
from .features import _integer
from .keltner_features import compute_keltner_features
from .research import _hash, _path, load_bar_snapshot

_JSON = TypeAdapter(dict[str, JsonValue])


def research_keltner(
    database: Path,
    output: Path,
    *,
    asset: str,
    environment: Environment,
    source: str,
    symbol: str,
    timeframe_s: int,
    as_of_ms: int,
    ema_period: int,
    atr_period: int,
    atr_multiplier: float,
    max_latest_age_ms: int,
    max_bars: int = 2000,
    history_start_ms: int | None = None,
) -> dict[str, JsonValue]:
    """Read both-clock-visible committed bars without source writes or a trial."""
    _integer("max_latest_age_ms", max_latest_age_ms, 0, (1 << 63) - 1)
    output = _path(output, must_exist=False)

    def calculate(snapshot: PointInTimeSnapshot) -> dict[str, JsonValue]:
        result = compute_keltner_features(
            snapshot,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            ema_period=ema_period,
            atr_period=atr_period,
            atr_multiplier=atr_multiplier,
            max_latest_age_ms=max_latest_age_ms,
            max_bars=max_bars,
        )
        return _JSON.validate_python(result.model_dump(mode="json"))

    # Invalid feature policy cannot cause source reads or output creation.
    calculate(PointInTimeSnapshot(as_of_ms, asset, environment, ()))
    database = _path(database)
    if output == database or output == database.parent:
        raise ValueError("Keltner source and output paths must be distinct")
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
            "keltner_report": report,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("KELTNER_SOURCE_CHANGED")
    output.mkdir(parents=True, exist_ok=False)
    with (output / "keltner.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
