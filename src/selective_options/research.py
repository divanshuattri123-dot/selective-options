"""Explicit offline research commands with immutable inputs and registered runs.

These commands do not collect feeds, construct signals, send orders, or promote
milestones. Source-window quality and simulator limitations remain visible.
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt, TypeAdapter

from .backtest import (
    FeePolicy,
    HistoricQuote,
    InstrumentSpec,
    SimulatedOrder,
    SimulationPolicy,
    SimulationResult,
    simulate,
)
from .errors import IntegrityError
from .events import Environment, MarketEvent, PointInTimeSnapshot
from .features import FeatureReport, compute_features
from .trend_features import compute_trend_features
from .trials import run_registered, trial_counts

MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_SOURCE_BYTES = 512 * 1024 * 1024
_JSON = TypeAdapter(dict[str, JsonValue])


class BacktestInput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    schema_version: StrictInt = Field(default=1, ge=1, le=1)
    data_kind: Literal["synthetic_fixture", "historical_research"]
    provenance: dict[str, JsonValue]
    as_of_ms: StrictInt = Field(gt=0, le=2**63 - 1)
    instrument: InstrumentSpec
    fees: FeePolicy
    policy: SimulationPolicy
    orders: tuple[SimulatedOrder, ...] = Field(max_length=1000)
    quotes: tuple[HistoricQuote, ...] = Field(max_length=10000)


def _path(path: Path, *, must_exist: bool = True) -> Path:
    absolute = path.absolute()
    for part in (absolute, *absolute.parents):
        if part.is_symlink() or (
            part.exists() and getattr(part.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("research symlink/reparse/junction paths are refused")
    if must_exist and not absolute.is_file():
        raise FileNotFoundError("research input must be an existing regular file")
    return absolute.resolve()


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IntegrityError("RESEARCH_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _read_input(path: Path) -> tuple[BacktestInput, str]:
    path = _path(path)
    if path.stat().st_size > MAX_INPUT_BYTES:
        raise IntegrityError("RESEARCH_INPUT_BYTE_BUDGET")
    with path.open("rb") as stream:
        content = stream.read(MAX_INPUT_BYTES + 1)
    if len(content) > MAX_INPUT_BYTES:
        raise IntegrityError("RESEARCH_INPUT_BYTE_BUDGET")
    raw = json.loads(content, object_pairs_hook=_duplicate_keys)
    canonical = json.dumps(raw, allow_nan=False)
    return BacktestInput.model_validate_json(canonical), hashlib.sha256(content).hexdigest()


def load_bar_snapshot(
    database: Path,
    *,
    asset: str,
    environment: Environment,
    source: str,
    symbol: str,
    as_of_ms: int,
    max_bars: int,
    history_start_ms: int | None = None,
) -> tuple[PointInTimeSnapshot, str]:
    """Read selected committed bars under BOTH clocks; never truncate history.

    The bound limits Python materialization, not database engine scan cost. A
    window containing incomplete bars is passed unchanged to feature quality.
    """
    if type(as_of_ms) is not int or not 0 < as_of_ms <= 2**63 - 1:
        raise ValueError("invalid research cutoff")
    if type(max_bars) is not int or not 1 <= max_bars <= 10000:
        raise ValueError("invalid research row bound")
    if history_start_ms is not None and (
        type(history_start_ms) is not int or not 0 < history_start_ms <= as_of_ms
    ):
        raise ValueError("invalid explicit research history start")
    if environment not in ("testnet", "public_reference") or not all(
        type(value) is str and 0 < len(value) <= 256 for value in (asset, source, symbol)
    ):
        raise ValueError("invalid research source scope")
    database = _path(database)
    if database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RESEARCH_SOURCE_MUST_BE_CLOSED")
    if database.stat().st_size > MAX_SOURCE_BYTES:
        raise IntegrityError("RESEARCH_SOURCE_BYTE_BUDGET")
    digest = _hash(database)
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            """SELECT * FROM events WHERE kind='bar' AND asset=? AND environment=?
               AND source=? AND symbol=? AND event_time_ms<=? AND observed_at_ms<=?
               AND event_time_ms>=?
               ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?""",
            [
                asset,
                environment,
                source,
                symbol,
                as_of_ms,
                as_of_ms,
                history_start_ms if history_start_ms is not None else 1,
                max_bars + 1,
            ],
        )
        columns = [item[0] for item in cursor.description]
        values = cursor.fetchall()
    if len(values) > max_bars:
        raise IntegrityError("RESEARCH_BAR_HISTORY_BUDGET")
    rows = tuple(
        MarketEvent.model_validate(dict(zip(columns, row, strict=True)), strict=True)
        for row in values
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RESEARCH_SOURCE_CHANGED")
    return PointInTimeSnapshot(as_of_ms, asset, environment, rows), digest


def research_features(
    database: Path,
    output: Path,
    *,
    asset: str,
    environment: Environment,
    source: str,
    symbol: str,
    timeframe_s: int,
    as_of_ms: int,
    max_bars: int = 2000,
    annualization_periods_per_year: float | None = None,
    history_start_ms: int | None = None,
    include_trend: bool = False,
    percentile_window: int = 100,
    percentile_min_history: int = 20,
) -> dict[str, JsonValue]:
    if type(include_trend) is not bool:
        raise ValueError("include_trend requires an explicit boolean")
    if not include_trend and (percentile_window != 100 or percentile_min_history != 20):
        raise ValueError("percentile policy requires include_trend")
    output = _path(output, must_exist=False)
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
    report: FeatureReport = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_bars=max_bars,
        annualization_periods_per_year=annualization_periods_per_year,
    )
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
            },
            "feature_report": report.model_dump(mode="json"),
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    if include_trend:
        trend = compute_trend_features(
            snapshot,
            source=source,
            symbol=symbol,
            timeframe_s=timeframe_s,
            max_bars=max_bars,
            annualization_periods_per_year=annualization_periods_per_year,
            percentile_window=percentile_window,
            percentile_min_history=percentile_min_history,
        )
        if trend.base != report:
            raise IntegrityError("RESEARCH_TREND_BASE_MISMATCH")
        value["trend_report"] = _JSON.validate_python(trend.model_dump(mode="json"))
    output.mkdir(parents=True, exist_ok=False)
    (output / "features.json").write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return value


def _result_summary(result: SimulationResult) -> dict[str, JsonValue]:
    encoded = result.model_dump_json().encode()
    return {
        "simulation_id": result.simulation_id,
        "result_sha256": hashlib.sha256(encoded).hexdigest(),
        "fills": len(result.fills),
        "closed_roundtrip_net_pnl": str(result.closed_roundtrip_net_pnl)
        if result.closed_roundtrip_net_pnl is not None
        else None,
        "net_cash_flow": str(result.net_cash_flow),
        "inventory_contracts": str(result.inventory_contracts),
        "gate2_complete": False,
        "trading_enabled": False,
    }


def research_backtest(input_path: Path, output: Path, registry: Path) -> dict[str, JsonValue]:
    output = _path(output, must_exist=False)
    registry = _path(registry, must_exist=False)
    input_path = _path(input_path)
    report_path = output / "backtest.json"
    if (
        input_path == registry
        or input_path == output
        or registry == output
        or registry == report_path
    ):
        raise ValueError("research inputs, outputs and journal must be distinct")
    request, digest = _read_input(input_path)
    output.mkdir(parents=True, exist_ok=False)
    parameters = _JSON.validate_python(
        request.model_dump(mode="json", exclude={"provenance", "quotes"})
        | {"input_sha256": digest, "quote_count": len(request.quotes)}
    )
    provenance: dict[str, JsonValue] = {
        "input_sha256": digest,
        "data_kind": request.data_kind,
        "declared": request.provenance,
        "historical_data_independently_verified": False,
        "research_runner_version": 1,
    }

    def run() -> SimulationResult:
        if _hash(input_path) != digest:
            raise IntegrityError("RESEARCH_SOURCE_CHANGED")
        result = simulate(
            orders=request.orders,
            quotes=request.quotes,
            instrument=request.instrument,
            fees=request.fees,
            policy=request.policy,
            as_of_ms=request.as_of_ms,
        )
        if _hash(input_path) != digest:
            raise IntegrityError("RESEARCH_SOURCE_CHANGED")
        return result

    handle, result = run_registered(registry, parameters, provenance, run, _result_summary)
    value = _JSON.validate_python(
        {
            "trial_id": handle.trial_id,
            "input_sha256": digest,
            "data_kind": request.data_kind,
            "provenance": provenance,
            "registry_counts": trial_counts(registry),
            "simulation": result.model_dump(mode="json"),
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    # Exclusive publication cannot replace a cooperating journal or another report.
    with report_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
    return value
