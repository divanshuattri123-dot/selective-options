"""Deterministic, scoped closed-bar features; no signal, execution, or readiness path.

Version 1 seeds EMA(p) with the first p-close arithmetic mean and then alpha=2/(p+1).
EMA slope is (latest EMA - EMA k bars earlier)/k, in price units per bar. RSI14 and
ATR14 use 14 close-to-close changes/true ranges, an arithmetic seed, then Wilder
alpha=1/14; both need 15 bars. A flat RSI denominator is explicitly unavailable.
MACD is EMA12-EMA26; its signal EMA9 starts at the first valid MACD, needing 34
bars. Bollinger width is 4*population_std(close20)/mean(close20), dimensionless.
ROC(k) is close/close[k bars earlier]-1. Realized volatility is sample standard
deviation of the last k logarithmic close returns, multiplied by sqrt(explicit
periods_per_year). Windows are BAR counts, never silently interpreted as days.

All supplied history is audited. The history budget refuses excess rows rather
than silently reseeding indicators on a truncated tail. Each output records the
exact history digest, formula version, warmup, units, and annualization assumption.
"""

import hashlib
import json
import math
import re
from collections.abc import Callable
from typing import Literal

from pydantic import ConfigDict, Field, ValidationError

from .errors import IntegrityError
from .events import Environment, MarketEvent, PointInTimeSnapshot, make_event
from .models import ClosedBarPayload, ExternalModel

FORMULA_VERSION: Literal["closed-bars-v1"] = "closed-bars-v1"
_CLOCK_FIELDS = {
    "event_time_ms",
    "observed_at_ms",
    "open_time_ms",
    "close_time_ms",
    "coverage_start_ms",
}


class _Bar(ClosedBarPayload):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)
    volume_unit: Literal["base_asset"] | None = None
    aggregation_version: Literal[1] | None = None
    lateness_ms: int | None = Field(default=None, ge=0)
    coverage_start_ms: int | None = Field(default=None, ge=0)


class FeatureValue(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    name: str
    state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"]
    value: float | None
    unit: str
    bars_required: int
    reason: str | None


class FeatureReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["closed-bars-v1"] = FORMULA_VERSION
    state: Literal["FEATURES_COMPUTED", "UNAVAILABLE"]
    asset: str
    environment: Environment
    source: str
    symbol: str
    timeframe_s: int
    as_of_ms: int
    history_budget: int
    bars_available: int
    bars_used: int
    first_close_ms: int | None
    latest_close_ms: int | None
    latest_bar_age_ms: int | None
    max_latest_age_ms: int
    history_sha256: str
    annualization_periods_per_year: float | None
    blockers: tuple[str, ...]
    features: tuple[FeatureValue, ...]
    history_truncated: Literal[False] = False
    source_frontier_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False

    def feature(self, name: str) -> FeatureValue:
        for feature in self.features:
            if feature.name == name:
                return feature
        raise KeyError(name)


def _integer(name: str, value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")


def _windows(name: str, values: tuple[int, ...], minimum: int) -> tuple[int, ...]:
    if type(values) is not tuple or not values or len(values) > 32:
        raise ValueError(f"{name} requires a nonempty tuple of at most 32 unique windows")
    for value in values:
        _integer(name, value, minimum, 10000)
    if len(set(values)) != len(values):
        raise ValueError(f"{name} requires unique windows")
    return tuple(sorted(values))


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for name, value in pairs:
        if name in output:
            raise IntegrityError("FEATURE_DUPLICATE_JSON_KEY")
        output[name] = value
    return output


def _nested_clocks(value: object, cutoff: int) -> None:
    if isinstance(value, dict):
        for name, child in value.items():
            if name in _CLOCK_FIELDS:
                if type(child) is not int or child < 0:
                    raise IntegrityError("FEATURE_INVALID_NESTED_CLOCK")
                if child > cutoff:
                    raise IntegrityError("FEATURE_LOOKAHEAD_NESTED_CLOCK")
            _nested_clocks(child, cutoff)
    elif isinstance(value, list):
        for child in value:
            _nested_clocks(child, cutoff)


def _audited_rows(snapshot: PointInTimeSnapshot) -> tuple[MarketEvent, ...]:
    if type(snapshot) is not PointInTimeSnapshot or any(
        not hasattr(snapshot, name) for name in ("as_of_ms", "asset", "environment", "_events")
    ):
        raise IntegrityError("FEATURE_INVALID_SNAPSHOT")
    if type(snapshot._events) is not tuple:
        raise IntegrityError("FEATURE_INVALID_SNAPSHOT")
    if type(snapshot.as_of_ms) is not int or not 1 <= snapshot.as_of_ms < 1 << 63:
        raise IntegrityError("FEATURE_INVALID_SNAPSHOT_CLOCK")
    if (
        type(snapshot.asset) is not str
        or not snapshot.asset.strip()
        or type(snapshot.environment) is not str
        or snapshot.environment not in {"testnet", "public_reference"}
    ):
        raise IntegrityError("FEATURE_INVALID_SNAPSHOT_SCOPE")
    for row in snapshot._events:
        if type(row) is not MarketEvent or set(row.__dict__) != set(MarketEvent.model_fields):
            raise IntegrityError("FEATURE_INVALID_EVENT")
        if type(row.schema_version) is not int or row.schema_version != 1:
            raise IntegrityError("FEATURE_INVALID_EVENT_VERSION")
        if row.asset != snapshot.asset or row.environment != snapshot.environment:
            raise IntegrityError("FEATURE_MIXED_SNAPSHOT_SCOPE")
        if any(
            type(clock) is not int or clock <= 0
            for clock in (row.event_time_ms, row.observed_at_ms)
        ):
            raise IntegrityError("FEATURE_INVALID_EVENT_CLOCK")
        if row.event_time_ms > snapshot.as_of_ms or row.observed_at_ms > snapshot.as_of_ms:
            raise IntegrityError("FEATURE_LOOKAHEAD_EVENT")
        try:
            _nested_clocks(
                json.loads(row.payload_json, object_pairs_hook=_pairs), snapshot.as_of_ms
            )
            MarketEvent.model_validate(row.model_dump(), strict=True)
            json.dumps(row.payload(), allow_nan=False)
        except (ValueError, TypeError, AttributeError, RecursionError, ValidationError) as exc:
            raise IntegrityError("FEATURE_INVALID_EVENT") from exc
    return snapshot._events


def _bar(row: MarketEvent) -> _Bar:
    try:
        payload = row.payload()
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if canonical != row.payload_json:
            raise IntegrityError("FEATURE_NONCANONICAL_BAR_PAYLOAD")
        if payload.get("closed") is not True or type(payload.get("complete")) is not bool:
            raise IntegrityError("FEATURE_NONCANONICAL_BAR_CONTROL")
        if "aggregation_version" in payload and type(payload["aggregation_version"]) is not int:
            raise IntegrityError("FEATURE_NONCANONICAL_AGGREGATION_VERSION")
        parsed = _Bar.model_validate(payload, strict=True)
        if row.timestamp_kind != "source" or not re.fullmatch(r"[0-9a-f]{64}", row.event_id):
            raise IntegrityError("FEATURE_NONCANONICAL_BAR_ENVELOPE")
        expected = make_event(
            source=row.source,
            environment=row.environment,
            asset=row.asset,
            symbol=row.symbol,
            kind="bar",
            event_time_ms=row.event_time_ms,
            observed_at_ms=row.observed_at_ms,
            payload=payload,
        )
        if expected != row:
            raise IntegrityError("FEATURE_NONCANONICAL_BAR_IDENTITY")
        if (
            parsed.complete
            and parsed.coverage_start_ms is not None
            and parsed.open_time_ms < parsed.coverage_start_ms
        ):
            raise IntegrityError("FEATURE_FALSE_COMPLETE_COVERAGE")
        return parsed
    except (ValueError, TypeError, ValidationError) as exc:
        raise IntegrityError("FEATURE_INVALID_BAR") from exc


def _mean(values: list[float]) -> float:
    return math.fsum(value / len(values) for value in values)


def _ema(values: list[float], period: int, *, alpha: float | None = None) -> list[float]:
    if len(values) < period:
        return []
    result = [_mean(values[:period])]
    weight = 2 / (period + 1) if alpha is None else alpha
    for value in values[period:]:
        result.append(result[-1] + weight * (value - result[-1]))
    return result


def compute_features(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    max_bars: int = 2000,
    max_latest_age_ms: int | None = None,
    annualization_periods_per_year: float | None = None,
    roc_windows: tuple[int, ...] = (5, 20),
    realized_vol_windows: tuple[int, ...] = (20, 60),
    slope_bars: int = 1,
) -> FeatureReport:
    """Compute only exact selected, complete, contiguous, available local closed bars.

    Malformed input raises IntegrityError; honest missing/incomplete/stale/history
    diagnostics return UNAVAILABLE. Adequate but short history is named WARMUP.
    The default latest-close age bound is two explicitly selected bar intervals.
    """
    if (
        not isinstance(source, str)
        or not source.strip()
        or not isinstance(symbol, str)
        or not symbol.strip()
    ):
        raise ValueError("source and symbol must be explicit nonempty strings")
    _integer("timeframe_s", timeframe_s, 1, 86400)
    _integer("max_bars", max_bars, 1, 10000)
    _integer("slope_bars", slope_bars, 1, 1000)
    maximum_age = 2 * timeframe_s * 1000 if max_latest_age_ms is None else max_latest_age_ms
    _integer("max_latest_age_ms", maximum_age, 0, (1 << 63) - 1)
    roc = _windows("roc_windows", roc_windows, 1)
    volatility = _windows("realized_vol_windows", realized_vol_windows, 2)
    annualization = annualization_periods_per_year
    if annualization is not None:
        try:
            valid_annualization = (
                type(annualization) in {int, float}
                and math.isfinite(annualization)
                and annualization > 0
            )
        except OverflowError:
            valid_annualization = False
        if not valid_annualization:
            raise ValueError("annualization requires explicit finite positive periods per year")
    specs: list[tuple[str, str, int]] = []
    for period in (8, 21, 50, 200):
        specs.extend(
            (
                (f"ema_{period}", "price", period),
                (f"ema_{period}_slope", "price_per_bar", period + slope_bars),
            )
        )
    specs.extend(
        (
            ("rsi_14", "index_0_to_100", 15),
            ("atr_14", "price", 15),
            ("macd_12_26", "price", 26),
            ("macd_signal_9", "price", 34),
            ("macd_histogram", "price", 34),
            ("bollinger_width_20", "fraction", 20),
        )
    )
    specs.extend((f"roc_{period}", "fraction", period + 1) for period in roc)
    specs.extend(
        (f"realized_vol_{period}", "annualized_log_return_std", period + 1) for period in volatility
    )
    rows = _audited_rows(snapshot)
    selected: dict[int, MarketEvent] = {}
    seen: dict[str, MarketEvent] = {}
    available = 0
    overflow = False
    for row in rows:
        if row.kind != "bar" or row.source != source or row.symbol != symbol:
            continue
        bar = _bar(row)
        if bar.timeframe_s != timeframe_s:
            continue
        if row.event_id in seen:
            if seen[row.event_id] != row:
                raise IntegrityError("FEATURE_CONFLICTING_EVENT_ID")
            continue
        if row.event_time_ms in selected:
            raise IntegrityError("FEATURE_TIED_BAR_CLOSE")
        available += 1
        overflow = available > max_bars
        seen[row.event_id] = row
        selected[row.event_time_ms] = row
    ordered_rows = [selected[key] for key in sorted(selected)]
    ordered = [(row, _bar(row)) for row in ordered_rows] if not overflow else []
    bars = [pair[1] for pair in ordered]
    latest = ordered_rows[-1].event_time_ms if ordered_rows else None
    earliest = ordered_rows[0].event_time_ms if ordered_rows else None
    age = snapshot.as_of_ms - latest if latest is not None else None
    blockers: list[str] = []
    if overflow:
        blockers.append("HISTORY_BUDGET_EXCEEDED")
    if not ordered_rows:
        blockers.append("NO_CLOSED_BARS")
    if any(not bar.complete for bar in bars):
        blockers.append("INCOMPLETE_BAR")
    if any(
        right.close_time_ms - left.close_time_ms != timeframe_s * 1000
        for left, right in zip(bars, bars[1:], strict=False)
    ):
        blockers.append("MISSING_INTERIOR_INTERVAL")
    if age is not None and age > maximum_age:
        blockers.append("STALE_LATEST_BAR")
    digest = hashlib.sha256(
        json.dumps(
            [[row.event_id, row.event_time_ms, row.observed_at_ms] for row in ordered_rows],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    output: list[FeatureValue] = []
    closes = [bar.close for bar in bars]
    calculations: dict[str, Callable[[], float]] = {}
    for period in (8, 21, 50, 200):

        def ema_value(p: int = period) -> float:
            return _ema(closes, p)[-1]

        def slope(p: int = period) -> float:
            values = _ema(closes, p)
            return (values[-1] - values[-1 - slope_bars]) / slope_bars

        calculations[f"ema_{period}"] = ema_value
        calculations[f"ema_{period}_slope"] = slope
    changes = [right - left for left, right in zip(closes, closes[1:], strict=False)]
    gains = [max(change, 0) for change in changes]
    losses = [max(-change, 0) for change in changes]

    def rsi() -> float:
        gain = _ema(gains, 14, alpha=1 / 14)[-1]
        loss = _ema(losses, 14, alpha=1 / 14)[-1]
        if gain == loss == 0:
            raise ZeroDivisionError("FLAT_RSI_UNDEFINED")
        if loss == 0:
            return 100.0
        return 100 - 100 / (1 + gain / loss)

    ranges = [
        max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
        for previous, bar in zip(bars, bars[1:], strict=False)
    ]
    calculations["rsi_14"] = rsi
    calculations["atr_14"] = lambda: _ema(ranges, 14, alpha=1 / 14)[-1]

    def macd() -> list[float]:
        fast = _ema(closes, 12)
        slow = _ema(closes, 26)
        return [left - right for left, right in zip(fast[14:], slow, strict=True)]

    calculations["macd_12_26"] = lambda: macd()[-1]
    calculations["macd_signal_9"] = lambda: _ema(macd(), 9)[-1]
    calculations["macd_histogram"] = lambda: macd()[-1] - _ema(macd(), 9)[-1]
    calculations["bollinger_width_20"] = lambda: (
        4
        * (
            math.hypot(*(value - _mean(closes[-20:]) for value in closes[-20:]))
            / math.sqrt(20)
            / _mean(closes[-20:])
        )
    )
    for period in roc:

        def rate(p: int = period) -> float:
            return closes[-1] / closes[-1 - p] - 1

        calculations[f"roc_{period}"] = rate
    for period in volatility:

        def realized(p: int = period) -> float:
            assert annualization is not None
            window = closes[-p - 1 :]
            returns = [
                math.log(right) - math.log(left)
                for left, right in zip(window, window[1:], strict=False)
            ]
            mean = _mean(returns)
            return (
                math.hypot(*(value - mean for value in returns))
                / math.sqrt(p - 1)
                * math.sqrt(annualization)
            )

        calculations[f"realized_vol_{period}"] = realized
    for name, unit, required in specs:
        value: float | None = None
        reason: str | None = None
        state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
        if blockers:
            state, reason = "UNAVAILABLE", blockers[0]
        elif len(bars) < required:
            state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
        elif name.startswith("realized_vol_") and annualization is None:
            state, reason = "UNAVAILABLE", "ANNUALIZATION_NOT_SPECIFIED"
        else:
            try:
                value = calculations[name]()
                if not math.isfinite(value):
                    raise ArithmeticError("nonfinite feature")
            except ZeroDivisionError:
                state, reason = (
                    "UNAVAILABLE",
                    "FLAT_RSI_UNDEFINED" if name == "rsi_14" else "NUMERIC_UNAVAILABLE",
                )
            except (ArithmeticError, ValueError):
                state, reason = "UNAVAILABLE", "NUMERIC_UNAVAILABLE"
                value = None
        output.append(
            FeatureValue(
                name=name,
                state=state,
                value=value,
                unit=unit,
                bars_required=required,
                reason=reason,
            )
        )
    return FeatureReport(
        state="UNAVAILABLE" if blockers else "FEATURES_COMPUTED",
        asset=snapshot.asset,
        environment=snapshot.environment,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        as_of_ms=snapshot.as_of_ms,
        history_budget=max_bars,
        bars_available=available,
        bars_used=0 if blockers else len(bars),
        first_close_ms=earliest,
        latest_close_ms=latest,
        latest_bar_age_ms=age,
        max_latest_age_ms=maximum_age,
        history_sha256=digest,
        annualization_periods_per_year=annualization,
        blockers=tuple(blockers),
        features=tuple(output),
    )
