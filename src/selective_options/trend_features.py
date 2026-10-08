"""Causal Wilder directional indices and bounded historical volatility ranks.

trend-bars-v1 follows Wilder's unrounded directional movement convention as
documented in TA-Lib's primary implementation:
https://github.com/TA-Lib/ta-lib/blob/main/src/ta_func/ta_ADX.c
Positive DM is high-high_previous when strictly greater than positive downward
movement; negative DM is low_previous-low under the opposite strict comparison.
Ties/inside bars give both legs zero. TR uses previous close. Seed the first 13
changes' sums, then smooth sum <- sum-sum/14+change at the 14th and later changes.
DI is 100*smoothed_DM/smoothed_TR (15 bars). DX is 100*abs(+DI--DI)/(+DI+-DI).
ADX seeds the arithmetic mean of the first 14 DX values, then Wilder alpha1/14
(28 bars). Internally scaled sums avoid unnecessary overflow; no rounding occurs.
Undefined TR or DX is unavailable, never an invented zero ADX or a dropped row.

Width20 and sample-log-return RV20 match closed-bars-v1. Ranks compare current
with at most percentile_window PRIOR contiguous observations, excluding current.
Exact floating equality uses midrank:100*(count_less+0.5*count_equal)/N. A constant
valid series ranks50. Minimum history is explicit; warmup includes the underlying
20/21 bars. No interpolation, tolerances, calendar guess, or partial-history
reseed. Reports retain both-clock history provenance and reference-close bounds.
"""

import math
from typing import Literal

from pydantic import ConfigDict

from .events import MarketEvent, PointInTimeSnapshot
from .features import FeatureReport, FeatureValue, _bar, _integer, _mean, compute_features
from .models import ExternalModel


class PercentileReference(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    window: int
    minimum_history: int
    candidate_observations: int
    observations: int
    first_close_ms: int | None
    latest_close_ms: int | None
    current_excluded: Literal[True] = True
    tie_policy: Literal["exact_midrank"] = "exact_midrank"


class TrendFeatureReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["trend-bars-v1"] = "trend-bars-v1"
    base: FeatureReport
    features: tuple[FeatureValue, ...]
    percentile_references: tuple[PercentileReference, ...]
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False
    regime_classification_complete: Literal[False] = False

    def feature(self, name: str) -> FeatureValue:
        for value in self.features:
            if value.name == name:
                return value
        raise KeyError(name)


def _directional(
    rows: list[MarketEvent],
) -> tuple[float | None, float | None, float | None, str | None]:
    bars = [_bar(row) for row in rows]
    legs: list[tuple[float, float, float]] = []
    for left, right in zip(bars, bars[1:], strict=False):
        up, down = right.high - left.high, left.low - right.low
        legs.append(
            (
                up if up > 0 and up > down else 0,
                down if down > 0 and down > up else 0,
                max(
                    right.high - right.low,
                    abs(right.high - left.close),
                    abs(right.low - left.close),
                ),
            )
        )
    if len(legs) < 14:
        return None, None, None, None
    plus = math.fsum(leg[0] / 14 for leg in legs[:13])
    minus = math.fsum(leg[1] / 14 for leg in legs[:13])
    tr = math.fsum(leg[2] / 14 for leg in legs[:13])
    dx: list[float] = []
    invalid_dx = False
    plus_di: float | None = None
    minus_di: float | None = None
    adx: float | None = None
    reason: str | None = None
    for p, m, t in legs[13:]:
        plus, minus, tr = (
            plus - plus / 14 + p / 14,
            minus - minus / 14 + m / 14,
            tr - tr / 14 + t / 14,
        )
        if tr == 0:
            plus_di = minus_di = None
            invalid_dx, reason = True, "TRUE_RANGE_ZERO"
            continue
        plus_di, minus_di = 100 * (plus / tr), 100 * (minus / tr)
        total = plus_di + minus_di
        if total == 0:
            invalid_dx, reason = True, "DIRECTIONAL_MOVEMENT_UNDEFINED"
            continue
        value = 100 * (abs(plus_di - minus_di) / total)
        dx.append(value)
        if len(dx) == 14:
            adx = _mean(dx)
        elif len(dx) > 14 and adx is not None:
            adx += (value - adx) / 14
    # A missing earlier denominator never gets silently skipped/reseeded.
    return plus_di, minus_di, None if invalid_dx else adx, reason


def _width(closes: list[float]) -> float:
    window = closes[-20:]
    mean = _mean(window)
    return 4 * (math.hypot(*(value - mean for value in window)) / math.sqrt(20) / mean)


def _volatility(closes: list[float], annualization: float) -> float:
    window = closes[-21:]
    returns = [math.log(b) - math.log(a) for a, b in zip(window, window[1:], strict=False)]
    mean = _mean(returns)
    return (
        math.hypot(*(value - mean for value in returns)) / math.sqrt(19) * math.sqrt(annualization)
    )


def compute_trend_features(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    max_bars: int = 2000,
    max_latest_age_ms: int | None = None,
    annualization_periods_per_year: float | None = None,
    percentile_window: int = 100,
    percentile_min_history: int = 20,
) -> TrendFeatureReport:
    """Reuse strict base history audit; never infer regimes or specialist feeds."""
    _integer("percentile_window", percentile_window, 1, 1000)
    _integer("percentile_min_history", percentile_min_history, 1, percentile_window)
    base = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_bars=max_bars,
        max_latest_age_ms=max_latest_age_ms,
        annualization_periods_per_year=annualization_periods_per_year,
    )
    rows = (
        sorted(
            {
                row.event_id: row
                for row in snapshot.rows(source, "bar")
                if row.symbol == symbol and _bar(row).timeframe_s == timeframe_s
            }.values(),
            key=lambda row: row.event_time_ms,
        )
        if not base.blockers
        else []
    )
    output: list[FeatureValue] = []
    references: list[PercentileReference] = []
    directional: tuple[float | None, float | None, float | None, str | None] = (
        None,
        None,
        None,
        None,
    )
    if len(rows) >= 15:
        try:
            directional = _directional(rows)
        except (ArithmeticError, ValueError):
            directional = (None, None, None, "NUMERIC_UNAVAILABLE")
    for name, required, value in (
        ("plus_di_14", 15, directional[0]),
        ("minus_di_14", 15, directional[1]),
        ("adx_14", 28, directional[2]),
    ):
        state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
        reason: str | None = None
        if base.blockers:
            state, reason = "UNAVAILABLE", base.blockers[0]
        elif len(rows) < required:
            state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
        elif value is None or not math.isfinite(value):
            state, reason = "UNAVAILABLE", directional[3] or "NUMERIC_UNAVAILABLE"
        output.append(
            FeatureValue(
                name=name,
                state=state,
                value=value if state == "AVAILABLE" else None,
                unit="index_0_to_100",
                bars_required=required,
                reason=reason,
            )
        )
    closes = [_bar(row).close for row in rows]
    for name, initial in (
        ("bollinger_width_20_percentile", 20),
        ("realized_vol_20_percentile", 21),
    ):
        required = initial + percentile_min_history
        state = "AVAILABLE"
        reason = None
        rank: float | None = None
        evaluated_positions: list[int] = []
        # Valid underlying observations start at index initial-1; reference ends before current.
        start = max(initial - 1, len(rows) - 1 - percentile_window)
        positions = list(range(start, len(rows) - 1))
        if base.blockers:
            state, reason = "UNAVAILABLE", base.blockers[0]
        elif name.startswith("realized_vol") and annualization_periods_per_year is None:
            state, reason = "UNAVAILABLE", "ANNUALIZATION_NOT_SPECIFIED"
        elif len(rows) < initial:
            state, reason = "WARMUP", "INSUFFICIENT_PRIOR_OBSERVATIONS"
        else:
            try:

                def observation(end: int, initial_bars: int = initial) -> float:
                    window = closes[max(0, end - 20) : end + 1]
                    if initial_bars == 20:
                        return _width(window)
                    assert annualization_periods_per_year is not None
                    return _volatility(window, annualization_periods_per_year)

                current = observation(len(rows) - 1)
                prior = [observation(end) for end in positions]
                if not all(math.isfinite(value) for value in [current, *prior]):
                    raise ArithmeticError("nonfinite observation")
                evaluated_positions = positions
                if len(prior) < percentile_min_history:
                    state, reason = "WARMUP", "INSUFFICIENT_PRIOR_OBSERVATIONS"
                else:
                    rank = (
                        100
                        * (
                            sum(value < current for value in prior)
                            + 0.5 * sum(value == current for value in prior)
                        )
                        / len(prior)
                    )
            except (ArithmeticError, ValueError):
                state, reason = "UNAVAILABLE", "NUMERIC_UNAVAILABLE"
        references.append(
            PercentileReference(
                name=name,
                window=percentile_window,
                minimum_history=percentile_min_history,
                candidate_observations=len(positions),
                observations=len(evaluated_positions),
                first_close_ms=rows[evaluated_positions[0]].event_time_ms
                if evaluated_positions
                else None,
                latest_close_ms=rows[evaluated_positions[-1]].event_time_ms
                if evaluated_positions
                else None,
            )
        )
        output.append(
            FeatureValue(
                name=name,
                state=state,
                value=rank,
                unit="percentile_0_to_100",
                bars_required=required,
                reason=reason,
            )
        )
    return TrendFeatureReport(
        base=base, features=tuple(output), percentile_references=tuple(references)
    )
