"""Prior-only percentile of Wilder ATR over audited closed bars.

atr-percentile-bars-v1 uses the existing closed-bars-v1 True Range and Wilder
seed convention: max(high-low, abs(high-previous close), abs(low-previous
close)), seeded by the arithmetic mean of the first ``period`` ranges, then
smoothed with alpha 1/period. The current ATR is ranked only against strictly
prior contiguous ATR observations. Exact float ties use midrank; the value is
descriptive and creates no threshold, regime, signal, or order.
"""

import math
from typing import Literal

from pydantic import ConfigDict

from .events import MarketEvent, PointInTimeSnapshot
from .features import FeatureReport, FeatureValue, _bar, _ema, _integer, compute_features
from .models import ExternalModel


class ATRPercentileReference(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    atr_period: int
    window: int
    minimum_history: int
    candidate_observations: int
    observations: int
    first_close_ms: int | None
    latest_close_ms: int | None
    current_excluded: Literal[True] = True
    tie_policy: Literal["exact_midrank"] = "exact_midrank"


class ATRPercentileReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["atr-percentile-bars-v1"] = "atr-percentile-bars-v1"
    base: FeatureReport
    atr_period: int
    percentile_window: int
    minimum_history: int
    feature: FeatureValue
    reference: ATRPercentileReference
    input_event_ids: tuple[str, ...]
    current_feed_health_verified: Literal[False] = False
    source_frontier_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def _atr_series(rows: list[MarketEvent], period: int) -> list[float | None]:
    """Return one ATR slot per input bar, with the first value at index period."""
    if len(rows) < period + 1:
        return []
    bars = [_bar(row) for row in rows]
    true_ranges = [
        max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        for previous, current in zip(bars, bars[1:], strict=False)
    ]
    if not all(math.isfinite(value) and value >= 0 for value in true_ranges):
        raise ArithmeticError("ATR_TRUE_RANGE_UNAVAILABLE")
    smoothed = _ema(true_ranges, period, alpha=1 / period)
    if not smoothed or not all(math.isfinite(value) and value >= 0 for value in smoothed):
        raise ArithmeticError("ATR_WILDER_VALUE_UNAVAILABLE")
    return [None] * period + smoothed


def compute_atr_percentile(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    atr_period: int = 14,
    percentile_window: int = 100,
    percentile_min_history: int = 20,
    max_bars: int = 2000,
    max_latest_age_ms: int | None = None,
) -> ATRPercentileReport:
    """Rank current ATR against bounded prior observations after quality checks."""
    _integer("atr_period", atr_period, 1, 1000)
    _integer("percentile_window", percentile_window, 1, 1000)
    _integer("percentile_min_history", percentile_min_history, 1, percentile_window)
    base = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_bars=max_bars,
        max_latest_age_ms=max_latest_age_ms,
    )
    selected: dict[str, MarketEvent] = {}
    for row in snapshot.rows(source, "bar"):
        if row.symbol != symbol or _bar(row).timeframe_s != timeframe_s:
            continue
        previous = selected.get(row.event_id)
        if previous is not None and previous != row:
            raise ArithmeticError("ATR_PERCENTILE_CONFLICTING_EVENT_ID")
        selected[row.event_id] = row
    rows = sorted(selected.values(), key=lambda row: row.event_time_ms) if not base.blockers else []
    ids = tuple(row.event_id for row in rows)
    bars_required = atr_period + 1 + percentile_min_history
    rank: float | None = None
    state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
    reason: str | None = None
    observations: list[float] = []
    positions: list[int] = []
    if base.blockers:
        state, reason = "UNAVAILABLE", base.blockers[0]
    elif len(rows) < atr_period + 1:
        state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
    else:
        try:
            values = _atr_series(rows, atr_period)
            start = max(atr_period, len(rows) - 1 - percentile_window)
            positions = list(range(start, len(rows) - 1))
            prior = [values[index] for index in positions]
            current = values[-1]
            if current is None or any(value is None for value in prior):
                raise ArithmeticError("ATR_WILDER_VALUE_UNAVAILABLE")
            observations = [value for value in prior if value is not None]
            if not all(math.isfinite(value) for value in [current, *observations]):
                raise ArithmeticError("ATR_PERCENTILE_NONFINITE")
            if len(observations) < percentile_min_history:
                state, reason = "WARMUP", "INSUFFICIENT_PRIOR_OBSERVATIONS"
            else:
                rank = (
                    100
                    * (
                        sum(value < current for value in observations)
                        + 0.5 * sum(value == current for value in observations)
                    )
                    / len(observations)
                )
                if not math.isfinite(rank) or not 0 <= rank <= 100:
                    raise ArithmeticError("ATR_PERCENTILE_OUT_OF_RANGE")
        except (ArithmeticError, OverflowError, ValueError):
            state, reason, rank = "UNAVAILABLE", "NUMERIC_UNAVAILABLE", None
    reference = ATRPercentileReference(
        atr_period=atr_period,
        window=percentile_window,
        minimum_history=percentile_min_history,
        candidate_observations=len(positions),
        observations=len(observations),
        first_close_ms=rows[positions[0]].event_time_ms if positions and rows else None,
        latest_close_ms=rows[positions[-1]].event_time_ms if positions and rows else None,
    )
    feature = FeatureValue(
        name=f"atr_{atr_period}_percentile",
        state=state,
        value=rank if state == "AVAILABLE" else None,
        unit="percentile_0_to_100",
        bars_required=bars_required,
        reason=reason,
    )
    return ATRPercentileReport(
        base=base,
        atr_period=atr_period,
        percentile_window=percentile_window,
        minimum_history=percentile_min_history,
        feature=feature,
        reference=reference,
        input_event_ids=ids,
    )
