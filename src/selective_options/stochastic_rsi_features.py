"""Causal Stochastic RSI from exact complete, contiguous closed-bar history.

stoch-rsi-bars-v1 uses the close-based RSI convention in features.py (arithmetic
seed, Wilder 1/period recursion). It computes raw StochRSI over the latest
stochastic_period RSI observations, then %K=SMA(raw, smooth_k), %D=SMA(%K,
smooth_d). Values use the conventional 0..100 scale. Periods count bars, not
time. Undefined flat RSI/ranges remain unavailable; no values are rounded or
filled. The common 14/14/3/3 inputs are user-selectable, not evidence of a
trading edge. Formula overview and the common parameterization:
https://www.tradingview.com/support/solutions/43000502333-stochastic-rsi-stoch-rsi/
TA-Lib's RSI seed/recursion reference:
https://github.com/TA-Lib/ta-lib/blob/v0.6.4/src/ta_func/ta_RSI.c

Every event and nested clock is audited by compute_features before this module
calculates values. The complete selected bar history is consumed without
re-seeding a tail; any base quality blocker withholds every StochRSI output.
"""

import math
from collections.abc import Sequence
from typing import Literal

from pydantic import ConfigDict

from .events import MarketEvent, PointInTimeSnapshot
from .features import FeatureReport, FeatureValue, _bar, _ema, _integer, compute_features
from .models import ExternalModel


class StochasticRSIReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["stoch-rsi-bars-v1"] = "stoch-rsi-bars-v1"
    state: Literal["STOCH_RSI_COMPUTED", "UNAVAILABLE"]
    base: FeatureReport
    rsi_period: int
    stochastic_period: int
    smooth_k: int
    smooth_d: int
    rsi_seed: Literal["first_period_arithmetic_mean"] = "first_period_arithmetic_mean"
    rsi_smoothing: Literal["wilder_alpha_1_over_period"] = "wilder_alpha_1_over_period"
    stochastic_scale: Literal["0_to_100"] = "0_to_100"
    features: tuple[FeatureValue, ...]
    input_event_ids: tuple[str, ...]
    current_feed_health_verified: Literal[False] = False
    source_frontier_complete: Literal[False] = False
    regime_classification_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False

    def feature(self, name: str) -> FeatureValue:
        for item in self.features:
            if item.name == name:
                return item
        raise KeyError(name)


def _wilder(values: Sequence[float], period: int) -> list[float]:
    result = _ema(list(values), period, alpha=1 / period)
    if any(not math.isfinite(value) for value in result):
        raise ArithmeticError("NONFINITE_WILDER_VALUE")
    return result


def _rsi_series(closes: Sequence[float], period: int) -> list[float | None]:
    changes = [right - left for left, right in zip(closes, closes[1:], strict=False)]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gains = _wilder(gains, period)
    average_losses = _wilder(losses, period)
    values: list[float | None] = [None] * min(period, len(closes))
    for gain, loss in zip(average_gains, average_losses, strict=True):
        if gain == 0 and loss == 0:
            values.append(None)
            continue
        scale = max(gain, loss)
        scaled_gain, scaled_loss = gain / scale, loss / scale
        value = 100 * scaled_gain / (scaled_gain + scaled_loss)
        if not math.isfinite(value) or not 0 <= value <= 100:
            raise ArithmeticError("RSI_OUT_OF_RANGE")
        values.append(value)
    return values


def _rolling_stoch(
    values: Sequence[float | None], period: int
) -> tuple[list[float | None], list[str | None]]:
    result: list[float | None] = [None] * len(values)
    reasons: list[str | None] = ["INSUFFICIENT_HISTORY"] * len(values)
    for end in range(period - 1, len(values)):
        window = values[end - period + 1 : end + 1]
        if any(value is None for value in window):
            reasons[end] = "FLAT_RSI_UNDEFINED"
            continue
        concrete = [value for value in window if value is not None]
        low, high, current = min(concrete), max(concrete), concrete[-1]
        if high == low:
            reasons[end] = "FLAT_STOCHASTIC_RSI_RANGE"
            continue
        value = 100 * ((current - low) / (high - low))
        if not math.isfinite(value) or not 0 <= value <= 100:
            reasons[end] = "NUMERIC_UNAVAILABLE"
            continue
        result[end], reasons[end] = value, None
    return result, reasons


def _sma(
    values: Sequence[float | None], reasons: Sequence[str | None], period: int
) -> tuple[list[float | None], list[str | None]]:
    result: list[float | None] = [None] * len(values)
    output_reasons: list[str | None] = ["INSUFFICIENT_HISTORY"] * len(values)
    for end in range(period - 1, len(values)):
        window = values[end - period + 1 : end + 1]
        if any(value is None for value in window):
            output_reasons[end] = next(
                (reason for reason in reasons[end - period + 1 : end + 1] if reason),
                "NUMERIC_UNAVAILABLE",
            )
            continue
        concrete = [value for value in window if value is not None]
        value = math.fsum(item / period for item in concrete)
        if not math.isfinite(value) or not 0 <= value <= 100:
            output_reasons[end] = "NUMERIC_UNAVAILABLE"
            continue
        result[end], output_reasons[end] = value, None
    return result, output_reasons


def compute_stochastic_rsi(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    rsi_period: int = 14,
    stochastic_period: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
    max_bars: int = 2000,
    max_latest_age_ms: int | None = None,
) -> StochasticRSIReport:
    """Compute three latest values after strict base quality and history audit."""
    for name, value in (
        ("rsi_period", rsi_period),
        ("stochastic_period", stochastic_period),
        ("smooth_k", smooth_k),
        ("smooth_d", smooth_d),
    ):
        _integer(name, value, 1, 1000)
    base = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_bars=max_bars,
        max_latest_age_ms=max_latest_age_ms,
    )
    selected_by_id: dict[str, MarketEvent] = {}
    for row in snapshot.rows(source, "bar"):
        if row.symbol != symbol or _bar(row).timeframe_s != timeframe_s:
            continue
        previous = selected_by_id.get(row.event_id)
        if previous is not None and previous != row:
            # The base audit normally raises first; retain a defensive local check.
            raise ArithmeticError("CONFLICTING_EVENT_ID")
        selected_by_id[row.event_id] = row
    selected = sorted(selected_by_id.values(), key=lambda row: row.event_time_ms)
    ids = tuple(row.event_id for row in selected) if not base.blockers else ()
    outputs: list[FeatureValue] = []
    periods = (
        ("stoch_rsi", stochastic_period + rsi_period, None),
        ("stoch_rsi_k", stochastic_period + rsi_period + smooth_k - 1, smooth_k),
        (
            "stoch_rsi_d",
            stochastic_period + rsi_period + smooth_k + smooth_d - 2,
            smooth_d,
        ),
    )
    if base.blockers:
        outputs = [
            FeatureValue(
                name=name,
                state="UNAVAILABLE",
                value=None,
                unit="index_0_to_100",
                bars_required=required,
                reason=base.blockers[0],
            )
            for name, required, _ in periods
        ]
    else:
        closes = [_bar(row).close for row in selected]
        try:
            rsi_values = _rsi_series(closes, rsi_period)
            raw, raw_reasons = _rolling_stoch(rsi_values, stochastic_period)
            k_values, k_reasons = _sma(raw, raw_reasons, smooth_k)
            d_values, d_reasons = _sma(k_values, k_reasons, smooth_d)
            candidates = ((raw, raw_reasons), (k_values, k_reasons), (d_values, d_reasons))
            for (name, required, _), (values, reasons) in zip(periods, candidates, strict=True):
                state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"]
                feature_value: float | None
                reason: str | None
                if len(closes) < required:
                    state, feature_value, reason = "WARMUP", None, "INSUFFICIENT_CLOSED_BARS"
                elif values[-1] is None:
                    state, feature_value, reason = "UNAVAILABLE", None, reasons[-1]
                else:
                    state, feature_value, reason = "AVAILABLE", values[-1], None
                outputs.append(
                    FeatureValue(
                        name=name,
                        state=state,
                        value=feature_value,
                        unit="index_0_to_100",
                        bars_required=required,
                        reason=reason,
                    )
                )
        except (ArithmeticError, OverflowError, ValueError):
            outputs = [
                FeatureValue(
                    name=name,
                    state="UNAVAILABLE",
                    value=None,
                    unit="index_0_to_100",
                    bars_required=required,
                    reason="NUMERIC_UNAVAILABLE",
                )
                for name, required, _ in periods
            ]
    return StochasticRSIReport(
        state="UNAVAILABLE" if base.blockers else "STOCH_RSI_COMPUTED",
        base=base,
        rsi_period=rsi_period,
        stochastic_period=stochastic_period,
        smooth_k=smooth_k,
        smooth_d=smooth_d,
        features=tuple(outputs),
        input_event_ids=ids,
    )
