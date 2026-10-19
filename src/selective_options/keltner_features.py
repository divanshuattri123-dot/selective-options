"""Causal modern Keltner price channels, never a trading interpretation.

keltner-bars-v1: center is the EMA of closes, seeded with the first p-close
arithmetic mean, then E <- E + 2/(p+1)*(close-E). For each bar after the first,
TR=max(high-low,abs(high-previous_close),abs(low-previous_close)). ATR(q) seeds
the first q TRs' arithmetic mean, then A <- A + (TR-A)/q. Thus center requires p
bars, ATR q+1 bars, envelopes max(p,q+1). Upper/lower=center +/- multiplier*ATR.
Periods count bars, all four outputs use the selected quote's price units.

The 20/10/2 defaults and modern EMA/ATR construction are documented by StockCharts:
https://chartschool.stockcharts.com/table-of-contents/technical-indicators-and-overlays/technical-overlays/keltner-channels
The seed/recursion convention follows TA-Lib's default compatibility convention:
https://github.com/TA-Lib/ta-lib/blob/v0.6.4/src/ta_func/ta_EMA.c
https://github.com/TA-Lib/ta-lib/blob/v0.6.4/src/ta_func/ta_ATR.c
This names an established indicator, not a proprietary model, probability band,
validated edge, regime classifier, or bit-for-bit reproduction of another library.
All audited history is consumed from its first bar; no tail truncation/reseeding.
"""

import math
from collections.abc import Callable
from typing import Literal

from pydantic import ConfigDict

from .events import PointInTimeSnapshot
from .features import FeatureReport, FeatureValue, _bar, _integer, compute_features
from .models import ExternalModel


class KeltnerFeatureReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["keltner-bars-v1"] = "keltner-bars-v1"
    state: Literal["KELTNER_COMPUTED", "UNAVAILABLE"]
    base: FeatureReport
    ema_period: int
    atr_period: int
    atr_multiplier: float
    center_price: Literal["close"] = "close"
    ema_seed: Literal["first_period_arithmetic_mean"] = "first_period_arithmetic_mean"
    atr_seed: Literal["first_period_previous_close_true_ranges_mean"] = (
        "first_period_previous_close_true_ranges_mean"
    )
    atr_smoothing: Literal["wilder_alpha_1_over_period"] = "wilder_alpha_1_over_period"
    features: tuple[FeatureValue, ...]
    input_event_ids: tuple[str, ...]
    windows_are_bar_counts: Literal[True] = True
    current_feed_health_verified: Literal[False] = False
    source_frontier_complete: Literal[False] = False
    regime_classification_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False

    def feature(self, name: str) -> FeatureValue:
        for feature in self.features:
            if feature.name == name:
                return feature
        raise KeyError(name)


def _average(values: list[float], period: int, alpha: float) -> float:
    """Keep a failed historical numeric step unavailable, never reseed later."""
    seed = values[:period]
    try:
        # Sum before dividing so representable subnormal mass is retained.
        value = math.fsum(seed) / period
    except OverflowError:
        # The positive mean can be representable even when its sum overflows.
        scale = max(seed)
        value = scale * (math.fsum(current / scale for current in seed) / period)
    if not math.isfinite(value) or value < 0 or value == 0 and any(values[:period]):
        raise ArithmeticError("AVERAGE_SEED_COLLAPSED_OR_NONFINITE")
    for current in values[period:]:
        previous = value
        # A one-period average is exactly the current observation. The generic
        # recurrence can cancel significant bits even though current is finite.
        if alpha == 1:
            value = current
        else:
            # Evaluate the positive convex combination relative to its largest
            # input. Subtracting and then weighting can lose subnormal mass or
            # cancel a perfectly representable updated average.
            scale = max(previous, current)
            value = (
                scale * math.fsum(((1 - alpha) * (previous / scale), alpha * (current / scale)))
                if scale
                else 0.0
            )
        if not math.isfinite(value) or value < 0:
            raise ArithmeticError("AVERAGE_NONFINITE")
        if alpha != 1 and value == 0 and (previous != 0 or current != 0):
            raise ArithmeticError("AVERAGE_UNDERFLOW")
    return value


def compute_keltner_features(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    ema_period: int = 20,
    atr_period: int = 10,
    atr_multiplier: float = 2.0,
    max_latest_age_ms: int | None = None,
    max_bars: int = 2000,
) -> KeltnerFeatureReport:
    """Audit all selected complete contiguous both-clock-visible bars first.

    Flat zero ATR gives coincident valid envelopes. Negative lower envelopes are
    permitted mathematical outputs, not executable prices. Nonzero ATR whose
    envelope distance underflows or rounds back to its center is unavailable.
    """
    _integer("ema_period", ema_period, 1, 10000)
    _integer("atr_period", atr_period, 1, 9999)
    try:
        multiplier_valid = (
            type(atr_multiplier) in {int, float}
            and math.isfinite(atr_multiplier)
            and 0 < atr_multiplier <= 1000
        )
    except OverflowError:
        multiplier_valid = False
    if not multiplier_valid:
        raise ValueError("atr_multiplier must be finite and in (0, 1000]")
    if type(source) is not str or type(symbol) is not str:
        raise ValueError("source and symbol must be explicit strings")
    base = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_latest_age_ms=max_latest_age_ms,
        max_bars=max_bars,
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
    bars = [_bar(row) for row in rows]
    closes = [bar.close for bar in bars]
    ranges = [
        max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
        for previous, bar in zip(bars, bars[1:], strict=False)
    ]
    center: float | None = None
    atr: float | None = None
    center_reason: str | None = None
    atr_reason: str | None = None
    if len(rows) >= ema_period:
        try:
            center = _average(closes, ema_period, 2 / (ema_period + 1))
        except (ArithmeticError, ValueError):
            center_reason = "NUMERIC_UNAVAILABLE"
    if len(rows) >= atr_period + 1:
        try:
            atr = _average(ranges, atr_period, 1 / atr_period)
        except (ArithmeticError, ValueError):
            atr_reason = "NUMERIC_UNAVAILABLE"

    def envelope(sign: int) -> float:
        if center is None or atr is None:
            raise ArithmeticError(center_reason or atr_reason or "NUMERIC_UNAVAILABLE")
        distance = atr_multiplier * atr
        value = center + sign * distance
        if not math.isfinite(distance) or not math.isfinite(value):
            raise ArithmeticError("ENVELOPE_NONFINITE")
        if atr > 0 and (distance == 0 or value == center):
            raise ArithmeticError("ENVELOPE_COLLAPSED_TO_CENTER")
        return value

    calculations: tuple[tuple[str, int, Callable[[], float | None], str | None], ...] = (
        ("keltner_center", ema_period, lambda: center, center_reason),
        ("keltner_atr", atr_period + 1, lambda: atr, atr_reason),
        ("keltner_upper", max(ema_period, atr_period + 1), lambda: envelope(1), None),
        ("keltner_lower", max(ema_period, atr_period + 1), lambda: envelope(-1), None),
    )
    output: list[FeatureValue] = []
    for name, required, calculation, numeric_reason in calculations:
        state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
        reason: str | None = None
        value: float | None = None
        if base.blockers:
            state, reason = "UNAVAILABLE", base.blockers[0]
        elif len(rows) < required:
            state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
        else:
            try:
                value = calculation()
                if value is None or not math.isfinite(value):
                    raise ArithmeticError(numeric_reason or "NUMERIC_UNAVAILABLE")
            except (ArithmeticError, ValueError) as exc:
                state, reason = "UNAVAILABLE", str(exc) or "NUMERIC_UNAVAILABLE"
                value = None
        output.append(
            FeatureValue(
                name=name,
                state=state,
                value=value,
                unit="price",
                bars_required=required,
                reason=reason,
            )
        )
    return KeltnerFeatureReport(
        state="UNAVAILABLE" if base.blockers else "KELTNER_COMPUTED",
        base=base,
        ema_period=ema_period,
        atr_period=atr_period,
        atr_multiplier=atr_multiplier,
        features=tuple(output),
        input_event_ids=tuple(row.event_id for row in rows),
    )
