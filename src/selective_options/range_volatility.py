"""Causal OHLC range-variance estimates; windows are complete BAR counts.

Parkinson per-bar variance: ln(H/L)^2 / (4 ln2). Garman-Klass uses the
practical equation19a: .5 ln(H/L)^2 - (2 ln2 - 1) ln(C/O)^2. Window variance
is the arithmetic mean of these contributions; annualized volatility is
sqrt(mean_variance * explicit_periods_per_year), never a calendar inference.
Primary papers:
https://www.cmegroup.com/trading/fx/files/michael_parkinson.pdf
https://www.cmegroup.com/trading/fx/files/a_estimation_of_security_price.pdf

These zero-drift continuous-diffusion estimators do not model opening jumps or
correct discrete-extrema/microstructure bias. Contiguous stored intervals do
not verify continuous trading, observed extrema, or the assumed price process.
Zero is valid only for an actually zero observed range, never numerical collapse.
"""

import hashlib
import json
import math
from typing import Literal

from pydantic import ConfigDict

from .events import PointInTimeSnapshot
from .features import FeatureReport, FeatureValue, _Bar, _bar, _integer, _windows, compute_features
from .models import ExternalModel

Estimator = Literal["parkinson", "garman_klass"]
_LOG_TWO = math.log(2.0)


class RangeWindowReference(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    window: int
    bars_available: int
    bars_used: int
    first_open_ms: int | None
    first_close_ms: int | None
    latest_close_ms: int | None
    history_sha256: str | None


class RangeVolatilityReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["range-volatility-v1"] = "range-volatility-v1"
    state: Literal["RANGE_VOLATILITY_COMPUTED", "UNAVAILABLE"]
    base: FeatureReport
    windows: tuple[int, ...]
    features: tuple[FeatureValue, ...]
    window_references: tuple[RangeWindowReference, ...]
    input_event_ids: tuple[str, ...]
    windows_are_bar_counts: Literal[True] = True
    zero_drift_diffusion_assumed: Literal[True] = True
    zero_drift_verified: Literal[False] = False
    continuous_extrema_verified: Literal[False] = False
    opening_jumps_modelled: Literal[False] = False
    sampling_bias_corrected: Literal[False] = False
    annualization_calendar_verified: Literal[False] = False
    current_feed_health_verified: Literal[False] = False
    source_frontier_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False

    def feature(self, name: str) -> FeatureValue:
        for feature in self.features:
            if feature.name == name:
                return feature
        raise KeyError(name)


def _log_ratio(numerator: float, denominator: float) -> float:
    """Preserve near-unit float differences; avoid directly dividing extremes."""
    if numerator == denominator:
        return 0.0
    high, low = max(numerator, denominator), min(numerator, denominator)
    relative = (high - low) / low
    if math.isfinite(relative):
        value = math.log1p(relative)
    else:
        value = math.log(high) - math.log(low)
    if not math.isfinite(value) or value <= 0:
        raise ArithmeticError("LOG_RATIO_COLLAPSED_OR_NONFINITE")
    return value if numerator > denominator else -value


def _square(value: float) -> float:
    squared = value * value
    if not math.isfinite(squared):
        raise ArithmeticError("LOG_SQUARE_NONFINITE")
    if value != 0 and squared == 0:
        raise ArithmeticError("LOG_SQUARE_UNDERFLOW")
    return squared


def _contribution(bar: _Bar, estimator: Estimator) -> float:
    range_log = _log_ratio(bar.high, bar.low)
    range_squared = _square(range_log)
    if estimator == "parkinson":
        variance = range_squared / (4 * _LOG_TWO)
    else:
        body_squared = _square(_log_ratio(bar.close, bar.open))
        variance = math.fsum((0.5 * range_squared, -(2 * _LOG_TWO - 1) * body_squared))
    if not math.isfinite(variance):
        raise ArithmeticError("VARIANCE_NONFINITE")
    if variance < 0:
        raise ArithmeticError("NEGATIVE_VARIANCE")
    if range_log != 0 and variance == 0:
        raise ArithmeticError("VARIANCE_COLLAPSED_TO_ZERO")
    return variance


def _window_variance(values: list[float | str]) -> tuple[float | None, str | None]:
    for value in values:
        if isinstance(value, str):
            return None, value
    numbers = [value for value in values if isinstance(value, float)]
    try:
        variance = math.fsum(numbers) / len(values)
        if not math.isfinite(variance):
            raise ArithmeticError("VARIANCE_MEAN_NONFINITE")
        if variance < 0:
            raise ArithmeticError("NEGATIVE_VARIANCE_MEAN")
        if any(value != 0 for value in numbers) and variance == 0:
            raise ArithmeticError("VARIANCE_MEAN_UNDERFLOW")
    except (ArithmeticError, ValueError) as exc:
        return None, str(exc) or "NUMERIC_UNAVAILABLE"
    return variance, None


def _annualized(variance: float, annualization: float) -> tuple[float | None, str | None]:
    try:
        scaled = variance * annualization
        if not math.isfinite(scaled):
            raise ArithmeticError("ANNUALIZED_VARIANCE_NONFINITE")
        if variance != 0 and scaled == 0:
            raise ArithmeticError("ANNUALIZED_VARIANCE_UNDERFLOW")
        value = math.sqrt(scaled)
        if not math.isfinite(value) or scaled != 0 and value == 0:
            raise ArithmeticError("ANNUALIZED_VOLATILITY_COLLAPSED_OR_NONFINITE")
    except (ArithmeticError, ValueError) as exc:
        return None, str(exc) or "NUMERIC_UNAVAILABLE"
    return value, None


def compute_range_volatility(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    windows: tuple[int, ...],
    max_latest_age_ms: int,
    max_bars: int = 2000,
    annualization_periods_per_year: float | None = None,
) -> RangeVolatilityReport:
    """Reuse the full canonical closed-bar audit; do not shorten bad history.

    Each window uses exactly its latest N complete, contiguous bars, requiring
    N>=2. This is a mean of per-bar range variances, not a sample variance with
    N-1 correction or a close-to-close return estimator requiring N+1 closes.
    A successful history audit can still yield WARMUP or unavailable individual
    values. Raw variance remains available without annualization; annualized
    values explicitly withhold when no periods-per-year assumption is supplied.
    """
    requested = _windows("windows", windows, 2)
    _integer("max_latest_age_ms", max_latest_age_ms, 0, (1 << 63) - 1)
    if type(source) is not str or type(symbol) is not str:
        raise ValueError("source and symbol must be explicit strings")
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
    bars = [_bar(row) for row in rows]
    estimators: tuple[Estimator, ...] = ("parkinson", "garman_klass")
    contributions: dict[Estimator, list[float | str]] = {}
    for estimator in estimators:
        computed: list[float | str] = []
        for bar in bars:
            try:
                computed.append(_contribution(bar, estimator))
            except (ArithmeticError, ValueError) as exc:
                computed.append(str(exc) or "NUMERIC_UNAVAILABLE")
        contributions[estimator] = computed
    output: list[FeatureValue] = []
    references: list[RangeWindowReference] = []
    for window in requested:
        enough = len(rows) >= window and not base.blockers
        selected = rows[-window:] if enough else []
        digest = (
            hashlib.sha256(
                json.dumps(
                    [[row.event_id, row.event_time_ms, row.observed_at_ms] for row in selected],
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            if selected
            else None
        )
        references.append(
            RangeWindowReference(
                window=window,
                bars_available=base.bars_available,
                bars_used=len(selected),
                first_open_ms=_bar(selected[0]).open_time_ms if selected else None,
                first_close_ms=selected[0].event_time_ms if selected else None,
                latest_close_ms=selected[-1].event_time_ms if selected else None,
                history_sha256=digest,
            )
        )
        for estimator in estimators:
            state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
            variance: float | None = None
            reason: str | None = None
            if base.blockers:
                state, reason = "UNAVAILABLE", base.blockers[0]
            elif not enough:
                state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
            else:
                variance, reason = _window_variance(contributions[estimator][-window:])
                if reason is not None:
                    state = "UNAVAILABLE"
            output.append(
                FeatureValue(
                    name=f"{estimator}_variance_per_bar_{window}",
                    state=state,
                    value=variance,
                    unit="log_return_variance_per_bar",
                    bars_required=window,
                    reason=reason,
                )
            )
            annualized: float | None = None
            if state == "AVAILABLE":
                if annualization_periods_per_year is None:
                    state, reason = "UNAVAILABLE", "ANNUALIZATION_NOT_SPECIFIED"
                else:
                    assert variance is not None
                    annualized, reason = _annualized(variance, annualization_periods_per_year)
                    if reason is not None:
                        state = "UNAVAILABLE"
            output.append(
                FeatureValue(
                    name=f"{estimator}_vol_{window}",
                    state=state,
                    value=annualized,
                    unit="annualized_log_return_std",
                    bars_required=window,
                    reason=reason,
                )
            )
    return RangeVolatilityReport(
        state="UNAVAILABLE" if base.blockers else "RANGE_VOLATILITY_COMPUTED",
        base=base,
        windows=requested,
        features=tuple(output),
        window_references=tuple(references),
        input_event_ids=tuple(row.event_id for row in rows),
    )
