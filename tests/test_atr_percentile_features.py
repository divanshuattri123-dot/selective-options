"""Causal ATR percentile fixtures and point-in-time quality gates."""

from fractions import Fraction

import pytest

from selective_options.atr_percentile_features import compute_atr_percentile
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event


def bar(index: int, true_range: float, *, complete: bool = True) -> MarketEvent:
    opening = 300_000 + index * 300_000
    return make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="bar",
        event_time_ms=opening + 300_000,
        observed_at_ms=opening + 300_005,
        payload={
            "open_time_ms": opening,
            "close_time_ms": opening + 300_000,
            "timeframe_s": 300,
            "open": 100.0,
            "high": 100.0 + true_range / 2,
            "low": 100.0 - true_range / 2,
            "close": 100.0,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": complete,
        },
    )


def point(rows: list[MarketEvent], *, as_of_ms: int | None = None) -> PointInTimeSnapshot:
    cutoff = as_of_ms or max((row.observed_at_ms for row in rows), default=1_000)
    return PointInTimeSnapshot(cutoff, "BTC", "public_reference", tuple(rows))


def compute(rows: list[MarketEvent], **options):
    return compute_atr_percentile(
        point(rows), source="fixture", symbol="BTC/USD", timeframe_s=300, **options
    )


def exact_wilder_ranges(ranges: list[int], period: int) -> list[Fraction]:
    transitions = [Fraction(value) for value in ranges[1:]]
    if len(transitions) < period:
        return []
    current = sum(transitions[:period], Fraction(0)) / period
    result = [current]
    for true_range in transitions[period:]:
        current = (current * (period - 1) + true_range) / period
        result.append(current)
    return result


def exact_midrank(prior: list[Fraction], current: Fraction) -> Fraction:
    return (
        100
        * (
            sum(value < current for value in prior)
            + Fraction(1, 2) * sum(value == current for value in prior)
        )
        / len(prior)
    )


def test_wilder_seed_smoothing_and_midrank_match_independent_fraction_reference():
    ranges = [99, 1, 2, 4, 1, 3, 2, 8]
    rows = [bar(index, true_range) for index, true_range in enumerate(ranges)]
    report = compute(rows, atr_period=3, percentile_window=4, percentile_min_history=4)
    atrs = exact_wilder_ranges(ranges, 3)
    expected = exact_midrank(atrs[:-1], atrs[-1])
    feature = report.feature
    reference = report.reference
    assert feature.state == "AVAILABLE"
    assert feature.value == pytest.approx(float(expected), rel=0, abs=1e-12)
    assert report.formula_version == "atr-percentile-bars-v1"
    assert feature.bars_required == 8
    assert reference.candidate_observations == reference.observations == 4
    assert reference.first_close_ms == rows[3].event_time_ms
    assert reference.latest_close_ms == rows[6].event_time_ms
    assert reference.latest_close_ms < rows[-1].event_time_ms
    assert reference.current_excluded is True and reference.tie_policy == "exact_midrank"
    assert report.ready_for_signals is report.trading_enabled is False


def test_constant_true_ranges_use_midrank_50():
    report = compute(
        [bar(index, 4) for index in range(8)],
        atr_period=3,
        percentile_window=4,
        percentile_min_history=4,
    )
    assert report.feature.state == "AVAILABLE"
    assert report.feature.value == 50


def test_current_outlier_is_not_added_to_reference_and_window_is_bounded():
    ranges = [99, 1, 10, 2, 3, 4, 100]
    rows = [bar(index, value) for index, value in enumerate(ranges)]
    report = compute(rows, atr_period=1, percentile_window=2, percentile_min_history=2)
    assert report.feature.value == 100
    assert report.reference.candidate_observations == report.reference.observations == 2
    assert report.reference.first_close_ms == rows[4].event_time_ms
    assert report.reference.latest_close_ms == rows[5].event_time_ms
    assert report.reference.latest_close_ms < rows[6].event_time_ms


@pytest.mark.parametrize("count", [4, 5])
def test_short_history_remains_warmup_without_partial_rank(count):
    rows = [bar(index, index + 1) for index in range(count)]
    report = compute(rows, atr_period=2, percentile_window=8, percentile_min_history=3)
    assert report.feature.state == "WARMUP"
    assert report.feature.value is None
    assert report.feature.reason == "INSUFFICIENT_PRIOR_OBSERVATIONS"
    assert report.feature.bars_required == 6


def test_incomplete_and_missing_interval_histories_withhold_rank():
    rows = [bar(index, index + 1) for index in range(8)]
    incomplete = [*rows[:-1], bar(7, 8, complete=False)]
    assert (
        compute(
            incomplete, atr_period=2, percentile_window=3, percentile_min_history=3
        ).feature.reason
        == "INCOMPLETE_BAR"
    )
    gap = [*rows[:4], *rows[5:]]
    report = compute(gap, atr_period=2, percentile_window=3, percentile_min_history=3)
    assert report.feature.state == "UNAVAILABLE"
    assert report.feature.reason == "MISSING_INTERIOR_INTERVAL"
    assert report.input_event_ids == ()


def test_stale_latest_bar_is_unavailable_and_future_input_clock_is_rejected():
    rows = [bar(index, index + 1) for index in range(8)]
    latest = rows[-1]
    stale = compute_atr_percentile(
        point(rows, as_of_ms=latest.observed_at_ms + 600_001),
        source="fixture",
        symbol="BTC/USD",
        timeframe_s=300,
        atr_period=2,
        percentile_window=3,
        percentile_min_history=3,
        max_latest_age_ms=600_000,
    )
    assert stale.feature.state == "UNAVAILABLE"
    assert stale.feature.reason == "STALE_LATEST_BAR"
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        compute_atr_percentile(
            point(rows, as_of_ms=latest.observed_at_ms - 1),
            source="fixture",
            symbol="BTC/USD",
            timeframe_s=300,
            atr_period=2,
            percentile_window=3,
            percentile_min_history=3,
        )


@pytest.mark.parametrize(
    "options",
    [
        {"atr_period": 0},
        {"percentile_window": 0},
        {"percentile_window": 3, "percentile_min_history": 4},
        {"atr_period": True},
    ],
)
def test_invalid_feature_policy_is_rejected(options):
    with pytest.raises(ValueError):
        compute([bar(index, index + 1) for index in range(8)], **options)
