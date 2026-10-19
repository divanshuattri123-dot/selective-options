"""Causal, deterministic swing-structure fixtures."""

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.swing_structure_features import compute_swing_structure


def bar(index: int, high: float, low: float, *, complete: bool = True) -> MarketEvent:
    opening = 300_000 + index * 300_000
    return make_event(
        source="swing-fixture",
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
            "high": high,
            "low": low,
            "close": 100.0,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": complete,
        },
    )


def compute(rows: list[MarketEvent], *, as_of_ms: int | None = None, **options):
    cutoff = as_of_ms or max((row.observed_at_ms for row in rows), default=1_000)
    return compute_swing_structure(
        PointInTimeSnapshot(cutoff, "BTC", "public_reference", tuple(rows)),
        source="swing-fixture",
        symbol="BTC/USD",
        timeframe_s=300,
        **options,
    )


def test_pivot_is_visible_only_after_every_right_bar_closes():
    highs = [100, 101, 105, 102, 101]
    lows = [99, 98, 97.5, 97, 96]
    rows = [
        bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows, strict=True))
    ]
    before_confirmation = compute(rows[:4], left_bars=1, right_bars=2)
    assert before_confirmation.state == "AVAILABLE"
    assert before_confirmation.pivots == ()
    confirmed = compute(rows, left_bars=1, right_bars=2)
    assert len(confirmed.pivots) == 1
    pivot = confirmed.pivots[0]
    assert pivot.kind == "SWING_HIGH"
    assert pivot.pivot_event_id == rows[2].event_id
    assert pivot.pivot_price == 105
    assert pivot.pivot_close_ms == rows[2].event_time_ms
    assert pivot.confirmation_event_id == rows[4].event_id
    assert pivot.confirmation_close_ms == rows[4].event_time_ms
    assert pivot.right_confirmation_event_ids == (rows[3].event_id, rows[4].event_id)
    assert confirmed.ready_for_signals is confirmed.trading_enabled is False


def test_equal_high_and_low_plateaus_choose_rightmost_confirmed_bar():
    highs = [100, 102, 103, 103, 101, 100]
    lows = [100, 99, 98, 98, 99, 100]
    rows = [
        bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows, strict=True))
    ]
    report = compute(rows, left_bars=1, right_bars=1)
    assert [pivot.kind for pivot in report.pivots] == ["SWING_HIGH", "SWING_LOW"]
    assert {pivot.pivot_event_id for pivot in report.pivots} == {rows[3].event_id}
    assert all(pivot.confirmation_event_id == rows[4].event_id for pivot in report.pivots)
    assert report.tie_policy == "rightmost_equal_extreme"


def test_a_unique_swing_low_is_confirmed_after_right_bars():
    highs = [101, 102, 103, 104, 105]
    lows = [100, 99, 95, 98, 100]
    report = compute(
        [bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows, strict=True))],
        left_bars=1,
        right_bars=2,
    )
    assert len(report.pivots) == 1
    assert report.pivots[0].kind == "SWING_LOW"
    assert report.pivots[0].pivot_close_ms < report.pivots[0].confirmation_close_ms


def test_short_valid_history_is_warmup_not_an_unconfirmed_pivot():
    report = compute([bar(0, 100, 99), bar(1, 105, 95)], left_bars=1, right_bars=1)
    assert report.state == "WARMUP"
    assert report.reason == "INSUFFICIENT_CLOSED_BARS"
    assert report.bars_required == 3
    assert report.pivots == ()


def test_gap_incomplete_bar_stale_history_and_future_cutoff_fail_closed():
    rows = [bar(index, 100 + index, 100 - index) for index in range(7)]
    gap = compute([*rows[:3], *rows[4:]], left_bars=1, right_bars=1)
    assert gap.state == "UNAVAILABLE"
    assert gap.reason == "MISSING_INTERIOR_INTERVAL"
    incomplete = bar(6, 106, 94, complete=False)
    bad = compute([*rows[:-1], incomplete], left_bars=1, right_bars=1)
    assert bad.state == "UNAVAILABLE"
    assert bad.reason == "INCOMPLETE_BAR"
    stale = compute(
        rows,
        left_bars=1,
        right_bars=1,
        as_of_ms=rows[-1].observed_at_ms + 600_001,
        max_latest_age_ms=600_000,
    )
    assert stale.state == "UNAVAILABLE"
    assert stale.reason == "STALE_LATEST_BAR"
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        compute(rows, left_bars=1, right_bars=1, as_of_ms=rows[-1].observed_at_ms - 1)


@pytest.mark.parametrize("kwargs", [{"left_bars": 0}, {"right_bars": True}])
def test_invalid_window_parameters_are_rejected(kwargs):
    with pytest.raises(ValueError):
        compute([bar(index, 101, 99) for index in range(6)], **kwargs)
