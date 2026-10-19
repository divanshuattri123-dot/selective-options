"""Independent arithmetic and quality checks on labelled synthetic bars."""

import random
from fractions import Fraction

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.stochastic_rsi_features import compute_stochastic_rsi


def bar(index, close, *, complete=True):
    opening = 300_000 + index * 300_000
    return make_event(
        source="synthetic_fixture",
        environment="public_reference",
        asset="BTC",
        symbol="FIXTURE-BTC/USD",
        kind="bar",
        event_time_ms=opening + 300_000,
        observed_at_ms=opening + 300_005,
        payload={
            "open_time_ms": opening,
            "close_time_ms": opening + 300_000,
            "timeframe_s": 300,
            "open": close,
            "high": close + 2,
            "low": close - 2,
            "close": close,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": complete,
        },
    )


def point(rows, *, as_of=None):
    return PointInTimeSnapshot(
        as_of if as_of is not None else max((row.observed_at_ms for row in rows), default=1),
        "BTC",
        "public_reference",
        tuple(rows),
    )


def compute(rows, **options):
    policy = {
        "source": "synthetic_fixture",
        "symbol": "FIXTURE-BTC/USD",
        "timeframe_s": 300,
    }
    policy.update(options)
    return compute_stochastic_rsi(
        point(rows),
        **policy,
    )


def independent_reference(closes, rsi_period, stoch_period, smooth_k, smooth_d):
    """Fraction expansion independent of the production float recurrence."""
    values = [Fraction(str(value)) for value in closes]
    changes = [right - left for left, right in zip(values, values[1:], strict=False)]
    gains = [max(value, 0) for value in changes]
    losses = [max(-value, 0) for value in changes]

    def wilder(items):
        if len(items) < rsi_period:
            return []
        average = sum(items[:rsi_period]) / rsi_period
        result = [average]
        for value in items[rsi_period:]:
            average = (average * (rsi_period - 1) + value) / rsi_period
            result.append(average)
        return result

    rsi = [None] * rsi_period
    for gain, loss in zip(wilder(gains), wilder(losses), strict=True):
        rsi.append(None if gain == loss == 0 else 100 * gain / (gain + loss))

    raw = [None] * len(rsi)
    for end in range(stoch_period - 1, len(rsi)):
        window = rsi[end - stoch_period + 1 : end + 1]
        if any(value is None for value in window):
            continue
        low, high = min(window), max(window)
        if high != low:
            raw[end] = 100 * (window[-1] - low) / (high - low)

    def sma(series, period):
        result = [None] * len(series)
        for end in range(period - 1, len(series)):
            window = series[end - period + 1 : end + 1]
            if all(value is not None for value in window):
                result[end] = sum(window) / period
        return result

    k = sma(raw, smooth_k)
    d = sma(k, smooth_d)
    return raw[-1], k[-1], d[-1]


def test_default_values_match_independent_fraction_reference_and_are_not_signals():
    closes = [100 + (index * 7) % 13 + index // 6 for index in range(48)]
    rows = [bar(index, close) for index, close in enumerate(closes)]
    report = compute(rows)
    expected = independent_reference(closes, 14, 14, 3, 3)
    actual = [report.feature(name).value for name in ("stoch_rsi", "stoch_rsi_k", "stoch_rsi_d")]
    assert actual == pytest.approx([float(value) for value in expected])
    assert all(
        report.feature(name).state == "AVAILABLE"
        for name in ("stoch_rsi", "stoch_rsi_k", "stoch_rsi_d")
    )
    assert report.input_event_ids == tuple(row.event_id for row in rows)
    assert report.trading_enabled is report.ready_for_signals is False
    assert report.current_feed_health_verified is report.regime_classification_complete is False
    assert report.stochastic_scale == "0_to_100"


def test_default_lookback_boundaries_are_explicit_and_do_not_invent_values():
    for count, expected_states in (
        (27, ("WARMUP", "WARMUP", "WARMUP")),
        (28, ("AVAILABLE", "WARMUP", "WARMUP")),
        (30, ("AVAILABLE", "AVAILABLE", "WARMUP")),
        (32, ("AVAILABLE", "AVAILABLE", "AVAILABLE")),
    ):
        rows = [bar(index, 100 + (index * 7) % 13 + index // 6) for index in range(count)]
        report = compute(rows)
        assert tuple(item.state for item in report.features) == expected_states
        assert [item.bars_required for item in report.features] == [28, 30, 32]
        assert all(item.value is None for item in report.features if item.state == "WARMUP")


def test_flat_rsi_and_flat_stochastic_range_are_unavailable_with_reasons():
    flat = compute([bar(index, 100) for index in range(40)])
    assert flat.feature("stoch_rsi").reason == "FLAT_RSI_UNDEFINED"
    assert flat.feature("stoch_rsi_k").reason == "FLAT_RSI_UNDEFINED"
    rising = compute([bar(index, 100 + index) for index in range(40)])
    assert rising.feature("stoch_rsi").reason == "FLAT_STOCHASTIC_RSI_RANGE"
    assert rising.feature("stoch_rsi_k").reason == "FLAT_STOCHASTIC_RSI_RANGE"
    assert rising.feature("stoch_rsi_d").reason == "FLAT_STOCHASTIC_RSI_RANGE"
    assert all(item.value is None for item in (*flat.features, *rising.features))


@pytest.mark.parametrize("case", ["empty", "gap", "incomplete", "stale", "budget"])
def test_base_quality_blocker_withholds_all_outputs(case):
    rows = [bar(index, 100 + (index * 7) % 13 + index // 6) for index in range(40)]
    options = {}
    if case == "empty":
        rows = []
    elif case == "gap":
        rows.pop(5)
    elif case == "incomplete":
        rows[5] = bar(5, rows[5].payload()["close"], complete=False)
    elif case == "stale":
        options["max_latest_age_ms"] = 0
    else:
        options["max_bars"] = 39
    report = compute(rows, **options)
    assert report.state == "UNAVAILABLE"
    assert report.base.blockers
    assert report.input_event_ids == ()
    assert all(item.state == "UNAVAILABLE" and item.value is None for item in report.features)


def test_future_source_or_receipt_rows_are_rejected_not_filtered_or_retimed():
    rows = [bar(index, 100 + (index * 7) % 13) for index in range(40)]
    cutoff = rows[-2].observed_at_ms
    for future in (rows[-1], rows[-1].model_copy(update={"event_time_ms": cutoff + 1})):
        with pytest.raises(IntegrityError):
            snapshot = point([*rows[:-1], future], as_of=cutoff)
            compute_stochastic_rsi(
                snapshot,
                source="synthetic_fixture",
                symbol="FIXTURE-BTC/USD",
                timeframe_s=300,
            )


def test_input_order_and_exact_duplicate_replay_are_deterministic():
    rows = [bar(index, 100 + (index * 7) % 13 + index // 6) for index in range(40)]
    expected = compute(rows).model_dump_json()
    shuffled = rows[:]
    random.Random(190).shuffle(shuffled)
    assert compute([*shuffled, *rows[:4]]).model_dump_json() == expected


def test_seeded_random_histories_match_independent_fraction_reference():
    generator = random.Random(544)
    close = 250
    closes = []
    for _ in range(96):
        close += generator.choice((-7, -3, 1, 2, 6))
        closes.append(close)
    rows = [bar(index, value) for index, value in enumerate(closes)]
    report = compute(
        rows,
        rsi_period=9,
        stochastic_period=8,
        smooth_k=4,
        smooth_d=3,
    )
    expected = independent_reference(closes, 9, 8, 4, 3)
    actual = [report.feature(name).value for name in ("stoch_rsi", "stoch_rsi_k", "stoch_rsi_d")]
    assert actual == pytest.approx([float(value) for value in expected])


@pytest.mark.parametrize(
    "field,value",
    [
        ("rsi_period", True),
        ("rsi_period", 0),
        ("stochastic_period", 1001),
        ("smooth_k", False),
        ("smooth_d", -1),
        ("max_bars", 0),
        ("timeframe_s", 0),
    ],
)
def test_invalid_indicator_policy_is_refused(field, value):
    rows = [bar(index, 100 + index) for index in range(40)]
    with pytest.raises(ValueError):
        compute(rows, **{field: value})
