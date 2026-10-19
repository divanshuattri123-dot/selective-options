"""Independent exact-arithmetic and causal audits on explicitly synthetic bars."""

import json
import math
import random
from fractions import Fraction

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.keltner_features import compute_keltner_features


def bar(index, close=100.0, **changes):
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
            "complete": True,
            **changes,
        },
    )


def point(rows, as_of=None):
    return PointInTimeSnapshot(
        as_of if as_of is not None else max((row.observed_at_ms for row in rows), default=1000),
        "BTC",
        "public_reference",
        tuple(rows),
    )


def compute(rows, **options):
    return compute_keltner_features(
        point(rows),
        source="synthetic_fixture",
        symbol="FIXTURE-BTC/USD",
        timeframe_s=300,
        **options,
    )


def exact_average(values, period, alpha):
    """Finite geometric expansion, independent of the production recurrence."""
    values = [Fraction(str(value)) for value in values]
    beta = 1 - alpha
    updates = len(values) - period
    return sum(values[:period]) / period * beta**updates + sum(
        alpha * value * beta ** (len(values) - 1 - index)
        for index, value in enumerate(values[period:], start=period)
    )


def reference(rows, p, q, multiplier):
    payloads = [row.payload() for row in rows]
    closes = [item["close"] for item in payloads]
    ranges = [
        max(
            Fraction(str(right["high"])) - Fraction(str(right["low"])),
            abs(Fraction(str(right["high"])) - Fraction(str(left["close"]))),
            abs(Fraction(str(right["low"])) - Fraction(str(left["close"]))),
        )
        for left, right in zip(payloads, payloads[1:], strict=False)
    ]
    center = exact_average(closes, p, Fraction(2, p + 1))
    atr = exact_average(ranges, q, Fraction(1, q))
    return (center, atr, center + multiplier * atr, center - multiplier * atr)


def test_standard_defaults_match_independent_exact_geometric_sum_with_gaps_and_reversals():
    closes = [100.0 + (index * 7) % 31 - index / 4 for index in range(45)]
    rows = [bar(i, close) for i, close in enumerate(closes)]
    report = compute(rows)
    expected = reference(rows, 20, 10, Fraction(2))
    assert report.formula_version == "keltner-bars-v1"
    assert (report.ema_period, report.atr_period, report.atr_multiplier) == (20, 10, 2)
    assert [item.value for item in report.features] == pytest.approx([float(v) for v in expected])
    assert all(item.unit == "price" and item.state == "AVAILABLE" for item in report.features)
    assert report.input_event_ids == tuple(row.event_id for row in rows)
    assert report.base.bars_used == len(rows)
    assert not any(
        (
            report.current_feed_health_verified,
            report.source_frontier_complete,
            report.regime_classification_complete,
            report.ready_for_signals,
            report.trading_enabled,
        )
    )
    with pytest.raises(KeyError):
        report.feature("missing")


def test_exact_seed_and_next_update_have_hand_calculated_goldens():
    rows = [bar(i, close) for i, close in enumerate([100.0, 103.0, 99.0, 110.0])]
    seeded = compute(rows[:3], ema_period=3, atr_period=2, atr_multiplier=1.5)
    assert [item.value for item in seeded.features] == pytest.approx(
        [float(Fraction(302, 3)), 5.5, float(Fraction(1307, 12)), float(Fraction(1109, 12))]
    )
    updated = compute(rows, ema_period=3, atr_period=2, atr_multiplier=1.5)
    assert [item.value for item in updated.features] == pytest.approx(
        [float(Fraction(316, 3)), 9.25, float(Fraction(2861, 24)), float(Fraction(2195, 24))]
    )


@pytest.mark.parametrize(
    "count,center,atr,bands",
    [
        (2, "WARMUP", "WARMUP", "WARMUP"),
        (10, "WARMUP", "WARMUP", "WARMUP"),
        (11, "WARMUP", "AVAILABLE", "WARMUP"),
        (19, "WARMUP", "AVAILABLE", "WARMUP"),
        (20, "AVAILABLE", "AVAILABLE", "AVAILABLE"),
    ],
)
def test_standard_warmup_counts_are_explicit(count, center, atr, bands):
    report = compute([bar(i, 100.0 + i) for i in range(count)])
    assert [item.state for item in report.features] == [center, atr, bands, bands]
    assert [item.bars_required for item in report.features] == [20, 11, 20, 20]
    for item in report.features:
        if item.state == "WARMUP":
            assert item.value is None and item.reason == "INSUFFICIENT_CLOSED_BARS"


def test_period_one_and_longer_atr_keep_separate_warmups():
    rows = [bar(i, 100.0 + i) for i in range(4)]
    report = compute(rows, ema_period=1, atr_period=1, atr_multiplier=2)
    assert [item.value for item in report.features] == [103, 4, 111, 95]
    report = compute(rows, ema_period=1, atr_period=5)
    assert report.feature("keltner_center").value == 103
    assert [item.state for item in report.features] == ["AVAILABLE", "WARMUP", "WARMUP", "WARMUP"]
    assert report.feature("keltner_upper").bars_required == 6


def test_flat_zero_atr_is_valid_and_lower_envelope_can_be_negative():
    report = compute([bar(i, high=100.0, low=100.0) for i in range(25)])
    assert [item.value for item in report.features] == [100, 0, 100, 100]
    broad = compute([bar(i, 1.0, high=101.0, low=0.5) for i in range(25)])
    assert broad.feature("keltner_lower").value < 0
    assert broad.feature("keltner_lower").state == "AVAILABLE"


def test_first_bar_range_is_not_an_invented_previous_close_true_range():
    rows = [bar(i) for i in range(21)]
    changed = [bar(0, high=10000.0, low=0.01), *rows[1:]]
    left, right = compute(rows), compute(changed)
    assert [item.value for item in left.features] == [item.value for item in right.features]
    assert left.base.history_sha256 != right.base.history_sha256


def test_entire_seed_history_matters_and_is_never_replaced_by_latest_tail():
    rows = [bar(0, 1000.0), *[bar(i) for i in range(1, 35)]]
    complete = compute(rows)
    tail = compute(rows[10:])
    assert complete.feature("keltner_center").value != tail.feature("keltner_center").value
    assert complete.feature("keltner_atr").value != tail.feature("keltner_atr").value
    assert complete.base.first_close_ms == rows[0].event_time_ms


def test_input_order_duplicate_replay_and_other_timeframe_scope_are_deterministic():
    rows = [bar(i, 100.0 + i) for i in range(30)]
    expected = compute(rows).model_dump_json()
    shuffled = rows[:]
    random.Random(8).shuffle(shuffled)
    assert compute([*shuffled, *rows[:3]]).model_dump_json() == expected
    other = make_event(
        source="synthetic_fixture",
        environment="public_reference",
        asset="BTC",
        symbol="FIXTURE-BTC/USD",
        kind="bar",
        event_time_ms=1_800_000,
        observed_at_ms=1_800_005,
        payload=rows[0].payload()
        | {"open_time_ms": 900_000, "close_time_ms": 1_800_000, "timeframe_s": 900},
    )
    assert compute([*rows, other]).model_dump_json() == expected


@pytest.mark.parametrize("case", ["empty", "gap", "incomplete", "stale", "budget"])
def test_quality_failures_withhold_all_outputs_without_rescuing_tail(case):
    rows = [bar(i, 100.0 + i) for i in range(35)]
    options = {}
    if case == "empty":
        rows = []
    elif case == "gap":
        rows.pop(1)
    elif case == "incomplete":
        rows[1] = bar(1, 101.0, complete=False)
    elif case == "stale":
        options["max_latest_age_ms"] = 0
    else:
        options["max_bars"] = 34
    report = compute(rows, **options)
    assert report.state == "UNAVAILABLE" and report.base.blockers
    assert report.input_event_ids == ()
    assert all(item.state == "UNAVAILABLE" and item.value is None for item in report.features)


@pytest.mark.parametrize(
    "case",
    [
        "event_clock",
        "receipt_clock",
        "mixed_scope",
        "identity",
        "nested_clock",
        "tied_close",
        "conflicting_replay",
        "extra_field",
        "boolean_version",
    ],
)
def test_forged_or_future_snapshot_input_is_refused(case):
    rows = [bar(i, 100.0 + i) for i in range(30)]
    snapshot = point(rows)
    original = rows[0]
    if case == "event_clock":
        forged = original.model_copy(update={"event_time_ms": snapshot.as_of_ms + 1})
    elif case == "receipt_clock":
        forged = original.model_copy(update={"observed_at_ms": snapshot.as_of_ms + 1})
    elif case == "mixed_scope":
        forged = original.model_copy(update={"asset": "ETH"})
    elif case == "identity":
        forged = original.model_copy(update={"event_id": "0" * 64})
    elif case == "nested_clock":
        forged = original.model_copy(
            update={
                "payload_json": json.dumps(
                    original.payload() | {"audit": {"observed_at_ms": snapshot.as_of_ms + 1}}
                )
            }
        )
    elif case == "tied_close":
        forged = bar(0, 101.0)
    elif case == "conflicting_replay":
        forged = original.model_copy(update={"observed_at_ms": original.observed_at_ms + 1})
    elif case == "extra_field":
        forged = original.model_copy(update={"untrusted": 1})
    else:
        forged = original.model_copy(update={"schema_version": True})
    object.__setattr__(snapshot, "_events", (*rows, forged))
    with pytest.raises(IntegrityError):
        compute_keltner_features(
            snapshot, source="synthetic_fixture", symbol="FIXTURE-BTC/USD", timeframe_s=300
        )


@pytest.mark.parametrize(
    "options",
    [
        {"ema_period": True},
        {"ema_period": 0},
        {"ema_period": 10001},
        {"atr_period": True},
        {"atr_period": 0},
        {"atr_period": 10000},
        {"atr_multiplier": True},
        {"atr_multiplier": 0},
        {"atr_multiplier": -1},
        {"atr_multiplier": float("inf")},
        {"atr_multiplier": float("nan")},
        {"atr_multiplier": 1001},
        {"atr_multiplier": 10**1000},
        {"source": True},
    ],
)
def test_strict_bounded_policy(options):
    if "source" in options:
        with pytest.raises(ValueError):
            compute_keltner_features(
                point([]), symbol="FIXTURE-BTC/USD", timeframe_s=300, **options
            )
    else:
        with pytest.raises(ValueError):
            compute([], **options)


def test_nonfinite_envelopes_and_underflow_are_unavailable_never_fake_flat():
    rows = [bar(i, 1e308, high=1.5e308, low=0.5e308) for i in range(25)]
    report = compute(rows, atr_multiplier=1000)
    assert report.feature("keltner_center").state == "AVAILABLE"
    assert report.feature("keltner_atr").state == "AVAILABLE"
    assert report.feature("keltner_upper").reason == "ENVELOPE_NONFINITE"
    assert report.feature("keltner_lower").reason == "ENVELOPE_NONFINITE"
    small = [bar(i, 100.0 + i) for i in range(25)]
    report = compute(small, atr_multiplier=5e-324)
    assert report.feature("keltner_upper").reason == "ENVELOPE_COLLAPSED_TO_CENTER"
    assert all(item.value is None or math.isfinite(item.value) for item in report.features)


def test_price_scale_changes_channels_in_price_units_without_changing_formula():
    rows = [bar(i, 100.0 + (i * 7) % 31) for i in range(45)]
    scaled = []
    for row in rows:
        body = row.payload()
        scaled.append(
            make_event(
                source=row.source,
                environment=row.environment,
                asset=row.asset,
                symbol=row.symbol,
                kind="bar",
                event_time_ms=row.event_time_ms,
                observed_at_ms=row.observed_at_ms,
                payload=body
                | {name: body[name] * 1e-100 for name in ("open", "high", "low", "close")},
            )
        )
    original, tiny = compute(rows), compute(scaled)
    assert [item.value for item in tiny.features] == pytest.approx(
        [item.value * 1e-100 for item in original.features], abs=0, rel=1e-12
    )


def test_period_one_exactly_uses_latest_close_and_true_range_after_large_scale_change():
    rows = [bar(0, 1e16, high=1e16, low=1e16), bar(1, 3.0, high=3.0, low=3.0)]
    report = compute(rows, ema_period=1, atr_period=1)
    assert report.feature("keltner_center").value == 3.0
    # Extend through another small change so ATR(1) must also discard the large
    # previous observation without subtractive cancellation (4 vs exact 3).
    rows.append(bar(2, 6.0, high=6.0, low=6.0))
    report = compute(rows, ema_period=1, atr_period=1)
    assert report.feature("keltner_center").value == 6.0
    assert report.feature("keltner_atr").value == 3.0


def test_representable_subnormal_seed_retains_mass_before_dividing():
    tiny, twice = 5e-324, 1e-323
    rows = [bar(0, tiny, high=tiny, low=tiny), bar(1, twice, high=twice, low=twice)]
    exact_binary_mean = float((Fraction(tiny) + Fraction(twice)) / 2)
    assert exact_binary_mean == twice
    report = compute(rows, ema_period=2, atr_period=1)
    assert report.feature("keltner_center").state == "AVAILABLE"
    assert report.feature("keltner_center").value == exact_binary_mean
    # ATR(2) seeds the two genuine nonzero subnormal true ranges likewise.
    rows.append(bar(2, 2e-323, high=2e-323, low=2e-323))
    report = compute(rows, ema_period=2, atr_period=2)
    expected = float((Fraction(twice - tiny) + Fraction(2e-323 - twice)) / 2)
    assert report.feature("keltner_atr").value == expected


def test_seed_mean_survives_overflowing_sum_when_price_mean_is_finite():
    rows = [bar(index, 1e308, high=1e308, low=1e308) for index in range(25)]
    report = compute(rows)
    assert [item.value for item in report.features] == [1e308, 0, 1e308, 1e308]


def test_period_one_atr_legitimately_returns_to_zero_without_underflow_refusal():
    rows = [
        bar(index, close, high=close, low=close)
        for index, close in enumerate((100.0, 101.0, 101.0))
    ]
    report = compute(rows, ema_period=1, atr_period=1)
    assert [item.state for item in report.features] == ["AVAILABLE"] * 4
    assert [item.value for item in report.features] == [101, 0, 101, 101]


@pytest.mark.parametrize("step,expected", [(5e-324, None), (1.5e-323, 1e-323)])
def test_subnormal_atr_update_rounds_representable_mass_and_refuses_total_underflow(step, expected):
    minimum = 5e-324
    closes = [minimum, minimum + step, minimum + 2 * step, minimum + 2 * step]
    rows = [bar(index, close, high=close, low=close) for index, close in enumerate(closes)]
    report = compute(rows, ema_period=1, atr_period=2)
    feature = report.feature("keltner_atr")
    exact = float(Fraction(step) / 2)
    assert exact == (expected or 0)
    if expected is None:
        assert feature.state == "UNAVAILABLE" and feature.value is None
        assert feature.reason == "NUMERIC_UNAVAILABLE"
    else:
        assert feature.state == "AVAILABLE" and feature.value == expected
