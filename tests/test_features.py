import json
import math
import random
import statistics
from fractions import Fraction

import pytest
from pydantic import ValidationError

from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.features import compute_features


def bar(
    index,
    close=100.0,
    *,
    source="kraken",
    symbol="BTC/USD",
    timeframe=300,
    complete=True,
    **changes,
):
    opening = 1800000 + index * timeframe * 1000
    body = {
        "open_time_ms": opening,
        "close_time_ms": opening + timeframe * 1000,
        "timeframe_s": timeframe,
        "open": close,
        "high": close + 2,
        "low": close - 2,
        "close": close,
        "volume": 1.0,
        "trade_count": 1,
        "closed": True,
        "complete": complete,
        "volume_unit": "base_asset",
        "aggregation_version": 1,
        "lateness_ms": 5,
        "coverage_start_ms": 0,
        **changes,
    }
    return make_event(
        source=source,
        environment="public_reference",
        asset="BTC",
        symbol=symbol,
        kind="bar",
        event_time_ms=body["close_time_ms"],
        observed_at_ms=body["close_time_ms"] + 5,
        payload=body,
    )


def snapshot(rows, *, as_of=None):
    return PointInTimeSnapshot(
        as_of if as_of is not None else max((row.observed_at_ms for row in rows), default=1000),
        "BTC",
        "public_reference",
        tuple(rows),
    )


def calculate(rows, **changes):
    return compute_features(
        snapshot(rows), source="kraken", symbol="BTC/USD", timeframe_s=300, **changes
    )


def ramp(count=220):
    return [bar(index, 100.0 + index) for index in range(count)]


def test_formula_seed_units_slopes_macd_bollinger_roc_and_explicit_annualization():
    rows = ramp()
    report = calculate(rows, annualization_periods_per_year=100)
    last = 319.0
    for period in (8, 21, 50, 200):
        assert report.feature(f"ema_{period}").value == pytest.approx(last - (period - 1) / 2)
        assert report.feature(f"ema_{period}_slope").value == pytest.approx(1)
        assert report.feature(f"ema_{period}_slope").unit == "price_per_bar"
    assert report.feature("rsi_14").value == 100
    assert report.feature("atr_14").value == 4
    assert report.feature("macd_12_26").value == pytest.approx(7)
    assert report.feature("macd_signal_9").value == pytest.approx(7)
    assert report.feature("macd_histogram").value == pytest.approx(0)
    assert report.feature("bollinger_width_20").value == pytest.approx(
        4 * math.sqrt(Fraction(399, 12)) / (last - 9.5)
    )
    assert report.feature("roc_5").value == pytest.approx(last / (last - 5) - 1)
    assert report.feature("roc_20").value == pytest.approx(last / (last - 20) - 1)
    for period in (20, 60):
        closes = [100.0 + index for index in range(220 - period - 1, 220)]
        returns = [
            math.log(right) - math.log(left)
            for left, right in zip(closes, closes[1:], strict=False)
        ]
        assert report.feature(f"realized_vol_{period}").value == pytest.approx(
            statistics.stdev(returns) * 10
        )
        assert report.feature(f"realized_vol_{period}").bars_required == period + 1
    assert report.bars_used == report.bars_available == 220
    assert report.formula_version == "closed-bars-v1"
    assert (
        report.ready_for_signals
        is report.trading_enabled
        is report.source_frontier_complete
        is False
    )


def test_wilder_rsi_seed_and_next_recurrence_have_hand_checked_fraction_reference():
    closes = [
        44.34,
        44.09,
        44.15,
        43.61,
        44.33,
        44.83,
        45.10,
        45.42,
        45.84,
        46.08,
        45.89,
        46.03,
        45.61,
        46.28,
        46.28,
    ]
    seeded = calculate([bar(index, close) for index, close in enumerate(closes)])
    # Fourteen deltas sum to 3.34 gains and 1.40 losses.
    assert seeded.feature("rsi_14").value == pytest.approx(float(Fraction(334, 474) * 100))
    next_report = calculate([bar(index, close) for index, close in enumerate([*closes, 46.00])])
    assert next_report.feature("rsi_14").value == pytest.approx(float(Fraction(4342, 6554) * 100))


def test_wilder_atr_uses_previous_close_and_recurrence_after_fourteen_ranges():
    rows = ramp(15)
    assert calculate(rows).feature("atr_14").value == 4
    rows.append(bar(15, 300))
    # New true range = 302 - previous close 114 = 188.
    assert calculate(rows).feature("atr_14").value == pytest.approx(
        float(Fraction(4 * 13 + 188, 14))
    )


@pytest.mark.parametrize(
    "count,name,required",
    [
        (7, "ema_8", 8),
        (8, "ema_8_slope", 9),
        (14, "rsi_14", 15),
        (14, "atr_14", 15),
        (25, "macd_12_26", 26),
        (33, "macd_signal_9", 34),
        (33, "macd_histogram", 34),
        (19, "bollinger_width_20", 20),
        (20, "realized_vol_20", 21),
        (199, "ema_200", 200),
        (200, "ema_200_slope", 201),
    ],
)
def test_each_feature_reports_exact_warmup_without_inventing_missing_zero(count, name, required):
    report = calculate(ramp(count), annualization_periods_per_year=100)
    value = report.feature(name)
    assert value.state == "WARMUP" and value.value is None
    assert value.bars_required == required and value.reason == "INSUFFICIENT_CLOSED_BARS"


def test_flat_rsi_explicitly_unavailable_while_computed_zero_features_remain_available():
    rows = [bar(index, 100.0, high=100.0, low=100.0) for index in range(220)]
    report = calculate(rows, annualization_periods_per_year=100)
    assert report.feature("rsi_14").state == "UNAVAILABLE"
    assert report.feature("rsi_14").reason == "FLAT_RSI_UNDEFINED"
    assert report.feature("rsi_14").value is None
    for name in (
        "atr_14",
        "ema_8_slope",
        "macd_histogram",
        "bollinger_width_20",
        "roc_5",
        "realized_vol_20",
    ):
        assert report.feature(name).state == "AVAILABLE" and report.feature(name).value == 0


def test_annualization_requires_explicit_assumption_not_a_guessed_crypto_or_gold_calendar():
    report = calculate(ramp())
    assert report.annualization_periods_per_year is None
    for name in ("realized_vol_20", "realized_vol_60"):
        assert report.feature(name).state == "UNAVAILABLE"
        assert report.feature(name).value is None
        assert report.feature(name).reason == "ANNUALIZATION_NOT_SPECIFIED"
    assert report.feature("ema_200").state == "AVAILABLE"


def test_custom_bar_windows_and_slope_are_explicit_and_order_independent():
    rows = ramp()
    first = calculate(
        rows,
        roc_windows=(10, 2),
        realized_vol_windows=(5, 3),
        slope_bars=3,
        annualization_periods_per_year=365,
    )
    second = calculate(
        rows,
        roc_windows=(2, 10),
        realized_vol_windows=(3, 5),
        slope_bars=3,
        annualization_periods_per_year=365,
    )
    assert first.model_dump_json() == second.model_dump_json()
    assert first.feature("ema_200_slope").bars_required == 203
    assert first.feature("ema_200_slope").value == pytest.approx(1)
    assert first.feature("realized_vol_3").bars_required == 4
    with pytest.raises(KeyError):
        first.feature("roc_5")


@pytest.mark.parametrize(
    "reason",
    [
        "NO_CLOSED_BARS",
        "INCOMPLETE_BAR",
        "MISSING_INTERIOR_INTERVAL",
        "STALE_LATEST_BAR",
        "HISTORY_BUDGET_EXCEEDED",
    ],
)
def test_bad_or_missing_history_has_named_global_unavailability(reason):
    rows = ramp(25)
    options = {}
    as_of = None
    if reason == "NO_CLOSED_BARS":
        rows = []
    elif reason == "INCOMPLETE_BAR":
        rows[5] = bar(5, 105.0, complete=False)
    elif reason == "MISSING_INTERIOR_INTERVAL":
        rows.pop(5)
    elif reason == "STALE_LATEST_BAR":
        as_of = rows[-1].observed_at_ms + 600001
    else:
        options["max_bars"] = 24
    report = compute_features(
        snapshot(rows, as_of=as_of), source="kraken", symbol="BTC/USD", timeframe_s=300, **options
    )
    assert report.state == "UNAVAILABLE" and reason in report.blockers
    assert report.bars_used == 0 and report.history_truncated is False
    assert all(value.state == "UNAVAILABLE" and value.value is None for value in report.features)


@pytest.mark.parametrize("limit", [2000, 219])
def test_identical_snapshot_feature_bytes_are_stable_under_input_order_and_exact_duplicate_replay(
    limit,
):
    rows = ramp()
    expected = calculate(rows, max_bars=limit, annualization_periods_per_year=100).model_dump_json()
    shuffled = rows[:]
    random.Random(2718).shuffle(shuffled)
    assert (
        calculate(shuffled, max_bars=limit, annualization_periods_per_year=100).model_dump_json()
        == expected
    )
    assert (
        calculate(
            [*shuffled, *rows[:10]], max_bars=limit, annualization_periods_per_year=100
        ).model_dump_json()
        == expected
    )


def test_exact_source_symbol_timeframe_filter_prevents_mixed_ohlc_series():
    rows = ramp(30)
    extra = [bar(index, 999.0, source="other") for index in range(30)]
    extra += [bar(index, 777.0, symbol="BTC/USDT") for index in range(30)]
    extra += [bar(index, 555.0, timeframe=900) for index in range(10)]
    cutoff = max(row.observed_at_ms for row in [*rows, *extra])
    baseline = compute_features(
        snapshot(rows, as_of=cutoff), source="kraken", symbol="BTC/USD", timeframe_s=300
    )
    selected = compute_features(
        snapshot([*rows, *extra], as_of=cutoff), source="kraken", symbol="BTC/USD", timeframe_s=300
    )
    assert baseline.model_dump_json() == selected.model_dump_json()
    assert selected.bars_used == 30


@pytest.mark.parametrize(
    "case",
    [
        "source_clock",
        "receipt_clock",
        "nested_clock",
        "other_asset",
        "other_environment",
        "bad_envelope",
        "mutable_event_container",
    ],
)
def test_fabricated_snapshot_or_event_cannot_bypass_point_in_time_integrity(case):
    rows = ramp(20)
    point = snapshot(rows)
    cutoff = point.as_of_ms
    forged = rows[0]
    if case == "source_clock":
        forged = forged.model_copy(update={"event_time_ms": cutoff + 1})
    elif case == "receipt_clock":
        forged = forged.model_copy(update={"observed_at_ms": cutoff + 1})
    elif case == "nested_clock":
        body = forged.payload() | {"audit": {"observed_at_ms": cutoff + 1}}
        forged = forged.model_copy(
            update={"payload_json": json.dumps(body, sort_keys=True, separators=(",", ":"))}
        )
    elif case == "other_asset":
        forged = forged.model_copy(update={"asset": "ETH"})
    elif case == "other_environment":
        forged = forged.model_copy(update={"environment": "testnet"})
    elif case == "bad_envelope":
        forged = forged.model_copy(update={"schema_version": 2})
    if case == "mutable_event_container":
        object.__setattr__(point, "_events", rows)
    else:
        object.__setattr__(point, "_events", (forged, *rows[1:]))
    with pytest.raises(IntegrityError):
        compute_features(point, source="kraken", symbol="BTC/USD", timeframe_s=300)


@pytest.mark.parametrize(
    "case",
    [
        "whitespace_json",
        "duplicate_key",
        "event_id",
        "receipt_kind",
        "complete_integer",
        "false_coverage",
        "unknown_field",
        "wrong_close",
    ],
)
def test_noncanonical_or_falsely_complete_bar_is_refused(case):
    rows = ramp(20)
    original = rows[3]
    body = original.payload()
    if case == "whitespace_json":
        forged = original.model_copy(update={"payload_json": json.dumps(body, indent=2)})
    elif case == "duplicate_key":
        encoded = original.payload_json.replace('"closed":true', '"closed":true,"closed":true')
        forged = original.model_copy(update={"payload_json": encoded})
    elif case == "event_id":
        forged = original.model_copy(update={"event_id": "0" * 64})
    elif case == "receipt_kind":
        forged = original.model_copy(update={"timestamp_kind": "receipt"})
    else:
        changes = {
            "complete_integer": {"complete": 1},
            "false_coverage": {"coverage_start_ms": body["open_time_ms"] + 1},
            "unknown_field": {"future_synthetic_fill": 1},
            "wrong_close": {"close": 1000.0},
        }[case]
        changed = body | changes
        forged = original.model_copy(
            update={"payload_json": json.dumps(changed, sort_keys=True, separators=(",", ":"))}
        )
    point = snapshot(rows)
    object.__setattr__(point, "_events", (*rows[:3], forged, *rows[4:]))
    with pytest.raises(IntegrityError):
        compute_features(point, source="kraken", symbol="BTC/USD", timeframe_s=300)


def test_same_close_distinct_bar_or_same_identity_receipt_conflict_are_refused():
    rows = ramp(20)
    with pytest.raises(IntegrityError, match="TIED_BAR_CLOSE"):
        calculate([*rows, bar(3, 104.0)])
    with pytest.raises(IntegrityError, match="CONFLICTING_EVENT_ID"):
        calculate(
            [*rows, rows[3].model_copy(update={"observed_at_ms": rows[3].observed_at_ms + 1})]
        )


@pytest.mark.parametrize(
    "policy",
    [
        {"source": ""},
        {"symbol": " "},
        {"timeframe_s": True},
        {"timeframe_s": 0},
        {"max_bars": True},
        {"max_bars": 10001},
        {"max_latest_age_ms": -1},
        {"slope_bars": 0},
        {"roc_windows": (1, 1)},
        {"roc_windows": [1]},
        {"roc_windows": (True,)},
        {"roc_windows": ([1],)},
        {"realized_vol_windows": (1,)},
        {"annualization_periods_per_year": True},
        {"annualization_periods_per_year": float("inf")},
        {"annualization_periods_per_year": float("nan")},
        {"annualization_periods_per_year": 0},
        {"annualization_periods_per_year": 10**1000},
    ],
)
def test_invalid_feature_policy_is_refused(policy):
    with pytest.raises(ValueError):
        compute_features(
            snapshot(ramp(20)),
            **{"source": "kraken", "symbol": "BTC/USD", "timeframe_s": 300, **policy},
        )


def test_report_and_feature_values_are_frozen_and_cannot_claim_readiness():
    report = calculate(ramp(220))
    with pytest.raises(ValidationError):
        report.ready_for_signals = True
    with pytest.raises(ValidationError):
        report.feature("ema_8").value = 0


def test_ema_and_macd_impulses_verify_seeds_and_nonconstant_signal_recurrence():
    rows = [bar(index, 100.0) for index in range(8)] + [bar(8, 180.0)]
    report = calculate(rows)
    assert report.feature("ema_8").value == pytest.approx(100 + float(Fraction(160, 9)))
    assert report.feature("ema_8_slope").value == pytest.approx(float(Fraction(160, 9)))
    rows = [bar(index, 100.0) for index in range(33)] + [bar(33, 114.0)]
    report = calculate(rows)
    line = Fraction(28, 13) - Fraction(28, 27)
    assert report.feature("macd_12_26").value == pytest.approx(float(line))
    assert report.feature("macd_signal_9").value == pytest.approx(float(line / 9))
    assert report.feature("macd_histogram").value == pytest.approx(float(line * 8 / 9))
    report = calculate([*rows, bar(34, 100.0)])
    next_line = Fraction(28, 13) * Fraction(11, 13) - Fraction(28, 27) * Fraction(25, 27)
    next_signal = Fraction(4, 5) * line / 9 + Fraction(1, 5) * next_line
    assert report.feature("macd_12_26").value == pytest.approx(float(next_line))
    assert report.feature("macd_signal_9").value == pytest.approx(float(next_signal))
    assert report.feature("macd_histogram").value == pytest.approx(float(next_line - next_signal))


def test_rsi_downtrend_is_computed_zero_and_latest_age_boundary_is_inclusive():
    rows = [bar(index, 100.0 - index) for index in range(20)]
    cutoff = rows[-1].event_time_ms + 600000
    report = compute_features(
        snapshot(rows, as_of=cutoff), source="kraken", symbol="BTC/USD", timeframe_s=300
    )
    assert report.blockers == ()
    assert report.latest_bar_age_ms == report.max_latest_age_ms == 600000
    assert report.feature("rsi_14").state == "AVAILABLE"
    assert report.feature("rsi_14").value == 0


def test_receipt_delay_is_preserved_in_digest_and_exact_two_clock_cutoff_is_accepted():
    rows = ramp(20)
    delayed = rows[:-1] + [
        rows[-1].model_copy(update={"observed_at_ms": rows[-1].observed_at_ms + 500})
    ]
    cutoff = delayed[-1].observed_at_ms
    baseline = compute_features(
        snapshot(rows, as_of=cutoff), source="kraken", symbol="BTC/USD", timeframe_s=300
    )
    report = calculate(delayed)
    assert report.as_of_ms == cutoff and report.bars_used == 20
    assert report.history_sha256 != baseline.history_sha256
    assert report.features == baseline.features
    too_early = snapshot(delayed)
    object.__setattr__(too_early, "as_of_ms", cutoff - 1)
    with pytest.raises(IntegrityError, match="LOOKAHEAD_EVENT"):
        compute_features(too_early, source="kraken", symbol="BTC/USD", timeframe_s=300)


def test_realized_vol_uses_explicit_bar_periods_without_timeframe_calendar_guess():
    first = calculate(ramp(30), annualization_periods_per_year=100)
    alternate = compute_features(
        snapshot([bar(index, 100.0 + index, timeframe=900) for index in range(30)]),
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=900,
        annualization_periods_per_year=400,
    )
    assert alternate.feature("realized_vol_20").value == pytest.approx(
        first.feature("realized_vol_20").value * 2
    )


@pytest.mark.parametrize(
    "case",
    [
        "missing_snapshot_field",
        "snapshot_environment_container",
        "snapshot_blank_asset",
        "snapshot_boolean_clock",
        "missing_constructed_event_field",
        "model_copy_extra_field",
        "boolean_schema_version",
        "non_event",
        "event_subclass",
    ],
)
def test_constructed_or_object_mutated_structures_are_reaudited(case):
    rows = ramp(20)
    point = snapshot(rows)
    if case == "missing_snapshot_field":
        object.__delattr__(point, "as_of_ms")
    elif case == "snapshot_environment_container":
        object.__setattr__(point, "environment", ["public_reference"])
    elif case == "snapshot_blank_asset":
        object.__setattr__(point, "asset", " ")
    elif case == "snapshot_boolean_clock":
        object.__setattr__(point, "as_of_ms", True)
    else:
        if case == "missing_constructed_event_field":
            body = rows[0].model_dump()
            body.pop("observed_at_ms")
            forged = MarketEvent.model_construct(**body)
        elif case == "model_copy_extra_field":
            forged = rows[0].model_copy(update={"untrusted_extra": "ignored by model_dump"})
        elif case == "boolean_schema_version":
            forged = rows[0].model_copy(update={"schema_version": True})
        elif case == "event_subclass":

            class DerivedEvent(MarketEvent):
                pass

            forged = DerivedEvent.model_validate(rows[0].model_dump())
        else:
            forged = rows[0].model_dump()
        object.__setattr__(point, "_events", (forged, *rows[1:]))
    with pytest.raises(IntegrityError):
        compute_features(point, source="kraken", symbol="BTC/USD", timeframe_s=300)


def test_valid_model_construct_is_fully_revalidated_without_arbitrary_origin_rejection():
    rows = ramp(20)
    constructed = [MarketEvent.model_construct(**row.model_dump()) for row in rows]
    assert calculate(constructed).model_dump_json() == calculate(rows).model_dump_json()


@pytest.mark.parametrize("clock", ["event_time_ms", "observed_at_ms", "coverage_start_ms"])
def test_leakage_in_nested_lists_or_unselected_source_cannot_hide_behind_scope_filter(clock):
    rows = ramp(20)
    point = snapshot(rows)
    unrelated = make_event(
        source="other",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="status",
        event_time_ms=point.as_of_ms,
        observed_at_ms=point.as_of_ms,
        payload={"audit": [{"nested": {clock: point.as_of_ms + 1}}]},
    )
    object.__setattr__(point, "_events", (*rows, unrelated))
    with pytest.raises(IntegrityError, match="LOOKAHEAD_NESTED_CLOCK"):
        compute_features(point, source="kraken", symbol="BTC/USD", timeframe_s=300)


def test_conflict_is_not_hidden_after_exceeding_history_budget():
    rows = ramp(30)
    with pytest.raises(IntegrityError, match="TIED_BAR_CLOSE"):
        calculate([*rows, bar(29, 1000.0)], max_bars=10)


@pytest.mark.parametrize(
    "change",
    [
        {"aggregation_version": True},
        {"closed": 1},
        {"trade_count": True},
        {"volume": True},
        {"high": float("nan")},
        {"coverage_start_ms": -1},
    ],
)
def test_forged_bar_controls_and_nonfinite_prices_do_not_pass_coercive_validation(change):
    rows = ramp(20)
    forged = rows[0].model_copy(
        update={
            "payload_json": json.dumps(
                rows[0].payload() | change, sort_keys=True, separators=(",", ":")
            )
        }
    )
    point = snapshot(rows)
    object.__setattr__(point, "_events", (forged, *rows[1:]))
    with pytest.raises(IntegrityError):
        compute_features(point, source="kraken", symbol="BTC/USD", timeframe_s=300)


def test_extreme_valid_prices_produce_named_numeric_unavailability_without_nonfinite_values():
    closes = [1e-300] * 20 + [1e300] * 5
    rows = [
        bar(index, close, open=close, high=close, low=close) for index, close in enumerate(closes)
    ]
    report = calculate(rows, annualization_periods_per_year=100)
    for name in ("roc_5", "roc_20"):
        assert report.feature(name).state == "UNAVAILABLE"
        assert report.feature(name).reason == "NUMERIC_UNAVAILABLE"
        assert report.feature(name).value is None
    assert report.feature("realized_vol_20").state == "AVAILABLE"
    assert all(value.value is None or math.isfinite(value.value) for value in report.features)
    assert "Infinity" not in report.model_dump_json()
    assert "NaN" not in report.model_dump_json()
