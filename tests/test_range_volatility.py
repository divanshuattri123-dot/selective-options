"""Independent Decimal references on synthetic OHLC, with explicit numerical faults."""

import hashlib
import json
import math
from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from selective_options import range_volatility
from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.range_volatility import compute_range_volatility


def bars(prices=((100.0, 110.0, 90.0, 105.0),) * 4):
    return [
        make_event(
            source="kraken",
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USD",
            kind="bar",
            event_time_ms=(index + 1) * 1_000,
            observed_at_ms=(index + 1) * 1_000 + 10,
            payload={
                "open_time_ms": index * 1_000,
                "close_time_ms": (index + 1) * 1_000,
                "timeframe_s": 1,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10.0,
                "trade_count": 5,
                "closed": True,
                "complete": True,
            },
        )
        for index, (opening, high, low, close) in enumerate(prices)
    ]


def snapshot(rows, *, as_of=None):
    cutoff = max((row.observed_at_ms for row in rows), default=1) if as_of is None else as_of
    return PointInTimeSnapshot(cutoff, "BTC", "public_reference", tuple(rows))


def compute(rows, **policy):
    return compute_range_volatility(
        snapshot(rows),
        **{
            "source": "kraken",
            "symbol": "BTC/USD",
            "timeframe_s": 1,
            "windows": (2, 4),
            "max_latest_age_ms": 50,
            "annualization_periods_per_year": 12.0,
            **policy,
        },
    )


def replace(row, *, observed=None, event_time=None, **changes):
    body = row.payload() | changes
    return make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind=row.kind,
        event_time_ms=row.event_time_ms if event_time is None else event_time,
        observed_at_ms=row.observed_at_ms if observed is None else observed,
        payload=body,
    )


def decimal_reference(rows, estimator):
    with localcontext() as context:
        context.prec = 100
        values = []
        log_two = Decimal(2).ln()
        for row in rows:
            body = row.payload()
            opening, high, low, close = (
                Decimal(str(body[name])) for name in ("open", "high", "low", "close")
            )
            range_squared = (high / low).ln() ** 2
            if estimator == "parkinson":
                variance = range_squared / (4 * log_two)
            else:
                variance = range_squared / 2 - (2 * log_two - 1) * (close / opening).ln() ** 2
            values.append(variance)
        return float(sum(values) / len(values))


@pytest.mark.parametrize("window", [2, 4])
@pytest.mark.parametrize("estimator", ["parkinson", "garman_klass"])
def test_primary_formula_decimal_reference_and_explicit_annualization(window, estimator):
    rows = bars(((100, 110, 90, 105), (100, 130, 95, 120), (120, 125, 80, 85), (85, 87, 82, 83)))
    report = compute(rows)
    raw = report.feature(f"{estimator}_variance_per_bar_{window}")
    vol = report.feature(f"{estimator}_vol_{window}")
    expected = decimal_reference(rows[-window:], estimator)
    assert raw.state == vol.state == "AVAILABLE"
    assert raw.value == pytest.approx(expected, rel=1e-13)
    assert vol.value == pytest.approx(math.sqrt(expected * 12), rel=1e-13)
    assert raw.unit == "log_return_variance_per_bar"
    assert vol.unit == "annualized_log_return_std"
    assert raw.bars_required == vol.bars_required == window


def test_mean_of_bar_variances_not_n_minus_one_or_average_volatility():
    rows = bars(((100, 101, 99, 100), (100, 170, 90, 150)))
    report = compute(rows, windows=(2,), annualization_periods_per_year=1)
    for estimator in ("parkinson", "garman_klass"):
        raw = report.feature(f"{estimator}_variance_per_bar_2")
        first, second = (
            decimal_reference(rows[:1], estimator),
            decimal_reference(rows[1:], estimator),
        )
        assert raw.value == pytest.approx((first + second) / 2)
        assert report.feature(f"{estimator}_vol_2").value != pytest.approx(
            (math.sqrt(first) + math.sqrt(second)) / 2
        )


def test_windows_are_bar_counts_with_exact_tail_and_frozen_references():
    rows = bars()
    report = compute(rows, windows=(4, 2))
    assert report.windows == (2, 4) and report.windows_are_bar_counts
    assert report.input_event_ids == tuple(row.event_id for row in rows)
    for reference in report.window_references:
        selected = rows[-reference.window :]
        assert reference.bars_used == reference.window and reference.bars_available == 4
        assert reference.first_open_ms == selected[0].payload()["open_time_ms"]
        assert reference.first_close_ms == selected[0].event_time_ms
        assert reference.latest_close_ms == selected[-1].event_time_ms
        expected = hashlib.sha256(
            json.dumps(
                [[row.event_id, row.event_time_ms, row.observed_at_ms] for row in selected],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        assert reference.history_sha256 == expected
    assert report.base.annualization_periods_per_year == 12
    assert report.zero_drift_diffusion_assumed
    assert not report.zero_drift_verified and not report.continuous_extrema_verified
    assert not report.opening_jumps_modelled and not report.sampling_bias_corrected
    assert not report.annualization_calendar_verified and not report.current_feed_health_verified
    assert not report.source_frontier_complete and not report.ready_for_signals
    assert not report.trading_enabled
    with pytest.raises(ValidationError):
        report.windows = (3,)
    with pytest.raises(ValidationError):
        report.window_references[0].bars_used = 0
    with pytest.raises(KeyError):
        report.feature("not_a_feature")


def test_window_requires_exact_n_bars_without_close_to_close_extra_bar():
    report = compute(bars()[:2], windows=(2, 3))
    assert report.feature("parkinson_variance_per_bar_2").state == "AVAILABLE"
    assert report.feature("garman_klass_vol_2").state == "AVAILABLE"
    assert report.feature("parkinson_vol_3").state == "WARMUP"
    assert report.feature("parkinson_vol_3").value is None
    assert report.feature("garman_klass_variance_per_bar_3").reason == "INSUFFICIENT_CLOSED_BARS"
    assert report.window_references[1].bars_used == 0
    assert report.window_references[1].history_sha256 is None


def test_no_annualization_keeps_raw_variance_and_explicitly_withholds_volatility():
    report = compute(bars(), annualization_periods_per_year=None)
    for estimator in ("parkinson", "garman_klass"):
        assert report.feature(f"{estimator}_variance_per_bar_4").state == "AVAILABLE"
        value = report.feature(f"{estimator}_vol_4")
        assert value.state == "UNAVAILABLE" and value.value is None
        assert value.reason == "ANNUALIZATION_NOT_SPECIFIED"
    warm = compute(bars()[:1], windows=(2,), annualization_periods_per_year=None)
    assert all(value.state == "WARMUP" for value in warm.features)


def test_observed_flat_range_can_be_zero_without_invented_warmup_zero():
    report = compute(bars(((100, 100, 100, 100),) * 2), windows=(2,))
    assert all(value.state == "AVAILABLE" and value.value == 0 for value in report.features)
    short = compute(bars(((100, 100, 100, 100),)), windows=(2,))
    assert all(value.state == "WARMUP" and value.value is None for value in short.features)


def test_scale_invariance_and_open_close_direction_symmetry():
    prices = ((100, 110, 90, 105),) * 2
    baseline = compute(bars(prices), windows=(2,))
    scaled = compute(
        bars(tuple(tuple(value * 1e250 for value in row) for row in prices)), windows=(2,)
    )
    reversed_body = compute(bars(((105, 110, 90, 100),) * 2), windows=(2,))
    for value in baseline.features:
        assert scaled.feature(value.name).value == pytest.approx(value.value, rel=1e-13)
        assert reversed_body.feature(value.name).value == pytest.approx(value.value, rel=1e-13)


def test_opening_jump_is_not_mistaken_for_modelled_overnight_variance():
    calm = compute(bars(((100, 100, 100, 100),) * 2), windows=(2,))
    jump = compute(bars(((100, 100, 100, 100), (1_000, 1_000, 1_000, 1_000))), windows=(2,))
    assert all(value.value == 0 for value in jump.features)
    assert jump.features == calm.features
    assert not jump.opening_jumps_modelled and not jump.zero_drift_verified


def test_reordered_identical_rows_deduplicate_without_changing_provenance():
    rows = bars()
    assert compute([*reversed(rows), rows[0]]) == compute(rows)


@pytest.mark.parametrize("issue", ["partial_prefix", "gap_prefix", "history_budget"])
def test_bad_prefix_blocks_tail_estimates_without_shortening_history(issue):
    rows = bars()
    policy = {"windows": (2,)}
    if issue == "partial_prefix":
        rows[0] = replace(rows[0], complete=False)
        expected = "INCOMPLETE_BAR"
    elif issue == "gap_prefix":
        rows.pop(0)
        rows.pop(0)
        rows = [bars()[0], *rows]
        expected = "MISSING_INTERIOR_INTERVAL"
    else:
        policy["max_bars"] = 3
        expected = "HISTORY_BUDGET_EXCEEDED"
    report = compute(rows, **policy)
    assert report.state == "UNAVAILABLE" and expected in report.base.blockers
    assert report.base.bars_used == 0 and report.input_event_ids == ()
    assert all(value.state == "UNAVAILABLE" and value.value is None for value in report.features)
    assert all(value.reason == expected for value in report.features)
    assert all(reference.bars_used == 0 for reference in report.window_references)


def test_no_bars_and_stale_latest_explicitly_unavailable():
    empty = compute([])
    assert empty.base.blockers == ("NO_CLOSED_BARS",)
    assert all(value.reason == "NO_CLOSED_BARS" for value in empty.features)
    rows = bars()
    report = compute_range_volatility(
        snapshot(rows, as_of=rows[-1].observed_at_ms + 100),
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=1,
        windows=(2,),
        max_latest_age_ms=50,
    )
    assert report.base.blockers == ("STALE_LATEST_BAR",)
    assert all(value.value is None for value in report.features)


def test_other_symbol_source_and_timeframe_do_not_contaminate_selected_ohlc():
    rows = bars()
    other_symbol = make_event(
        source="kraken",
        environment="public_reference",
        asset="BTC",
        symbol="OTHER",
        kind="bar",
        event_time_ms=rows[-1].event_time_ms,
        observed_at_ms=rows[-1].observed_at_ms,
        payload=rows[-1].payload() | {"high": 999},
    )
    other_source = make_event(
        source="OTHER",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="bar",
        event_time_ms=rows[-1].event_time_ms,
        observed_at_ms=rows[-1].observed_at_ms,
        payload=rows[-1].payload(),
    )
    other_frame = replace(
        rows[-1],
        open_time_ms=2_000,
        close_time_ms=4_000,
        timeframe_s=2,
    )
    assert compute([*rows, other_symbol, other_source, other_frame]) == compute(rows)


def test_conflicting_identity_tied_close_and_forged_native_payload_refuse():
    rows = bars()
    conflict = rows[0].model_copy(update={"observed_at_ms": rows[0].observed_at_ms + 1})
    with pytest.raises(IntegrityError, match="CONFLICTING_EVENT_ID"):
        compute([*rows, conflict])
    tied = replace(rows[0], high=111)
    with pytest.raises(IntegrityError, match="TIED_BAR_CLOSE"):
        compute([*rows, tied])
    forged = rows[0].model_copy(update={"event_id": "f" * 64})
    with pytest.raises(IntegrityError, match="NONCANONICAL_BAR_IDENTITY"):
        compute([forged, *rows[1:]])


@pytest.mark.parametrize("clock", ["event_time_ms", "observed_at_ms"])
def test_forged_future_rows_cannot_bypass_both_clock_audit(clock):
    rows = bars()
    existing = snapshot(rows)
    forged = rows[0].model_copy(update={clock: existing.as_of_ms + 1})
    object.__setattr__(existing, "_events", (forged, *rows[1:]))
    with pytest.raises(IntegrityError, match="LOOKAHEAD_EVENT"):
        compute_range_volatility(
            existing,
            source="kraken",
            symbol="BTC/USD",
            timeframe_s=1,
            windows=(2,),
            max_latest_age_ms=50,
        )


@pytest.mark.parametrize(
    "changes", [{"high": 1}, {"low": 0}, {"complete": "true"}, {"trade_count": 0}]
)
def test_invalid_canonical_ohlc_or_control_fields_refuse(changes):
    rows = bars()
    # Normal construction itself validates many OHLC fields; use a forged event
    # to exercise the transformation's independent boundary check.
    body = rows[0].payload() | changes
    forged = rows[0].model_copy(
        update={"payload_json": json.dumps(body, sort_keys=True, separators=(",", ":"))}
    )
    with pytest.raises(IntegrityError):
        compute([forged, *rows[1:]])


@pytest.mark.parametrize(
    "windows", [(), [], (1,), (2, 2), (True,), (2.0,), (10_001,), tuple(range(2, 35))]
)
def test_windows_must_be_explicit_unique_bounded_integer_bar_counts(windows):
    with pytest.raises(ValueError):
        compute(bars(), windows=windows)


@pytest.mark.parametrize("annualization", [True, False, 0, -1, math.inf, math.nan, "365", 10**1000])
def test_annualization_must_be_explicit_finite_positive_and_never_coerced(annualization):
    with pytest.raises(ValueError):
        compute(bars(), annualization_periods_per_year=annualization)


@pytest.mark.parametrize(
    "policy,value",
    [
        ("source", ""),
        ("symbol", " "),
        ("source", 1),
        ("symbol", True),
        ("timeframe_s", True),
        ("timeframe_s", 86_401),
        ("max_bars", 0),
        ("max_bars", 10_001),
        ("max_latest_age_ms", None),
        ("max_latest_age_ms", True),
        ("max_latest_age_ms", -1),
    ],
)
def test_scope_age_and_budget_policies_are_explicit_and_strict(policy, value):
    with pytest.raises(ValueError):
        compute(bars(), **{policy: value})


def test_extreme_price_ratio_avoids_division_overflow_and_matches_decimal_logs():
    rows = bars(((1.0, 1e308, 1e-308, 2.0),) * 2)
    assert math.isinf(1e308 / 1e-308)
    report = compute(rows, windows=(2,), annualization_periods_per_year=1)
    for estimator in ("parkinson", "garman_klass"):
        expected = decimal_reference(rows, estimator)
        assert report.feature(f"{estimator}_variance_per_bar_2").value == pytest.approx(
            expected, rel=1e-13
        )
        assert report.feature(f"{estimator}_vol_2").value == pytest.approx(
            math.sqrt(expected), rel=1e-13
        )


def test_near_equal_large_prices_preserve_nonzero_range_without_log_cancellation():
    low = 1e308
    high = math.nextafter(low, math.inf)
    assert math.log(high) - math.log(low) == 0
    rows = bars(((low, high, low, high),) * 2)
    report = compute(rows, windows=(2,), annualization_periods_per_year=1)
    for estimator in ("parkinson", "garman_klass"):
        value = report.feature(f"{estimator}_variance_per_bar_2")
        assert value.state == "AVAILABLE" and value.value > 0
        # Decimal(str(float)) represents a slightly different decimal rounding
        # than the exact binary floats; the independent ratio below retains both.
        with localcontext() as context:
            context.prec = 100
            log_ratio = (Decimal(high) / Decimal(low)).ln()
            log_two = Decimal(2).ln()
            expected = (
                log_ratio**2 / (4 * log_two)
                if estimator == "parkinson"
                else (Decimal("0.5") - (2 * log_two - 1)) * log_ratio**2
            )
        assert value.value == pytest.approx(float(expected), rel=1e-13, abs=0)


@pytest.mark.parametrize(
    "annualization,reason",
    [
        (1e308, "ANNUALIZED_VARIANCE_NONFINITE"),
        (math.nextafter(0.0, 1.0), "ANNUALIZED_VARIANCE_UNDERFLOW"),
    ],
)
def test_annualized_overflow_or_underflow_keeps_raw_variance_without_fake_value(
    annualization, reason
):
    prices = (
        ((1.0, 1e308, 1e-308, 1.0),) * 2
        if annualization == 1e308
        else ((100.0, 110.0, 90.0, 100.0),) * 2
    )
    report = compute(bars(prices), windows=(2,), annualization_periods_per_year=annualization)
    for estimator in ("parkinson", "garman_klass"):
        assert report.feature(f"{estimator}_variance_per_bar_2").state == "AVAILABLE"
        value = report.feature(f"{estimator}_vol_2")
        assert value.state == "UNAVAILABLE" and value.value is None and value.reason == reason


@pytest.mark.parametrize(
    "numerical_fault,reason",
    [
        (0.0, "LOG_RATIO_COLLAPSED_OR_NONFINITE"),
        (math.inf, "LOG_RATIO_COLLAPSED_OR_NONFINITE"),
        (math.nan, "LOG_RATIO_COLLAPSED_OR_NONFINITE"),
        (1e-200, "LOG_SQUARE_UNDERFLOW"),
    ],
)
def test_injected_log_arithmetic_fault_is_withheld_not_clipped(
    monkeypatch, numerical_fault, reason
):
    # This is a calculation fault injection, not observed market evidence.
    monkeypatch.setattr(range_volatility.math, "log1p", lambda _: numerical_fault)
    report = compute(bars()[:2], windows=(2,))
    assert all(value.state == "UNAVAILABLE" and value.value is None for value in report.features)
    assert all(value.reason == reason for value in report.features)


def test_injected_negative_garman_klass_variance_is_not_abs_or_zero(monkeypatch):
    # Valid OHLC mathematically prevents a negative practical GK contribution.
    # Inject inconsistent logarithm arithmetic to test the refusal boundary.
    native_log = range_volatility._log_ratio
    monkeypatch.setattr(
        range_volatility,
        "_log_ratio",
        lambda numerator, denominator: (
            2.0
            if numerator == 105.0 and denominator == 100.0
            else native_log(numerator, denominator)
        ),
    )
    report = compute(bars()[:2], windows=(2,))
    assert report.feature("parkinson_vol_2").state == "AVAILABLE"
    raw = report.feature("garman_klass_variance_per_bar_2")
    assert raw.state == "UNAVAILABLE" and raw.reason == "NEGATIVE_VARIANCE" and raw.value is None
    assert report.feature("garman_klass_vol_2").value is None
