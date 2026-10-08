import json
import math
import random
import statistics
from fractions import Fraction

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.trend_features import compute_trend_features


def bar(index, close=100.0, **changes):
    opening = 300000 + index * 300000
    return make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="bar",
        event_time_ms=opening + 300000,
        observed_at_ms=opening + 300005,
        payload={
            "open_time_ms": opening,
            "close_time_ms": opening + 300000,
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
        as_of or max((row.observed_at_ms for row in rows), default=1000),
        "BTC",
        "public_reference",
        tuple(rows),
    )


def compute(rows, **options):
    return compute_trend_features(
        point(rows), source="fixture", symbol="BTC/USD", timeframe_s=300, **options
    )


def ramp(count=150, direction=1):
    return [bar(index, 200.0 + direction * index) for index in range(count)]


@pytest.mark.parametrize("direction", [1, -1])
def test_constant_direction_has_analytic_di_adx_and_price_scale_invariance(direction):
    rows = ramp(60, direction)
    report = compute(rows)
    plus, minus = (25, 0) if direction == 1 else (0, 25)
    assert report.feature("plus_di_14").value == pytest.approx(plus)
    assert report.feature("minus_di_14").value == pytest.approx(minus)
    assert report.feature("adx_14").value == pytest.approx(100)
    tiny = [
        bar(
            i,
            (200.0 + direction * i) * 1e-100,
            open=(200.0 + direction * i) * 1e-100,
            high=(202.0 + direction * i) * 1e-100,
            low=(198.0 + direction * i) * 1e-100,
        )
        for i in range(60)
    ]
    scaled = compute(tiny)
    for name in ("plus_di_14", "minus_di_14", "adx_14"):
        assert scaled.feature(name).value == pytest.approx(report.feature(name).value)


def test_directional_seed_and_reversal_are_checked_with_independent_rational_reference():
    rows = [bar(i, 100.0 + i) for i in range(14)] + [bar(14, 111.0)]
    report = compute(rows)
    assert report.feature("plus_di_14").value == pytest.approx(float(Fraction(16900, 732)))
    assert report.feature("minus_di_14").value == pytest.approx(float(Fraction(700, 183)))
    assert report.feature("adx_14").state == "WARMUP"
    # Thirteen initial rising changes, then 14 falling changes; calculate DX by
    # exact fractions, independently retaining unsummed directional legs.
    rows = [bar(i, 100.0 + i) for i in range(14)]
    rows += [bar(i, 113.0 - 2 * (i - 13)) for i in range(14, 28)]
    p, m, t = Fraction(13), Fraction(0), Fraction(52)
    dx = []
    for _ in range(14):
        p, m, t = p * Fraction(13, 14), m * Fraction(13, 14) + 2, t * Fraction(13, 14) + 4
        dx.append(100 * abs(p - m) / (p + m))
    report = compute(rows)
    first_adx = sum(dx) / 14
    assert report.feature("adx_14").value == pytest.approx(float(first_adx))
    p, m, t = p * Fraction(13, 14), m * Fraction(13, 14) + 2, t * Fraction(13, 14) + 4
    next_dx = 100 * abs(p - m) / (p + m)
    next_report = compute([*rows, bar(28, 83.0)])
    assert next_report.feature("adx_14").value == pytest.approx(
        float((first_adx * 13 + next_dx) / 14)
    )


@pytest.mark.parametrize("case", ["flat", "inside", "tied"])
def test_undefined_directional_denominators_are_not_invented_zero_adx(case):
    rows = []
    for i in range(40):
        if case == "flat":
            rows.append(bar(i, 100.0, high=100.0, low=100.0))
        elif case == "inside":
            rows.append(bar(i, 100.0, high=150.0 - i, low=50.0 + i))
        else:
            rows.append(bar(i, 100.0, high=102.0 + i, low=98.0 - i))
    report = compute(rows)
    assert report.feature("adx_14").state == "UNAVAILABLE"
    assert report.feature("adx_14").value is None
    assert report.feature("adx_14").reason == (
        "TRUE_RANGE_ZERO" if case == "flat" else "DIRECTIONAL_MOVEMENT_UNDEFINED"
    )
    if case != "flat":
        assert report.feature("plus_di_14").value == report.feature("minus_di_14").value == 0


@pytest.mark.parametrize(
    "count,name,required",
    [
        (14, "plus_di_14", 15),
        (14, "minus_di_14", 15),
        (27, "adx_14", 28),
        (39, "bollinger_width_20_percentile", 40),
        (40, "realized_vol_20_percentile", 41),
    ],
)
def test_warmup_counts_underlying_history_and_excludes_current_from_rank_reference(
    count, name, required
):
    report = compute(ramp(count), annualization_periods_per_year=100)
    assert report.feature(name).state == "WARMUP" and report.feature(name).value is None
    assert report.feature(name).bars_required == required


def test_rank_against_only_bounded_prior_observations_and_independent_sample_volatility():
    rows = ramp(80)
    report = compute(
        rows, annualization_periods_per_year=100, percentile_window=7, percentile_min_history=5
    )
    closes = [200.0 + i for i in range(80)]
    widths = []
    vols = []
    for end in range(72, 80):
        width = closes[end - 19 : end + 1]
        widths.append(4 * statistics.pstdev(width) / statistics.mean(width))
        returns = [
            math.log(b) - math.log(a)
            for a, b in zip(closes[end - 20 : end], closes[end - 19 : end + 1], strict=True)
        ]
        vols.append(statistics.stdev(returns) * 10)
    for name, values in (
        ("bollinger_width_20_percentile", widths),
        ("realized_vol_20_percentile", vols),
    ):
        current = values[-1]
        expected = (
            100
            * (sum(v < current for v in values[:-1]) + 0.5 * sum(v == current for v in values[:-1]))
            / 7
        )
        assert report.feature(name).value == expected == 0
    for reference in report.percentile_references:
        assert reference.observations == 7 and reference.window == 7
        assert reference.first_close_ms == rows[72].event_time_ms
        assert reference.latest_close_ms == rows[78].event_time_ms < rows[79].event_time_ms
        assert reference.current_excluded and reference.tie_policy == "exact_midrank"


def test_exact_midrank_ties_and_current_exclusion_are_visible():
    rows = [bar(i, 100.0) for i in range(50)]
    report = compute(
        rows, annualization_periods_per_year=100, percentile_window=5, percentile_min_history=5
    )
    assert report.feature("bollinger_width_20_percentile").value == 50
    assert report.feature("realized_vol_20_percentile").value == 50
    shocked = compute(
        [*rows[:-1], bar(49, 200.0)],
        annualization_periods_per_year=100,
        percentile_window=5,
        percentile_min_history=5,
    )
    assert shocked.feature("bollinger_width_20_percentile").value == 100
    assert shocked.feature("realized_vol_20_percentile").value == 100


def test_no_guessed_annualization_and_no_regime_or_specialist_feed_claim():
    report = compute(ramp(80))
    value = report.feature("realized_vol_20_percentile")
    assert value.state == "UNAVAILABLE" and value.value is None
    assert value.reason == "ANNUALIZATION_NOT_SPECIFIED"
    assert report.feature("bollinger_width_20_percentile").state == "AVAILABLE"
    assert (
        report.regime_classification_complete
        is report.ready_for_signals
        is report.trading_enabled
        is False
    )


@pytest.mark.parametrize("case", ["gap", "incomplete", "overflow", "stale", "empty"])
def test_existing_quality_refusals_block_every_new_feature(case):
    rows = ramp(60)
    options = {}
    cutoff = None
    if case == "gap":
        rows.pop(10)
    elif case == "incomplete":
        rows[10] = bar(10, 210.0, complete=False)
    elif case == "overflow":
        options["max_bars"] = 59
    elif case == "stale":
        cutoff = rows[-1].observed_at_ms + 600001
    else:
        rows = []
    report = compute_trend_features(
        point(rows, cutoff), source="fixture", symbol="BTC/USD", timeframe_s=300, **options
    )
    assert report.base.blockers
    assert all(value.state == "UNAVAILABLE" and value.value is None for value in report.features)


@pytest.mark.parametrize(
    "case",
    [
        "future_source",
        "future_receipt",
        "nested_future",
        "mixed_asset",
        "extra_model_field",
        "boolean_version",
        "malformed_payload",
        "tied_close",
        "conflicted_id",
    ],
)
def test_strict_existing_identity_and_pit_boundary_is_not_weakened(case):
    rows = ramp(60)
    snapshot = point(rows)
    original = rows[0]
    if case == "future_source":
        forged = original.model_copy(update={"event_time_ms": snapshot.as_of_ms + 1})
    elif case == "future_receipt":
        forged = original.model_copy(update={"observed_at_ms": snapshot.as_of_ms + 1})
    elif case == "mixed_asset":
        forged = original.model_copy(update={"asset": "ETH"})
    elif case == "extra_model_field":
        forged = original.model_copy(update={"untrusted": 1})
    elif case == "boolean_version":
        forged = original.model_copy(update={"schema_version": True})
    elif case == "nested_future":
        forged = original.model_copy(
            update={
                "payload_json": json.dumps(
                    original.payload() | {"audit": {"observed_at_ms": snapshot.as_of_ms + 1}}
                )
            }
        )
    elif case == "malformed_payload":
        forged = MarketEvent.model_construct(**(original.model_dump() | {"payload_json": "[]"}))
    elif case == "tied_close":
        forged = bar(0, 300.0)
    else:
        forged = original.model_copy(update={"observed_at_ms": original.observed_at_ms + 1})
    object.__setattr__(snapshot, "_events", (*rows, forged))
    with pytest.raises(IntegrityError):
        compute_trend_features(snapshot, source="fixture", symbol="BTC/USD", timeframe_s=300)


def test_input_order_duplicate_replay_and_exact_scope_are_deterministic():
    rows = ramp(60)
    expected = compute(rows).model_dump_json()
    shuffled = rows[:]
    random.Random(5).shuffle(shuffled)
    assert compute([*shuffled, *rows[:5]]).model_dump_json() == expected
    unrelated = []
    other = rows[0].payload()
    unrelated.append(
        make_event(
            source="other",
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USDT",
            kind="bar",
            event_time_ms=rows[0].event_time_ms,
            observed_at_ms=rows[0].observed_at_ms,
            payload=other,
        )
    )
    assert compute([*rows, *unrelated]).model_dump_json() == expected


def test_missing_annualization_does_not_claim_verified_numeric_reference_observations():
    report = compute(ramp(60))
    reference = report.percentile_references[1]
    assert reference.candidate_observations == 39
    assert reference.observations == 0
    assert reference.first_close_ms is reference.latest_close_ms is None


def test_warmup_reference_counts_and_exact_first_available_rank():
    short = compute(ramp(39), annualization_periods_per_year=100)
    reference = short.percentile_references[0]
    assert reference.observations == reference.candidate_observations == 19
    assert short.feature("bollinger_width_20_percentile").state == "WARMUP"
    first = compute(ramp(40), annualization_periods_per_year=100)
    assert first.percentile_references[0].observations == 20
    assert first.feature("bollinger_width_20_percentile").state == "AVAILABLE"
    assert first.feature("realized_vol_20_percentile").state == "WARMUP"
    next_report = compute(ramp(41), annualization_periods_per_year=100)
    assert next_report.feature("realized_vol_20_percentile").state == "AVAILABLE"


def test_nonfinite_historical_width_is_refused_without_dropping_prior_observations():
    closes = [1e308] * 13 + [100.0] * 50
    rows = [bar(i, close, high=close, low=close) for i, close in enumerate(closes)]
    report = compute(rows, annualization_periods_per_year=100, percentile_window=60)
    # Latest width is valid zero; the prior history contains overflowing variance.
    assert report.base.feature("bollinger_width_20").value == 0
    value = report.feature("bollinger_width_20_percentile")
    assert value.state == "UNAVAILABLE" and value.value is None
    assert value.reason == "NUMERIC_UNAVAILABLE"
    reference = report.percentile_references[0]
    assert reference.candidate_observations == 43 and reference.observations == 0
    assert all(value.value is None or math.isfinite(value.value) for value in report.features)


def test_undefined_earlier_dx_never_silently_reseeds_adx_after_a_later_trend():
    rows = [bar(i, 100.0) for i in range(28)]
    rows += [bar(i, 100.0 + i - 27) for i in range(28, 70)]
    report = compute(rows)
    assert report.feature("plus_di_14").state == "AVAILABLE"
    assert report.feature("plus_di_14").value > 0
    assert report.feature("adx_14").state == "UNAVAILABLE"
    assert report.feature("adx_14").reason == "DIRECTIONAL_MOVEMENT_UNDEFINED"
    assert report.feature("adx_14").value is None


def test_bounded_reference_has_nontrivial_exact_tie_fraction_excluding_current():
    # One earlier spike remains in only the first two of four prior width windows.
    closes = [100.0] * 45
    closes[22] = 110.0
    rows = [bar(i, close) for i, close in enumerate(closes)]
    report = compute(
        rows, annualization_periods_per_year=100, percentile_window=4, percentile_min_history=4
    )
    # Prior endpoints 40,41 have positive width; 42,43 and current44 have width0.
    assert report.feature("bollinger_width_20_percentile").value == 25
    assert report.percentile_references[0].observations == 4


def test_other_timeframe_does_not_enter_directional_or_percentile_history():
    rows = ramp(60)
    other = make_event(
        source="fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="bar",
        event_time_ms=1800000,
        observed_at_ms=1800005,
        payload=rows[0].payload()
        | {"open_time_ms": 900000, "close_time_ms": 1800000, "timeframe_s": 900},
    )
    assert compute([*rows, other]).model_dump_json() == compute(rows).model_dump_json()


@pytest.mark.parametrize(
    "options",
    [
        {"percentile_window": True},
        {"percentile_window": 0},
        {"percentile_window": 1001},
        {"percentile_min_history": True},
        {"percentile_min_history": 0},
        {"percentile_window": 5, "percentile_min_history": 6},
    ],
)
def test_percentile_policy_is_strict_and_bounded(options):
    with pytest.raises(ValueError):
        compute(ramp(60), **options)
