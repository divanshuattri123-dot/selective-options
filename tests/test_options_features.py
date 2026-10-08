import math
import random

import pytest

from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.models import Instrument
from selective_options.options_features import compute_options_features


def native(
    instrument,
    ticker,
    *,
    ivs=(50.0, 60.0),
    strikes=(100.0, 110.0),
    selected_all=True,
    option_type="call",
):
    metadata = []
    quotes = []
    for expiry_index, iv in enumerate(ivs):
        for strike_index, strike in enumerate(strikes):
            symbol = f"BTC-FIXTURE-{expiry_index}-{strike_index}-{option_type}"
            model = Instrument.model_validate(
                instrument
                | {
                    "instrument_name": symbol,
                    "strike": strike,
                    "expiration_timestamp": 100000 + expiry_index * 100000,
                    "option_type": option_type,
                }
            )
            metadata.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset="BTC",
                    symbol=symbol,
                    kind="instrument",
                    timestamp_kind="receipt",
                    event_time_ms=1000,
                    observed_at_ms=1000,
                    payload=model.model_dump(mode="json"),
                )
            )
            body = ticker | {
                "instrument_name": symbol,
                "timestamp": 1900,
                "underlying_price": 102.0,
                "underlying_index": f"SYN-FIXTURE-{expiry_index}",
                "mark_iv": iv + strike_index * 5,
            }
            quotes.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset="BTC",
                    symbol=symbol,
                    kind="ticker",
                    event_time_ms=1900,
                    observed_at_ms=1905,
                    payload=body,
                )
            )
    symbols = [row.symbol for row in metadata]
    selected = symbols if selected_all else symbols[:1]
    manifest = make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        timestamp_kind="receipt",
        event_time_ms=1100,
        observed_at_ms=1100,
        payload={
            "reason": "UNIVERSE_DISCOVERED",
            "asset": "BTC",
            "available": len(symbols),
            "selected": len(selected),
            "coverage": "FULL" if selected_all else "PARTIAL_OR_ABSENT",
            "instruments": symbols,
            "selected_instruments": selected,
            "metadata_event_ids": [row.event_id for row in metadata],
        },
    )
    return metadata, quotes, manifest


def compute(metadata, quotes, manifest, **options):
    controls = [] if manifest is None else manifest if isinstance(manifest, list) else [manifest]
    return compute_options_features(
        PointInTimeSnapshot(2000, "BTC", "testnet", tuple(metadata + quotes)),
        PointInTimeSnapshot(2000, "SYSTEM", "testnet", tuple(controls)),
        **{
            "option_type": "call",
            "moneyness_method": "native_underlying_log_distance",
            "tie_policy": "exact_distance_refuse",
            "max_quote_age_ms": 200,
            "max_discovery_age_ms": 1000,
            "max_atm_log_moneyness": 0.1,
            "max_underlying_mismatch_fraction": 0.01,
            "flat_threshold_iv_points": 1.0,
            **options,
        },
    )


def replace_quote(row, **body):
    payload = row.payload() | body
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=row.symbol,
        kind="ticker",
        event_time_ms=payload["timestamp"],
        observed_at_ms=row.observed_at_ms,
        payload=payload,
    )


def replace_manifest(row, **body):
    return make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        timestamp_kind="receipt",
        event_time_ms=row.event_time_ms,
        observed_at_ms=row.observed_at_ms,
        payload=row.payload() | body,
    )


@pytest.mark.parametrize(
    "ivs,shape,differences",
    [
        ((50.0, 60.0), "CONTANGO", (10.0,)),
        ((60.0, 50.0), "BACKWARDATION", (-10.0,)),
        ((50.0, 51.0), "FLAT", (1.0,)),
        ((50.0, 60.0, 55.0), "MIXED", (10.0, -5.0)),
    ],
)
def test_discrete_native_atm_marks_and_adjacent_thresholds_have_analytic_references(
    instrument, ticker, ivs, shape, differences
):
    metadata, quotes, manifest = native(instrument, ticker, ivs=ivs)
    report = compute(metadata, quotes, manifest)
    assert report.term_shape == shape and report.adjacent_iv_differences_points == differences
    assert report.state == "DISCRETE_TERM_COMPUTED" and not report.blockers
    for i, expiry in enumerate(report.expiries):
        assert expiry.mark_iv_points == ivs[i]  # Native50 means50%, never0.50.
        assert expiry.strike == 100 and expiry.underlying_price == 102
        assert expiry.log_moneyness_distance == pytest.approx(math.log(102 / 100))
        assert (
            expiry.symbol == metadata[2 * i].symbol
            and expiry.quote_event_id == quotes[2 * i].event_id
        )
        assert expiry.metadata_event_id == metadata[2 * i].event_id
        assert expiry.quote_source_time_ms == 1900 and expiry.quote_observed_at_ms == 1905
        assert (
            expiry.metadata_observed_at_ms == 1000 and expiry.underlying_index == f"SYN-FIXTURE-{i}"
        )
        assert expiry.quote_currency == expiry.settlement_currency == "BTC"
    assert report.full_chain_verified is report.ready_for_signals is report.trading_enabled is False
    assert report.interpolated is report.current_feed_health_verified is False


def test_partial_subscription_never_becomes_full_chain_even_with_all_marks(instrument, ticker):
    report = compute(*native(instrument, ticker, selected_all=False))
    assert report.state == "DISCRETE_TERM_COMPUTED"
    assert (
        report.discovery_declared_coverage == "PARTIAL_OR_ABSENT" and not report.full_chain_verified
    )


def test_put_scope_and_native_reference_do_not_use_delta_or_spot(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker, option_type="put")
    quotes = [
        replace_quote(row, index_price=500.0, greeks=row.payload()["greeks"] | {"delta": -0.99})
        for row in quotes
    ]
    report = compute(metadata, quotes, manifest, option_type="put")
    assert all(expiry.strike == 100 for expiry in report.expiries)
    assert compute(metadata, quotes, manifest).blockers == ("NO_MATCHING_EXPIRIES",)


@pytest.mark.parametrize(
    "case,reason",
    [
        ("missing", "EXPIRY_QUOTE_MISSING"),
        ("stale", "EXPIRY_QUOTE_STALE"),
        ("prediscovery", "QUOTE_PREDATES_DISCOVERY"),
        ("ivzero", "MARK_IV_UNAVAILABLE"),
        ("reference_missing", "UNDERLYING_REFERENCE_MISSING"),
        ("reference_mixed", "UNDERLYING_REFERENCE_MISMATCH"),
        ("underlying_mixed", "UNDERLYING_PRICE_MISMATCH"),
        ("distance", "ATM_DISTANCE_EXCEEDED"),
        ("tie", "ATM_DISTANCE_AMBIGUOUS"),
        ("timestamp_conflict", "EXPIRY_QUOTE_AMBIGUOUS"),
    ],
)
def test_missing_or_ambiguous_nearer_quote_never_falls_back_to_farther_strike(
    instrument, ticker, case, reason
):
    metadata, quotes, manifest = native(
        instrument, ticker, strikes=(100.0, 100.0) if case == "tie" else (100.0, 110.0)
    )
    options = {}
    if case == "missing":
        quotes.pop(0)
    elif case == "stale":
        quotes[0] = replace_quote(quotes[0], timestamp=1700)
    elif case == "prediscovery":
        quotes[0] = replace_quote(quotes[0], timestamp=1050).model_copy(
            update={"observed_at_ms": 1050}
        )
    elif case == "ivzero":
        quotes[0] = replace_quote(quotes[0], mark_iv=0.0)
    elif case == "reference_missing":
        quotes[0] = replace_quote(quotes[0], underlying_index=None)
    elif case == "reference_mixed":
        quotes[0] = replace_quote(quotes[0], underlying_index="index_price")
    elif case == "underlying_mixed":
        quotes[0] = replace_quote(quotes[0], underlying_price=120.0)
    elif case == "distance":
        options["max_atm_log_moneyness"] = 0.001
    elif case == "timestamp_conflict":
        quotes.append(
            replace_quote(quotes[0], mark_iv=80.0).model_copy(update={"observed_at_ms": 1906})
        )
    if case == "prediscovery":
        options["max_quote_age_ms"] = 1000
    report = compute(metadata, quotes, manifest, **options)
    assert report.expiries[0].reason == reason and report.expiries[0].mark_iv_points is None
    assert report.term_shape == "UNAVAILABLE" and report.adjacent_iv_differences_points == ()


def test_freshest_source_quote_wins_and_latest_manifest_binding_beats_unreferenced_metadata(
    instrument, ticker
):
    metadata, quotes, manifest = native(instrument, ticker)
    old = replace_quote(quotes[0], timestamp=1800, mark_iv=80.0)
    extra = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=metadata[0].symbol,
        kind="instrument",
        timestamp_kind="receipt",
        event_time_ms=1800,
        observed_at_ms=1800,
        payload=metadata[0].payload() | {"strike": 101.0},
    )
    report = compute([*metadata, extra], [old, *quotes], manifest)
    assert report.expiries[0].mark_iv_points == 50 and report.expiries[0].strike == 100
    assert report.expiries[0].metadata_event_id == metadata[0].event_id


def test_input_order_exact_replay_and_receipt_provenance_are_deterministic(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker)
    baseline = compute(metadata, quotes, manifest)
    random.Random(2).shuffle(metadata)
    random.Random(3).shuffle(quotes)
    assert (
        compute([*metadata, *metadata], quotes, [manifest, manifest]).model_dump_json()
        == baseline.model_dump_json()
    )
    changed = [row.model_copy(update={"observed_at_ms": 1906}) for row in quotes]
    delayed = compute(metadata, changed, manifest)
    assert (
        delayed.input_sha256 != baseline.input_sha256 and delayed.term_shape == baseline.term_shape
    )


@pytest.mark.parametrize(
    "case,blocker",
    [
        ("missing", "DISCOVERY_MISSING"),
        ("stale", "DISCOVERY_STALE"),
        ("ambiguous", "DISCOVERY_AMBIGUOUS"),
        ("metadata_missing", "DISCOVERY_METADATA_MISSING"),
        ("one_expiry", "TERM_REQUIRES_TWO_EXPIRIES"),
        ("budget", "EXPIRY_BUDGET_EXCEEDED"),
    ],
)
def test_explicit_discovery_quality_and_expiry_budget_are_unavailable(
    instrument, ticker, case, blocker
):
    metadata, quotes, manifest = native(
        instrument, ticker, ivs=(50.0,) if case == "one_expiry" else (50.0, 60.0)
    )
    options = {}
    if case == "missing":
        manifest = None
    elif case == "stale":
        options["max_discovery_age_ms"] = 100
    elif case == "ambiguous":
        manifest = [
            manifest,
            replace_manifest(
                manifest,
                selected=1,
                selected_instruments=manifest.payload()["instruments"][:1],
                coverage="PARTIAL_OR_ABSENT",
            ),
        ]
    elif case == "metadata_missing":
        metadata.pop(0)
    elif case == "budget":
        options["max_expiries"] = 1
    report = compute(metadata, quotes, manifest, **options)
    assert blocker in report.blockers and report.term_shape == "UNAVAILABLE"


@pytest.mark.parametrize(
    "case",
    [
        "future_source",
        "future_receipt",
        "nested_future",
        "mixed_asset",
        "extra_event",
        "bool_schema",
        "missing_event_field",
        "false_id",
        "metadata_false_receipt",
        "manifest_duplicate_symbols",
    ],
)
def test_hostile_forged_models_and_leakage_cannot_bypass_integrity(instrument, ticker, case):
    metadata, quotes, manifest = native(instrument, ticker)
    if case == "future_source":
        quotes[0] = quotes[0].model_copy(update={"event_time_ms": 2001})
    elif case == "future_receipt":
        quotes[0] = quotes[0].model_copy(update={"observed_at_ms": 2001})
    elif case == "nested_future":
        quotes[0] = replace_quote(quotes[0], audit={"timestamp": 2001})
    elif case == "mixed_asset":
        quotes[0] = quotes[0].model_copy(update={"asset": "ETH"})
    elif case == "extra_event":
        quotes[0] = quotes[0].model_copy(update={"extra_untrusted": 1})
    elif case == "bool_schema":
        quotes[0] = quotes[0].model_copy(update={"schema_version": True})
    elif case == "missing_event_field":
        fields = quotes[0].model_dump()
        fields.pop("event_time_ms")
        quotes[0] = MarketEvent.model_construct(**fields)
    elif case == "false_id":
        quotes[0] = quotes[0].model_copy(update={"event_id": "0" * 64})
    elif case == "metadata_false_receipt":
        metadata[0] = metadata[0].model_copy(update={"observed_at_ms": 1001})
    else:
        manifest = replace_manifest(manifest, instruments=[metadata[0].symbol] * 4)
    snapshot = PointInTimeSnapshot(2000, "BTC", "testnet", tuple(metadata))
    object.__setattr__(snapshot, "_events", tuple(metadata + quotes))
    discovery = PointInTimeSnapshot(2000, "SYSTEM", "testnet", (manifest,))
    with pytest.raises(IntegrityError):
        compute_options_features(
            snapshot,
            discovery,
            option_type="call",
            moneyness_method="native_underlying_log_distance",
            tie_policy="exact_distance_refuse",
            max_quote_age_ms=200,
            max_discovery_age_ms=1000,
            max_atm_log_moneyness=0.1,
            max_underlying_mismatch_fraction=0.01,
            flat_threshold_iv_points=1.0,
        )


def test_same_identity_with_different_receipt_is_an_integrity_conflict(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker)
    with pytest.raises(IntegrityError, match="CONFLICTING_IDENTITY"):
        compute(
            metadata, [*quotes, quotes[0].model_copy(update={"observed_at_ms": 1906})], manifest
        )


def test_geometric_midpoint_tie_is_exact_even_when_log_subtraction_rounding_differs(
    instrument, ticker
):
    metadata, quotes, manifest = native(instrument, ticker, strikes=(100.0, 400.0))
    quotes = [replace_quote(row, underlying_price=200.0) for row in quotes]
    report = compute(metadata, quotes, manifest, max_atm_log_moneyness=1.0)
    assert all(expiry.reason == "ATM_DISTANCE_AMBIGUOUS" for expiry in report.expiries)


def test_both_quote_clock_ages_and_boundary_are_explicit(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker)
    quotes = [
        replace_quote(row, timestamp=1800).model_copy(update={"observed_at_ms": 1800})
        for row in quotes
    ]
    report = compute(metadata, quotes, manifest)
    assert report.state == "DISCRETE_TERM_COMPUTED"
    old_receipt = [row.model_copy(update={"observed_at_ms": 1799}) for row in quotes]
    assert compute(metadata, old_receipt, manifest).expiries[0].reason == "EXPIRY_QUOTE_STALE"


def test_expired_bound_universe_is_unavailable_and_future_expiration_is_allowed(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker)
    assert compute(metadata, quotes, manifest).expiries[0].expiration_timestamp > 2000
    expired = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=metadata[0].symbol,
        kind="instrument",
        timestamp_kind="receipt",
        event_time_ms=1000,
        observed_at_ms=1000,
        payload=metadata[0].payload() | {"expiration_timestamp": 1900},
    )
    metadata[0] = expired
    manifest = replace_manifest(manifest, metadata_event_ids=[row.event_id for row in metadata])
    assert "DISCOVERY_EXPIRED_REDISCOVERY_REQUIRED" in compute(metadata, quotes, manifest).blockers


def test_mixed_native_contract_units_are_refused_without_conversion(instrument, ticker):
    metadata, quotes, manifest = native(instrument, ticker)
    changed = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=metadata[0].symbol,
        kind="instrument",
        timestamp_kind="receipt",
        event_time_ms=1000,
        observed_at_ms=1000,
        payload=metadata[0].payload() | {"settlement_currency": "USDC"},
    )
    metadata[0] = changed
    manifest = replace_manifest(manifest, metadata_event_ids=[row.event_id for row in metadata])
    assert compute(metadata, quotes, manifest).expiries[0].reason == "OPTION_CONTRACT_UNIT_MISMATCH"


@pytest.mark.parametrize(
    "policy",
    [
        {"option_type": "both"},
        {"moneyness_method": "spot"},
        {"tie_policy": "first"},
        {"max_quote_age_ms": True},
        {"max_rows": 200001},
        {"max_expiries": 0},
        {"max_atm_log_moneyness": float("nan")},
        {"max_underlying_mismatch_fraction": True},
        {"flat_threshold_iv_points": -1},
        {"flat_threshold_iv_points": 10**1000},
    ],
)
def test_policy_requires_explicit_strict_finite_bounded_parameters(instrument, ticker, policy):
    with pytest.raises(ValueError):
        compute(*native(instrument, ticker), **policy)
