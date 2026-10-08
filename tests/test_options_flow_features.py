"""Synthetic native-field fixtures; no market-data or strategy-readiness claims."""

import hashlib
import json
from fractions import Fraction

import pytest
from pydantic import ValidationError

from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot, make_event
from selective_options.models import Instrument
from selective_options.options_flow_features import compute_put_call_ratios

AS_OF = 2_000


def fixture(instrument, ticker):
    metadata, quotes = [], []
    for expiry_idx, amounts in enumerate(((0.1, 0.2, 0.3, 0.6), (0.4, 0.5, 0.6, 0.7))):
        volumes = ((2, 3, 1, 4), (1, 3, 5, 7))[expiry_idx]
        for idx, (oi, volume) in enumerate(zip(amounts, volumes, strict=True)):
            side = "call" if idx < 2 else "put"
            symbol = f"BTC-FLOW-{expiry_idx}-{idx}-{side.upper()}"
            native = Instrument.model_validate(
                instrument
                | {
                    "instrument_name": symbol,
                    "option_type": side,
                    "expiration_timestamp": 10_000 + expiry_idx * 10_000,
                    "strike": 90_000 + idx * 10_000,
                }
            )
            metadata.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset="BTC",
                    symbol=symbol,
                    kind="instrument",
                    event_time_ms=1_000,
                    observed_at_ms=1_000,
                    timestamp_kind="receipt",
                    payload=native.model_dump(mode="json"),
                )
            )
            quotes.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset="BTC",
                    symbol=symbol,
                    kind="ticker",
                    event_time_ms=1_900,
                    observed_at_ms=1_905,
                    payload=ticker
                    | {
                        "instrument_name": symbol,
                        "timestamp": 1_900,
                        "open_interest": oi,
                        "stats": {"volume": volume, "volume_usd": 99_999_999},
                    },
                )
            )
    manifest = discovery_event(metadata)
    return snapshots(metadata, quotes, manifest)


def discovery_event(metadata, *, observed=1_100, **changes):
    names = [row.symbol for row in metadata]
    return make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=observed,
        observed_at_ms=observed,
        timestamp_kind="receipt",
        payload={
            "reason": "UNIVERSE_DISCOVERED",
            "asset": "BTC",
            "available": len(names),
            "selected": len(names),
            "coverage": "FULL",
            "instruments": names,
            "selected_instruments": names,
            "metadata_event_ids": [row.event_id for row in metadata],
        }
        | changes,
    )


def snapshots(metadata, quotes, manifest):
    return (
        metadata,
        quotes,
        manifest,
        PointInTimeSnapshot(AS_OF, "BTC", "testnet", tuple(metadata + quotes)),
        PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", (manifest,)),
    )


def compute(snapshot, discovery, **changes):
    policy = {
        "amount_unit_policy": "deribit_option_base_currency",
        "volume_field": "stats.volume",
        "max_quote_age_ms": 200,
        "max_discovery_age_ms": 1_000,
        "max_clock_skew_ms": 200,
    }
    return compute_put_call_ratios(snapshot, discovery, **(policy | changes))


def replace(row, *, observed=None, event_time=None, **changes):
    body = row.payload() | changes
    if row.kind == "instrument":
        body = Instrument.model_validate(body).model_dump(mode="json")
    if event_time is None:
        event_time = body["timestamp"] if row.kind == "ticker" else row.event_time_ms
    return make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind=row.kind,
        event_time_ms=event_time,
        observed_at_ms=row.observed_at_ms if observed is None else observed,
        timestamp_kind=row.timestamp_kind,
        payload=body,
    )


def replace_snapshot(snapshot, old, new):
    return PointInTimeSnapshot(
        AS_OF,
        "BTC",
        "testnet",
        tuple(new if row.event_id == old.event_id else row for row in snapshot.rows()),
    )


def fraction(amount):
    assert amount is not None
    return Fraction(int(amount.numerator), int(amount.denominator))


def test_exact_amount_ratios_and_aggregate_sums_not_mean_of_ratios(instrument, ticker):
    _, _, manifest, snapshot, discovery = fixture(instrument, ticker)
    report = compute(snapshot, discovery)
    assert report.state == "PUT_CALL_RATIOS_COMPUTED" and report.blockers == ()
    assert report.manifest_event_id == manifest.event_id
    assert report.discovery_coverage == "FULL" and report.discovered_members == 8
    assert fraction(report.expiries[0].open_interest.call_total) == Fraction(3, 10)
    assert fraction(report.expiries[0].open_interest.put_total) == Fraction(9, 10)
    assert fraction(report.expiries[0].open_interest.ratio_exact) == 3
    assert fraction(report.expiries[1].open_interest.ratio_exact) == Fraction(13, 9)
    assert fraction(report.aggregate_open_interest.ratio_exact) == Fraction(11, 6)
    assert report.aggregate_open_interest.value == pytest.approx(11 / 6)
    assert fraction(report.aggregate_volume.ratio_exact) == Fraction(17, 9)
    assert [row.volume.value for row in report.expiries] == [1.0, 3.0]
    assert report.volume_window == "native_rolling_24h" and not report.contract_count_ratio
    member = report.expiries[0].members[0]
    assert member.quote_source_time_ms == 1_900 and member.quote_observed_at_ms == 1_905
    assert member.metadata_observed_at_ms == 1_000
    assert fraction(member.open_interest) == Fraction(1, 10)
    assert fraction(member.volume_24h) == 2
    assert not report.full_chain_verified and not report.current_feed_health_verified
    assert not report.simultaneous_quote_snapshot_verified and not report.executable_quotes_verified
    assert not report.interpolated and not report.ready_for_signals and not report.trading_enabled


def test_exact_provenance_order_invariant_and_identical_duplicates(instrument, ticker):
    _, _, _, snapshot, discovery = fixture(instrument, ticker)
    report = compute(snapshot, discovery)
    reordered = PointInTimeSnapshot(
        AS_OF, "BTC", "testnet", (*reversed(snapshot.rows()), snapshot.rows()[0])
    )
    assert compute(reordered, discovery) == report
    all_rows = {row.event_id: row for row in snapshot.rows() + discovery.rows()}
    expected = hashlib.sha256(
        json.dumps(
            [all_rows[key].model_dump(mode="json") for key in sorted(all_rows)],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    assert report.input_sha256 == expected
    assert report.input_event_ids == tuple(sorted(all_rows))
    with pytest.raises(ValidationError):
        report.state = "UNAVAILABLE"
    with pytest.raises(ValidationError):
        report.expiries[0].members[0].symbol = "FORGED"


@pytest.mark.parametrize(
    "stats,reason",
    [
        (None, "VOLUME_STATS_NULL"),
        ({}, "VOLUME_FIELD_MISSING"),
        ({"volume": None}, "VOLUME_FIELD_NULL"),
        ({"volume_usd": 12_000}, "VOLUME_FIELD_MISSING"),
        ({"volume_notional": 99}, "VOLUME_FIELD_MISSING"),
    ],
)
def test_missing_volume_withholds_only_volume_and_never_uses_usd(instrument, ticker, stats, reason):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = replace_snapshot(snapshot, quotes[0], replace(quotes[0], stats=stats))
    report = compute(changed, discovery)
    assert report.state == "PARTIAL"
    assert report.aggregate_open_interest.state == "AVAILABLE"
    assert report.aggregate_open_interest.value == pytest.approx(11 / 6)
    assert report.aggregate_volume.state == "UNAVAILABLE"
    assert report.aggregate_volume.reason == "INCOMPLETE_MEMBER_VOLUME"
    assert report.aggregate_volume.call_total is None
    assert report.expiries[0].members[0].volume_reason == reason
    assert report.expiries[1].volume.state == "AVAILABLE"
    assert "INCOMPLETE_EXPIRY_VOLUME_RATIOS" in report.blockers


def test_absent_stats_is_distinct_and_does_not_consume_chain_summary(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    body = quotes[0].payload()
    del body["stats"]
    modified = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=quotes[0].symbol,
        kind="ticker",
        event_time_ms=1_900,
        observed_at_ms=1_905,
        payload=body,
    )
    extra = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=quotes[0].symbol,
        kind="chain_summary",
        event_time_ms=1_999,
        observed_at_ms=1_999,
        payload={"volume": 123, "open_interest": 345},
    )
    changed = replace_snapshot(snapshot, quotes[0], modified)
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*changed.rows(), extra))
    report = compute(changed, discovery)
    assert report.expiries[0].members[0].volume_reason == "VOLUME_STATS_MISSING"
    assert report.aggregate_open_interest.state == "AVAILABLE"
    assert report.aggregate_volume.state == "UNAVAILABLE"


@pytest.mark.parametrize("value", [True, False, "3", -1, [], {}])
def test_invalid_volume_numbers_fail_even_in_old_history(instrument, ticker, value):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    old = replace(quotes[0], timestamp=1_700, observed=1_705, stats={"volume": value})
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*snapshot.rows(), old))
    with pytest.raises(IntegrityError, match="NATIVE_AMOUNT_INVALID"):
        compute(changed, discovery)


@pytest.mark.parametrize("stats", [[], "3", True, 3])
def test_invalid_stats_structure_fails(instrument, ticker, stats):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = replace_snapshot(snapshot, quotes[0], replace(quotes[0], stats=stats))
    with pytest.raises(IntegrityError, match="VOLUME_STATS_INVALID"):
        compute(changed, discovery)


def test_missing_quote_blocks_complete_aggregate_without_omitting_expiry(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = PointInTimeSnapshot(
        AS_OF, "BTC", "testnet", tuple(row for row in snapshot.rows() if row != quotes[0])
    )
    report = compute(changed, discovery)
    assert report.state == "PARTIAL" and len(report.expiries) == 2
    assert len(report.expiries[0].members) == 4 and report.expiries[0].quote_coverage == 3
    assert "EXPIRY_QUOTE_MISSING" in report.expiries[0].blockers
    assert report.expiries[1].open_interest.state == "AVAILABLE"
    assert report.aggregate_open_interest.state == report.aggregate_volume.state == "UNAVAILABLE"
    assert report.aggregate_open_interest.call_total is None


@pytest.mark.parametrize(
    "changes,policy,reason",
    [
        ({"timestamp": 1_799}, {}, "EXPIRY_QUOTE_STALE"),
        ({"observed": 1_799}, {}, "EXPIRY_QUOTE_STALE"),
        ({"observed": 1_085}, {"max_quote_age_ms": 2_000}, "QUOTE_PREDATES_DISCOVERY"),
        ({"observed": 1_805}, {"max_clock_skew_ms": 50}, "QUOTE_CLOCK_SKEW_EXCEEDED"),
    ],
)
def test_both_quote_clocks_discovery_and_skew_gate(instrument, ticker, changes, policy, reason):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = replace_snapshot(snapshot, quotes[0], replace(quotes[0], **changes))
    report = compute(changed, discovery, **policy)
    assert report.expiries[0].members[0].quote_reason == reason
    assert report.aggregate_open_interest.state == "UNAVAILABLE"
    assert report.expiries[1].open_interest.state == "AVAILABLE"


def test_latest_source_not_latest_receipt_and_no_invalid_latest_fallback(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    older = replace(quotes[0], timestamp=1_850, observed=1_990, open_interest=500)
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*snapshot.rows(), older))
    report = compute(changed, discovery)
    assert report.aggregate_open_interest.value == pytest.approx(11 / 6)
    bad_latest = replace(quotes[0], timestamp=1_995, observed=1_500)
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*snapshot.rows(), bad_latest))
    report = compute(changed, discovery)
    assert report.aggregate_open_interest.state == "UNAVAILABLE"
    assert report.expiries[0].members[0].quote_event_id == bad_latest.event_id


def test_latest_source_tie_refuses_even_if_receipts_ordered(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    tied = replace(quotes[0], observed=1_990, open_interest=123)
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*snapshot.rows(), tied))
    with pytest.raises(IntegrityError, match="QUOTE_SOURCE_TIME_AMBIGUOUS"):
        compute(changed, discovery)


def test_zero_call_expiry_can_have_defined_complete_aggregate(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    for quote in quotes[:2]:
        snapshot = replace_snapshot(snapshot, quote, replace(quote, open_interest=0))
    report = compute(snapshot, discovery)
    assert report.state == "PARTIAL"
    assert report.expiries[0].open_interest.reason == "CALL_DENOMINATOR_ZERO"
    assert report.expiries[0].open_interest.input_complete
    assert fraction(report.expiries[0].open_interest.call_total) == 0
    assert report.expiries[0].open_interest.value is None
    assert fraction(report.aggregate_open_interest.ratio_exact) == Fraction(22, 9)
    assert report.aggregate_open_interest.state == "AVAILABLE"
    assert report.aggregate_volume.state == "AVAILABLE"


def test_zero_aggregate_denominator_unavailable_and_zero_numerator_is_zero(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    for quote in quotes:
        if quote.symbol.endswith("CALL"):
            snapshot = replace_snapshot(snapshot, quote, replace(quote, stats={"volume": 0}))
        else:
            snapshot = replace_snapshot(snapshot, quote, replace(quote, open_interest=0))
    report = compute(snapshot, discovery)
    assert report.aggregate_volume.reason == "CALL_DENOMINATOR_ZERO"
    assert report.aggregate_volume.value is None and report.aggregate_volume.input_complete
    assert report.aggregate_open_interest.value == 0
    assert fraction(report.aggregate_open_interest.ratio_exact) == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("quote_currency", "USDC"),
        ("settlement_currency", "USDC"),
        ("instrument_type", "linear"),
        ("price_index", "btc_usdc"),
        ("price_index", ""),
    ],
)
def test_incompatible_metadata_units_or_reference_withholds(instrument, ticker, field, value):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata[0] = replace(metadata[0], **{field: value})
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert "OPTION_UNIT_OR_REFERENCE_MISMATCH" in report.expiries[0].blockers
    assert report.aggregate_open_interest.reason == "OPTION_UNIT_OR_REFERENCE_MISMATCH"
    assert report.expiries[1].open_interest.state == "AVAILABLE"


def test_each_expiry_valid_but_cross_expiry_contract_family_mismatch(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata[4:] = [replace(row, quote_currency="USDC") for row in metadata[4:]]
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert all(row.open_interest.state == "AVAILABLE" for row in report.expiries)
    assert report.aggregate_open_interest.reason == "OPTION_UNIT_OR_REFERENCE_MISMATCH"
    assert report.state == "PARTIAL"


def test_contract_size_never_multiplies_native_base_amount(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata[0] = replace(metadata[0], contract_size=1_000)
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert report.expiries[0].members[0].contract_size == 1_000
    assert report.aggregate_open_interest.value == pytest.approx(11 / 6)
    assert report.aggregate_volume.value == pytest.approx(17 / 9)


def test_discovered_one_side_missing_unavailable(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata = [row for row in metadata if row.symbol.endswith("CALL")]
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert report.state == "UNAVAILABLE"
    assert report.aggregate_open_interest.reason == "DISCOVERED_OPTION_SIDE_MISSING"


def test_partial_discovery_never_ratios_selected_subset(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    manifest = discovery_event(
        metadata,
        selected=7,
        selected_instruments=[row.symbol for row in metadata[:-1]],
        coverage="PARTIAL_OR_ABSENT",
    )
    *_, snapshot, discovery = snapshots(metadata, quotes, manifest)
    report = compute(snapshot, discovery)
    assert report.state == "UNAVAILABLE" and report.expiries == ()
    assert "DISCOVERY_COVERAGE_NOT_FULL" in report.blockers
    assert report.aggregate_open_interest.reason == "DISCOVERY_UNAVAILABLE"


def test_discovery_missing_stale_and_missing_bound_metadata(instrument, ticker):
    metadata, _, _, snapshot, discovery = fixture(instrument, ticker)
    empty = PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", ())
    assert "DISCOVERY_MISSING" in compute(snapshot, empty).blockers
    assert "DISCOVERY_STALE" in compute(snapshot, discovery, max_discovery_age_ms=899).blockers
    changed = PointInTimeSnapshot(
        AS_OF, "BTC", "testnet", tuple(row for row in snapshot.rows() if row != metadata[0])
    )
    assert "DISCOVERY_METADATA_MISSING" in compute(changed, discovery).blockers


def test_latest_discovery_tie_and_bad_counts(instrument, ticker):
    metadata, _, manifest, snapshot, discovery = fixture(instrument, ticker)
    second = discovery_event(list(reversed(metadata)))
    changed = PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", (manifest, second))
    assert "DISCOVERY_AMBIGUOUS" in compute(snapshot, changed).blockers
    bad = discovery_event(metadata, available=9)
    changed = PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", (bad,))
    with pytest.raises(IntegrityError, match="DISCOVERY_COUNTS_OR_CANONICAL"):
        compute(snapshot, changed)
    old = discovery_event(
        metadata, observed=1_050, coverage="PARTIAL_OR_ABSENT", selected=0, selected_instruments=[]
    )
    changed = PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", (old, manifest))
    assert compute(snapshot, changed).state == "PUT_CALL_RATIOS_COMPUTED"


@pytest.mark.parametrize(
    "changes",
    [
        {"is_active": False},
        {"base_currency": "ETH"},
        {"expiration_timestamp": AS_OF},
    ],
)
def test_discovery_active_asset_and_expiry_bindings(instrument, ticker, changes):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata[0] = replace(metadata[0], **changes)
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    if "expiration_timestamp" in changes:
        assert "DISCOVERY_EXPIRED_REDISCOVERY_REQUIRED" in compute(snapshot, discovery).blockers
    else:
        with pytest.raises(IntegrityError, match="METADATA_DISCOVERY_SCOPE"):
            compute(snapshot, discovery)


def test_metadata_cannot_postdate_discovery(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    metadata[0] = replace(metadata[0], observed=1_200, event_time=1_200)
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    with pytest.raises(IntegrityError, match="METADATA_DISCOVERY_SCOPE"):
        compute(snapshot, discovery)


@pytest.mark.parametrize(
    "policy,value",
    [
        ("max_quote_age_ms", True),
        ("max_discovery_age_ms", 0),
        ("max_clock_skew_ms", -1),
        ("max_rows", 200_001),
        ("max_expiries", 0),
        ("amount_unit_policy", "contracts"),
        ("volume_field", "stats.volume_usd"),
        ("max_quote_age_ms", 1.0),
    ],
)
def test_explicit_policies_are_bounded_and_strict(instrument, ticker, policy, value):
    *_, snapshot, discovery = fixture(instrument, ticker)
    with pytest.raises(ValueError):
        compute(snapshot, discovery, **{policy: value})


def test_history_budget_refuses_and_expiry_budget_withholds(instrument, ticker):
    *_, snapshot, discovery = fixture(instrument, ticker)
    with pytest.raises(IntegrityError, match="INPUT_BUDGET"):
        compute(snapshot, discovery, max_rows=16)
    report = compute(snapshot, discovery, max_expiries=1)
    assert report.state == "UNAVAILABLE" and report.expiries == ()
    assert "EXPIRY_BUDGET_EXCEEDED" in report.blockers


@pytest.mark.parametrize("clock", ["event_time_ms", "observed_at_ms"])
def test_forged_future_snapshot_rows_fail_both_clocks(instrument, ticker, clock):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    forged = quotes[0].model_copy(update={clock: AS_OF + 1})
    changed = object.__new__(PointInTimeSnapshot)
    for key, value in {
        "as_of_ms": AS_OF,
        "asset": "BTC",
        "environment": "testnet",
        "_events": tuple(forged if row == quotes[0] else row for row in snapshot.rows()),
    }.items():
        object.__setattr__(changed, key, value)
    with pytest.raises(IntegrityError, match="LOOKAHEAD_EVENT"):
        compute(changed, discovery)


def test_nested_future_clock_invalid_envelope_and_conflicting_receipt(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    nested = replace(quotes[0], stats={"volume": 2, "timestamp": AS_OF + 1})
    with pytest.raises(IntegrityError, match="NESTED_CLOCK_INVALID_OR_FUTURE"):
        compute(replace_snapshot(snapshot, quotes[0], nested), discovery)
    forged = quotes[0].model_copy(update={"event_id": "f" * 64})
    with pytest.raises(IntegrityError, match="NATIVE_ENVELOPE_NONCANONICAL"):
        compute(replace_snapshot(snapshot, quotes[0], forged), discovery)
    conflicting = quotes[0].model_copy(update={"observed_at_ms": 1_990})
    changed = PointInTimeSnapshot(AS_OF, "BTC", "testnet", (*snapshot.rows(), conflicting))
    with pytest.raises(IntegrityError, match="CONFLICTING_IDENTITY"):
        compute(changed, discovery)


def test_snapshot_scope_and_cutoff_must_match(instrument, ticker):
    *_, snapshot, discovery = fixture(instrument, ticker)
    changed = PointInTimeSnapshot(AS_OF + 1, "SYSTEM", "testnet", discovery.rows())
    with pytest.raises(IntegrityError, match="SNAPSHOT_SCOPE"):
        compute(snapshot, changed)


def test_unrepresentable_float_ratio_keeps_exact_arithmetic_and_no_zero_placeholder(
    instrument, ticker
):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    for idx, quote in enumerate(quotes):
        quotes[idx] = replace(
            quote, open_interest=1e-300 if quote.symbol.endswith("CALL") else 1e300
        )
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert report.aggregate_open_interest.reason == "RATIO_FLOAT_UNREPRESENTABLE"
    assert report.aggregate_open_interest.value is None
    assert fraction(report.aggregate_open_interest.ratio_exact) == Fraction(10) ** 600
    assert report.aggregate_volume.state == "AVAILABLE"


def test_tiny_nonzero_ratio_is_unavailable_instead_of_silently_underflowing(instrument, ticker):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    for idx, quote in enumerate(quotes):
        quotes[idx] = replace(
            quote, open_interest=1e300 if quote.symbol.endswith("CALL") else 1e-300
        )
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata))
    report = compute(snapshot, discovery)
    assert report.aggregate_open_interest.reason == "RATIO_FLOAT_UNREPRESENTABLE"
    assert report.aggregate_open_interest.value is None
    assert fraction(report.aggregate_open_interest.ratio_exact) == Fraction(1, 10**600)


@pytest.mark.parametrize("value", [True, "3", -1, None])
def test_required_native_oi_field_cannot_be_coerced_or_replaced(instrument, ticker, value):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = replace_snapshot(snapshot, quotes[0], replace(quotes[0], open_interest=value))
    with pytest.raises(IntegrityError, match="INVALID_NATIVE_PAYLOAD"):
        compute(changed, discovery)


@pytest.mark.parametrize("changes", [{"available": True}, {"selected": "8"}, {"unknown": 1}])
def test_discovery_requires_strict_canonical_native_counts(instrument, ticker, changes):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    *_, snapshot, discovery = snapshots(metadata, quotes, discovery_event(metadata, **changes))
    with pytest.raises(IntegrityError, match="DISCOVERY_INVALID|DISCOVERY_COUNTS_OR_CANONICAL"):
        compute(snapshot, discovery)


@pytest.mark.parametrize("field", ["instruments", "selected_instruments"])
@pytest.mark.parametrize("duplicate_policy", ["append", "replace"])
def test_discovery_repeated_members_refuse_explicitly_before_zipping(
    instrument, ticker, field, duplicate_policy
):
    metadata, quotes, _, _, _ = fixture(instrument, ticker)
    names = [row.symbol for row in metadata]
    repeated = [*names, names[0]] if duplicate_policy == "append" else [*names[:-1], names[0]]
    *_, snapshot, discovery = snapshots(
        metadata, quotes, discovery_event(metadata, **{field: repeated})
    )
    with pytest.raises(IntegrityError, match="^OPTIONS_FLOW_DISCOVERY_COUNTS_OR_CANONICAL$"):
        compute(snapshot, discovery)


def test_native_source_payload_identity_cannot_be_noncanonical_json(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    modified = quotes[0].model_copy(
        update={"payload_json": json.dumps(quotes[0].payload(), indent=2)}
    )
    with pytest.raises(IntegrityError, match="NATIVE_ENVELOPE_NONCANONICAL"):
        compute(replace_snapshot(snapshot, quotes[0], modified), discovery)


def test_quote_clock_policy_accepts_source_ahead_only_after_both_visible(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    quote = replace(quotes[0], timestamp=1_995, observed=1_905)
    changed = replace_snapshot(snapshot, quotes[0], quote)
    report = compute(changed, discovery, max_clock_skew_ms=100)
    assert report.aggregate_open_interest.state == "AVAILABLE"
    assert report.expiries[0].members[0].quote_source_time_ms == 1_995
    assert report.expiries[0].members[0].quote_observed_at_ms == 1_905
    assert not report.simultaneous_quote_snapshot_verified
    report = compute(changed, discovery, max_clock_skew_ms=89)
    assert report.aggregate_open_interest.state == "UNAVAILABLE"
