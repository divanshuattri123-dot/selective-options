"""Causal selected-delta risk-reversal tests; fixtures are synthetic."""

import hashlib
import json

import pytest

from selective_options.cli import main
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, PointInTimeSnapshot, make_event
from selective_options.models import Instrument
from selective_options.option_skew_research import research_option_skew
from selective_options.options_skew import compute_risk_reversal
from selective_options.store import DuckStore

AS_OF = 2_000


def fixture(instrument, ticker, *, delta_sets=None, mark_ivs=None, selected_all=True):
    delta_sets = delta_sets or ((0.24, 0.32, -0.26, -0.34),) * 2
    mark_ivs = mark_ivs or ((60.0, 61.0, 65.0, 66.0), (61.0, 62.0, 70.0, 71.0))
    metadata = []
    quotes = []
    for expiry_idx in range(2):
        expiry = 10_000 + expiry_idx * 10_000
        for member_idx, (delta, iv) in enumerate(
            zip(delta_sets[expiry_idx], mark_ivs[expiry_idx], strict=True)
        ):
            option_type = "call" if member_idx < 2 else "put"
            symbol = f"BTC-SKEW-{expiry_idx}-{member_idx}-{option_type.upper()}"
            native = Instrument.model_validate(
                instrument
                | {
                    "instrument_name": symbol,
                    "option_type": option_type,
                    "expiration_timestamp": expiry,
                    "strike": 90_000 + member_idx * 10_000,
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
                    event_time_ms=1_000,
                    observed_at_ms=1_000,
                    payload=native.model_dump(mode="json"),
                )
            )
            body = ticker | {
                "instrument_name": symbol,
                "timestamp": 1_900,
                "mark_iv": iv,
                "underlying_price": 100_000.0,
                "underlying_index": "BTC-INDEX",
                "greeks": ticker["greeks"] | {"delta": delta},
            }
            quotes.append(
                make_event(
                    source="deribit",
                    environment="testnet",
                    asset="BTC",
                    symbol=symbol,
                    kind="ticker",
                    event_time_ms=1_900,
                    observed_at_ms=1_905,
                    payload=body,
                )
            )
    names = [row.symbol for row in metadata]
    selected = names if selected_all else names[:-1]
    manifest = make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        timestamp_kind="receipt",
        event_time_ms=1_100,
        observed_at_ms=1_100,
        payload={
            "reason": "UNIVERSE_DISCOVERED",
            "asset": "BTC",
            "available": len(names),
            "selected": len(selected),
            "coverage": "FULL" if selected_all else "PARTIAL_OR_ABSENT",
            "instruments": names,
            "selected_instruments": selected,
            "metadata_event_ids": [row.event_id for row in metadata],
        },
    )
    snapshot = PointInTimeSnapshot(AS_OF, "BTC", "testnet", tuple(metadata + quotes))
    discovery = PointInTimeSnapshot(AS_OF, "SYSTEM", "testnet", (manifest,))
    return metadata, quotes, manifest, snapshot, discovery


def compute(snapshot, discovery, **policy):
    arguments = {
        "max_quote_age_ms": 200,
        "max_discovery_age_ms": 1_000,
        "max_clock_skew_ms": 200,
    }
    arguments.update(policy)
    return compute_risk_reversal(
        snapshot,
        discovery,
        **arguments,
    )


def replace_quote(row: MarketEvent, *, event_time_ms=None, observed_at_ms=None, **changes):
    body = row.payload() | changes
    return make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind=row.kind,
        event_time_ms=body["timestamp"] if event_time_ms is None else event_time_ms,
        observed_at_ms=row.observed_at_ms if observed_at_ms is None else observed_at_ms,
        payload=body,
    )


def with_quote(snapshot, old, new, as_of=AS_OF):
    return PointInTimeSnapshot(
        as_of,
        "BTC",
        "testnet",
        tuple(new if row.event_id == old.event_id else row for row in snapshot.rows()),
    )


def test_exact_native_delta_pair_and_mark_iv_difference_per_expiry(instrument, ticker):
    metadata, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    report = compute(snapshot, discovery)
    assert report.state == "DISCRETE_RISK_REVERSAL_COMPUTED"
    assert report.blockers == ()
    assert [row.risk_reversal_points for row in report.expiries] == [-5.0, -9.0]
    first = report.expiries[0]
    assert first.call and first.put
    assert first.call.symbol.endswith("0-0-CALL") and first.put.symbol.endswith("0-2-PUT")
    assert first.call.delta == 0.24 and first.put.delta == -0.26
    assert first.call.delta_error == first.put.delta_error == pytest.approx(0.01)
    assert first.call.mark_iv_points == 60 and first.put.mark_iv_points == 65
    assert first.call.quote_source_time_ms == 1_900
    assert first.call.quote_observed_at_ms == 1_905
    assert first.call.metadata_event_id == metadata[0].event_id
    assert first.quote_coverage == 4
    assert report.discovered_members == 8 and report.discovery_coverage == "FULL"
    assert not report.full_chain_verified
    assert not report.current_feed_health_verified
    assert not report.executable_quotes_verified
    assert not report.interpolated and not report.ready_for_signals and not report.trading_enabled
    assert len(report.input_sha256) == 64 and report.input_event_ids


def test_exact_delta_tie_refuses_instead_of_symbol_tiebreak(instrument, ticker):
    _, _, _, snapshot, discovery = fixture(
        instrument,
        ticker,
        delta_sets=((0.24, 0.26, -0.24, -0.26),) * 2,
    )
    with pytest.raises(IntegrityError, match="SELECTION_AMBIGUOUS"):
        compute(snapshot, discovery)


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"timestamp": 1_600}, "EXPIRY_QUOTE_STALE"),
        ({"mark_iv": 0.0}, "MARK_IV_UNAVAILABLE"),
        ({"underlying_index": "OTHER-INDEX"}, "UNDERLYING_REFERENCE_MISMATCH"),
        ({"underlying_price": 110_000.0}, "UNDERLYING_PRICE_MISMATCH"),
        (
            {"greeks": {"delta": -0.24, "gamma": 0.1, "vega": 1, "theta": -1, "rho": 0}},
            "CALL_DELTA_SIGN_INVALID",
        ),
    ],
)
def test_invalid_pair_members_are_explicitly_unavailable(instrument, ticker, changes, expected):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    altered = replace_quote(quotes[0], **changes)
    changed_snapshot = with_quote(snapshot, quotes[0], altered)
    report = compute(changed_snapshot, discovery)
    assert report.state == "PARTIAL"
    expiry = report.expiries[0]
    assert expiry.state == "UNAVAILABLE" and expiry.reason == expected
    assert expiry.risk_reversal_points is None


def test_source_clock_skew_is_bounded_and_both_clocks_remain_auditable(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    altered = replace_quote(quotes[0], timestamp=1_910, observed_at_ms=1_905)
    changed = with_quote(snapshot, quotes[0], altered)
    report = compute(changed, discovery, max_clock_skew_ms=5)
    assert report.expiries[0].state == "AVAILABLE"
    assert report.expiries[0].call is not None
    assert report.expiries[0].call.quote_source_time_ms == 1_910
    assert report.expiries[0].call.quote_observed_at_ms == 1_905
    refused = compute(changed, discovery, max_clock_skew_ms=4)
    assert refused.expiries[0].reason == "QUOTE_CLOCK_SKEW_EXCEEDED"


def test_mark_iv_feature_does_not_claim_an_executable_two_sided_market(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    altered = replace_quote(quotes[0], best_bid_price=None, best_bid_amount=0.0)
    report = compute(with_quote(snapshot, quotes[0], altered), discovery)
    assert report.expiries[0].state == "AVAILABLE"
    assert report.expiries[0].risk_reversal_points == -5.0
    assert report.executable_quotes_verified is False


def test_missing_any_discovered_quote_blocks_instead_of_using_partial_neighborhood(
    instrument, ticker
):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    changed = PointInTimeSnapshot(
        AS_OF,
        "BTC",
        "testnet",
        tuple(row for row in snapshot.rows() if row.event_id != quotes[1].event_id),
    )
    report = compute(changed, discovery)
    assert report.expiries[0].reason == "EXPIRY_QUOTE_MISSING"
    assert report.state == "PARTIAL"


def test_partial_discovery_cannot_certify_25_delta_pair(instrument, ticker):
    _, _, _, snapshot, partial = fixture(instrument, ticker, selected_all=False)
    report = compute(snapshot, partial)
    assert report.blockers == ("DISCOVERY_COVERAGE_NOT_FULL",)
    assert report.expiries == () and report.state == "UNAVAILABLE"


def test_cutoff_and_row_budgets_refuse(instrument, ticker):
    _, quotes, _, snapshot, discovery = fixture(instrument, ticker)
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        future = replace_quote(quotes[0], timestamp=AS_OF + 1, observed_at_ms=AS_OF + 1)
        PointInTimeSnapshot(AS_OF, "BTC", "testnet", snapshot.rows() + (future,))
    with pytest.raises(IntegrityError, match="INPUT_BUDGET"):
        compute(snapshot, discovery, max_rows=1)


def test_bad_delta_tolerance_blocks_target_selection(instrument, ticker):
    _, _, _, snapshot, discovery = fixture(instrument, ticker)
    report = compute(snapshot, discovery, maximum_delta_error=0.001)
    assert report.state == "UNAVAILABLE"
    assert all(row.reason == "DELTA_TARGET_UNAVAILABLE" for row in report.expiries)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"target_absolute_delta": 0.0},
        {"target_absolute_delta": float("nan")},
        {"maximum_delta_error": -0.1},
        {"maximum_delta_error": float("inf")},
        {"max_quote_age_ms": True},
        {"max_clock_skew_ms": True},
        {"max_expiries": 0},
    ],
)
def test_invalid_policy_is_rejected(instrument, ticker, kwargs):
    _, _, _, snapshot, discovery = fixture(instrument, ticker)
    with pytest.raises(ValueError):
        compute(snapshot, discovery, **kwargs)


async def test_closed_database_api_and_cli_publish_analytics_only(
    tmp_path, monkeypatch, instrument, ticker
):
    _, _, _, snapshot, discovery = fixture(instrument, ticker)
    database = tmp_path / "saved.duckdb"
    store = await DuckStore.open(database)
    try:
        await store.append((*snapshot.rows(), *discovery.rows()), exact=True)
    finally:
        await store.close()
    digest_before = hashlib.sha256(database.read_bytes()).hexdigest()
    policy = {
        "asset": "BTC",
        "as_of_ms": AS_OF,
        "target_absolute_delta": 0.25,
        "maximum_delta_error": 0.05,
        "max_quote_age_ms": 200,
        "max_discovery_age_ms": 1_000,
        "max_clock_skew_ms": 200,
        "max_underlying_mismatch_fraction": 0.01,
    }
    result_dir = tmp_path / "api-result"
    result = research_option_skew(database, result_dir, **policy)
    assert result["source_database_sha256"] == digest_before
    assert result["risk_reversal_report"]["expiries"][0]["risk_reversal_points"] == -5.0
    assert not result["trading_enabled"] and not result["ready_for_signals"]
    assert json.loads((result_dir / "risk-reversal.json").read_text()) == result
    assert hashlib.sha256(database.read_bytes()).hexdigest() == digest_before

    args = [
        "research-option-skew",
        "--database",
        str(database),
        "--output",
        str(tmp_path / "cli-result"),
        "--asset",
        "BTC",
        "--as-of-ms",
        str(AS_OF),
        "--target-absolute-delta",
        "0.25",
        "--maximum-delta-error",
        "0.05",
        "--max-quote-age-ms",
        "200",
        "--max-discovery-age-ms",
        "1000",
        "--max-clock-skew-ms",
        "200",
        "--max-underlying-mismatch-fraction",
        "0.01",
    ]
    monkeypatch.setattr("sys.argv", ["selective-options", *args])
    assert main() == 0
    cli_result = json.loads((tmp_path / "cli-result" / "risk-reversal.json").read_text())
    assert cli_result["risk_reversal_report"]["state"] == "DISCRETE_RISK_REVERSAL_COMPUTED"
    assert not cli_result["trading_enabled"]


def test_invalid_research_policy_refuses_before_source_open(tmp_path):
    with pytest.raises(ValueError):
        research_option_skew(
            tmp_path / "missing.duckdb",
            tmp_path / "no-output",
            asset="BTC",
            as_of_ms=AS_OF,
            target_absolute_delta=1.0,
            maximum_delta_error=0.05,
            max_quote_age_ms=200,
            max_discovery_age_ms=1_000,
            max_clock_skew_ms=200,
            max_underlying_mismatch_fraction=0.01,
        )
    assert not (tmp_path / "no-output").exists()
