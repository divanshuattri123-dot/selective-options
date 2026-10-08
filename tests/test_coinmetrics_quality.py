"""Bounded daily valuation policies and causal quality; all rows here are fixtures."""

import json

import pytest
from pydantic import ValidationError

from selective_options.coinmetrics import (
    DAY_MS,
    AcceptedObservation,
    CoinMetricsCollector,
    DailyRow,
    PollHealth,
    coinmetrics_poll_health,
    native_time_ms,
    normalize_row,
)
from selective_options.coinmetrics_quality import coinmetrics_quality
from selective_options.config import AppConfig, IngestionConfig

LABEL = native_time_ms("2026-10-05T00:00:00.000000000Z")
START = LABEL + 2 * DAY_MS + 100
RECEIPT = START + 10
FINISH = START + 20


def observation(*, asset="btc", label=LABEL, receipt=RECEIPT, started=START, **values):
    from datetime import UTC, datetime

    body = {
        "asset": asset,
        "time": datetime.fromtimestamp(label / 1000, UTC).strftime("%Y-%m-%dT00:00:00.000000000Z"),
        "CapMrktCurUSD": "1234567890.1234567890123456789",
        "CapMVRVCur": "1.234567890123456789",
        **values,
    }
    return normalize_row(DailyRow.model_validate(body), receipt, started)


def health(*events, finished=FINISH, started=START):
    return coinmetrics_poll_health(
        events[0].asset,
        PollHealth(
            frequency="1d",
            window_start_ms=LABEL - DAY_MS,
            window_end_ms=LABEL + DAY_MS,
            ok=True,
            reason="DAILY_REGIME_OBSERVED",
            poll_started_at_ms=started,
            poll_finished_at_ms=finished,
            response_received_at_ms=max(event.observed_at_ms for event in events),
            accepted_observations=tuple(
                AcceptedObservation(
                    event_id=event.event_id,
                    observed_at_ms=event.observed_at_ms,
                    period_label_ms=event.event_time_ms,
                )
                for event in events
            ),
        ),
    )


@pytest.mark.parametrize(
    "policies",
    [
        {"asset": "SOL"},
        {"as_of_ms": True},
        {"as_of_ms": "1000"},
        {"max_poll_age_ms": 0},
        {"max_data_age_ms": 2**63},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
def test_quality_policy_refuses_before_reading_input(policies):
    def unread():
        raise AssertionError("invalid policy must not consume input")
        yield

    with pytest.raises(ValueError):
        coinmetrics_quality(unread(), **{"asset": "BTC", "as_of_ms": FINISH, **policies})


def test_quality_counts_physical_eligible_rows_before_duplicate_budget():
    event = observation()
    bound = health(event)
    assert (
        coinmetrics_quality([event, event, bound], asset="BTC", as_of_ms=FINISH, max_rows=2)[
            "blocker"
        ]
        == "COINMETRICS_QUALITY_OVER_BUDGET"
    )
    assert (
        coinmetrics_quality([event, event, bound], asset="BTC", as_of_ms=FINISH, max_rows=3)[
            "historical_observations"
        ]
        == 1
    )


def test_conflicting_same_identity_never_becomes_last_writer_wins():
    event = observation()
    forged = event.model_copy(update={"payload_json": json.dumps({"forged": True})})
    for rows in ([event, forged, health(event)], [forged, event, health(event)]):
        assert coinmetrics_quality(rows, asset="BTC", as_of_ms=FINISH)["state"] == "BLOCKED"


def test_invalid_old_evidence_cannot_hide_behind_newer_success():
    event = observation()
    old = observation(receipt=RECEIPT - 50, started=START - 50)
    old = old.model_copy(update={"payload_json": "{}"})
    assert (
        coinmetrics_quality([old, event, health(event)], asset="BTC", as_of_ms=FINISH)["blocker"]
        == "COINMETRICS_INVALID_EVIDENCE"
    )


def test_exact_scope_excludes_unrelated_and_future_evidence_before_budget():
    event = observation()
    unrelated = [
        event.model_copy(update={field: value})
        for field, value in [
            ("source", "other"),
            ("asset", "ETH"),
            ("environment", "testnet"),
            ("symbol", "OTHER"),
            ("kind", "ticker"),
        ]
    ]
    future = observation(receipt=FINISH + 1, started=START + 1)
    result = coinmetrics_quality(
        [*unrelated, future, event, health(event)], asset="BTC", as_of_ms=FINISH, max_rows=2
    )
    assert result["state"] == "DAILY_REGIME_OBSERVED"


def test_two_revisions_of_one_daily_period_in_same_bound_poll_are_ambiguous():
    first = observation()
    second = observation(CapMVRVCur="2.0", receipt=RECEIPT + 1)
    assert (
        coinmetrics_quality([first, second, health(first, second)], asset="BTC", as_of_ms=FINISH)[
            "blocker"
        ]
        == "COINMETRICS_AMBIGUOUS_POLL_PERIOD"
    )


@pytest.mark.parametrize("receipt,started", [(RECEIPT + 1, START + 1), (FINISH + 1, START - 1)])
def test_unfinished_newer_or_late_old_poll_cannot_borrow_prior_health(receipt, started):
    current = observation()
    unbound = observation(receipt=receipt, started=started, CapMVRVCur="2.0")
    assert (
        coinmetrics_quality([current, health(current), unbound], asset="BTC", as_of_ms=FINISH + 2)[
            "blocker"
        ]
        == "COINMETRICS_UNBOUND_CURRENT_OR_NEWER_DATA"
    )


def test_missing_latest_period_does_not_fallback_to_older_full_metrics():
    old = observation(label=LABEL - DAY_MS)
    latest = observation(CapMrktCurUSD=None)
    result = coinmetrics_quality([old, latest, health(old, latest)], asset="BTC", as_of_ms=FINISH)
    assert result["accepted_event_id"] == latest.event_id
    assert result["blocker"] == "COINMETRICS_LATEST_PERIOD_MISSING_METRICS"
    assert result["missing_metrics"] == ("CapMrktCurUSD",)


def test_old_prior_poll_preserved_but_latest_bound_native_values_selected():
    old = observation(receipt=RECEIPT - 100, started=START - 100, CapMVRVCur="1.0")
    latest = observation(CapMVRVCur="2.0")
    result = coinmetrics_quality(
        [old, health(old, finished=FINISH - 100, started=START - 100), latest, health(latest)],
        asset="BTC",
        as_of_ms=FINISH,
    )
    assert result["historical_observations"] == 2 and result["bound_cohort_observations"] == 1
    assert result["native_row"]["CapMVRVCur"] == "2.0"


@pytest.mark.parametrize("age,expected", [(10, "DAILY_REGIME_OBSERVED"), (9, "BLOCKED")])
def test_actual_response_age_uses_inclusive_boundary(age, expected):
    event = observation()
    result = coinmetrics_quality(
        [event, health(event)], asset="BTC", as_of_ms=FINISH, max_poll_age_ms=age
    )
    assert result["state"] == expected
    assert result["poll_age_ms"] == 0 and result["response_age_ms"] == 10


@pytest.mark.parametrize(
    "field,value",
    [
        ("coinmetrics_poll_interval_s", 59),
        ("coinmetrics_poll_interval_s", 86401),
        ("coinmetrics_lookback_days", 0),
        ("coinmetrics_lookback_days", 31),
        ("coinmetrics_max_data_age_s", 86399),
        ("coinmetrics_max_data_age_s", 2592001),
        ("coinmetrics_max_pages", 0),
        ("coinmetrics_max_pages", 9),
        ("coinmetrics_max_rows", 0),
        ("coinmetrics_max_rows", 1025),
        ("coinmetrics_max_response_bytes", 1023),
        ("coinmetrics_max_response_bytes", 4000001),
        ("coinmetrics_timeout_s", 0),
        ("coinmetrics_timeout_s", 121),
        ("coinmetrics_enabled", "true"),
        ("coinmetrics_poll_interval_s", True),
    ],
)
def test_config_bounds_match_collector_refusal(field, value):
    with pytest.raises(ValidationError):
        IngestionConfig(**{field: value})


@pytest.mark.parametrize("edge", ["minimum", "maximum"])
def test_all_valid_config_extremes_construct_exact_collector_policy(edge):
    bounds = [
        ("poll_interval_s", 60, 86400),
        ("lookback_days", 1, 30),
        ("max_data_age_s", 86400, 2592000),
        ("max_pages", 1, 8),
        ("max_rows", 1, 1024),
        ("max_response_bytes", 1024, 4000000),
        ("timeout_s", 1, 120),
    ]
    config = IngestionConfig(
        **{
            "coinmetrics_" + field: low if edge == "minimum" else high
            for field, low, high in bounds
        }
    )

    async def sink(event):
        raise AssertionError("policy validation must not publish")

    collector = CoinMetricsCollector(
        sink,
        poll_interval_s=config.coinmetrics_poll_interval_s,
        lookback_days=config.coinmetrics_lookback_days,
        max_age_ms=config.coinmetrics_max_data_age_s * 1000,
        max_pages=config.coinmetrics_max_pages,
        max_rows=config.coinmetrics_max_rows,
        max_response_bytes=config.coinmetrics_max_response_bytes,
        timeout_s=config.coinmetrics_timeout_s,
    )
    assert collector.max_age_ms == config.coinmetrics_max_data_age_s * 1000
    assert collector.max_response_bytes == config.coinmetrics_max_response_bytes


def test_enabled_unsupported_assets_refuse_before_capture():
    with pytest.raises(ValidationError):
        AppConfig(system={"assets": ["SOL", "PAXG"]}, ingestion={"coinmetrics_enabled": True})
    assert AppConfig(system={"assets": ["SOL", "PAXG"]}).ingestion.coinmetrics_enabled is False
