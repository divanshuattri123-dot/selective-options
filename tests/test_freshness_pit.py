"""Latest-only diagnostics cannot give future evidence an as-of freshness pass."""

import pytest

from selective_options.events import make_event
from selective_options.news import news_poll_health
from selective_options.quality import FeedRequirement, FreshnessMonitor


@pytest.mark.parametrize("ok", [True, False])
def test_future_health_within_skew_is_unavailable_until_receipt(ok):
    monitor = FreshnessMonitor(5000)
    health = news_poll_health(
        "federal_reserve",
        1001,
        response_received_at_ms=1001 if ok else None,
        error_type=None if ok else "TimeoutError",
    )
    monitor.observe(health)
    requirements = [FeedRequirement("federal_reserve", "GLOBAL", "feed_health", 5000)]
    assert monitor.check(requirements, 1000)[0]["state"] == "UNAVAILABLE_AT_AS_OF"
    assert monitor.check(requirements, 1001)[0]["state"] == (
        "FRESH" if ok else "UNAVAILABLE_OR_DEGRADED"
    )


@pytest.mark.parametrize("source_time,receipt", [(1001, 1000), (999, 1001)])
def test_both_clocks_required_for_spot_freshness_and_divergence(source_time, receipt):
    monitor = FreshnessMonitor(5000)
    for source, timestamp, observed in [("kraken", 999, 999), ("coinbase", source_time, receipt)]:
        monitor.observe(
            make_event(
                source=source,
                environment="public_reference",
                asset="BTC",
                symbol="BTC/USD",
                kind="spot",
                event_time_ms=timestamp,
                observed_at_ms=observed,
                payload={"price": 100.0},
            )
        )
    result = monitor.check([FeedRequirement("coinbase", "BTC", "spot", 5000)], 1000)
    assert result[0]["state"] == "UNAVAILABLE_AT_AS_OF"
    assert monitor.divergence("BTC", 1000, 5000, 25)["state"] == "INSUFFICIENT_SOURCES"
    assert monitor.divergence("BTC", 1001, 5000, 25)["state"] == "AGREE"


@pytest.mark.parametrize("source_time,receipt", [(1001, 1000), (999, 1001)])
def test_option_latest_future_leg_cannot_pass_within_skew(source_time, receipt):
    monitor = FreshnessMonitor(5000)
    monitor.stream_state = "STREAM_SUBSCRIBED"
    monitor.expected_options = {("BTC", "BTC-OPTION")}
    monitor.observe(
        make_event(
            source="deribit",
            environment="testnet",
            asset="BTC",
            symbol="BTC-OPTION",
            kind="book",
            event_time_ms=source_time,
            observed_at_ms=receipt,
            payload={"bids": [[100, 1]], "asks": [[101, 1]]},
        )
    )
    book = next(row for row in monitor.option_freshness(1000, 5000) if row["kind"] == "book")
    assert book["state"] == "UNAVAILABLE_AT_AS_OF"
    assert (
        next(row for row in monitor.option_freshness(1001, 5000) if row["kind"] == "book")["state"]
        == "FRESH"
    )


def test_future_latest_does_not_claim_historical_fallback():
    monitor = FreshnessMonitor(5000)
    monitor.observe(news_poll_health("federal_reserve", 1000, response_received_at_ms=1000))
    monitor.observe(news_poll_health("federal_reserve", 1001, error_type="TimeoutError"))
    assert (
        monitor.check([FeedRequirement("federal_reserve", "GLOBAL", "feed_health", 5000)], 1000)[0][
            "state"
        ]
        == "UNAVAILABLE_AT_AS_OF"
    )


@pytest.mark.parametrize("reverse", [True, False])
def test_tied_health_is_ambiguous_in_both_arrival_orders_then_later_poll_recovers(reverse):
    monitor = FreshnessMonitor(5000)
    polls = [
        news_poll_health("federal_reserve", 1000, response_received_at_ms=1000),
        news_poll_health("federal_reserve", 1000, error_type="TimeoutError"),
    ]
    for poll in reversed(polls) if reverse else polls:
        monitor.observe(poll)
    requirements = [FeedRequirement("federal_reserve", "GLOBAL", "feed_health", 5000)]
    assert monitor.check(requirements, 1000)[0]["state"] == "AMBIGUOUS_LATEST"
    monitor.observe(polls[0])  # An exact duplicate cannot clear an established tie.
    assert monitor.check(requirements, 1000)[0]["state"] == "AMBIGUOUS_LATEST"
    monitor.observe(news_poll_health("federal_reserve", 1001, response_received_at_ms=1001))
    assert monitor.check(requirements, 1001)[0]["state"] == "FRESH"


def test_tied_spot_is_excluded_from_divergence_until_later_observation():
    monitor = FreshnessMonitor(5000)

    def spot(source, price, receipt=1000):
        return make_event(
            source=source,
            environment="public_reference",
            asset="BTC",
            symbol="BTC/USD",
            kind="spot",
            event_time_ms=receipt,
            observed_at_ms=receipt,
            payload={"price": price},
        )

    monitor.observe(spot("kraken", 100))
    monitor.observe(spot("coinbase", 100))
    monitor.observe(spot("coinbase", 101))
    assert monitor.divergence("BTC", 1000, 5000, 25)["state"] == "INSUFFICIENT_SOURCES"
    monitor.observe(spot("coinbase", 100, 1001))
    assert monitor.divergence("BTC", 1001, 5000, 25)["state"] == "AGREE"
