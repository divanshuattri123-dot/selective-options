"""Exact per-asset daily valuation quality, preserving receipts and poll outcomes.

Native dates label provider daily periods. They are not publication timestamps or
independent finality evidence. A health row binds an observed cohort; it does not
certify that all requested historical dates or revisions are available.
"""

from collections.abc import Iterable

from pydantic import ValidationError

from .coinmetrics import (
    METRICS,
    SOURCE,
    SYMBOL,
    UNAVAILABLE_METRICS,
    DailySnapshot,
    PollHealth,
    canonical_coinmetrics,
    canonical_coinmetrics_health,
)
from .errors import IntegrityError
from .events import MarketEvent


def coinmetrics_quality(
    rows: Iterable[MarketEvent],
    *,
    asset: str,
    as_of_ms: int,
    max_poll_age_ms: int = 7_200_000,
    max_data_age_ms: int = 259_200_000,
    max_rows: int = 10000,
) -> dict[str, object]:
    if type(asset) is not str or asset not in ("BTC", "ETH"):
        raise ValueError("Coin Metrics quality requires BTC or ETH")
    if any(
        type(x) is not int or not 0 < x < 2**63
        for x in (as_of_ms, max_poll_age_ms, max_data_age_ms)
    ):
        raise ValueError("Coin Metrics quality requires exact positive integer clocks and ages")
    if type(max_rows) is not int or not 1 <= max_rows <= 100000:
        raise ValueError("Coin Metrics quality requires an explicit bounded row budget")
    result: dict[str, object] = {
        "source": SOURCE,
        "asset": asset,
        "symbol": SYMBOL,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "poll_ok": None,
        "poll_reason": None,
        "health_received_at_ms": None,
        "response_received_at_ms": None,
        "poll_age_ms": None,
        "response_age_ms": None,
        "period_age_ms": None,
        "max_poll_age_ms": max_poll_age_ms,
        "max_data_age_ms": max_data_age_ms,
        "max_rows": max_rows,
        "requested_window_start_ms": None,
        "requested_window_end_ms": None,
        "accepted_event_id": None,
        "accepted_observed_at_ms": None,
        "period_label_ms": None,
        "native_row": None,
        "native_units": None,
        "historical_observations": 0,
        "bound_cohort_observations": 0,
        "native_metric_names": METRICS,
        "unavailable_metrics": UNAVAILABLE_METRICS,
        "missing_metrics": (),
        "publication_time_available": False,
        "provider_finality_independently_verified": False,
        "exchange_attribution_available": False,
        "history_complete": False,
        "ready_for_signals": False,
        "trading_enabled": False,
    }

    def blocked(reason: str) -> dict[str, object]:
        result["blocker"] = reason
        return result

    data: dict[str, tuple[MarketEvent, DailySnapshot]] = {}
    health: dict[str, tuple[MarketEvent, PollHealth]] = {}
    identities: dict[str, MarketEvent] = {}
    count = 0
    try:
        for row in rows:
            if not isinstance(row, MarketEvent):
                return blocked("COINMETRICS_INVALID_ENVELOPE")
            if (row.source, row.environment, row.asset, row.symbol) != (
                SOURCE,
                "public_reference",
                asset,
                SYMBOL,
            ):
                continue
            if row.kind not in ("on_chain", "feed_health"):
                continue
            if any(
                type(x) is not int or not 0 < x < 2**63
                for x in (row.event_time_ms, row.observed_at_ms)
            ):
                return blocked("COINMETRICS_INVALID_ENVELOPE")
            if row.event_time_ms > as_of_ms or row.observed_at_ms > as_of_ms:
                continue
            count += 1
            if count > max_rows:
                return blocked("COINMETRICS_QUALITY_OVER_BUDGET")
            if row.event_id in identities:
                if identities[row.event_id] != row:
                    return blocked("COINMETRICS_CONFLICTING_EVENT_ID")
                continue
            identities[row.event_id] = row
            if row.kind == "on_chain":
                data[row.event_id] = (row, canonical_coinmetrics(row))
            else:
                health[row.event_id] = (row, canonical_coinmetrics_health(row))
    except (IntegrityError, ValidationError, ValueError, TypeError, OverflowError):
        return blocked("COINMETRICS_INVALID_EVIDENCE")
    result["historical_observations"] = len(data)
    if not health:
        return blocked("COINMETRICS_NO_BOUND_POLL")
    latest_receipt = max(row.observed_at_ms for row, _ in health.values())
    last = [pair for pair in health.values() if pair[0].observed_at_ms == latest_receipt]
    if len(last) != 1:
        return blocked("COINMETRICS_AMBIGUOUS_LATEST_HEALTH")
    health_row, poll = last[0]
    result.update(
        health_event_id=health_row.event_id,
        health_received_at_ms=health_row.observed_at_ms,
        poll_ok=poll.ok,
        poll_reason=poll.reason,
        response_received_at_ms=poll.response_received_at_ms,
        poll_age_ms=as_of_ms - poll.poll_finished_at_ms,
        requested_window_start_ms=poll.window_start_ms,
        requested_window_end_ms=poll.window_end_ms,
    )
    if not poll.ok:
        return blocked("COINMETRICS_LATEST_POLL_FAILED")
    if poll.response_received_at_ms is None:
        return blocked("COINMETRICS_SUCCESS_WITHOUT_RESPONSE")
    result["response_age_ms"] = as_of_ms - poll.response_received_at_ms
    if (
        as_of_ms - poll.poll_finished_at_ms > max_poll_age_ms
        or as_of_ms - poll.response_received_at_ms > max_poll_age_ms
    ):
        return blocked("COINMETRICS_STALE_POLL_OR_RESPONSE")
    cohort: list[tuple[MarketEvent, DailySnapshot]] = []
    for ref in poll.accepted_observations:
        pair = data.get(ref.event_id)
        if pair is None:
            return blocked("COINMETRICS_MISSING_BOUND_OBSERVATION")
        event, body = pair
        if (event.observed_at_ms, body.period_label_ms) != (
            ref.observed_at_ms,
            ref.period_label_ms,
        ) or body.poll_started_at_ms != poll.poll_started_at_ms:
            return blocked("COINMETRICS_BOUND_OBSERVATION_MISMATCH")
        cohort.append(pair)
    references = {event.event_id for event, _ in cohort}
    for event, body in data.values():
        if event.event_id not in references and (
            body.poll_started_at_ms >= poll.poll_started_at_ms
            or event.observed_at_ms > poll.poll_finished_at_ms
        ):
            return blocked("COINMETRICS_UNBOUND_CURRENT_OR_NEWER_DATA")
    result["bound_cohort_observations"] = len(cohort)
    periods = {body.period_label_ms for _, body in cohort}
    if len(periods) != len(cohort):
        return blocked("COINMETRICS_AMBIGUOUS_POLL_PERIOD")
    event, body = max(cohort, key=lambda pair: pair[1].period_label_ms)
    period_age = as_of_ms - body.period_label_ms
    result.update(
        accepted_event_id=event.event_id,
        accepted_observed_at_ms=event.observed_at_ms,
        period_label_ms=body.period_label_ms,
        period_age_ms=period_age,
        native_row=body.native_row.model_dump(mode="json", exclude_unset=True),
        native_units=body.units,
    )
    missing = tuple(metric for metric in METRICS if getattr(body.native_row, metric) is None)
    result["missing_metrics"] = missing
    if missing:
        return blocked("COINMETRICS_LATEST_PERIOD_MISSING_METRICS")
    if period_age > max_data_age_ms:
        return blocked("COINMETRICS_STALE_DAILY_PERIOD")
    result.update(state="DAILY_REGIME_OBSERVED", blocker=None)
    return result
