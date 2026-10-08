"""Independent synthetic poll/commit checks; these are not public collection evidence."""

import asyncio
import json
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest
from pydantic import ValidationError

from selective_options import ingestion
from selective_options.bus import EventBus, HotBuffer
from selective_options.bybit import FeedUnavailable
from selective_options.coinmetrics import (
    DAY_MS,
    URL,
    AcceptedObservation,
    CoinMetricsCollector,
    DailyRow,
    PollHealth,
    canonical_coinmetrics,
    canonical_coinmetrics_health,
    coinmetrics_poll_health,
    native_time_ms,
    normalize_row,
)
from selective_options.coinmetrics_quality import coinmetrics_quality
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import consume
from selective_options.quality import FreshnessMonitor
from selective_options.store import DuckStore

LABEL = native_time_ms("2026-10-05T00:00:00.000000000Z")
START = LABEL + 2 * DAY_MS + 100
RECEIPT = START + 10
FINISH = START + 20


def row(asset="btc", date="2026-10-05", **updates):
    return {
        "asset": asset,
        "time": date + "T00:00:00.000000000Z",
        "CapMrktCurUSD": "1234567890.1234567890123456789",
        "CapMVRVCur": "1.234567890123456789",
        **updates,
    }


def target(monkeypatch, sink, **policies):
    clock = iter((START, FINISH))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clock))
    return CoinMetricsCollector(sink, **policies)


@pytest.mark.parametrize(
    "field,value",
    [("observed_at_ms", str(RECEIPT)), ("event_time_ms", str(LABEL)), ("schema_version", True)],
)
def test_forged_envelope_types_refused_without_coercion(field, value):
    event = normalize_row(DailyRow.model_validate(row()), RECEIPT, START)
    with pytest.raises((IntegrityError, ValidationError)):
        canonical_coinmetrics(event.model_copy(update={field: value}))


@pytest.mark.parametrize(
    "field,value",
    [("observed_at_ms", str(FINISH)), ("event_time_ms", str(FINISH)), ("schema_version", True)],
)
def test_forged_health_types_refused_without_coercion(field, value):
    health = PollHealth(
        frequency="1d",
        window_start_ms=LABEL - DAY_MS,
        window_end_ms=LABEL,
        ok=False,
        reason="ACCESS_DENIED",
        code=403,
        poll_started_at_ms=START,
        poll_finished_at_ms=FINISH,
        response_received_at_ms=None,
    )
    event = coinmetrics_poll_health("BTC", health)
    with pytest.raises((IntegrityError, ValidationError)):
        canonical_coinmetrics_health(event.model_copy(update={field: value}))


async def test_actual_bus_commit_preserves_decimal_strings_and_first_receipt(tmp_path, monkeypatch):
    store = await DuckStore.open(tmp_path / "events.duckdb")
    config = AppConfig()
    bus = EventBus(20, 2)
    stopped = asyncio.Event()
    writer = asyncio.create_task(
        consume(config, bus, store, HotBuffer(20), FreshnessMonitor(5000), stopped, None)
    )
    collector = target(monkeypatch, bus.publish)
    collector.request = AsyncMock(return_value=({"data": [row(), row("eth")]}, RECEIPT))
    try:
        assert await collector.collect(object())
        await bus.flush()
        before = await store.snapshot("BTC", RECEIPT - 1, "public_reference")
        at = await store.snapshot("BTC", RECEIPT, "public_reference")
        finished = await store.snapshot("BTC", FINISH, "public_reference")
        assert before.rows() == () and len(at.rows()) == 1 and len(finished.rows()) == 2
        data = canonical_coinmetrics(at.rows()[0])
        assert data.native_row.CapMrktCurUSD == row()["CapMrktCurUSD"]
        assert data.native_row.CapMVRVCur == row()["CapMVRVCur"]
        assert at.rows()[0].observed_at_ms == RECEIPT and at.rows()[0].event_time_ms == LABEL
        health = canonical_coinmetrics_health(
            next(r for r in finished.rows() if r.kind == "feed_health")
        )
        assert health.accepted_observations[0].event_id == at.rows()[0].event_id
        assert health.accepted_observations[0].observed_at_ms == RECEIPT
        assert await store.pending_delivery_count() == 4
        assert not data.publication_time_available and not data.exchange_attribution_available
        assert data.unavailable_metrics == ("CapRealUSD", "SOPR")
    finally:
        stopped.set()
        try:
            await writer
        finally:
            await store.close()


@pytest.mark.parametrize(
    "bad", ["bad_decimal", "future_period", "asset", "repeat", "outside_window", "repeated_token"]
)
async def test_later_invalid_page_has_no_data_emission_and_failure_health(monkeypatch, bad):
    sink = AsyncMock()
    collector = target(monkeypatch, sink, lookback_days=2)

    async def request(_session, params):
        if "next_page_token" not in params:
            return {
                "data": [row()],
                "next_page_token": "NEXT",
                "next_page_url": URL + "?" + urlencode(params | {"next_page_token": "NEXT"}),
            }, RECEIPT
        value = {
            "bad_decimal": row("eth", CapMVRVCur="NaN"),
            "future_period": row("eth", date="2026-10-07"),
            "asset": row("sol"),
            "repeat": row(),
            "outside_window": row("eth", date="2026-10-01"),
            "repeated_token": row("eth"),
        }[bad]
        body = {"data": [value]}
        if bad == "repeated_token":
            body.update(next_page_token="NEXT", next_page_url=URL + "?" + urlencode(params))
        return body, RECEIPT + 1

    collector.request = AsyncMock(side_effect=request)
    assert await collector.collect(object()) is False
    emitted = [call.args[0] for call in sink.await_args_list]
    assert len(emitted) == 2 and all(r.kind == "feed_health" for r in emitted)
    for event in emitted:
        health = canonical_coinmetrics_health(event)
        assert health.reason == "INVALID_DATA" and health.accepted_observations == ()
        assert health.window_start_ms == LABEL and health.window_end_ms == LABEL + DAY_MS


async def test_cancel_second_page_leaves_sink_empty_and_allows_new_poll(monkeypatch):
    sink = AsyncMock()
    collector = target(monkeypatch, sink)
    entered = asyncio.Event()

    async def request(_session, params):
        if "next_page_token" not in params:
            return {
                "data": [row()],
                "next_page_token": "NEXT",
                "next_page_url": URL + "?" + urlencode(params | {"next_page_token": "NEXT"}),
            }, RECEIPT
        entered.set()
        await asyncio.Event().wait()

    collector.request = AsyncMock(side_effect=request)
    task = asyncio.create_task(collector.collect(object()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    sink.assert_not_awaited()
    clock = iter((START, FINISH))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clock))
    collector.request = AsyncMock(return_value=({"data": [row(), row("eth")]}, RECEIPT))
    assert await collector.collect(object())
    assert sink.await_count == 4


@pytest.mark.parametrize("cancelled", [False, True])
async def test_ambiguous_emission_blocks_reuse_before_another_request(monkeypatch, cancelled):
    accepted = []

    async def sink(event):
        if accepted:
            if cancelled:
                raise asyncio.CancelledError
            raise RuntimeError("EXPLICIT_SYNTHETIC_SINK_FAILURE")
        accepted.append(event)

    collector = target(monkeypatch, sink)
    collector.request = AsyncMock(return_value=({"data": [row(), row("eth")]}, RECEIPT))
    with pytest.raises(asyncio.CancelledError if cancelled else IntegrityError):
        await collector.collect(object())
    assert len(accepted) == 1 and accepted[0].kind == "on_chain"
    with pytest.raises(IntegrityError, match="EMISSION_OUTCOME_UNKNOWN"):
        await collector.collect(object())
    with pytest.raises(IntegrityError, match="EMISSION_OUTCOME_UNKNOWN"):
        await collector.run()
    assert collector.request.await_count == 1


async def test_revision_and_null_latest_are_new_observations_not_first_publications(monkeypatch):
    sink = AsyncMock()
    collector = target(monkeypatch, sink, assets=("BTC",))
    collector.request = AsyncMock(return_value=({"data": [row()]}, RECEIPT))
    assert await collector.collect(object())
    first = sink.await_args_list[0].args[0]
    clock = iter((START + 100, FINISH + 100))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clock))
    collector.request = AsyncMock(return_value=({"data": [row(CapMVRVCur=None)]}, RECEIPT + 100))
    assert await collector.collect(object()) is False
    latest = sink.await_args_list[2].args[0]
    health = canonical_coinmetrics_health(sink.await_args_list[3].args[0])
    assert (
        first.event_time_ms == latest.event_time_ms == LABEL and first.event_id != latest.event_id
    )
    assert latest.payload()["native_row"]["CapMVRVCur"] is None
    assert health.reason == "MISSING_METRICS" and not health.ok
    assert health.accepted_observations[0].event_id == latest.event_id


async def test_native_access_failure_after_healthy_poll_emits_new_unavailable_health(monkeypatch):
    sink = AsyncMock()
    collector = target(monkeypatch, sink, assets=("BTC",))
    collector.request = AsyncMock(return_value=({"data": [row()]}, RECEIPT))
    assert await collector.collect(object())
    clock = iter((START + 100, FINISH + 100))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clock))
    collector.request = AsyncMock(side_effect=FeedUnavailable("ACCESS_DENIED", 403))
    assert await collector.collect(object()) is False
    emitted = sink.await_args_list[2].args[0]
    health = canonical_coinmetrics_health(emitted)
    assert emitted.event_time_ms == FINISH + 100 and health.reason == "ACCESS_DENIED"
    assert health.accepted_observations == () and health.response_received_at_ms is None


def bound_health(event, *, finished=FINISH, **updates):
    body = canonical_coinmetrics(event)
    health = PollHealth.model_validate(
        {
            "frequency": "1d",
            "window_start_ms": LABEL - DAY_MS,
            "window_end_ms": LABEL + DAY_MS,
            "ok": True,
            "reason": "DAILY_REGIME_OBSERVED",
            "poll_started_at_ms": body.poll_started_at_ms,
            "poll_finished_at_ms": finished,
            "response_received_at_ms": event.observed_at_ms,
            "accepted_observations": (
                AcceptedObservation(
                    event_id=event.event_id,
                    observed_at_ms=event.observed_at_ms,
                    period_label_ms=event.event_time_ms,
                ),
            ),
            **updates,
        }
    )
    return coinmetrics_poll_health(event.asset, health)


def valid_pair():
    event = normalize_row(DailyRow.model_validate(row()), RECEIPT, START)
    return event, bound_health(event)


def test_quality_exposes_native_units_and_conservative_scope():
    event, health = valid_pair()
    result = coinmetrics_quality((event, health), asset="BTC", as_of_ms=FINISH)
    assert result["state"] == "DAILY_REGIME_OBSERVED" and result["blocker"] is None
    assert result["accepted_event_id"] == event.event_id
    assert result["accepted_observed_at_ms"] == RECEIPT and result["period_label_ms"] == LABEL
    assert result["native_row"] == row()
    assert result["native_units"] == {"CapMrktCurUSD": "USD", "CapMVRVCur": "Dimensionless"}
    assert result["requested_window_start_ms"] == LABEL - DAY_MS
    assert result["requested_window_end_ms"] == LABEL + DAY_MS
    for flag in (
        "publication_time_available",
        "provider_finality_independently_verified",
        "history_complete",
        "exchange_attribution_available",
        "ready_for_signals",
        "trading_enabled",
    ):
        assert result[flag] is False


def test_latest_failed_health_and_equal_clock_conflicts_block_previous_success():
    event, healthy = valid_pair()
    failed = coinmetrics_poll_health(
        "BTC",
        PollHealth(
            frequency="1d",
            window_start_ms=LABEL - DAY_MS,
            window_end_ms=LABEL + DAY_MS,
            ok=False,
            reason="ACCESS_DENIED",
            code=403,
            poll_started_at_ms=START + 100,
            poll_finished_at_ms=FINISH + 100,
            response_received_at_ms=RECEIPT + 100,
        ),
    )
    result = coinmetrics_quality((event, healthy, failed), asset="BTC", as_of_ms=FINISH + 100)
    assert result["blocker"] == "COINMETRICS_LATEST_POLL_FAILED"
    assert result["health_event_id"] == failed.event_id
    tie = coinmetrics_poll_health(
        "BTC",
        PollHealth(
            frequency="1d",
            window_start_ms=LABEL - DAY_MS,
            window_end_ms=LABEL + DAY_MS,
            ok=False,
            reason="ACCESS_DENIED",
            code=403,
            poll_started_at_ms=START,
            poll_finished_at_ms=FINISH,
            response_received_at_ms=RECEIPT,
        ),
    )
    for sequence in ((event, healthy, tie), (tie, healthy, event)):
        assert (
            coinmetrics_quality(sequence, asset="BTC", as_of_ms=FINISH)["blocker"]
            == "COINMETRICS_AMBIGUOUS_LATEST_HEALTH"
        )


@pytest.mark.parametrize(
    "fault,blocker",
    [
        ("missing", "COINMETRICS_MISSING_BOUND_OBSERVATION"),
        ("ref_clock", "COINMETRICS_BOUND_OBSERVATION_MISMATCH"),
        ("unbound", "COINMETRICS_UNBOUND_CURRENT_OR_NEWER_DATA"),
        ("null", "COINMETRICS_LATEST_PERIOD_MISSING_METRICS"),
        ("receipt_future", "COINMETRICS_NO_BOUND_POLL"),
        ("source_future", "COINMETRICS_NO_BOUND_POLL"),
    ],
)
def test_quality_rejects_causal_or_binding_gaps(fault, blocker):
    event, health = valid_pair()
    rows = [event, health]
    cutoff = FINISH
    if fault == "missing":
        rows = [health]
    elif fault == "ref_clock":
        health = bound_health(
            event,
            accepted_observations=(
                AcceptedObservation(
                    event_id=event.event_id, observed_at_ms=RECEIPT + 1, period_label_ms=LABEL
                ),
            ),
            response_received_at_ms=RECEIPT + 1,
        )
        rows = [event, health]
    elif fault == "unbound":
        rows.append(
            normalize_row(DailyRow.model_validate(row(CapMVRVCur="1.9")), RECEIPT + 1, START)
        )
    elif fault == "null":
        event = normalize_row(DailyRow.model_validate(row(CapMVRVCur=None)), RECEIPT, START)
        rows = [event, bound_health(event)]
    elif fault == "receipt_future":
        cutoff = RECEIPT - 1
    else:
        # Deliberately forged source-only future clock must filter before parsing/budget.
        rows = [event, health.model_copy(update={"event_time_ms": FINISH + 1})]
    result = coinmetrics_quality(rows, asset="BTC", as_of_ms=cutoff)
    assert result["state"] == "BLOCKED" and result["blocker"] == blocker


@pytest.mark.parametrize(
    "age_policy,blocker",
    [
        ({"max_poll_age_ms": 1}, "COINMETRICS_STALE_POLL_OR_RESPONSE"),
        ({"max_data_age_ms": DAY_MS}, "COINMETRICS_STALE_DAILY_PERIOD"),
    ],
)
def test_period_and_actual_response_age_are_independent(age_policy, blocker):
    result = coinmetrics_quality(valid_pair(), asset="BTC", as_of_ms=FINISH, **age_policy)
    assert result["blocker"] == blocker


async def test_store_quality_filters_future_rows_before_overflow_and_preserves_all_pending(
    tmp_path,
):
    event, health = valid_pair()
    future_data = normalize_row(DailyRow.model_validate(row()), RECEIPT + 100, START + 100)
    future_health = bound_health(future_data, finished=FINISH + 100)
    future_source = health.model_copy(update={"event_id": "f" * 64, "event_time_ms": FINISH + 1})
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([event, health, future_data, future_health, future_source], exact=True)
        result = await store.coinmetrics_quality("BTC", FINISH, max_rows=2)
        assert result["state"] == "DAILY_REGIME_OBSERVED" and result["historical_observations"] == 1
        assert result["accepted_event_id"] == event.event_id
        overflow = await store.coinmetrics_quality("BTC", FINISH, max_rows=1)
        assert overflow["state"] == "BLOCKED"
        assert overflow["blocker"] == "COINMETRICS_QUALITY_OVER_BUDGET"
        assert await store.pending_delivery_count() == 5
    finally:
        await store.close()


def quiet_capture(monkeypatch):
    async def idle(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(ingestion.DeribitCollector, "run", idle)
    monkeypatch.setattr(ingestion.SpotTradeCollector, "run", idle)
    monkeypatch.setattr(ingestion, "poll_chains", idle)
    monkeypatch.setattr(ingestion, "now_ms", lambda: FINISH)


def capture_config(enabled):
    return AppConfig(
        system={"assets": ["BTC", "SOL"]},
        ingestion={
            "coinmetrics_enabled": enabled,
            "reference_sources": [],
            "derivatives_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
            "coinmetrics_poll_interval_s": 60,
            "coinmetrics_lookback_days": 2,
            "coinmetrics_max_data_age_s": 259200,
            "coinmetrics_max_pages": 2,
            "coinmetrics_max_rows": 10,
            "coinmetrics_max_response_bytes": 1024,
            "coinmetrics_timeout_s": 1,
        },
    )


async def test_capture_default_does_not_construct_optional_collector(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("disabled optional collector constructed")

    monkeypatch.setattr("selective_options.coinmetrics.CoinMetricsCollector", forbidden)
    report = await ingestion.capture(capture_config(False), tmp_path / "capture", 0.04)
    assert report["coinmetrics_quality"] is None and report["status"] == "CAPTURE_FINISHED"


async def test_supervised_capture_actual_poll_commit_quality_and_asset_scope(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)
    clock = iter((START, FINISH))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clock))
    selected = []

    async def run(self):
        selected.append(self)
        assert await self.collect(object())
        await asyncio.Event().wait()

    monkeypatch.setattr(CoinMetricsCollector, "run", run)
    monkeypatch.setattr(
        CoinMetricsCollector, "request", AsyncMock(return_value=({"data": [row()]}, RECEIPT))
    )
    report = await ingestion.capture(capture_config(True), tmp_path / "capture", 0.1)
    assert report["status"] == "CAPTURE_FINISHED"
    assert len(selected) == 1 and selected[0].assets == ("BTC",)
    collector = selected[0]
    assert (
        collector.poll_interval_s,
        collector.lookback_days,
        collector.max_age_ms,
        collector.max_pages,
        collector.max_rows,
        collector.max_response_bytes,
        collector.timeout_s,
    ) == (60, 2, 259200000, 2, 10, 1024, 1)
    quality = report["coinmetrics_quality"]
    assert quality["supported_configured_assets"] == ["BTC"]
    assert quality["unsupported_configured_assets"] == ["SOL"]
    assert quality["assets"][0]["state"] == "DAILY_REGIME_OBSERVED"
    assert not quality["history_complete"] and not quality["ready_for_signals"]
    store = await DuckStore.open(tmp_path / "capture/events.duckdb")
    try:
        rows = (await store.snapshot("BTC", FINISH, "public_reference")).rows()
        assert len(rows) == 2 and await store.pending_delivery_count() == 2
        assert (
            canonical_coinmetrics(
                next(r for r in rows if r.kind == "on_chain")
            ).native_row.CapMrktCurUSD
            == row()["CapMrktCurUSD"]
        )
    finally:
        await store.close()


async def test_capture_producer_failure_cannot_publish_success(tmp_path, monkeypatch):
    quiet_capture(monkeypatch)

    async def failed(_self):
        raise IntegrityError("COINMETRICS_EMISSION_FAILED")

    monkeypatch.setattr(CoinMetricsCollector, "run", failed)
    with pytest.raises(IntegrityError, match="COINMETRICS_EMISSION_FAILED"):
        await ingestion.capture(capture_config(True), tmp_path / "capture", 1)
    report = json.loads((tmp_path / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and not report["ready_for_signals"]
    assert report["coinmetrics_quality"]["assets"][0]["state"] == "BLOCKED"
