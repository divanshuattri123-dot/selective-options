"""Deterministic fixture checks; public availability is evidenced separately."""

import asyncio
import json
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest
from pydantic import ValidationError

from selective_options.bybit import FeedUnavailable
from selective_options.coinmetrics import (
    DAY_MS,
    METRICS,
    SOURCE,
    SYMBOL,
    URL,
    CoinMetricsCollector,
    DailyRow,
    PollHealth,
    canonical_coinmetrics,
    canonical_coinmetrics_health,
    coinmetrics_poll_health,
    native_time_ms,
    normalize_row,
)
from selective_options.errors import IntegrityError
from selective_options.events import PointInTimeSnapshot

LABEL = native_time_ms("2026-10-05T00:00:00.000000000Z")
STARTED = LABEL + 2 * DAY_MS + 100
RECEIVED = STARTED + 10
FINISHED = RECEIVED + 10


def native(asset="btc", date="2026-10-05", **updates):
    return {
        "asset": asset,
        "time": date + "T00:00:00.000000000Z",
        "CapMrktCurUSD": "1724754820477.82986243790963106",
        "CapMVRVCur": "1.596729720053423595",
        **updates,
    }


def collector(monkeypatch, sink=None, **settings):
    times = iter((STARTED, FINISHED))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(times))
    return CoinMetricsCollector(sink or AsyncMock(), **settings)


def test_exact_native_precision_and_receipt_revision():
    row = DailyRow.model_validate(native())
    first = normalize_row(row, RECEIVED, STARTED)
    second = normalize_row(row, RECEIVED + 1, STARTED)
    typed = canonical_coinmetrics(first)
    assert typed.native_row.CapMrktCurUSD == native()["CapMrktCurUSD"]
    assert first.event_time_ms == LABEL and first.observed_at_ms == RECEIVED
    assert first.event_id != second.event_id
    assert first.source == SOURCE and first.symbol == SYMBOL
    assert typed.units == {"CapMrktCurUSD": "USD", "CapMVRVCur": "Dimensionless"}
    assert typed.unavailable_metrics == ("CapRealUSD", "SOPR")
    assert typed.publication_time_available is False
    assert typed.exchange_attribution_available is False
    with pytest.raises(IntegrityError, match="LOOKAHEAD"):
        PointInTimeSnapshot(RECEIVED - 1, "BTC", "public_reference", (first,))
    assert PointInTimeSnapshot(RECEIVED, "BTC", "public_reference", (first,)).rows() == (first,)


def test_absent_and_null_are_distinct_native_observations():
    absent = native()
    del absent["CapMVRVCur"]
    a = normalize_row(DailyRow.model_validate(absent), RECEIVED, STARTED)
    b = normalize_row(DailyRow.model_validate(native(CapMVRVCur=None)), RECEIVED, STARTED)
    assert "CapMVRVCur" not in a.payload()["native_row"]
    assert b.payload()["native_row"]["CapMVRVCur"] is None
    assert a.event_id != b.event_id
    assert canonical_coinmetrics(a).native_row.CapMVRVCur is None


@pytest.mark.parametrize(
    "changes",
    [
        {"asset": "sol"},
        {"asset": "BTC"},
        {"time": "2026-10-05"},
        {"time": "2026-10-05T12:00:00.000000000Z"},
        {"time": "2026-10-05T00:00:00.000000001Z"},
        {"time": "2026-02-30T00:00:00.000000000Z"},
        {"CapMVRVCur": 1.5},
        {"CapMVRVCur": True},
        {"CapMVRVCur": "NaN"},
        {"CapMVRVCur": "Infinity"},
        {"CapMVRVCur": "1e3"},
        {"CapMVRVCur": "0"},
        {"CapMrktCurUSD": "-1"},
        {"CapMrktCurUSD": "01"},
        {"CapMrktCurUSD": "9" * 129},
        {"SOPR": "1.02"},
    ],
)
def test_native_refusals(changes):
    with pytest.raises((ValidationError, ValueError)):
        DailyRow.model_validate({**native(), **changes})


@pytest.mark.parametrize(
    "receipt,started",
    [(True, STARTED), (RECEIVED, True), (STARTED - 1, STARTED), (RECEIVED, LABEL + DAY_MS - 1)],
)
def test_clock_and_complete_period_refusal(receipt, started):
    with pytest.raises(ValidationError):
        normalize_row(DailyRow.model_validate(native()), receipt, started)


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "0" * 64},
        {"asset": "ETH"},
        {"source": "other"},
        {"symbol": "other"},
        {"environment": "testnet"},
        {"timestamp_kind": "receipt"},
        {"observed_at_ms": RECEIVED + 1},
        {"kind": "spot"},
    ],
)
def test_exact_canonical_envelope_refusals(changes):
    event = normalize_row(DailyRow.model_validate(native()), RECEIVED, STARTED)
    with pytest.raises(IntegrityError):
        canonical_coinmetrics(event.model_copy(update=changes))


@pytest.mark.parametrize(
    "policy",
    [
        {"assets": ()},
        {"assets": ("SOL",)},
        {"assets": ("BTC", "BTC")},
        {"assets": ["BTC"]},
        {"poll_interval_s": True},
        {"poll_interval_s": 59},
        {"lookback_days": 31},
        {"max_age_ms": DAY_MS - 1},
        {"max_pages": 9},
        {"max_rows": 1025},
        {"max_response_bytes": 1023},
        {"timeout_s": 0},
        {"timeout_s": 1.5},
    ],
)
def test_policy_refusal_before_transport(policy):
    with pytest.raises(ValueError):
        CoinMetricsCollector(AsyncMock(), **policy)


async def test_both_assets_poll_data_then_bound_health(monkeypatch):
    target = collector(monkeypatch)
    target.request = AsyncMock(return_value=({"data": [native(), native("eth")]}, RECEIVED))
    assert await target.collect(None) is True
    delivered = [c.args[0] for c in target.sink.await_args_list]
    assert [e.kind for e in delivered] == ["on_chain", "on_chain", "feed_health", "feed_health"]
    for data, health in zip(delivered[:2], delivered[2:], strict=True):
        typed = canonical_coinmetrics_health(health)
        assert typed.ok and health.asset == data.asset
        assert typed.accepted_observations[0].event_id == data.event_id
        assert typed.accepted_observations[0].observed_at_ms == RECEIVED
        assert typed.response_received_at_ms == RECEIVED
    params = target.request.await_args.args[1]
    assert params["metrics"] == ",".join(METRICS)
    assert params["start_time"] == "2026-09-30" and params["end_time"] == "2026-10-06"


@pytest.mark.parametrize("missing", [None, "absent"])
async def test_latest_missing_rejects_older_valid_numeric_substitute(monkeypatch, missing):
    target = collector(monkeypatch, assets=("BTC",))
    last = native(date="2026-10-06", CapMVRVCur=None)
    if missing == "absent":
        del last["CapMVRVCur"]
    target.request = AsyncMock(return_value=({"data": [native(), last]}, RECEIVED))
    assert await target.collect(None) is False
    delivered = [c.args[0] for c in target.sink.await_args_list]
    health = canonical_coinmetrics_health(delivered[-1])
    assert not health.ok and health.reason == "MISSING_METRICS"
    assert len(health.accepted_observations) == 2


@pytest.mark.parametrize("data,reason", [([], "EMPTY"), ([native(date="2026-10-01")], "STALE")])
async def test_empty_or_stale_is_unavailable(monkeypatch, data, reason):
    target = collector(monkeypatch, assets=("BTC",))
    target.request = AsyncMock(return_value=({"data": data}, RECEIVED))
    assert await target.collect(None) is False
    assert canonical_coinmetrics_health(target.sink.await_args_list[-1].args[0]).reason == reason


@pytest.mark.parametrize(
    "error,reason",
    [
        (FeedUnavailable("ACCESS_DENIED", 403), "ACCESS_DENIED"),
        (FeedUnavailable("RATE_LIMITED", 429), "RATE_LIMITED"),
        (OSError("fixture"), "TRANSPORT_UNAVAILABLE"),
        (IntegrityError("fixture"), "INVALID_DATA"),
    ],
)
async def test_single_failure_no_automatic_retry(monkeypatch, error, reason):
    target = collector(monkeypatch)
    target.request = AsyncMock(side_effect=error)
    assert await target.collect(None) is False
    assert target.request.await_count == 1
    for call in target.sink.await_args_list:
        health = canonical_coinmetrics_health(call.args[0])
        assert health.reason == reason and not health.accepted_observations


def next_page(params, token="one", **changes):
    query = {**params, "next_page_token": token, **changes}
    return {"next_page_token": token, "next_page_url": URL + "?" + urlencode(query)}


async def test_bounded_multi_page_receipts_preserved(monkeypatch):
    target = collector(monkeypatch)

    async def request(session, params):
        if "next_page_token" not in params:
            return {"data": [native()], **next_page(params)}, RECEIVED
        return {"data": [native("eth")]}, RECEIVED + 1

    target.request = AsyncMock(side_effect=request)
    assert await target.collect(None) is True
    events = [c.args[0] for c in target.sink.await_args_list]
    assert [e.observed_at_ms for e in events[:2]] == [RECEIVED, RECEIVED + 1]


@pytest.mark.parametrize(
    "bad",
    [
        "metric",
        "route",
        "duplicate",
        "cycle",
        "empty",
        "budget",
        "future",
        "wrong_asset",
        "receipt",
    ],
)
async def test_later_page_failure_stages_no_data_prefix(monkeypatch, bad):
    target = collector(monkeypatch, max_pages=2)

    async def request(session, params):
        if "next_page_token" not in params:
            pagination = next_page(params)
            if bad == "route":
                pagination["next_page_url"] = "https://evil.example/?next_page_token=one"
            return {"data": [native()], **pagination}, RECEIVED
        data = [native("eth")]
        pagination = {}
        if bad == "metric":
            data = [native("eth", CapMVRVCur="NaN")]
        if bad == "duplicate":
            data = [native()]
        if bad == "cycle":
            pagination = next_page({k: v for k, v in params.items() if k != "next_page_token"})
        if bad == "empty":
            data = []
            pagination = next_page(params, "two")
        if bad == "budget":
            pagination = next_page(params, "two")
        if bad == "future":
            data = [native("eth", date="2026-10-07")]
        if bad == "wrong_asset":
            data = [native("sol")]
        return {"data": data, **pagination}, RECEIVED - 1 if bad == "receipt" else RECEIVED + 1

    target.request = AsyncMock(side_effect=request)
    assert await target.collect(None) is False
    assert all(c.args[0].kind == "feed_health" for c in target.sink.await_args_list)
    assert all(
        canonical_coinmetrics_health(c.args[0]).reason == "INVALID_DATA"
        for c in target.sink.await_args_list
    )


@pytest.mark.parametrize("cancellation", [False, True])
async def test_ambiguous_sink_prefix_failstop_no_success_health(monkeypatch, cancellation):
    delivered = []

    async def sink(event):
        if delivered:
            if cancellation:
                raise asyncio.CancelledError
            raise OSError("ambiguous sink")
        delivered.append(event)

    target = collector(monkeypatch, sink)
    target.request = AsyncMock(return_value=({"data": [native(), native("eth")]}, RECEIVED))
    with pytest.raises(asyncio.CancelledError if cancellation else IntegrityError):
        await target.collect(None)
    assert len(delivered) == 1 and delivered[0].kind == "on_chain"
    with pytest.raises(IntegrityError, match="OUTCOME_UNKNOWN"):
        await target.collect(None)
    with pytest.raises(IntegrityError, match="OUTCOME_UNKNOWN"):
        await target.run()
    assert target.request.await_count == 1


async def test_fetch_cancellation_no_emission_no_poison(monkeypatch):
    target = collector(monkeypatch)
    target.request = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await target.collect(None)
    target.sink.assert_not_awaited()
    assert not target._emission_failed


class Content:
    def __init__(self, data):
        self.data = data

    async def iter_chunked(self, size):
        for offset in range(0, len(self.data), size):
            yield self.data[offset : offset + size]


class Response:
    def __init__(self, data, status=200):
        self.status = status
        self.content = Content(data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Session:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.response


async def test_fixed_https_native_response_hook(monkeypatch):
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: RECEIVED)
    target = CoinMetricsCollector(AsyncMock())
    raw = json.dumps({"data": [native()]}).encode()
    target.response_received = AsyncMock()
    session = Session(Response(raw))
    result, receipt = await target.request(session, {"assets": "btc"})
    assert result == {"data": [native()]} and receipt == RECEIVED
    assert session.calls == [((URL,), {"params": {"assets": "btc"}, "allow_redirects": False})]
    target.response_received.assert_awaited_once_with({"assets": "btc"}, 200, raw, RECEIVED)


@pytest.mark.parametrize("raw", [b'{"data":[],"data":[]}', b'{"data":[NaN]}', b"not json"])
async def test_invalid_native_json_refused(monkeypatch, raw):
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: RECEIVED)
    with pytest.raises((IntegrityError, ValueError)):
        await CoinMetricsCollector(AsyncMock()).request(Session(Response(raw)), {})


async def test_http_and_response_budget(monkeypatch):
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: RECEIVED)
    target = CoinMetricsCollector(AsyncMock(), max_response_bytes=1024)
    with pytest.raises(FeedUnavailable, match="ACCESS_DENIED"):
        await target.request(Session(Response(b"{}", 403)), {})
    with pytest.raises(IntegrityError, match="RESPONSE_BUDGET"):
        await target.request(Session(Response(b" " * 1025)), {})


def valid_health():
    event = normalize_row(DailyRow.model_validate(native()), RECEIVED, STARTED)
    return {
        "frequency": "1d",
        "window_start_ms": LABEL - 5 * DAY_MS,
        "window_end_ms": LABEL + DAY_MS,
        "ok": True,
        "reason": "DAILY_REGIME_OBSERVED",
        "poll_started_at_ms": STARTED,
        "poll_finished_at_ms": FINISHED,
        "response_received_at_ms": RECEIVED,
        "accepted_observations": (
            {"event_id": event.event_id, "observed_at_ms": RECEIVED, "period_label_ms": LABEL},
        ),
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"ok": 1},
        {"ok": False},
        {"code": 403},
        {"error_type": "OSError"},
        {"response_received_at_ms": None},
        {"response_received_at_ms": RECEIVED - 1},
        {"publication_time_available": True},
        {"accepted_observations": ()},
        {"reason": "ACCESS_DENIED", "ok": False},
        {"poll_started_at_ms": FINISHED + 1},
    ],
)
def test_success_health_strict_provenance_bindings(changes):
    with pytest.raises(ValidationError):
        PollHealth.model_validate({**valid_health(), **changes})


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "1" * 64},
        {"asset": "ETH"},
        {"kind": "on_chain"},
        {"observed_at_ms": FINISHED + 1},
        {"timestamp_kind": "source"},
    ],
)
def test_health_canonical_scope_refusal(changes):
    event = coinmetrics_poll_health("BTC", PollHealth.model_validate(valid_health()))
    with pytest.raises(IntegrityError):
        canonical_coinmetrics_health(event.model_copy(update=changes))


async def test_total_poll_timeout_failure_without_data_prefix(monkeypatch):
    target = collector(monkeypatch, timeout_s=1)

    async def stall(session, params):
        await asyncio.Event().wait()

    target.request = AsyncMock(side_effect=stall)
    assert await target.collect(None) is False
    assert all(c.args[0].kind == "feed_health" for c in target.sink.await_args_list)
    assert all(
        canonical_coinmetrics_health(c.args[0]).reason == "TRANSPORT_UNAVAILABLE"
        for c in target.sink.await_args_list
    )


async def test_strict_poll_row_budget_refuses_before_publication(monkeypatch):
    target = collector(monkeypatch, max_rows=1)
    target.request = AsyncMock(return_value=({"data": [native(), native("eth")]}, RECEIVED))
    assert await target.collect(None) is False
    assert all(c.args[0].kind == "feed_health" for c in target.sink.await_args_list)


async def test_poll_clock_regression_does_not_publish(monkeypatch):
    target = collector(monkeypatch)
    times = iter((STARTED, STARTED - 1))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(times))
    target.request = AsyncMock(return_value=({"data": [native()]}, RECEIVED))
    with pytest.raises(IntegrityError, match="LOCAL_CLOCK_REGRESSION"):
        await target.collect(None)
    target.sink.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [("observed_at_ms", str(RECEIVED)), ("event_time_ms", float(LABEL)), ("schema_version", True)],
)
def test_forged_envelope_types_are_not_coerced(field, value):
    event = normalize_row(DailyRow.model_validate(native()), RECEIVED, STARTED)
    health = coinmetrics_poll_health("BTC", PollHealth.model_validate(valid_health()))
    for original, validate in (
        (event, canonical_coinmetrics),
        (health, canonical_coinmetrics_health),
    ):
        with pytest.raises((IntegrityError, ValidationError)):
            validate(original.model_copy(update={field: value}))


@pytest.mark.parametrize(
    "changes",
    [
        {"frequency": "1h"},
        {"window_start_ms": True},
        {"window_start_ms": LABEL - 1},
        {"window_end_ms": LABEL + DAY_MS + 1},
        {"window_start_ms": LABEL + DAY_MS},
        {"window_start_ms": LABEL + 2 * DAY_MS},
        {"window_end_ms": LABEL + 2 * DAY_MS},
        {"window_start_ms": LABEL - 30 * DAY_MS},
    ],
)
def test_health_exact_closed_daily_window_refusals(changes):
    with pytest.raises(ValidationError):
        PollHealth.model_validate({**valid_health(), **changes})


@pytest.mark.parametrize(
    "status,raw,reason",
    [(403, b'{"error":"denied"}', "ACCESS_DENIED"), (200, b"not json", "INVALID_DATA")],
)
async def test_complete_http_failure_retains_actual_response_receipt(
    monkeypatch, status, raw, reason
):
    clocks = iter((STARTED, RECEIVED, FINISHED))
    monkeypatch.setattr("selective_options.coinmetrics.now_ms", lambda: next(clocks))
    sink = AsyncMock()
    target = CoinMetricsCollector(sink)
    target.response_received = AsyncMock()
    assert await target.collect(Session(Response(raw, status))) is False
    target.response_received.assert_awaited_once()
    for call in sink.await_args_list:
        health = canonical_coinmetrics_health(call.args[0])
        assert health.reason == reason
        assert health.response_received_at_ms == RECEIVED
        assert health.window_start_ms == LABEL - 5 * DAY_MS
        assert health.window_end_ms == LABEL + DAY_MS
        assert not health.accepted_observations
