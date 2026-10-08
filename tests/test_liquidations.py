"""Strict native liquidation snapshots and finite public session evidence."""

import asyncio
import copy
import json

import pytest

from selective_options import liquidations as mod
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError


def native(clock=1000, **changes):
    row = {
        "e": "forceOrder",
        "E": clock,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "o": "LIMIT",
            "f": "IOC",
            "q": "1.000",
            "p": "60000.123456789012345678901234567890",
            "ap": "60000.00",
            "X": "FILLED",
            "l": "1.000",
            "z": "1.000",
            "T": clock - 1,
        },
    }
    row.update(changes)
    return row


def health(clock=1100, state="CONNECTED", reason="SUBSCRIPTION_ACKNOWLEDGED", row=None, **changes):
    body = dict(
        state=state,
        ok=state == "CONNECTED",
        reason=reason,
        session_id="a" * 32,
        session_started_at_ms=900,
        streams=("btcusdt@forceOrder",),
        ignored_unrequested_frames=0,
        accepted_event_id=None if row is None else row.event_id,
        accepted_observed_at_ms=None if row is None else row.observed_at_ms,
    )
    body.update(changes)
    return mod.health_event("BTC", clock, **body)


def view(rows, clock=1300, **changes):
    return mod.liquidations_quality(rows, asset="BTC", as_of_ms=clock, **changes)


def collector(rows):
    async def sink(row):
        rows.append(row)

    return mod.LiquidationCollector(AppConfig(system={"assets": ["BTC"]}), sink)


def combined(raw):
    return {"stream": raw["o"]["s"].lower() + "@forceOrder", "data": raw}


def test_native_precision_identity_and_option_absence():
    row = mod.native_event(native(), 1100, "BTC")
    assert row.payload()["native"] == native()
    assert "st" not in row.payload()["native"]
    assert mod.native_event(native(), 1200, "BTC").event_id == row.event_id
    assert row.payload()["trading_approval"] is False


@pytest.mark.parametrize(
    "key,value",
    [
        ("q", "0"),
        ("q", "-1"),
        ("q", "1e1"),
        ("q", 1),
        ("q", "+1"),
        ("q", "01"),
        ("q", "NaN"),
        ("z", "2"),
        ("l", "2"),
        ("z", "0"),
        ("S", "LONG"),
        ("s", "ETHUSDT"),
        ("T", True),
        ("T", 1001),
        ("unknown", "1"),
    ],
)
def test_invalid_native_order(key, value):
    raw = native()
    raw["o"][key] = value
    with pytest.raises((ValueError, IntegrityError)):
        mod.native_event(raw, 1100, "BTC")


@pytest.mark.parametrize(
    "raw,receipt",
    [
        (native(E=True), 1100),
        (native(E=70000), 1100),
        (native(E=1), 70000),
        (native(st=2), 1100),
        (native(ps="OTHER"), 1100),
        (native(), True),
    ],
)
def test_invalid_native_outer_clocks(raw, receipt):
    with pytest.raises((ValueError, IntegrityError)):
        mod.native_event(raw, receipt, "BTC")


@pytest.mark.parametrize(
    "frame",
    ['{"a":1,"a":2}', '{"a":NaN}', "[]", '"x"', b"x" * (mod.MAX_FRAME_BYTES + 1)],
    ids=["duplicate", "nonfinite", "list", "string", "oversize"],
)
def test_frame_guard(frame):
    with pytest.raises((ValueError, IntegrityError)):
        mod.decode_frame(frame)


def test_quiet_market_is_unobserved_and_stale_fails():
    quiet = view([health()])
    assert quiet["state"] == "LOSSY_STREAM_SESSION_OBSERVED"
    assert quiet["observation_state"] == "UNOBSERVED"
    assert quiet["complete_feed_available"] is False
    assert view([health()], 1500, max_health_age_ms=399)["blocker"] == "LIQUIDATION_HEALTH_STALE"


def test_bound_native_quality_and_both_clock_pit():
    row = mod.native_event(native(), 1200, "BTC")
    rows = [health(), row, health(1250, reason="FRAME_OBSERVED", row=row)]
    result = view(rows)
    assert result["selected_session_observations"] == 1
    assert result["latest_observation_event_id"] == row.event_id
    assert view(rows, 1199)["observation_state"] == "UNOBSERVED"
    source_future = row.model_copy(update={"event_time_ms": 1400})
    assert view([health(), source_future])["observation_state"] == "UNOBSERVED"
    assert view(rows, max_rows=2)["blocker"] == "LIQUIDATION_QUALITY_OVER_BUDGET"


def test_failed_health_ties_ack_reference_and_scope():
    row = mod.native_event(native(), 1200, "BTC")
    ack = health()
    bound = health(1250, reason="FRAME_OBSERVED", row=row)
    assert (
        view([ack, bound])["blocker"] == "LIQUIDATION_REFERENCED_OBSERVATION_MISSING_OR_CONFLICTING"
    )
    assert view([row, bound])["blocker"] == "LIQUIDATION_SESSION_ACK_MISSING_OR_AMBIGUOUS"
    failed = health(1300, "DISCONNECTED", "CONNECTION_CLOSED")
    assert view([ack, row, bound, failed])["blocker"] == "LIQUIDATION_STREAM_UNAVAILABLE"
    assert view([ack, bound, row, health(1250)])["blocker"] == "LIQUIDATION_HEALTH_AMBIGUOUS"
    changed = health(1260, reason="FRAME_OBSERVED", session_started_at_ms=901)
    assert view([ack, changed])["blocker"] == "LIQUIDATION_SESSION_ACK_MISSING_OR_AMBIGUOUS"
    assert (
        view([ack, health(1150, "INVALID", "INVALID_DATA"), bound, row])["blocker"]
        == "LIQUIDATION_SESSION_INVALIDATED"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"ok": False},
        {"streams": ("ethusdt@forceOrder",)},
        {"streams": ("btcusdt@forceOrder", "btcusdt@forceOrder")},
        {"session_started_at_ms": 1200},
        {"accepted_observed_at_ms": 1000},
        {"ignored_unrequested_frames": True},
    ],
)
def test_health_semantic_guards(changes):
    with pytest.raises((ValueError, IntegrityError)):
        health(**changes)


@pytest.mark.parametrize(
    "changes",
    [{"asset": "OTHER"}, {"as_of_ms": True}, {"max_rows": 100001}, {"max_health_age_ms": 0}],
)
def test_policy_preflight(changes):
    args = dict(asset="BTC", as_of_ms=1300)
    args.update(changes)
    with pytest.raises(ValueError):
        mod.liquidations_quality(iter(()), **args)


async def test_collector_duplicates_conflicts_late_and_receipt_order(monkeypatch):
    rows = []
    feed = collector(rows)
    feed.session_started_at_ms = 900
    await feed.accept(combined(native()), 1100)
    first = rows[0]
    await feed.accept(combined(native(1200)), 1250)
    latest = rows[1]
    await feed.accept(combined(native()), 1300)
    assert rows == [first, latest]
    assert feed.latest["BTC"] == latest
    with pytest.raises(IntegrityError, match="RECEIPT_REGRESSION"):
        await feed.accept(combined(native()), 1299)
    raw = native()
    raw["o"]["p"] = "60001"
    with pytest.raises(IntegrityError, match="CONFLICT"):
        await feed.accept(combined(raw), 1400)
    monkeypatch.setattr(mod, "MAX_NATIVE_IDENTITIES", 2)
    with pytest.raises(IntegrityError, match="BUDGET"):
        await feed.accept(combined(native(1500)), 1550)


async def test_sink_failure_does_not_accept_cursor():
    async def fail(_row):
        raise OSError("fixture sink")

    feed = mod.LiquidationCollector(AppConfig(system={"assets": ["BTC"]}), fail)
    with pytest.raises(OSError):
        await feed.accept(combined(native()), 1100)
    assert not feed.seen and not feed.latest


async def test_unrequested_and_stream_mismatch():
    rows = []
    feed = collector(rows)
    raw = native()
    raw["o"]["s"] = "ETHUSDT"
    await feed.accept(combined(raw), 1100)
    assert feed.ignored_unrequested_frames == 1 and not rows
    with pytest.raises(IntegrityError, match="MISMATCH"):
        await feed.accept({"stream": "btcusdt@forceOrder", "data": raw}, 1200)


class Socket:
    def __init__(self, frames):
        self.frames = iter(frames)
        self.sent = []

    async def send(self, body):
        self.sent.append(json.loads(body))

    async def recv(self):
        try:
            return next(self.frames)
        except StopIteration:
            raise asyncio.CancelledError from None


async def test_session_preack_buffer_then_ack_and_native(monkeypatch):
    rows = []
    feed = collector(rows)
    feed.session_started_at_ms = 900
    clocks = iter([1100, 1150, 1200, 1250])
    monkeypatch.setattr(mod, "now_ms", lambda: next(clocks))
    socket = Socket(
        [json.dumps(combined(native())), json.dumps({"result": None, "id": mod.REQUEST_ID})]
    )
    with pytest.raises(asyncio.CancelledError):
        await feed.session(socket)
    assert socket.sent == [
        {"method": "SUBSCRIBE", "params": ["btcusdt@forceOrder"], "id": mod.REQUEST_ID}
    ]
    assert view(rows, 1300)["observation_state"] == "NATIVE_SNAPSHOT_OBSERVED"


@pytest.mark.parametrize(
    "frames",
    [
        [json.dumps({"result": None, "id": "wrong"})],
        [json.dumps({"error": "bad"})],
        [json.dumps(combined(native()))] * 33,
    ],
)
async def test_session_ack_and_buffer_guard(frames, monkeypatch):
    rows = []
    feed = collector(rows)
    monkeypatch.setattr(mod, "now_ms", lambda: 1100)
    with pytest.raises(IntegrityError):
        await feed.session(Socket(frames))
    assert not rows


async def test_run_cancel_and_transport_cleanup(monkeypatch):
    for failure, reason in [
        (asyncio.CancelledError(), "CANCELLED"),
        (OSError("fixture"), "TRANSPORT_UNAVAILABLE"),
    ]:
        rows = []
        feed = collector(rows)

        class Context:
            async def __aenter__(self):
                raise failure  # noqa: B023 - context immediately awaited in each loop

            async def __aexit__(self, *_args):
                return False

        monkeypatch.setattr(mod, "connect", lambda *args, **kw: Context())
        monkeypatch.setattr(mod, "now_ms", lambda: 1100)
        with pytest.raises(type(failure)):
            await feed.run()
        assert rows[-1].payload()["reason"] == reason
        assert view(rows)["state"] == "BLOCKED"
        with pytest.raises(IntegrityError, match="ALREADY_STARTED"):
            await feed.run()


async def test_run_invalid_frame_emits_invalid_and_raises(monkeypatch):
    rows = []
    feed = collector(rows)

    class Context:
        async def __aenter__(self):
            return Socket(["{}"])

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(mod, "connect", lambda *args, **kw: Context())
    monkeypatch.setattr(mod, "now_ms", lambda: 1100)
    with pytest.raises(IntegrityError, match="INVALID_STREAM"):
        await feed.run()
    assert rows[-1].payload()["state"] == "INVALID"


def test_conflicting_native_identity_and_corrupt_canonical_evidence():
    raw = native()
    row = mod.native_event(raw, 1200, "BTC")
    changed = copy.deepcopy(raw)
    changed["o"]["ap"] = "60001"
    conflicting = mod.native_event(changed, 1250, "BTC")
    assert view([health(), row, conflicting])["blocker"] == "LIQUIDATION_NATIVE_SNAPSHOT_CONFLICT"
    corrupted = row.model_copy(update={"event_id": "f" * 64})
    assert view([health(), corrupted])["blocker"] == "LIQUIDATION_OBSERVATION_CANONICAL"
