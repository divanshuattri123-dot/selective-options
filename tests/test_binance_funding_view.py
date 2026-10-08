import json
from pathlib import Path

import duckdb
import pytest

from selective_options.binance import Instrument
from selective_options.binance_funding_history import parse_page
from selective_options.binance_funding_view import funding_history_at, funding_view_summary
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.store import DuckStore


def event(ts=4000, receipt=10000, rate="0.00010000", **fields):
    instrument = Instrument(
        symbol="BTCUSDT",
        baseAsset="BTC",
        quoteAsset="USDT",
        marginAsset="USDT",
        contractType="PERPETUAL",
        status="TRADING",
    )
    return parse_page(
        instrument,
        [
            {
                "symbol": "BTCUSDT",
                "fundingTime": ts,
                "fundingRate": rate,
                "markPrice": "12345.6000",
                "rateType": "Regular",
                **fields,
            }
        ],
        receipt,
        ts,
        ts,
        1,
    )[0]


def view(rows, **changes):
    return funding_history_at(
        rows,
        **{
            "asset": "BTC",
            "symbol": "BTCUSDT",
            "start_ms": 1000,
            "end_ms": 9500,
            "as_of_ms": 14000,
            **changes,
        },
    )


async def test_a_b_a_replay_and_unordered_window_survive_store(tmp_path):
    a, b, again = event(), event(receipt=11000, rate="-0.00020000"), event(receipt=12000)
    other = event(8000, 10001)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([again, a, b, other, again])
        snapshot = (await store.snapshot("BTC", 14000, "public_reference")).rows()
        assert len(snapshot) == 4 and await store.pending_delivery_count() == 4
        assert view(snapshot, as_of_ms=9999) == ()
        assert view(snapshot, as_of_ms=10000) == (a,)
        assert view(snapshot, as_of_ms=11000) == (b, other)
        assert view(snapshot, as_of_ms=12000) == (again, other)
        assert view(reversed(snapshot)) == view(snapshot)
        assert view([again, again, other]) == (again, other)
        assert view(snapshot, start_ms=4000, end_ms=4000) == (again,)
        assert view(snapshot, start_ms=4001) == (other,)
    finally:
        await store.close()


def test_conflicting_latest_receipt_refuses_and_later_clear_revision_resolves():
    a, conflict = event(), event(rate="0.00020000")
    with pytest.raises(IntegrityError, match="AMBIGUOUS_REVISION"):
        view([a, conflict])
    latest = event(receipt=11000)
    assert view([a, conflict, latest]) == (latest,)
    with pytest.raises(IntegrityError, match="AMBIGUOUS_REVISION"):
        view([a, conflict, latest], as_of_ms=10000)
    summary = funding_view_summary(
        [a, conflict], asset="BTC", symbol="BTCUSDT", start_ms=1000, end_ms=9500, as_of_ms=10000
    )
    assert summary["state"] == "BLOCKED" and summary["selected_charges"] == 0


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "bad"},
        {"payload_json": "{}"},
        {"payload_json": "[]"},
        {"observed_at_ms": 10001},
        {"event_time_ms": 4001},
        {"timestamp_kind": "receipt"},
    ],
)
def test_corrupt_eligible_observation_is_not_ignored(change):
    with pytest.raises(IntegrityError, match="OBSERVATION_INVALID"):
        view([event().model_copy(update=change)])


@pytest.mark.parametrize(
    "change",
    [
        {"source": "bybit_history"},
        {"asset": "ETH"},
        {"symbol": "ETHUSDT"},
        {"kind": "perpetual"},
        {"environment": "testnet"},
        {"observed_at_ms": 15000},
        {"event_time_ms": 15000},
        {"event_time_ms": 999},
        {"event_time_ms": 9501},
    ],
)
def test_scope_window_and_both_future_clocks_filter_before_validation(change):
    valid = event()
    bad = valid.model_copy(update={**change, "payload_json": "invalid"})
    assert view([valid, bad]) == (valid,)


def test_window_can_extend_beyond_as_of_without_exposing_future_charge():
    row = event(9000, 9500)
    assert view([row], as_of_ms=8999) == ()
    assert view([row], as_of_ms=9499) == ()
    assert view([row], as_of_ms=9500) == (row,)


@pytest.mark.parametrize(
    "change",
    [
        {"asset": " "},
        {"symbol": " "},
        {"start_ms": 0},
        {"end_ms": 0},
        {"start_ms": 9501},
        {"start_ms": True},
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_rows": 0},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
def test_invalid_projection_arguments(change):
    with pytest.raises(ValueError):
        view([], **change)


def test_budget_counts_replays_but_excludes_future_and_other_scopes():
    row = event()
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        view([row, row], max_rows=1)
    assert view([row, event(receipt=15000)], max_rows=1) == (row,)
    assert view([row, event().model_copy(update={"asset": "ETH"})], max_rows=1) == (row,)


def test_reported_types_are_retained_without_interval_or_rate_sum():
    special = event(rate="-0.0000", rateType="Special")
    unknown = event(8000, rateType=None)
    selected = view([special, unknown])
    assert selected[0].payload()["reported_rate_type"] == "Special"
    assert selected[1].payload()["reported_rate_type"] is None
    assert all(row.payload()["funding_interval_minutes"] is None for row in selected)
    summary = funding_view_summary(
        selected, asset="BTC", symbol="BTCUSDT", start_ms=1000, end_ms=9500, as_of_ms=14000
    )
    assert summary["state"] == "REVISION_VIEW_VERIFIED" and summary["selected_charges"] == 2
    assert summary["latest_selected_receipt_ms"] == 10000 and summary["trading_approval"] is False
    assert summary["historical_schedule_completeness"] == "NOT_PROVEN"
    empty = funding_view_summary(
        [], asset="BTC", symbol="BTCUSDT", start_ms=1000, end_ms=9500, as_of_ms=14000
    )
    assert empty["state"] == "MISSING" and empty["latest_funding_time_ms"] is None


def test_saved_public_receipts_and_window_selection():
    root = Path("evidence/binance-funding-public-01")
    report = json.loads((root / "report.json").read_text())
    with duckdb.connect(str(root / "events.duckdb"), read_only=True) as db:
        result = db.execute("SELECT * FROM events")
        columns = [c[0] for c in result.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
            for r in result.fetchall()
        ]
    policy = {"start_ms": report["start_ms"], "end_ms": report["end_ms"]}
    assert len(view(rows, as_of_ms=report["ended_at_ms"], **policy)) == 6
    assert view(rows, as_of_ms=min(r.observed_at_ms for r in rows) - 1, **policy) == ()
    for receipt in sorted({r.observed_at_ms for r in rows}):
        expected = tuple(
            sorted((r for r in rows if r.observed_at_ms <= receipt), key=lambda r: r.event_time_ms)
        )
        assert view(rows, as_of_ms=receipt, **policy) == expected
