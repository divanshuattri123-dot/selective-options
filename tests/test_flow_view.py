import asyncio
from pathlib import Path

import duckdb
import pytest

from selective_options.binance import Instrument
from selective_options.binance_accounts import account_events
from selective_options.binance_taker import taker_events
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.flow_view import flow_at, flow_view_summary
from selective_options.store import DuckStore


def event(kind, received=1_000_000, value="1.5000", end=900_000):
    instrument = Instrument(
        symbol="BTCUSDT",
        baseAsset="BTC",
        quoteAsset="USDT",
        marginAsset="USDT",
        contractType="PERPETUAL",
        status="TRADING",
    )
    if kind == "taker_flow":
        return taker_events(
            instrument,
            [
                {
                    "timestamp": end - 300_000,
                    "buyVol": value,
                    "sellVol": "2.0000",
                    "buySellRatio": "0.7500",
                }
            ],
            received,
            received,
        )[0]
    return account_events(
        instrument,
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": end,
                "longAccount": "0.6000",
                "shortAccount": "0.4000",
                "longShortRatio": value,
            }
        ],
        received,
        received,
    )[0]


def view(rows, flow_kind, asof=1_300_000, **changes):
    args = dict(asset="BTC", symbol="BTCUSDT", kind=flow_kind, as_of_ms=asof)
    args.update(changes)
    return flow_at(rows, **args)


@pytest.mark.parametrize("kind", ["taker_flow", "account_ratio"])
async def test_a_b_a_and_exact_poll_replay_survive_storage(tmp_path, kind):
    a, b, again = event(kind), event(kind, 1_100_000, "1.6000"), event(kind, 1_200_000)
    assert len({r.event_id for r in [a, b, again]}) == 3
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append([a, b, again, again])
        rows = (await store.snapshot("BTC", 1_300_000, "public_reference")).rows()
        assert len(rows) == 3
        assert view(rows, kind, 999_999) == ()
        assert view(rows, kind, 1_000_000) == (a,)
        assert view(rows, kind, 1_100_000) == (b,)
        assert view(rows, kind, 1_200_000) == (again,)
        assert view(reversed(rows), kind) == (again,)
        assert await store.pending_delivery_count() == 3
    finally:
        await store.close()


@pytest.mark.parametrize("kind", ["taker_flow", "account_ratio"])
def test_tied_revisions_refuse_and_future_cannot_change_prior_view(kind):
    a = event(kind)
    changed = event(kind, 1_000_000, "1.6000")
    with pytest.raises(IntegrityError, match="AMBIGUOUS_REVISION"):
        view([a, changed], kind)
    future = event(kind, 1_100_000, "1.6000").model_copy(update={"payload_json": "{invalid"})
    assert view([a, future], kind, 1_000_000) == (a,)
    assert (
        flow_view_summary(
            [a, changed], asset="BTC", symbol="BTCUSDT", kind=kind, as_of_ms=1_000_000
        )["state"]
        == "BLOCKED"
    )


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "bad"},
        {"timestamp_kind": "receipt"},
        {"payload_json": "{}"},
        {"observed_at_ms": 1_000_001},
        {"payload_json": "[]"},
    ],
)
def test_corrupt_available_observation_is_not_silently_skipped(change):
    bad = event("account_ratio").model_copy(update=change)
    with pytest.raises(IntegrityError, match="OBSERVATION_INVALID"):
        view([bad], "account_ratio")


@pytest.mark.parametrize(
    "change",
    [
        {"source": "other"},
        {"environment": "testnet"},
        {"asset": "ETH"},
        {"symbol": "ETHUSDT"},
        {"kind": "spot"},
    ],
)
def test_scope_isolation(change):
    irrelevant = event("taker_flow").model_copy(update={**change, "payload_json": "invalid"})
    original = event("taker_flow")
    assert view([original, irrelevant], "taker_flow") == (original,)


@pytest.mark.parametrize(
    "args",
    [
        {"as_of_ms": 0},
        {"as_of_ms": True},
        {"max_rows": 0},
        {"max_rows": 100_001},
        {"asset": " "},
        {"kind": "spot"},
    ],
)
def test_arguments_and_budget(args):
    with pytest.raises(ValueError):
        view([], "taker_flow", **args)


def test_eligible_budget_and_empty_summary():
    row = event("taker_flow")
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        view([row, row], "taker_flow", max_rows=1)
    assert view([row, event("taker_flow", 1_100_000)], "taker_flow", 1_000_000, max_rows=1) == (
        row,
    )
    summary = flow_view_summary(
        [], asset="BTC", symbol="BTCUSDT", kind="account_ratio", as_of_ms=1_000_000
    )
    assert summary["state"] == "MISSING" and summary["trading_approval"] is False


@pytest.mark.parametrize(
    "kind,directory",
    [
        ("taker_flow", "binance-taker-public-02"),
        ("account_ratio", "binance-accounts-public-01"),
    ],
)
def test_legacy_saved_public_rows_and_old_neighbors(kind, directory):
    path = Path("evidence") / directory / "events.duckdb"
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events WHERE asset='BTC' AND kind=?", [kind])
        columns = [c[0] for c in result.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
            for r in result.fetchall()
        ]
    assert len(rows) == 3 and all("observation_received_at_ms" not in r.payload() for r in rows)
    selected = view(rows, kind, max(r.observed_at_ms for r in rows))
    assert len(selected) == 3 and [r.event_time_ms for r in selected] == sorted(
        r.event_time_ms for r in rows
    )
    assert view(rows, kind, min(r.observed_at_ms for r in rows) - 1) == ()


def test_legacy_duplicate_receipts_keep_earliest():
    row = event("account_ratio")
    payload = row.payload()
    del payload["observation_received_at_ms"]
    legacy = make_event(
        source=row.source,
        environment=row.environment,
        asset=row.asset,
        symbol=row.symbol,
        kind=row.kind,
        event_time_ms=row.event_time_ms,
        observed_at_ms=row.observed_at_ms,
        native_id=str(row.event_time_ms),
        payload=payload,
    )
    late = legacy.model_copy(update={"observed_at_ms": 1_100_000})
    assert view([late, legacy], "account_ratio") == (legacy,)


@pytest.mark.parametrize(
    "kind,switch,cls",
    [
        ("taker_flow", "binance_taker_enabled", "BinanceTakerCollector"),
        ("account_ratio", "binance_account_ratio_enabled", "BinanceAccountCollector"),
    ],
)
async def test_capture_reports_revision_view(tmp_path, monkeypatch, kind, switch, cls):
    from selective_options.collectors import now_ms
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    async def run(self):
        received = now_ms() - 20
        end = received - 100_000
        await self.sink(event(kind, received, "1.5000", end))
        await self.sink(event(kind, received + 10, "1.6000", end))
        await idle()

    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    monkeypatch.setattr("selective_options.ingestion." + cls + ".run", run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                switch: True,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "capture", 0.1)
    assert report["rows"] == 2
    summary = report["flow_revision_views"][0]
    assert summary["state"] == "REVISION_VIEW_VERIFIED" and summary["selected_periods"] == 1
    assert summary["kind"] == kind and summary["trading_approval"] is False
