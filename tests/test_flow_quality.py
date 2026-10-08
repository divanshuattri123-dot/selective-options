import asyncio
from pathlib import Path

import duckdb
import pytest

from selective_options.binance import Instrument
from selective_options.binance_accounts import account_events
from selective_options.binance_taker import taker_events
from selective_options.config import AppConfig
from selective_options.events import MarketEvent, make_event
from selective_options.flow_quality import flow_quality
from selective_options.flow_view import SOURCES


def flow(kind="taker_flow", end=1_200_000, receipt=1_300_000, value="1.5000"):
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
                    "sellVol": "2.0",
                    "buySellRatio": "0.75",
                }
            ],
            receipt,
            receipt,
            check_latest=False,
        )[0]
    return account_events(
        instrument,
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": end,
                "longAccount": "0.6",
                "shortAccount": "0.4",
                "longShortRatio": value,
            }
        ],
        receipt,
        receipt,
        check_latest=False,
    )[0]


def health(kind="taker_flow", receipt=1_300_001, ok=True, **changes):
    return make_event(
        **{
            "source": SOURCES[kind],
            "asset": "BTC",
            "symbol": "USDT_PERPETUAL",
            "environment": "public_reference",
            "kind": "feed_health",
            "event_time_ms": receipt,
            "observed_at_ms": receipt,
            "timestamp_kind": "receipt",
            "payload": {"ok": ok, "reason": "OK" if ok else "HTTP_ERROR", "code": 0 if ok else 403},
            **changes,
        }
    )


def window(kind="taker_flow"):
    ends = [600_000, 900_000, 1_200_000]
    receipt = 1_300_000
    return [flow(kind, end, receipt) for end in ends] + [health(kind, receipt + 1)]


def check(rows, flow_kind="taker_flow", **changes):
    return flow_quality(
        rows,
        **{
            "asset": "BTC",
            "symbol": "BTCUSDT",
            "kind": flow_kind,
            "as_of_ms": 1_700_000,
            **changes,
        },
    )


@pytest.mark.parametrize("kind", ["taker_flow", "account_ratio"])
def test_window_and_actual_revision_receipts(kind):
    rows = window(kind)
    result = check(rows, kind)
    assert result["state"] == "OBSERVED_WINDOW_AVAILABLE" and result["blockers"] == []
    assert result["continuous_history_verified"] is result["trading_approval"] is False
    changed = flow(kind, rows[0].event_time_ms, 1_650_000, "1.6000")
    assert "HEALTH_PRECEDES_FLOW" in check(rows + [changed], kind)["blockers"]
    assert (
        check(rows + [changed, health(kind, 1_650_001)], kind)["state"]
        == "OBSERVED_WINDOW_AVAILABLE"
    )
    assert check(rows + rows, kind) == result


@pytest.mark.parametrize("kind", ["taker_flow", "account_ratio"])
def test_missing_middle_and_no_unobserved_period_fill(kind):
    rows = window(kind)
    result = check([rows[0], rows[2], rows[3]], kind)
    assert "MISSING_RECENT_PERIODS" in result["blockers"]
    assert result["missing_period_ends_ms"] == [rows[1].event_time_ms]
    assert check(rows, kind, as_of_ms=rows[0].observed_at_ms - 1)["blockers"] == [
        "MISSING_FLOW",
        "MISSING_HEALTH",
    ]


@pytest.mark.parametrize(
    "rows,policy,blocker",
    [
        ([], {}, "MISSING_FLOW"),
        (window()[:3], {}, "MISSING_HEALTH"),
        (window() + [health(receipt=1_500_000, ok=False)], {}, "SOURCE_UNAVAILABLE"),
        (window(), {"as_of_ms": 1_800_001}, "STALE_FLOW_PERIOD"),
        (window(), {"max_observation_age_ms": 399_999}, "STALE_FLOW_OBSERVATION"),
        (window(), {"max_health_age_ms": 399_998}, "STALE_HEALTH"),
        (window()[:3] + [health(receipt=1_299_999)], {}, "HEALTH_PRECEDES_FLOW"),
        (window() + [health(ok=False)], {}, "FLOW_HEALTH_AMBIGUOUS"),
        ([flow(end=1_200_001), health()], {"required_periods": 1}, "FLOW_PERIOD_ALIGNMENT"),
        (window() + [flow(value="1.6000")], {}, "FLOW_VIEW_AMBIGUOUS_REVISION"),
        (window(), {"max_rows": 3}, "FLOW_QUALITY_OVER_BUDGET"),
    ],
)
def test_explicit_blockers(rows, policy, blocker):
    result = check(rows, **policy)
    assert result["state"] == "BLOCKED" and blocker in result["blockers"]


def test_age_boundaries_and_future_failure_do_not_leak():
    rows = window()
    assert check(rows, as_of_ms=1_800_000)["state"] == "OBSERVED_WINDOW_AVAILABLE"
    future = health(receipt=1_800_001, ok=False)
    assert check(rows + [future], as_of_ms=1_800_000) == check(rows, as_of_ms=1_800_000)
    assert "SOURCE_UNAVAILABLE" in check(rows + [future], as_of_ms=1_800_001)["blockers"]
    assert "STALE_FLOW_OBSERVATION" not in check(rows, as_of_ms=1_900_000)["blockers"]
    assert "STALE_FLOW_OBSERVATION" in check(rows, as_of_ms=1_900_001)["blockers"]


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "bad"},
        {"timestamp_kind": "source"},
        {"event_time_ms": 1_300_000},
        {"payload_json": "[]"},
        {"payload_json": '{"ok":1,"reason":"OK","code":0}'},
        {"payload_json": '{"ok":true,"reason":"OK","code":false}'},
    ],
)
def test_invalid_health_is_refused(change):
    assert (
        "FLOW_HEALTH_INVALID"
        in check(window()[:3] + [health().model_copy(update=change)])["blockers"]
    )


@pytest.mark.parametrize("kind", ["taker_flow", "account_ratio"])
def test_global_discovery_outage_and_asset_recovery(kind):
    rows = window(kind)
    outage = health(kind, 1_650_000, False, asset="GLOBAL")
    result = check(rows + [outage], kind)
    assert "SOURCE_UNAVAILABLE" in result["blockers"] and result["health_scope_asset"] == "GLOBAL"
    assert (
        check(rows + [outage, health(kind, 1_650_001)], kind)["state"]
        == "OBSERVED_WINDOW_AVAILABLE"
    )
    assert (
        "FLOW_HEALTH_INVALID"
        in check(rows + [health(kind, 1_650_000, asset="GLOBAL")], kind)["blockers"]
    )


@pytest.mark.parametrize(
    "change",
    [
        {"asset": "ETH"},
        {"source": "other"},
        {"environment": "testnet"},
        {"symbol": "OTHER"},
        {"kind": "spot"},
    ],
)
def test_other_scopes_and_future_corruption_are_ignored(change):
    rows = window()
    corrupt = health().model_copy(update={**change, "payload_json": "invalid"})
    future_clock = health().model_copy(
        update={"event_time_ms": 1_800_000, "payload_json": "invalid"}
    )
    assert check(rows + [corrupt, future_clock]) == check(rows)
    assert "FLOW_HEALTH_INVALID" in check(rows + [future_clock], as_of_ms=1_800_000)["blockers"]


@pytest.mark.parametrize(
    "policy",
    [
        {"as_of_ms": True},
        {"max_period_age_ms": 0},
        {"max_health_age_ms": True},
        {"max_observation_age_ms": 0},
        {"required_periods": 0},
        {"required_periods": 289},
        {"max_rows": 100_001},
        {"max_rows": False},
        {"asset": " "},
        {"kind": "spot"},
    ],
)
def test_invalid_policy(policy):
    with pytest.raises(ValueError):
        check([], **policy)


@pytest.mark.parametrize(
    "kind,directory",
    [("taker_flow", "binance-taker-public-02"), ("account_ratio", "binance-accounts-public-01")],
)
def test_saved_actual_public_window_expires(kind, directory):
    with duckdb.connect(str(Path("evidence") / directory / "events.duckdb"), read_only=True) as db:
        result = db.execute("SELECT * FROM events WHERE asset='BTC'")
        columns = [c[0] for c in result.description]
        rows = [
            MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
            for r in result.fetchall()
        ]
    clock = max(r.observed_at_ms for r in rows)
    assert check(rows, kind, as_of_ms=clock)["state"] == "OBSERVED_WINDOW_AVAILABLE"
    assert check(rows, kind, as_of_ms=clock + 600_001)["state"] == "BLOCKED"


@pytest.mark.parametrize(
    "kind,switch,cls",
    [
        ("taker_flow", "binance_taker_enabled", "BinanceTakerCollector"),
        ("account_ratio", "binance_account_ratio_enabled", "BinanceAccountCollector"),
    ],
)
@pytest.mark.parametrize("healthy", [True, False])
async def test_capture_reports_window_or_global_failure(
    tmp_path, monkeypatch, kind, switch, cls, healthy
):
    from selective_options.collectors import now_ms
    from selective_options.ingestion import capture

    async def idle(*args):
        await asyncio.Event().wait()

    async def run(self):
        receipt = now_ms() - 10
        if healthy:
            end = receipt // 300_000 * 300_000
            for offset in [600_000, 300_000, 0]:
                await self.sink(flow(kind, end - offset, receipt))
            await self.sink(health(kind, receipt + 1))
        else:
            await self.sink(health(kind, receipt, False, asset="GLOBAL"))
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
    quality = report["flow_quality"][0]
    if healthy:
        assert quality["state"] == "OBSERVED_WINDOW_AVAILABLE" and quality["blockers"] == []
        assert quality["health_scope_asset"] == "BTC"
    else:
        assert quality["state"] == "BLOCKED" and "SOURCE_UNAVAILABLE" in quality["blockers"]
        assert quality["health_scope_asset"] == "GLOBAL"
    assert quality["trading_approval"] is False
