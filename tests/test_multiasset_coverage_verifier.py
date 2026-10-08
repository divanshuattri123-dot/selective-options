"""Multiasset finite verification uses synthetic native events and real closed storage."""

import asyncio
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.collectors import ChannelRegistry, Notification
from selective_options.events import make_event
from selective_options.models import Instrument
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore
from tests.test_option_coverage import control

ROOT = Path(__file__).resolve().parents[1]


def sample(instrument, ticker, empty=False, stale=False):
    rows, manifests, members = [], [], []
    registry = ChannelRegistry()
    for asset in ("ETH", "SOL", "PAXG"):
        present = not (empty and asset == "PAXG")
        symbol = instrument["instrument_name"].replace("BTC", asset)
        parsed = Instrument.model_validate(
            instrument
            | {
                "base_currency": asset,
                "instrument_name": symbol,
                "price_index": asset.lower() + "_usd",
            }
        )
        metadata = make_event(
            source="deribit",
            environment="testnet",
            asset=asset,
            symbol=symbol,
            kind="instrument",
            timestamp_kind="receipt",
            event_time_ms=1000,
            observed_at_ms=1000,
            payload=parsed.model_dump(mode="json"),
        )
        if present:
            rows.append(metadata)
            members.append({"asset": asset, "symbol": symbol})
            registry.add_instrument(asset, symbol, parsed.price_index)
        manifest = control(
            "UNIVERSE_DISCOVERED",
            1100,
            asset=asset,
            available=int(present),
            selected=int(present),
            coverage="FULL" if present else "PARTIAL_OR_ABSENT",
            instruments=[symbol] if present else [],
            selected_instruments=[symbol] if present else [],
            metadata_event_ids=[metadata.event_id] if present else [],
        )
        manifests.append(manifest.event_id)
        rows.append(manifest)
        if present:
            for kind, data in (
                ("ticker", ticker | {"instrument_name": symbol, "timestamp": 1300}),
                (
                    "book",
                    {
                        "instrument_name": symbol,
                        "timestamp": 1300,
                        "change_id": 1,
                        "bids": [[99, 2]],
                        "asks": [[101, 2]],
                    },
                ),
            ):
                channel = (
                    f"ticker.{symbol}.100ms" if kind == "ticker" else f"book.{symbol}.none.20.100ms"
                )
                if stale and asset == "SOL" and kind == "book":
                    data["timestamp"] = 1
                rows.extend(
                    registry.normalize(
                        Notification.model_validate(
                            {
                                "jsonrpc": "2.0",
                                "method": "subscription",
                                "params": {"channel": channel, "data": data},
                            }
                        ),
                        1300,
                    )
                )
    rows.extend(
        [
            control("STREAM_RESYNC", 1200, connection=1),
            control(
                "STREAM_SUBSCRIBED",
                1400,
                connection=1,
                channels=len(registry.channels),
                instruments=members,
                universe_event_ids=manifests,
            ),
        ]
    )
    return rows


@pytest.mark.parametrize("outcome", ["fresh", "stale", "empty", "failure", "cancelled", "mismatch"])
async def test_requested_scope_and_real_closed_reload(
    monkeypatch, tmp_path, instrument, ticker, outcome
):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_option_coverage")
    rows = sample(instrument, ticker, empty=outcome == "empty", stale=outcome == "stale")
    cutoff = 31300 if outcome == "stale" else 1500

    async def capture(cfg, output, duration):
        assert cfg.system.assets == ("ETH", "SOL", "PAXG")
        assert cfg.ingestion.max_stream_instruments_per_asset is None
        assert cfg.ingestion.subscription_batch_size == 100
        assert not cfg.ingestion.derivatives_enabled and not cfg.ingestion.spot_trades_enabled
        assert cfg.venue.key_profile == "testnet" and cfg.system.mode == "testnet"
        assert duration == 60
        output.mkdir()
        store = await DuckStore.open(output / "events.duckdb")
        try:
            await store.append(rows)
            await store.append(rows)
        finally:
            await store.close()
        views = [coverage_at(rows, asset, cutoff, 30000) for asset in cfg.system.assets]
        if outcome == "mismatch":
            views.reverse()
        report = {"ended_at_ms": cutoff, "option_stream_coverage": views}
        (output / "report.json").write_text(json.dumps(report), encoding="utf-8")
        if outcome == "failure":
            raise OSError("synthetic after-commit failure")
        if outcome == "cancelled":
            raise asyncio.CancelledError
        return report

    monkeypatch.setattr(verifier, "capture", capture)
    output = tmp_path / "capture"
    if outcome in {"cancelled", "mismatch"}:
        with pytest.raises(asyncio.CancelledError if outcome == "cancelled" else AssertionError):
            await verifier.verify(output, 60, assets=("ETH", "SOL", "PAXG"))
        assert not (output / "verification.json").exists()
        return
    report = await verifier.verify(output, 60, assets=("ETH", "SOL", "PAXG"))
    assert report["assets"] == ["ETH", "SOL", "PAXG"]
    assert "asset" not in report and "coverage" not in report
    assert report["status"] == (
        "UNCAPPED_COVERAGE_NOT_VERIFIED"
        if outcome in {"empty", "failure"}
        else "UNCAPPED_SUBSCRIPTIONS_OBSERVED"
    )
    assert report["every_requested_asset_has_nonempty_acknowledged_universe"] is (
        outcome != "empty"
    )
    assert report["all_requested_legs_fresh"] is (outcome not in {"empty", "stale"})
    assert report["failure_type"] == ("OSError" if outcome == "failure" else None)
    assert report["saved_rows"] == len(rows)
    assert report["closed_reload_coverage_matches"] is True
    assert report["all_required_assets_or_feeds_verified"] is False
    assert report["sustained_capacity_verified"] is False
    assert report["milestone_2_complete"] is False and report["orders_submitted"] == 0
    if outcome == "empty":
        assert report["option_stream_coverage"][2]["blocker"] == "OPTION_SELECTED_UNIVERSE_EMPTY"
    if outcome == "stale":
        legs = report["option_stream_coverage"][1]["legs"]
        assert next(row for row in legs if row["kind"] == "book")["state"] == "STALE"
        assert next(row for row in legs if row["kind"] == "ticker")["state"] == "OBSERVED"


@pytest.mark.parametrize("assets", [(), ("ETH", "ETH"), ("XAUT",), ("eth",), ("ETH", "invalid")])
async def test_invalid_scope_preflight_no_capture_or_mutation(monkeypatch, tmp_path, assets):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_option_coverage")
    mocked = AsyncMock()
    monkeypatch.setattr(verifier, "capture", mocked)
    output = tmp_path / "capture"
    with pytest.raises(ValueError):
        await verifier.verify(output, assets=assets)
    mocked.assert_not_awaited()
    assert not output.exists()
