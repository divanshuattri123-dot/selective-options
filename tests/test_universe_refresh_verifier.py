"""Saved lifecycle checks with synthetic captures, not public availability evidence."""

import importlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.store import DuckStore
from tests.test_universe_refresh import control, quote

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("complete", [True, False])
async def test_finite_verifier_distinguishes_actual_acknowledged_cycle(
    monkeypatch, tmp_path, complete
):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_universe_refresh")

    async def capture(config, output, duration):
        assert config.ingestion.stream_universe_refresh_interval_s == 60
        assert config.ingestion.max_stream_instruments_per_asset == 2
        assert config.system.assets == ("BTC",)
        rows = [
            control("STREAM_RESYNC", 1000),
            control("STREAM_SUBSCRIBED", 1001),
            quote(receipt=1001),
        ]
        if complete:
            rows.extend(
                [
                    control("STREAM_REFRESH_DUE", 1100),
                    control("STREAM_RESYNC", 1101),
                    control("STREAM_SUBSCRIBED", 1102, ("B",)),
                    quote("B", 1102, 1102),
                ]
            )
        output.mkdir()
        store = await DuckStore.open(output / "events.duckdb")
        try:
            await store.append(rows)
        finally:
            await store.close()
        return {
            "deribit_universe_refreshes": int(complete),
            "deribit_connections": 2 if complete else 1,
            "rows": len(rows),
        }

    monkeypatch.setattr(verifier, "capture", capture)
    report = await verifier.verify(tmp_path / "capture")
    assert report["status"] == (
        "PUBLIC_CAPPED_REFRESH_VERIFIED" if complete else "REFRESH_CYCLE_NOT_OBSERVED"
    )
    assert report["inactive_quote_checks"] == 2 * int(complete)
    assert report["full_universe_stream_verified"] is False
    assert report["trade_history_continuity_verified"] is False
    assert report["trading_enabled"] is False


@pytest.mark.parametrize("duration", [89, 181, float("nan"), float("inf")])
async def test_invalid_duration_refuses_before_capture(monkeypatch, tmp_path, duration):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    verifier = importlib.import_module("verify_universe_refresh")
    mocked = AsyncMock()
    monkeypatch.setattr(verifier, "capture", mocked)
    with pytest.raises(ValueError):
        await verifier.verify(tmp_path / "capture", duration)
    mocked.assert_not_awaited()
