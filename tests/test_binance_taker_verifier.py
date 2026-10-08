import json

import pytest

from scripts import verify_binance_taker as verifier
from selective_options.binance_taker import BinanceTakerCollector
from selective_options.bybit import FeedUnavailable
from selective_options.collectors import now_ms


@pytest.mark.parametrize("available", [True, False])
async def test_verifier_reports_fixture_success_and_denial(tmp_path, monkeypatch, available):
    async def request(self, session, endpoint, params):
        received = now_ms()
        if not available:
            raise FeedUnavailable("ACCESS_DENIED", 403)
        if endpoint == "exchangeInfo":
            return {
                "symbols": [
                    {
                        "symbol": asset + "USDT",
                        "baseAsset": asset,
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "contractType": "PERPETUAL",
                        "status": "TRADING",
                    }
                    for asset in self.config.system.assets
                ]
            }, received
        return [
            {
                "timestamp": received - 600_000,
                "buyVol": "1.001",
                "sellVol": "2.002",
                "buySellRatio": "0.5000",
            }
        ], received

    monkeypatch.setattr(BinanceTakerCollector, "request", request)
    output = tmp_path / "fixture"
    await verifier.verify(output)
    report = json.loads((output / "report.json").read_text())
    assert report["trading_enabled"] is False and report["archive_exact_match"]
    assert report["public_requests_attempted"] == (5 if available else 1)
    assert report["public_responses_accepted"] == (5 if available else 0)
    assert report["repeat_deduplication_verified"] == available
    assert report["flow_rows"] == (4 if available else 0)
    assert report["pit_checks"] == report["flow_rows"]
    assert report["status"] == (
        "PUBLIC_TAKER_AGGREGATES_VERIFIED" if available else "PARTIAL_OR_UNAVAILABLE"
    )
    assert report["period_end_not_after_receipt"] == (True if available else None)
