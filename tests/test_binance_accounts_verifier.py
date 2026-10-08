import json

import pytest

from scripts import verify_binance_accounts as verifier
from selective_options.binance_accounts import BinanceAccountCollector
from selective_options.bybit import FeedUnavailable
from selective_options.collectors import now_ms


@pytest.mark.parametrize("available", [True, False])
async def test_verifier_fixture_success_and_outage(tmp_path, monkeypatch, available):
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
        assert endpoint == "globalLongShortAccountRatio"
        return [
            {
                "symbol": params["symbol"],
                "timestamp": received - 300_000,
                "longShortRatio": "1.5000",
                "longAccount": "0.6000",
                "shortAccount": "0.4000",
            }
        ], received

    monkeypatch.setattr(BinanceAccountCollector, "request", request)
    output = tmp_path / "fixture"
    await verifier.verify(output)
    report = json.loads((output / "report.json").read_text())
    assert report["trading_enabled"] is False and report["archive_exact_match"]
    assert report["public_requests_attempted"] == (5 if available else 1)
    assert report["public_responses_accepted"] == (5 if available else 0)
    assert report["repeat_deduplication_verified"] == available
    assert report["flow_rows"] == report["pit_checks"] == (4 if available else 0)
    assert report["account_field_semantics_independently_verified"] is False
    assert report["status"] == (
        "PUBLIC_GLOBAL_ACCOUNT_RATIOS_VERIFIED" if available else "PARTIAL_OR_UNAVAILABLE"
    )
