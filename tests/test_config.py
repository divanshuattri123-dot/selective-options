from pathlib import Path

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig, BucketConfig, load_config

CONFIG = Path(__file__).resolve().parents[1] / "config"


def test_default_and_profiles():
    config = load_config(CONFIG)
    assert config.system.assets == ("BTC", "ETH", "SOL", "PAXG")
    assert config.gate.min_expectancy_R == 0.35
    assert config.greeks.max_net_gamma_notional is None
    assert load_config(CONFIG, "production").venue.key_profile == "production"


def test_env_overrides_deep_merge(monkeypatch):
    monkeypatch.setenv("SELECTIVE_GATE__MIN_COMPOSITE_CONVICTION", "80")
    config = load_config(CONFIG)
    assert config.gate.min_composite_conviction == 80
    assert config.gate.min_confidence_each == 0.55


def test_cross_profile_override_rejected(monkeypatch):
    monkeypatch.setenv("SELECTIVE_SYSTEM__MODE", "live")
    monkeypatch.setenv("SELECTIVE_VENUE__KEY_PROFILE", "production")
    monkeypatch.setenv("SELECTIVE_VENUE__WS_URL", "wss://www.deribit.com/ws/api/v2")
    with pytest.raises(ValueError, match="cannot switch"):
        load_config(CONFIG)


@pytest.mark.parametrize(
    "override",
    [
        {"signals": {"weights": {"technical": 0.9}}},
        {"system": {"mode": "live"}},
        {"system": {"assets": ["BTC", "BTC"]}},
        {"venue": {"ws_url": "wss://test.deribit.com.evil.example/ws/api/v2"}},
        {"risk": {"risk_per_trade_pct": 3}},
        {"risk": {"kelly_cap_pct": 3}},
        {"risk": {"allow_naked_short": True}},
        {"execution": {"passive_first": False}},
        {"execution": {"cancel_on_disconnect": False}},
        {"promotion": {"min_paper_days": 1}},
        {"promotion": {"min_paper_trades": 24}},
        {"promotion": {"trades_per_ramp_stage": 19}},
        {"promotion": {"min_live_trades": 39}},
        {"data": {"min_sources_per_asset": 1}},
        {"tax": {"retention_years": 6}},
        {"promotion": {"live_ramp": [0.5, 0.75, 1]}},
        {"structure": {"min_dte": 70}},
        {"structure": {"target_long_delta": [0.6, 0.2]}},
        {"vol": {"sell_premium_above_iv_pct": 20}},
        {"gate": {"min_confidence_each": float("nan")}},
        {"data": {"max_staleness_s": 0}},
        {"unexpected": True},
    ],
)
def test_unsafe_config_fails(override):
    with pytest.raises(ValidationError):
        AppConfig.model_validate(override)


def test_invalid_profile():
    with pytest.raises(ValueError, match="unknown profile"):
        load_config(CONFIG, "../../testnet")


def test_unaffordable_bucket():
    with pytest.raises(ValidationError):
        BucketConfig(capacity=1, refill_per_s=1, cost=2)


def test_default_yaml_matches_typed_defaults():
    assert load_config(CONFIG).model_dump() == AppConfig().model_dump()
