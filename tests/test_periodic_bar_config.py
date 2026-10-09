"""Periodic snapshots are explicit bounded ingestion configuration."""

import pytest
from pydantic import ValidationError

from selective_options.config import AppConfig


def test_periodic_bar_defaults_disabled_and_explicit_policy_validates():
    config = AppConfig()
    assert config.ingestion.bar_checkpoint_interval_s is None
    assert config.ingestion.bar_checkpoint_max_generations == 32
    config = AppConfig.model_validate(
        {
            "ingestion": {
                "spot_trades_enabled": True,
                "bar_checkpoint_interval_s": 1,
                "bar_checkpoint_max_generations": 2,
            }
        }
    )
    assert config.ingestion.bar_checkpoint_interval_s == 1
    assert config.ingestion.bar_checkpoint_max_generations == 2
    assert config.system.mode == "testnet"


@pytest.mark.parametrize(
    "field,value",
    [
        ("bar_checkpoint_interval_s", True),
        ("bar_checkpoint_interval_s", 0),
        ("bar_checkpoint_interval_s", 86401),
        ("bar_checkpoint_interval_s", 1.0),
        ("bar_checkpoint_max_generations", True),
        ("bar_checkpoint_max_generations", 0),
        ("bar_checkpoint_max_generations", 257),
        ("bar_checkpoint_max_generations", 1.0),
    ],
)
def test_periodic_bar_limits_are_strict(field, value):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"ingestion": {"spot_trades_enabled": True, field: value}})


def test_periodic_bars_require_spot_trade_collection():
    with pytest.raises(
        ValidationError, match="periodic bar checkpoints require enabled spot trades"
    ):
        AppConfig.model_validate(
            {"ingestion": {"bar_checkpoint_interval_s": 1, "spot_trades_enabled": False}}
        )
