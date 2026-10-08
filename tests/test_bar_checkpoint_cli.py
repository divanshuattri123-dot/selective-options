"""Operator opt-in policy and explicit CLI routing without transport or orders."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from selective_options import cli
from selective_options.config import AppConfig


@pytest.mark.parametrize("value", [None, "true", 1, 0, [], {}])
def test_checkpoint_opt_in_is_a_strict_boolean(value):
    with pytest.raises(ValidationError):
        AppConfig(ingestion={"bar_checkpoint_on_stop": value})


@pytest.mark.parametrize("value", [0, -1, 100001, True, 1.0, "10", None])
def test_checkpoint_member_budget_is_strict_and_finite(value):
    with pytest.raises(ValidationError):
        AppConfig(ingestion={"bar_checkpoint_max_events": value})


def test_default_checkpoint_policy_is_disabled_without_altering_bar_configuration():
    config = AppConfig()
    assert config.ingestion.bar_checkpoint_on_stop is False
    assert config.ingestion.bar_checkpoint_max_events == 10000
    assert config.ingestion.bar_timeframes_s == (300, 900, 3600, 14400, 86400)


@pytest.mark.parametrize(
    "switch,expected",
    [(None, None), ("--save-bars-on-stop", True), ("--no-save-bars-on-stop", False)],
)
def test_cli_preserves_config_default_and_explicit_boolean_override(
    tmp_path, monkeypatch, capsys, switch, expected
):
    called = []

    async def fake_capture(config, output, duration, checkpoints, **kwargs):
        called.append((config, output, duration, checkpoints, kwargs))
        return {"status": "FIXTURE_ONLY", "trading_enabled": False}

    config = AppConfig(ingestion={"bar_checkpoint_on_stop": True})
    monkeypatch.setattr(cli, "load_config", lambda *_: config)
    monkeypatch.setattr(cli, "capture", fake_capture)
    arguments = [
        "selective-options",
        "capture",
        "--output",
        str(tmp_path / "capture"),
        "--duration",
        "0.1",
    ]
    if switch is not None:
        arguments.append(switch)
    monkeypatch.setattr("sys.argv", arguments)
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["trading_enabled"] is False
    assert len(called) == 1 and not (tmp_path / "capture").exists()
    kwargs = called[0][-1]
    if expected is None:
        assert "save_bars_on_stop" not in kwargs
    else:
        assert kwargs["save_bars_on_stop"] is expected


def test_cli_routes_explicit_checkpoint_directory_without_changing_trade_resume(
    tmp_path, monkeypatch, capsys
):
    called = []

    async def fake_capture(config, output, duration, checkpoints, **kwargs):
        called.append((checkpoints, kwargs))
        return {"status": "FIXTURE_ONLY", "trading_enabled": False}

    monkeypatch.setattr(cli, "load_config", lambda *_: AppConfig())
    monkeypatch.setattr(cli, "capture", fake_capture)
    checkpoint = tmp_path / "checkpoint with spaces"
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--resume-bars-from",
            str(checkpoint),
        ],
    )
    assert cli.main() == 0
    capsys.readouterr()
    assert len(called) == 1 and called[0][0] is None
    assert called[0][1]["resume_bars_from"] == checkpoint
    assert isinstance(called[0][1]["resume_bars_from"], Path)
    assert not checkpoint.exists() and not (tmp_path / "capture").exists()
