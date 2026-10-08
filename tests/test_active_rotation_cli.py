"""Real configured CLI boundaries for opt-in capture rotation; transport is mocked."""

import json
from unittest.mock import AsyncMock

import pytest
import yaml

from selective_options.cli import main
from selective_options.errors import IntegrityError


def settings(tmp_path, values):
    config_dir = tmp_path / "configuration"
    config_dir.mkdir()
    (config_dir / "default.yaml").write_text(
        yaml.safe_dump({"ingestion": values}), encoding="utf-8"
    )
    (config_dir / "testnet.yaml").write_text("{}\n", encoding="utf-8")
    return config_dir


def invoke(monkeypatch, directory, output):
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "--config-dir",
            str(directory),
            "capture",
            "--output",
            str(output),
            "--duration",
            "2",
        ],
    )
    return main()


def test_real_yaml_optin_and_budgets_reach_capture_without_arming(tmp_path, monkeypatch, capsys):
    values = {
        "archive_rotation_interval_s": 1.5,
        "archive_rotation_partition_ms": 1000,
        "archive_rotation_max_partitions": 2,
        "archive_rotation_max_snapshots": 3,
        "storage_batch_size": 7,
    }
    directory = settings(tmp_path, values)
    operation = AsyncMock(return_value={"status": "CAPTURE_FINISHED", "trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", operation)
    assert invoke(monkeypatch, directory, tmp_path / "new") == 0
    config, output, duration, resume = operation.await_args.args
    for key, value in values.items():
        assert getattr(config.ingestion, key) == value
    assert output == tmp_path / "new" and duration == 2 and resume is None
    assert config.system.mode == config.venue.key_profile == "testnet"
    assert json.loads(capsys.readouterr().out)["trading_enabled"] is False


@pytest.mark.parametrize(
    "values",
    [
        {"archive_rotation_interval_s": float("nan")},
        {"archive_rotation_partition_ms": True},
        {"archive_rotation_max_partitions": 0},
        {"archive_rotation_max_snapshots": 1001},
    ],
)
def test_bad_rotation_policy_refuses_cli_before_capture_or_output(tmp_path, monkeypatch, values):
    directory = settings(tmp_path, values)
    output = tmp_path / "refused"
    operation = AsyncMock()
    monkeypatch.setattr("selective_options.cli.capture", operation)
    assert invoke(monkeypatch, directory, output) == 2
    operation.assert_not_awaited()
    assert not output.exists()


def test_capture_rotation_failure_stays_failed_through_cli(tmp_path, monkeypatch, capsys):
    directory = settings(tmp_path, {"archive_rotation_interval_s": 1})
    output = tmp_path / "failed"

    async def failed(*args, **kwargs):
        output.mkdir()
        (output / "report.json").write_text(
            json.dumps({"status": "FAILED", "trading_enabled": False}), encoding="utf-8"
        )
        raise IntegrityError("ARCHIVE_ROTATION_SNAPSHOT_BUDGET")

    monkeypatch.setattr("selective_options.cli.capture", failed)
    assert invoke(monkeypatch, directory, output) == 2
    assert not capsys.readouterr().out
    assert json.loads((output / "report.json").read_text(encoding="utf-8"))["status"] == "FAILED"
