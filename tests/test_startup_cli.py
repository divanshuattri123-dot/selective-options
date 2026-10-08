import json
import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from selective_options.cli import main
from selective_options.config import AppConfig, load_config
from selective_options.errors import IntegrityError, TradingUnavailable, TransportError
from selective_options.interlock import LiveEngine
from selective_options.startup import inspect_connection, probe

CONFIG = Path(__file__).resolve().parents[1] / "config"


async def test_probe_reports_no_trade_even_with_good_public_quotes(
    fake_rpc, instrument, summary, ticker, tmp_path
):
    config = AppConfig.model_validate({"system": {"assets": ["BTC", "PAXG"]}})
    rpc = fake_rpc([int(time.time() * 1000), [instrument], [summary], ticker, []])
    report = await inspect_connection(rpc, config, chain_dir=tmp_path)
    assert report.assets[0].two_sided_count == 1
    assert not report.assets[0].tradable
    assert report.assets[1].capability == "GOLD_UNTRADEABLE"
    assert not report.trading_enabled
    assert (
        json.loads((tmp_path / "BTC-chain.json").read_text())["rows"][0]["summary"]["bid_price"]
        == 0.02
    )


async def test_stale_missing_crossed_quotes_never_imply_liquid_chain(
    fake_rpc, instrument, summary, ticker
):
    config = AppConfig.model_validate(
        {"system": {"assets": ["BTC"]}, "venue": {"ticker_samples_per_asset": 1}}
    )
    summary.update(creation_timestamp=1, ask_price=0.01)
    rpc = fake_rpc(
        [
            int(time.time() * 1000),
            [instrument, dict(instrument, instrument_name="second")],
            [summary],
            ticker,
        ]
    )
    report = await inspect_connection(rpc, config)
    result = report.assets[0]
    assert result.missing_summary_count == result.stale_summary_count == 1
    assert result.two_sided_count == 0 and "STALE_SUMMARIES" in result.reason


async def test_clock_skew_fails_before_discovery(fake_rpc):
    rpc = fake_rpc([1])
    with pytest.raises(IntegrityError, match="CLOCK_SKEW"):
        await inspect_connection(rpc, AppConfig())
    assert len(rpc.calls) == 1


async def test_stale_detailed_ticker_halts(fake_rpc, instrument, summary, ticker):
    ticker["timestamp"] = 1
    rpc = fake_rpc([int(time.time() * 1000), [instrument], [summary], ticker])
    with pytest.raises(IntegrityError, match="STALE_TICKER"):
        await inspect_connection(rpc, AppConfig.model_validate({"system": {"assets": ["BTC"]}}))


async def test_authenticated_path_checks_and_refreshes(monkeypatch, fake_rpc):
    monkeypatch.setenv("DERIBIT_TESTNET_CLIENT_ID", "fixture-id")
    monkeypatch.setenv("DERIBIT_TESTNET_CLIENT_SECRET", "fixture-secret")
    authenticate, refresh = AsyncMock(), AsyncMock()
    monkeypatch.setattr("selective_options.startup.AuthSession.authenticate", authenticate)
    monkeypatch.setattr("selective_options.startup.AuthSession.ensure_fresh", refresh)
    result = await inspect_connection(
        fake_rpc([int(time.time() * 1000), []]),
        AppConfig.model_validate({"system": {"assets": ["BTC"]}}),
        True,
    )
    assert result.authenticated_checks == "PASSED_ON_THIS_CONNECTION"
    authenticate.assert_awaited_once()
    refresh.assert_awaited_once()


async def test_public_reconnect_is_bounded_and_preserves_limiter(monkeypatch):
    limiters = []

    class BrokenConnection:
        def __init__(self, config, limiter):
            limiters.append(limiter)

        async def __aenter__(self):
            raise TransportError("down")

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("selective_options.startup.WebSocketRpc", BrokenConnection)
    monkeypatch.setattr("selective_options.startup.asyncio.sleep", AsyncMock())
    with pytest.raises(TransportError):
        await probe(AppConfig())
    assert len(limiters) == 3 and len({id(x) for x in limiters}) == 1
    limiters.clear()
    with pytest.raises(TransportError):
        await probe(AppConfig(), authenticated=True)
    assert len(limiters) == 1


async def test_probe_wrapper_success(monkeypatch, fake_rpc):
    rpc = fake_rpc([int(time.time() * 1000), []])

    class Connection:
        def __init__(self, *args):
            pass

        async def __aenter__(self):
            return rpc

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("selective_options.startup.WebSocketRpc", Connection)
    result = await probe(AppConfig.model_validate({"system": {"assets": ["BTC"]}}))
    assert result.assets[0].capability == "NO_OPTION_CHAIN"


async def test_production_probe_refused():
    with pytest.raises(TradingUnavailable):
        await probe(load_config(CONFIG, "production"))


def test_arm_always_fails():
    with pytest.raises(TradingUnavailable, match="MILESTONE_INCOMPLETE"):
        LiveEngine().arm()


@pytest.mark.parametrize("command,code", [("check-config", 0), ("arm", 2)])
def test_cli_main(monkeypatch, capsys, command, code):
    monkeypatch.setattr("sys.argv", ["selective-options", "--config-dir", str(CONFIG), command])
    assert main() == code
    if code == 0:
        assert json.loads(capsys.readouterr().out)["trading_enabled"] is False


def test_cli_safe_validation_error(monkeypatch, capsys):
    monkeypatch.setenv("SELECTIVE_GATE__MIN_CONFIDENCE_EACH", "secret-invalid-value")
    monkeypatch.setattr(
        "sys.argv", ["selective-options", "--config-dir", str(CONFIG), "check-config"]
    )
    assert main() == 2
    captured = capsys.readouterr()
    assert "secret-invalid-value" not in captured.err


def test_cli_probe_writes_report(monkeypatch, fake_rpc, tmp_path):
    import asyncio

    report = asyncio.run(
        inspect_connection(
            fake_rpc([int(time.time() * 1000), []]),
            AppConfig.model_validate({"system": {"assets": ["PAXG"]}}),
        )
    )
    monkeypatch.setattr("selective_options.cli.probe", AsyncMock(return_value=report))
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "--config-dir",
            str(CONFIG),
            "probe",
            "--output",
            str(tmp_path / "probe.json"),
        ],
    )
    assert main() == 0
    assert json.loads((tmp_path / "probe.json").read_text())["milestone"] == 1


def test_cli_interrupt_and_unexpected_error(monkeypatch):
    monkeypatch.setattr(
        "sys.argv", ["selective-options", "--config-dir", str(CONFIG), "check-config"]
    )
    monkeypatch.setattr(
        "selective_options.cli.load_config",
        lambda *args: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    assert main() == 130
    monkeypatch.setattr(
        "selective_options.cli.load_config",
        lambda *args: (_ for _ in ()).throw(RuntimeError("do not echo")),
    )
    assert main() == 2
