import hashlib
import json
import sys
from decimal import Decimal

import pytest

from selective_options.cli import main
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.research import load_bar_snapshot, research_backtest, research_features
from selective_options.store import DuckStore
from selective_options.trials import read_trials, trial_counts


def backtest_input():
    return {
        "schema_version": 1,
        "data_kind": "synthetic_fixture",
        "provenance": {"description": "manual spread and fee loss; no historical prices"},
        "as_of_ms": 5000,
        "instrument": {
            "instrument_id": "FIXTURE-LINEAR-PREMIUM",
            "currency": "USD",
            "quantity_unit": "contracts",
            "quote_price_unit": "currency_per_underlying_unit",
            "contract_multiplier": "10",
            "accounting": "single_instrument_long_only_linear_premium",
            "funding_required": False,
            "settlement_required": False,
            "transfer_tax_required": False,
        },
        "fees": {
            "policy_id": "synthetic-taker-fee-0.001",
            "maker_rate": "0",
            "taker_rate": "0.001",
            "basis": "executed_premium_notional",
        },
        "policy": {
            "latency_ms": 50,
            "max_quote_age_ms": 500,
            "min_displayed_contracts": "1",
            "adverse_slippage_rate": "0",
            "pessimistic": True,
            "passive_model": "strict_later_executable_crossing_taker_assumption",
        },
        "orders": [
            {
                "order_id": "buy",
                "instrument_id": "FIXTURE-LINEAR-PREMIUM",
                "side": "buy",
                "order_type": "market",
                "contracts": "2",
                "submitted_at_ms": 1000,
            },
            {
                "order_id": "sell",
                "instrument_id": "FIXTURE-LINEAR-PREMIUM",
                "side": "sell",
                "order_type": "market",
                "contracts": "2",
                "submitted_at_ms": 3000,
            },
        ],
        "quotes": [
            {
                "quote_id": "entry",
                "liquidity_id": "entry-native-allocation",
                "instrument_id": "FIXTURE-LINEAR-PREMIUM",
                "source_time_ms": 2000,
                "observed_at_ms": 2001,
                "health": "available",
                "bid": "99",
                "ask": "101",
                "bid_contracts": "3",
                "ask_contracts": "3",
            },
            {
                "quote_id": "exit",
                "liquidity_id": "exit-native-allocation",
                "instrument_id": "FIXTURE-LINEAR-PREMIUM",
                "source_time_ms": 4000,
                "observed_at_ms": 4001,
                "health": "available",
                "bid": "99",
                "ask": "101",
                "bid_contracts": "3",
                "ask_contracts": "3",
            },
        ],
    }


def write_request(path, request=None):
    path.write_text(json.dumps(backtest_input() if request is None else request), encoding="utf-8")
    return path


def bar(index, *, complete=True, receipt=None, symbol="BTC/USD", source="kraken"):
    opening = 300000 + index * 300000
    closing = opening + 300000
    return make_event(
        source=source,
        environment="public_reference",
        asset="BTC",
        symbol=symbol,
        kind="bar",
        event_time_ms=closing,
        observed_at_ms=closing + 10 if receipt is None else receipt,
        payload={
            "open_time_ms": opening,
            "close_time_ms": closing,
            "timeframe_s": 300,
            "open": 100.0 + index,
            "close": 101.0 + index,
            "high": 102.0 + index,
            "low": 99.0 + index,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": complete,
            "volume_unit": "base_asset",
            "aggregation_version": 1,
            "lateness_ms": 10,
            "coverage_start_ms": 300000,
        },
    )


async def save(path, events):
    store = await DuckStore.open(path)
    try:
        await store.append(events)
    finally:
        await store.close()


def test_registered_known_bad_roundtrip_and_immutable_input(tmp_path):
    path = write_request(tmp_path / "input.json")
    registry = tmp_path / "trials.jsonl"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    result = research_backtest(path, tmp_path / "result", registry)
    simulation = result["simulation"]
    assert simulation["closed_roundtrip_net_pnl"] == "-44.000"
    assert Decimal(simulation["gross_realized_pnl"]) == Decimal("-40")
    assert simulation["total_fees"] == "4.000"
    assert len(simulation["fills"]) == 2
    assert result["data_kind"] == "synthetic_fixture"
    assert result["input_sha256"] == digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert trial_counts(registry)["trial_count_N"] == trial_counts(registry)["completed"] == 1
    assert read_trials(registry)[0].parameters["input_sha256"] == digest
    assert not simulation["gate2_complete"] and not result["trading_enabled"]
    assert json.loads((tmp_path / "result/backtest.json").read_text(encoding="utf-8")) == result


def test_simulator_failure_is_registered_and_no_success_report(tmp_path):
    request = backtest_input()
    request["instrument"]["funding_required"] = True
    path = write_request(tmp_path / "input.json", request)
    registry = tmp_path / "trials.jsonl"
    with pytest.raises(ValueError, match="UNSUPPORTED"):
        research_backtest(path, tmp_path / "result", registry)
    assert trial_counts(registry)["failed"] == 1
    assert read_trials(registry)[-1].failure_type == "ValueError"
    assert not (tmp_path / "result/backtest.json").exists()


def test_existing_output_refused_before_another_trial(tmp_path):
    path = write_request(tmp_path / "input.json")
    registry = tmp_path / "trials.jsonl"
    research_backtest(path, tmp_path / "result", registry)
    prefix = registry.read_bytes()
    with pytest.raises(FileExistsError):
        research_backtest(path, tmp_path / "result", registry)
    assert registry.read_bytes() == prefix


def test_input_duplicate_keys_unknown_fields_and_nonfinite_refused_before_run(tmp_path):
    path = write_request(tmp_path / "input.json")
    raw = path.read_text(encoding="utf-8")
    path.write_text(raw[:-1] + ',"schema_version":1}', encoding="utf-8")
    with pytest.raises(IntegrityError, match="DUPLICATE"):
        research_backtest(path, tmp_path / "result", tmp_path / "trials.jsonl")
    assert not (tmp_path / "trials.jsonl").exists()
    request = backtest_input() | {"secret_extra": "not permitted"}
    write_request(path, request)
    with pytest.raises(ValueError):
        research_backtest(path, tmp_path / "result", tmp_path / "trials.jsonl")
    request = backtest_input()
    request["provenance"]["bad"] = float("nan")
    write_request(path, request)
    with pytest.raises(ValueError):
        research_backtest(path, tmp_path / "result", tmp_path / "trials.jsonl")


def test_registry_cannot_overwrite_input(tmp_path):
    path = write_request(tmp_path / "input.json")
    before = path.read_bytes()
    with pytest.raises(ValueError, match="distinct"):
        research_backtest(path, tmp_path / "result", path)
    assert path.read_bytes() == before


async def test_closed_source_both_clock_selection_and_unchanged_database(tmp_path):
    path = tmp_path / "events.duckdb"
    cutoff = 1200010
    rows = [bar(0), bar(1), bar(2, receipt=cutoff + 1), bar(3), bar(0, source="elsewhere")]
    await save(path, rows)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    snapshot, digest = load_bar_snapshot(
        path,
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        as_of_ms=cutoff,
        max_bars=10,
    )
    assert snapshot.rows() == tuple(rows[:2])
    assert digest == before == hashlib.sha256(path.read_bytes()).hexdigest()
    result = research_features(
        path,
        tmp_path / "features",
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=300,
        as_of_ms=900010,
    )
    assert result["feature_report"]["bars_used"] == 2
    assert all(row["value"] is None for row in result["feature_report"]["features"])
    assert result["source_mutated"] is False and result["trading_enabled"] is False


async def test_feature_cli_reads_actual_committed_fixture_bars(tmp_path, monkeypatch, capsys):
    path = tmp_path / "events.duckdb"
    rows = [bar(i) for i in range(40)]
    await save(path, rows)
    output = tmp_path / "features"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "selective-options",
            "research-features",
            "--database",
            str(path),
            "--output",
            str(output),
            "--asset",
            "BTC",
            "--environment",
            "public_reference",
            "--source",
            "kraken",
            "--symbol",
            "BTC/USD",
            "--timeframe-s",
            "300",
            "--as-of-ms",
            str(rows[-1].observed_at_ms),
            "--annualization-periods-per-year",
            "105120",
        ],
    )
    assert main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["feature_report"]["bars_used"] == 40
    assert any(row["state"] == "AVAILABLE" for row in result["feature_report"]["features"])
    assert (output / "features.json").is_file()


def test_backtest_cli_registers_and_exposes_fixture_limits(tmp_path, monkeypatch, capsys):
    path = write_request(tmp_path / "input.json")
    registry = tmp_path / "trials.jsonl"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "selective-options",
            "research-backtest",
            "--input",
            str(path),
            "--output",
            str(tmp_path / "result"),
            "--registry",
            str(registry),
        ],
    )
    assert main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["registry_counts"]["trial_count_N"] == 1
    assert result["data_kind"] == "synthetic_fixture"
    assert result["simulation"]["closed_roundtrip_net_pnl"] == "-44.000"


async def test_history_budget_and_wal_refuse_without_output(tmp_path):
    path = tmp_path / "events.duckdb"
    await save(path, [bar(i) for i in range(3)])
    args = dict(
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        as_of_ms=1200010,
        max_bars=2,
    )
    before = path.read_bytes()
    with pytest.raises(IntegrityError, match="HISTORY_BUDGET"):
        load_bar_snapshot(path, **args)
    assert path.read_bytes() == before
    path.with_suffix(".duckdb.wal").write_bytes(b"active-marker")
    with pytest.raises(IntegrityError, match="MUST_BE_CLOSED"):
        research_features(path, tmp_path / "features", timeframe_s=300, **args)
    assert not (tmp_path / "features").exists()


async def test_incomplete_history_remains_visible_and_unavailable(tmp_path):
    path = tmp_path / "events.duckdb"
    await save(path, [bar(0), bar(1, complete=False)])
    result = research_features(
        path,
        tmp_path / "features",
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=300,
        as_of_ms=900010,
    )
    assert result["feature_report"]["state"] == "UNAVAILABLE"
    assert result["feature_report"]["blockers"]
    assert all(row["value"] is None for row in result["feature_report"]["features"])


async def test_explicit_window_is_recorded_without_silent_tail_reseeding(tmp_path):
    path = tmp_path / "events.duckdb"
    await save(path, [bar(0, complete=False), bar(1), bar(2)])
    result = research_features(
        path,
        tmp_path / "features",
        asset="BTC",
        environment="public_reference",
        source="kraken",
        symbol="BTC/USD",
        timeframe_s=300,
        as_of_ms=1200010,
        history_start_ms=900000,
        max_bars=2,
    )
    assert result["source_scope_and_cutoff"]["explicit_history_start_ms"] == 900000
    assert result["feature_report"]["bars_used"] == 2
    assert result["feature_report"]["first_close_ms"] == 900000
    assert not result["feature_report"]["history_truncated"]
    assert not result["feature_report"]["blockers"]
    with pytest.raises(ValueError, match="history start"):
        load_bar_snapshot(
            path,
            asset="BTC",
            environment="public_reference",
            source="kraken",
            symbol="BTC/USD",
            as_of_ms=1200010,
            max_bars=2,
            history_start_ms=1200011,
        )
