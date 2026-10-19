"""Saved source/CLI point-in-time contract for offline Stochastic RSI."""

import hashlib
import json

import pytest
from test_stochastic_rsi_features import bar

from selective_options import cli
from selective_options import stochastic_rsi_research as research
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

POLICY = {
    "asset": "BTC",
    "environment": "public_reference",
    "source": "synthetic_fixture",
    "symbol": "FIXTURE-BTC/USD",
    "timeframe_s": 300,
    "as_of_ms": 12_300_005,
    "max_latest_age_ms": 300_000,
}


@pytest.fixture
async def saved(tmp_path):
    database = tmp_path / "source" / "bars.duckdb"
    database.parent.mkdir()
    store = await DuckStore.open(database)
    await store.append([bar(index, 100 + (index * 7) % 13 + index // 6) for index in range(40)])
    await store.close()
    return database


def test_saved_source_is_read_only_hashed_and_report_is_exclusive(saved, tmp_path):
    before = saved.read_bytes()
    output = tmp_path / "report"
    result = research.research_stochastic_rsi(saved, output, **POLICY)
    assert result == json.loads((output / "stochastic-rsi.json").read_text())
    assert saved.read_bytes() == before
    assert result["source_database_sha256"] == hashlib.sha256(before).hexdigest()
    assert result["source_mutated"] is False
    assert result["ready_for_signals"] is result["trading_enabled"] is False
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 40
    assert all(item["state"] == "AVAILABLE" for item in result["stochastic_rsi_report"]["features"])
    with pytest.raises(FileExistsError):
        research.research_stochastic_rsi(saved, output, **POLICY)


def test_both_clock_cutoff_excludes_later_bar_and_preserves_warmup(saved, tmp_path):
    result = research.research_stochastic_rsi(
        saved,
        tmp_path / "before-latest",
        **(POLICY | {"as_of_ms": 12_000_000, "max_latest_age_ms": 600_000}),
    )
    scope = result["source_scope_and_cutoff"]
    assert scope["materialized_rows"] == 38
    report = result["stochastic_rsi_report"]
    assert report["base"]["latest_close_ms"] == 11_700_000
    assert all(item["state"] == "AVAILABLE" for item in report["features"])


def test_history_start_is_inclusive_and_never_reseeds_a_tail(saved, tmp_path):
    result = research.research_stochastic_rsi(
        saved,
        tmp_path / "short-segment",
        **(POLICY | {"history_start_ms": 11_100_000}),
    )
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 5
    report = result["stochastic_rsi_report"]
    assert len(report["input_event_ids"]) == 5
    assert all(item["state"] == "WARMUP" for item in report["features"])


def test_invalid_feature_policy_is_rejected_before_source_read_or_output(
    saved, tmp_path, monkeypatch
):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid policy reached the saved source")

    monkeypatch.setattr(research, "load_bar_snapshot", refused_read)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        research.research_stochastic_rsi(saved, output, **(POLICY | {"rsi_period": True}))
    assert not output.exists()


def test_closed_source_and_wal_integrity_are_required(saved, tmp_path):
    wal = saved.with_suffix(saved.suffix + ".wal")
    wal.write_bytes(b"LABELED_SYNTHETIC_OPEN_WAL")
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research.research_stochastic_rsi(saved, tmp_path / "refused", **POLICY)
    wal.unlink()
    for alias in (saved, saved.parent):
        with pytest.raises(ValueError, match="distinct"):
            research.research_stochastic_rsi(saved, alias, **POLICY)


def test_actual_cli_equals_api_with_explicit_cutoff_and_custom_smoothing(
    saved, tmp_path, monkeypatch, capsys
):
    policy = POLICY | {"rsi_period": 5, "stochastic_period": 5, "smooth_k": 2, "smooth_d": 2}
    api = research.research_stochastic_rsi(saved, tmp_path / "api", **policy)
    output = tmp_path / "cli"
    arguments = [
        "selective-options",
        "research-stochastic-rsi",
        "--database",
        str(saved),
        "--output",
        str(output),
    ]
    for name, value in policy.items():
        arguments.extend(["--" + name.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", arguments)
    assert cli.main() == 0
    assert api == json.loads(capsys.readouterr().out)
    assert api == json.loads((output / "stochastic-rsi.json").read_text())
    report = api["stochastic_rsi_report"]
    assert report["rsi_period"] == report["stochastic_period"] == 5
    assert report["smooth_k"] == report["smooth_d"] == 2
