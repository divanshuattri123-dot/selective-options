"""Saved database, exclusive output, two-clock and CLI contracts."""

import hashlib
import json

import pytest
from test_atr_percentile_features import bar

from selective_options import atr_percentile_research as research
from selective_options import cli
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

POLICY = {
    "asset": "BTC",
    "environment": "public_reference",
    "source": "fixture",
    "symbol": "BTC/USD",
    "timeframe_s": 300,
    "as_of_ms": 12_300_005,
    "max_latest_age_ms": 300_000,
    "atr_period": 3,
    "percentile_window": 6,
    "percentile_min_history": 5,
}


@pytest.fixture
async def saved(tmp_path):
    database = tmp_path / "source" / "bars.duckdb"
    database.parent.mkdir()
    store = await DuckStore.open(database)
    await store.append([bar(index, (index % 5) + 1) for index in range(40)])
    await store.close()
    return database


def test_saved_source_is_read_only_hashed_and_output_is_exclusive(saved, tmp_path):
    before = saved.read_bytes()
    output = tmp_path / "report"
    result = research.research_atr_percentile(saved, output, **POLICY)
    report = result["atr_percentile_report"]
    assert result == json.loads((output / "atr-percentile.json").read_text())
    assert saved.read_bytes() == before
    assert result["source_database_sha256"] == hashlib.sha256(before).hexdigest()
    assert result["source_mutated"] is False
    assert result["ready_for_signals"] is result["trading_enabled"] is False
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 40
    assert report["feature"]["state"] == "AVAILABLE"
    with pytest.raises(FileExistsError):
        research.research_atr_percentile(saved, output, **POLICY)


def test_both_clock_cutoff_and_explicit_history_start_are_respected(saved, tmp_path):
    result = research.research_atr_percentile(
        saved,
        tmp_path / "early",
        **(POLICY | {"as_of_ms": 12_000_000, "max_latest_age_ms": 600_000}),
    )
    report = result["atr_percentile_report"]
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 38
    assert report["base"]["latest_close_ms"] == 11_700_000
    assert report["feature"]["state"] == "AVAILABLE"
    short = research.research_atr_percentile(
        saved, tmp_path / "short", **(POLICY | {"history_start_ms": 11_100_000})
    )
    short_report = short["atr_percentile_report"]
    assert short["source_scope_and_cutoff"]["materialized_rows"] == 5
    assert len(short_report["input_event_ids"]) == 5
    assert short_report["feature"]["state"] == "WARMUP"
    assert short_report["feature"]["value"] is None


def test_invalid_policy_is_rejected_before_database_read(saved, tmp_path, monkeypatch):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid percentile policy reached source")

    monkeypatch.setattr(research, "load_bar_snapshot", refused_read)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        research.research_atr_percentile(
            saved, output, **(POLICY | {"percentile_min_history": True})
        )
    assert not output.exists()


def test_closed_source_wal_and_row_budget_are_enforced(saved, tmp_path):
    wal = saved.with_suffix(saved.suffix + ".wal")
    wal.write_bytes(b"LABELED_SYNTHETIC_OPEN_WAL")
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research.research_atr_percentile(saved, tmp_path / "wal", **POLICY)
    wal.unlink()
    with pytest.raises(IntegrityError, match="BAR_HISTORY_BUDGET"):
        research.research_atr_percentile(
            saved, tmp_path / "too-many", **(POLICY | {"max_bars": 10})
        )


def test_actual_cli_matches_api_for_custom_atr_and_window(saved, tmp_path, monkeypatch, capsys):
    policy = POLICY | {"atr_period": 5, "percentile_window": 7, "percentile_min_history": 5}
    api = research.research_atr_percentile(saved, tmp_path / "api", **policy)
    output = tmp_path / "cli"
    arguments = [
        "selective-options",
        "research-atr-percentile",
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
    assert api == json.loads((output / "atr-percentile.json").read_text())
