"""Saved-source integrity, isolation, and CLI parity for swing research."""

import hashlib
import json

import pytest
from test_swing_structure_features import bar

from selective_options import cli
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore
from selective_options.swing_structure_research import research_swing_structure

POLICY = {
    "asset": "BTC",
    "environment": "public_reference",
    "source": "swing-fixture",
    "symbol": "BTC/USD",
    "timeframe_s": 300,
    "as_of_ms": 3_000_005,
    "max_latest_age_ms": 300_000,
    "left_bars": 2,
    "right_bars": 2,
}


@pytest.fixture
async def saved(tmp_path):
    database = tmp_path / "source" / "bars.duckdb"
    database.parent.mkdir()
    highs = [100, 102, 106, 103, 101, 105, 108, 104, 100]
    lows = [99, 97, 95, 98, 99, 96, 93, 97, 100]
    store = await DuckStore.open(database)
    await store.append(
        [bar(index, high, low) for index, (high, low) in enumerate(zip(highs, lows, strict=True))]
    )
    await store.close()
    return database


def test_saved_source_hash_cutoff_and_exclusive_output(saved, tmp_path):
    before = saved.read_bytes()
    output = tmp_path / "report"
    result = research_swing_structure(saved, output, **POLICY)
    report = result["swing_structure_report"]
    assert result == json.loads((output / "swing-structure.json").read_text())
    assert saved.read_bytes() == before
    assert result["source_database_sha256"] == hashlib.sha256(before).hexdigest()
    assert result["source_mutated"] is False
    assert result["ready_for_signals"] is result["trading_enabled"] is False
    assert report["state"] == "AVAILABLE"
    assert report["pivots"]
    with pytest.raises(FileExistsError):
        research_swing_structure(saved, output, **POLICY)


def test_both_clock_cutoff_and_history_start_are_respected(saved, tmp_path):
    early = research_swing_structure(
        saved,
        tmp_path / "early",
        **(POLICY | {"as_of_ms": 1_800_005, "max_latest_age_ms": 600_000}),
    )
    assert early["source_scope_and_cutoff"]["materialized_rows"] == 5
    report = early["swing_structure_report"]
    assert all(pivot["confirmation_close_ms"] <= 1_800_000 for pivot in report["pivots"])
    short = research_swing_structure(
        saved,
        tmp_path / "short",
        **(POLICY | {"history_start_ms": 2_400_000}),
    )
    assert short["swing_structure_report"]["state"] == "WARMUP"
    assert short["swing_structure_report"]["pivots"] == []


def test_invalid_parameters_precede_database_read(saved, tmp_path, monkeypatch):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid policy reached saved source")

    monkeypatch.setattr(
        "selective_options.swing_structure_research.load_bar_snapshot", refused_read
    )
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        research_swing_structure(saved, output, **(POLICY | {"left_bars": True}))
    assert not output.exists()


def test_source_wal_and_row_budget_are_enforced(saved, tmp_path):
    wal = saved.with_suffix(saved.suffix + ".wal")
    wal.write_bytes(b"LABELED_SYNTHETIC_OPEN_WAL")
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research_swing_structure(saved, tmp_path / "wal", **POLICY)
    wal.unlink()
    with pytest.raises(IntegrityError, match="BAR_HISTORY_BUDGET"):
        research_swing_structure(saved, tmp_path / "oversize", **(POLICY | {"max_bars": 5}))


def test_cli_matches_api_for_explicit_confirmation_windows(saved, tmp_path, monkeypatch, capsys):
    policy = POLICY | {"left_bars": 1, "right_bars": 3}
    api = research_swing_structure(saved, tmp_path / "api", **policy)
    output = tmp_path / "cli"
    argv = [
        "selective-options",
        "research-swing-structure",
        "--database",
        str(saved),
        "--output",
        str(output),
    ]
    for name, value in policy.items():
        argv.extend(["--" + name.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", argv)
    assert cli.main() == 0
    assert api == json.loads(capsys.readouterr().out)
    assert api == json.loads((output / "swing-structure.json").read_text())
