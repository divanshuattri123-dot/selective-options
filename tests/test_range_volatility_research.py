"""Exclusive offline source/CLI boundaries using explicitly synthetic bars."""

import hashlib
import json

import pytest
from test_features import bar

from selective_options import cli
from selective_options import range_volatility_research as research
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

POLICY = {
    "asset": "BTC",
    "environment": "public_reference",
    "source": "kraken",
    "symbol": "BTC/USD",
    "timeframe_s": 300,
    "as_of_ms": 3_000_005,
    "windows": (2, 3),
    "max_latest_age_ms": 10,
    "annualization_periods_per_year": 100.0,
}


@pytest.fixture
async def saved(tmp_path):
    database = tmp_path / "synthetic-bars.duckdb"
    store = await DuckStore.open(database)
    await store.append([bar(index, 100.0 + index) for index in range(4)])
    await store.close()
    return database


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_scoped_report_keeps_source_unchanged_and_output_exclusive(saved, tmp_path):
    before = sha(saved)
    output = tmp_path / "report"
    result = research.research_range_volatility(saved, output, **POLICY)
    assert result == json.loads((output / "range-volatility.json").read_text())
    assert sha(saved) == before == result["source_database_sha256"]
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 4
    assert result["ready_for_signals"] is False and result["trading_enabled"] is False
    report = result["range_volatility_report"]
    assert len(report["features"]) == 8
    assert all(item["state"] == "AVAILABLE" for item in report["features"])
    original = (output / "range-volatility.json").read_bytes()
    with pytest.raises((ValueError, FileExistsError)):
        research.research_range_volatility(saved, output, **POLICY)
    assert (output / "range-volatility.json").read_bytes() == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("windows", ()),
        ("windows", (1,)),
        ("windows", (2, 2)),
        ("max_latest_age_ms", True),
        ("max_latest_age_ms", -1),
        ("max_bars", 0),
        ("max_bars", 10_001),
        ("annualization_periods_per_year", 0.0),
        ("annualization_periods_per_year", float("inf")),
        ("timeframe_s", 0),
    ],
)
def test_invalid_policy_never_reads_source_or_creates_output(tmp_path, monkeypatch, field, value):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid policy reached the source reader")

    monkeypatch.setattr(research, "load_bar_snapshot", refused_read)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        research.research_range_volatility(
            tmp_path / "missing.duckdb", output, **(POLICY | {field: value})
        )
    assert not output.exists()


def test_overflow_refuses_without_truncating_history(saved, tmp_path):
    output = tmp_path / "overflow"
    with pytest.raises(IntegrityError, match="RESEARCH_BAR_HISTORY_BUDGET"):
        research.research_range_volatility(saved, output, **(POLICY | {"max_bars": 3}))
    assert not output.exists()


def test_open_source_wal_and_changed_source_refuse_publication(saved, tmp_path, monkeypatch):
    wal = saved.with_suffix(".duckdb.wal")
    wal.write_bytes(b"EXPLICIT_SYNTHETIC_UNCLOSED_WAL")
    output = tmp_path / "wal-report"
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research.research_range_volatility(saved, output, **POLICY)
    assert not output.exists()
    wal.unlink()
    monkeypatch.setattr(research, "_hash", lambda _: "changed-source-digest")
    with pytest.raises(IntegrityError, match="RANGE_VOLATILITY_SOURCE_CHANGED"):
        research.research_range_volatility(saved, output, **POLICY)
    assert not output.exists()


def test_alias_refusal_and_explicit_history_scope(saved, tmp_path):
    before = sha(saved)
    for output in (saved, saved.parent):
        with pytest.raises(ValueError):
            research.research_range_volatility(saved, output, **POLICY)
    result = research.research_range_volatility(
        saved, tmp_path / "history", **(POLICY | {"history_start_ms": 2_700_000})
    )
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 2
    features = result["range_volatility_report"]["features"]
    assert sum(item["state"] == "WARMUP" for item in features) == 4
    assert sha(saved) == before


def test_annualization_is_optional_and_never_inferred(saved, tmp_path):
    result = research.research_range_volatility(
        saved, tmp_path / "unannualized", **(POLICY | {"annualization_periods_per_year": None})
    )
    features = result["range_volatility_report"]["features"]
    assert sum(item["state"] == "AVAILABLE" for item in features) == 4
    assert sum(item["state"] == "UNAVAILABLE" for item in features) == 4


def test_actual_cli_reads_saved_bars_and_keeps_trading_disabled(
    saved, tmp_path, monkeypatch, capsys
):
    output = tmp_path / "cli-report"
    arguments = [
        "selective-options",
        "research-range-volatility",
        "--database",
        str(saved),
        "--output",
        str(output),
    ]
    for name, value in POLICY.items():
        arguments.append("--" + name.replace("_", "-"))
        if name == "windows":
            arguments.extend(str(window) for window in value)
        else:
            arguments.append(str(value))
    monkeypatch.setattr("sys.argv", arguments)
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads((output / "range-volatility.json").read_text())
    assert result["trading_enabled"] is False


def test_cli_requires_explicit_window_and_freshness_policy(tmp_path):
    with pytest.raises(SystemExit) as exc:
        cli.parser().parse_args(
            [
                "research-range-volatility",
                "--database",
                str(tmp_path / "missing.duckdb"),
                "--output",
                str(tmp_path / "report"),
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
                "3000005",
            ]
        )
    assert exc.value.code == 2
    assert not (tmp_path / "report").exists()
