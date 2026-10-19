"""Saved-source boundaries on explicitly synthetic complete closed bars."""

import hashlib
import json

import pytest
from test_keltner_features import bar

from selective_options import cli
from selective_options import keltner_research as research
from selective_options import research as base_research
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

POLICY = {
    "asset": "BTC",
    "environment": "public_reference",
    "source": "synthetic_fixture",
    "symbol": "FIXTURE-BTC/USD",
    "timeframe_s": 300,
    "as_of_ms": 7_800_005,
    "ema_period": 20,
    "atr_period": 10,
    "atr_multiplier": 2.0,
    "max_latest_age_ms": 10,
}


@pytest.fixture
async def saved(tmp_path):
    database = tmp_path / "source" / "synthetic-bars.duckdb"
    database.parent.mkdir()
    store = await DuckStore.open(database)
    await store.append([bar(index, 100.0 + index) for index in range(25)])
    await store.close()
    return database


def test_exact_source_hash_report_and_exclusive_publication(saved, tmp_path):
    before = saved.read_bytes()
    output = tmp_path / "report"
    result = research.research_keltner(saved, output, **POLICY)
    assert result == json.loads((output / "keltner.json").read_text())
    assert saved.read_bytes() == before
    assert result["source_database_sha256"] == hashlib.sha256(before).hexdigest()
    assert (
        result["source_mutated"]
        is result["ready_for_signals"]
        is result["trading_enabled"]
        is False
    )
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 25
    assert all(item["state"] == "AVAILABLE" for item in result["keltner_report"]["features"])
    original = (output / "keltner.json").read_bytes()
    with pytest.raises(FileExistsError):
        research.research_keltner(saved, output, **POLICY)
    assert (output / "keltner.json").read_bytes() == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("ema_period", True),
        ("ema_period", 0),
        ("atr_period", 10000),
        ("atr_multiplier", True),
        ("atr_multiplier", 0),
        ("atr_multiplier", float("inf")),
        ("max_latest_age_ms", True),
        ("max_latest_age_ms", -1),
        ("max_bars", 0),
        ("max_bars", 10001),
        ("timeframe_s", 0),
    ],
)
def test_invalid_policy_never_reads_source_or_creates_output(tmp_path, monkeypatch, field, value):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid feature policy reached source reader")

    monkeypatch.setattr(research, "load_bar_snapshot", refused_read)
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        research.research_keltner(tmp_path / "missing.duckdb", output, **(POLICY | {field: value}))
    assert not output.exists()


def test_source_row_and_byte_budgets_refuse_without_tail_truncation(saved, tmp_path, monkeypatch):
    output = tmp_path / "bounded"
    with pytest.raises(IntegrityError, match="RESEARCH_BAR_HISTORY_BUDGET"):
        research.research_keltner(saved, output, **(POLICY | {"max_bars": 24}))
    monkeypatch.setattr(base_research, "MAX_SOURCE_BYTES", saved.stat().st_size - 1)
    with pytest.raises(IntegrityError, match="RESEARCH_SOURCE_BYTE_BUDGET"):
        research.research_keltner(saved, output, **POLICY)
    assert not output.exists()


def test_both_clocks_determine_saved_visibility(saved, tmp_path):
    result = research.research_keltner(
        saved,
        tmp_path / "before-last-receipt",
        **(POLICY | {"as_of_ms": 7_800_000, "max_latest_age_ms": 300_000}),
    )
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 24
    report = result["keltner_report"]
    assert report["base"]["latest_close_ms"] == 7_500_000
    assert report["base"]["as_of_ms"] == 7_800_000


def test_inclusive_two_bar_scope_preserves_standard_warmup(saved, tmp_path):
    result = research.research_keltner(
        saved, tmp_path / "two-bars", **(POLICY | {"history_start_ms": 7_500_000})
    )
    assert result["source_scope_and_cutoff"]["materialized_rows"] == 2
    assert all(
        item["state"] == "WARMUP" and item["value"] is None
        for item in result["keltner_report"]["features"]
    )


def test_open_wal_changed_hash_and_alias_refuse_publication(saved, tmp_path, monkeypatch):
    output = tmp_path / "refused"
    for alias in (saved, saved.parent):
        with pytest.raises(ValueError, match="distinct"):
            research.research_keltner(saved, alias, **POLICY)
    wal = saved.with_suffix(".duckdb.wal")
    wal.write_bytes(b"EXPLICIT_SYNTHETIC_OPEN_WAL")
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research.research_keltner(saved, output, **POLICY)
    wal.unlink()
    monkeypatch.setattr(research, "_hash", lambda _: "changed-source-digest")
    with pytest.raises(IntegrityError, match="KELTNER_SOURCE_CHANGED"):
        research.research_keltner(saved, output, **POLICY)
    assert not output.exists()


def test_actual_cli_equals_api_with_explicit_custom_policy(saved, tmp_path, monkeypatch, capsys):
    policy = POLICY | {
        "ema_period": 5,
        "atr_period": 3,
        "atr_multiplier": 1.5,
        "history_start_ms": 900_000,
        "max_bars": 30,
    }
    api = research.research_keltner(saved, tmp_path / "api", **policy)
    output = tmp_path / "cli"
    arguments = [
        "selective-options",
        "research-keltner",
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
    assert api == json.loads((output / "keltner.json").read_text())
    assert api["keltner_report"]["ema_period"] == 5
    assert api["keltner_report"]["atr_period"] == 3
    assert api["keltner_report"]["atr_multiplier"] == 1.5


@pytest.mark.parametrize(
    "omitted", ["ema_period", "atr_period", "atr_multiplier", "max_latest_age_ms"]
)
def test_cli_requires_explicit_channel_and_freshness_policy(saved, tmp_path, omitted):
    arguments = [
        "research-keltner",
        "--database",
        str(saved),
        "--output",
        str(tmp_path / "no-output"),
    ]
    for name, value in POLICY.items():
        if name != omitted:
            arguments.extend(["--" + name.replace("_", "-"), str(value)])
    with pytest.raises(SystemExit) as exc:
        cli.parser().parse_args(arguments)
    assert exc.value.code == 2
    assert not (tmp_path / "no-output").exists()
