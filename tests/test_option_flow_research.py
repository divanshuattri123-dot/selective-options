"""Read-only report/public CLI boundary checks using labelled synthetic data."""

import hashlib
import json

import pytest
from test_options_skew import fixture

from selective_options import cli
from selective_options import option_flow_research as research
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

POLICY = {
    "asset": "BTC",
    "as_of_ms": 2000,
    "amount_unit_policy": "deribit_option_base_currency",
    "volume_field": "stats.volume",
    "max_quote_age_ms": 200,
    "max_discovery_age_ms": 1000,
    "max_clock_skew_ms": 10,
}


@pytest.fixture
async def saved(tmp_path, instrument, ticker):
    metadata, quotes, manifest, _, _ = fixture(instrument, ticker | {"stats": {"volume": 30.0}})
    database = tmp_path / "synthetic-options.duckdb"
    store = await DuckStore.open(database)
    await store.append([*metadata, *quotes, manifest])
    await store.close()
    return database


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_saved_native_units_report_is_exclusive_and_source_stays_unchanged(saved, tmp_path):
    before = sha(saved)
    output = tmp_path / "report"
    report = research.research_option_flow(saved, output, **POLICY)
    assert report == json.loads((output / "put-call.json").read_text())
    assert sha(saved) == before == report["source_database_sha256"]
    assert report["source_scope_and_cutoff"]["materialized_rows"] == 17
    assert report["ready_for_signals"] is False and report["trading_enabled"] is False
    result = report["put_call_report"]
    assert result["aggregate_open_interest"]["value"] == 1.0
    assert result["aggregate_volume"]["value"] == 1.0
    assert len(result["expiries"]) == 2
    original = (output / "put-call.json").read_bytes()
    with pytest.raises((ValueError, FileExistsError)):
        research.research_option_flow(saved, output, **POLICY)
    assert (output / "put-call.json").read_bytes() == original


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount_unit_policy", "contracts"),
        ("amount_unit_policy", None),
        ("volume_field", "stats.volume_usd"),
        ("volume_field", None),
        ("max_quote_age_ms", True),
        ("max_discovery_age_ms", 0),
        ("max_clock_skew_ms", -1),
        ("max_rows", 0),
        ("max_rows", 100_001),
        ("max_expiries", 0),
    ],
)
def test_bad_policy_refuses_before_source_read_or_output_creation(
    tmp_path, monkeypatch, field, value
):
    def refused_read(*args, **kwargs):
        pytest.fail("invalid policy reached source reader")

    monkeypatch.setattr(research, "load_options_snapshots", refused_read)
    output = tmp_path / "absent-report"
    with pytest.raises(ValueError):
        research.research_option_flow(
            tmp_path / "absent.duckdb", output, **(POLICY | {field: value})
        )
    assert not output.exists()


def test_history_overflow_refuses_without_publishing_prefix(saved, tmp_path):
    output = tmp_path / "overflow"
    with pytest.raises(IntegrityError, match="OPTIONS_RESEARCH_HISTORY_OVER_BUDGET"):
        research.research_option_flow(saved, output, **(POLICY | {"max_rows": 2}))
    assert not output.exists()


def test_open_source_wal_refuses_before_output(saved, tmp_path):
    saved.with_suffix(".duckdb.wal").write_bytes(b"EXPLICIT_SYNTHETIC_UNCLOSED_WAL")
    output = tmp_path / "wal-report"
    with pytest.raises(IntegrityError, match="SOURCE_MUST_BE_CLOSED"):
        research.research_option_flow(saved, output, **POLICY)
    assert not output.exists()


def test_source_change_after_calculation_refuses_before_publication(saved, tmp_path, monkeypatch):
    monkeypatch.setattr(research, "_hash", lambda _: "changed-source-digest")
    output = tmp_path / "changed"
    with pytest.raises(IntegrityError, match="OPTION_FLOW_SOURCE_CHANGED"):
        research.research_option_flow(saved, output, **POLICY)
    assert not output.exists()


def test_output_cannot_alias_or_replace_source(saved):
    before = sha(saved)
    for output in (saved, saved.parent):
        with pytest.raises(ValueError):
            research.research_option_flow(saved, output, **POLICY)
    assert sha(saved) == before


def test_actual_cli_reads_saved_fixture_and_publishes_disabled_report(
    saved, tmp_path, monkeypatch, capsys
):
    output = tmp_path / "cli-report"
    arguments = [
        "selective-options",
        "research-option-flow",
        "--database",
        str(saved),
        "--output",
        str(output),
    ]
    for name, value in POLICY.items():
        arguments.extend(["--" + name.replace("_", "-"), str(value)])
    monkeypatch.setattr("sys.argv", arguments)
    assert cli.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report == json.loads((output / "put-call.json").read_text())
    assert report["trading_enabled"] is False


def test_cli_refuses_implicit_or_usd_volume_policy(tmp_path):
    base = [
        "research-option-flow",
        "--database",
        str(tmp_path / "missing.duckdb"),
        "--output",
        str(tmp_path / "report"),
        "--asset",
        "BTC",
        "--as-of-ms",
        "2000",
        "--max-quote-age-ms",
        "200",
        "--max-discovery-age-ms",
        "1000",
        "--max-clock-skew-ms",
        "10",
    ]
    for extra in (
        [],
        [
            "--amount-unit-policy",
            "deribit_option_base_currency",
            "--volume-field",
            "stats.volume_usd",
        ],
    ):
        with pytest.raises(SystemExit) as exc:
            cli.parser().parse_args([*base, *extra])
        assert exc.value.code == 2
    assert not (tmp_path / "report").exists()
