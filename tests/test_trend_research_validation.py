"""Actual closed-store and CLI boundary checks for opt-in trend research."""

import hashlib
import json
import sys

import pytest

from selective_options import research
from selective_options.cli import main, parser
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.research import research_features
from selective_options.store import DuckStore


def bar(index, close=None, *, receipt=None, source="fixture", symbol="BTC/USD", complete=True):
    close = 100.0 + index if close is None else close
    opening = 300000 + index * 300000
    return make_event(
        source=source,
        environment="public_reference",
        asset="BTC",
        symbol=symbol,
        kind="bar",
        event_time_ms=opening + 300000,
        observed_at_ms=receipt if receipt is not None else opening + 300005,
        payload={
            "open_time_ms": opening,
            "close_time_ms": opening + 300000,
            "timeframe_s": 300,
            "open": close,
            "high": close + 2,
            "low": close - 2,
            "close": close,
            "volume": 1.0,
            "trade_count": 1,
            "closed": True,
            "complete": complete,
        },
    )


async def save(path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(rows, exact=True)
    finally:
        await store.close()


def arguments(rows):
    return {
        "asset": "BTC",
        "environment": "public_reference",
        "source": "fixture",
        "symbol": "BTC/USD",
        "timeframe_s": 300,
        "as_of_ms": rows[-1].observed_at_ms,
    }


def values(trend):
    return {value["name"]: value for value in trend["features"]}


async def test_actual_scoped_both_clock_closed_store_prior_only_ranks_and_provenance(tmp_path):
    rows = [bar(index) for index in range(60)]
    cutoff = rows[-1].observed_at_ms
    future_receipt = bar(59, 777.0, receipt=cutoff + 1)
    future_source = bar(60)
    unrelated = [bar(0, 999.0, source="elsewhere"), bar(0, 999.0, symbol="BTC/USDT")]
    database = tmp_path / "source.duckdb"
    await save(database, [*rows, future_receipt, future_source, *unrelated])
    original = database.read_bytes()
    output = tmp_path / "report"
    report = research_features(
        database,
        output,
        **arguments(rows),
        include_trend=True,
        annualization_periods_per_year=105120,
        percentile_window=7,
        percentile_min_history=5,
    )
    assert report == json.loads((output / "features.json").read_text(encoding="utf-8"))
    assert report["source_database_sha256"] == hashlib.sha256(original).hexdigest()
    assert database.read_bytes() == original
    assert not database.with_suffix(".duckdb.wal").exists()
    trend = report["trend_report"]
    assert trend["base"] == report["feature_report"]
    assert trend["base"]["bars_available"] == trend["base"]["bars_used"] == 60
    assert trend["base"]["latest_close_ms"] == rows[-1].event_time_ms
    assert trend["base"]["annualization_periods_per_year"] == 105120.0
    computed = values(trend)
    assert computed["adx_14"]["value"] == pytest.approx(100)
    assert computed["plus_di_14"]["value"] == pytest.approx(25)
    assert computed["minus_di_14"]["value"] == 0
    assert computed["bollinger_width_20_percentile"]["value"] == 0
    assert computed["realized_vol_20_percentile"]["value"] == 0
    for reference in trend["percentile_references"]:
        assert reference["observations"] == reference["candidate_observations"] == 7
        assert reference["minimum_history"] == 5 and reference["window"] == 7
        assert reference["first_close_ms"] == rows[52].event_time_ms
        assert reference["latest_close_ms"] == rows[58].event_time_ms
        assert reference["current_excluded"] and reference["tie_policy"] == "exact_midrank"
    assert trend["regime_classification_complete"] is trend["ready_for_signals"] is False
    assert report["source_mutated"] is report["trading_enabled"] is False


async def test_default_output_shape_is_unchanged_and_missing_annualization_is_honest(tmp_path):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    default = research_features(database, tmp_path / "default", **arguments(rows))
    explicit = research_features(
        database, tmp_path / "false", **arguments(rows), include_trend=False
    )
    assert default == explicit and "trend_report" not in default
    enabled = research_features(
        database, tmp_path / "enabled", **arguments(rows), include_trend=True
    )
    assert {key: value for key, value in enabled.items() if key != "trend_report"} == default
    trend = enabled["trend_report"]
    value = values(trend)["realized_vol_20_percentile"]
    assert value["state"] == "UNAVAILABLE" and value["value"] is None
    assert value["reason"] == "ANNUALIZATION_NOT_SPECIFIED"
    assert trend["percentile_references"][1]["observations"] == 0
    assert values(trend)["bollinger_width_20_percentile"]["state"] == "AVAILABLE"


@pytest.mark.parametrize(
    "policy",
    [
        {"include_trend": 1},
        {"include_trend": True, "percentile_window": True},
        {"include_trend": True, "percentile_window": 100.0},
        {"include_trend": True, "percentile_window": 0},
        {"include_trend": True, "percentile_window": 1001},
        {"include_trend": True, "percentile_min_history": True},
        {"include_trend": True, "percentile_window": 5, "percentile_min_history": 6},
        {"percentile_window": 5},
        {"include_trend": True, "annualization_periods_per_year": float("nan")},
    ],
)
async def test_invalid_policy_cannot_publish_output_or_mutate_source(tmp_path, policy):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    original = database.read_bytes()
    output = tmp_path / "result"
    with pytest.raises(ValueError):
        research_features(database, output, **arguments(rows), **policy)
    assert not output.exists()
    assert database.read_bytes() == original


@pytest.mark.parametrize("case", ["incomplete", "gap", "stale"])
async def test_quality_diagnostics_block_trend_and_base_together(tmp_path, case):
    rows = [bar(index) for index in range(60)]
    if case == "incomplete":
        rows[10] = bar(10, complete=False)
    elif case == "gap":
        rows.pop(10)
    options = arguments(rows)
    if case == "stale":
        options["as_of_ms"] += 600001
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    report = research_features(database, tmp_path / "result", **options, include_trend=True)
    assert report["feature_report"]["blockers"]
    assert report["trend_report"]["base"] == report["feature_report"]
    assert all(
        value["state"] == "UNAVAILABLE" and value["value"] is None
        for value in report["trend_report"]["features"]
    )


async def test_closed_source_requirement_and_history_budget_prevent_trend_publication(tmp_path):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    with pytest.raises(IntegrityError, match="HISTORY_BUDGET"):
        research_features(
            database, tmp_path / "overflow", **arguments(rows), include_trend=True, max_bars=59
        )
    marker = database.with_suffix(".duckdb.wal")
    marker.write_bytes(b"active-writer-marker")
    with pytest.raises(IntegrityError, match="MUST_BE_CLOSED"):
        research_features(database, tmp_path / "active", **arguments(rows), include_trend=True)
    assert not (tmp_path / "overflow").exists() and not (tmp_path / "active").exists()


async def test_explicit_history_window_preserves_warmup_and_reference_provenance(tmp_path):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    start = rows[30].event_time_ms
    result = research_features(
        database,
        tmp_path / "window",
        **arguments(rows),
        include_trend=True,
        annualization_periods_per_year=100,
        history_start_ms=start,
    )
    assert result["source_scope_and_cutoff"]["explicit_history_start_ms"] == start
    trend = result["trend_report"]
    assert trend["base"]["first_close_ms"] == start and trend["base"]["bars_used"] == 30
    assert values(trend)["adx_14"]["state"] == "AVAILABLE"
    assert values(trend)["bollinger_width_20_percentile"]["state"] == "WARMUP"
    assert values(trend)["realized_vol_20_percentile"]["state"] == "WARMUP"
    assert [reference["observations"] for reference in trend["percentile_references"]] == [10, 9]


async def test_mismatched_trend_base_cannot_be_published(tmp_path, monkeypatch):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    original = research.compute_trend_features

    def altered(*args, **kwargs):
        trend = original(*args, **kwargs)
        return trend.model_copy(
            update={"base": trend.base.model_copy(update={"as_of_ms": trend.base.as_of_ms - 1})}
        )

    monkeypatch.setattr(research, "compute_trend_features", altered)
    with pytest.raises(IntegrityError, match="TREND_BASE_MISMATCH"):
        research_features(database, tmp_path / "mismatch", **arguments(rows), include_trend=True)
    assert not (tmp_path / "mismatch").exists()


def cli_arguments(database, output, rows):
    return [
        "research-features",
        "--database",
        str(database),
        "--output",
        str(output),
        "--asset",
        "BTC",
        "--environment",
        "public_reference",
        "--source",
        "fixture",
        "--symbol",
        "BTC/USD",
        "--timeframe-s",
        "300",
        "--as-of-ms",
        str(rows[-1].observed_at_ms),
    ]


async def test_actual_cli_parser_and_main_wire_enabled_policy_into_committed_report(
    tmp_path, monkeypatch, capsys
):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    output = tmp_path / "cli-result"
    await save(database, rows)
    argv = cli_arguments(database, output, rows) + [
        "--include-trend",
        "--percentile-window",
        "7",
        "--percentile-min-history",
        "5",
        "--annualization-periods-per-year",
        "100",
    ]
    parsed = parser().parse_args(argv)
    assert (
        parsed.include_trend
        and parsed.percentile_window == 7
        and parsed.percentile_min_history == 5
    )
    assert parsed.annualization_periods_per_year == 100.0
    monkeypatch.setattr(sys, "argv", ["selective-options", *argv])
    assert main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads((output / "features.json").read_text(encoding="utf-8"))
    assert result["trend_report"]["base"]["annualization_periods_per_year"] == 100.0
    assert result["trend_report"]["percentile_references"][0]["window"] == 7
    assert values(result["trend_report"])["adx_14"]["state"] == "AVAILABLE"


async def test_cli_default_has_no_trend_and_invalid_opt_in_policy_returns_failure(
    tmp_path, monkeypatch, capsys
):
    rows = [bar(index) for index in range(60)]
    database = tmp_path / "source.duckdb"
    await save(database, rows)
    argv = cli_arguments(database, tmp_path / "default", rows)
    parsed = parser().parse_args(argv)
    assert parsed.include_trend is False and parsed.percentile_window == 100
    monkeypatch.setattr(sys, "argv", ["selective-options", *argv])
    assert main() == 0
    assert "trend_report" not in json.loads(capsys.readouterr().out)
    invalid = cli_arguments(database, tmp_path / "invalid", rows) + [
        "--include-trend",
        "--percentile-window",
        "1",
    ]
    monkeypatch.setattr(sys, "argv", ["selective-options", *invalid])
    assert main() == 2
    assert not (tmp_path / "invalid").exists()
