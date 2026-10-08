"""Native field inventory refuses defaults, coercion, lookahead and vacuous passes."""

import json

import pytest

from scripts.audit_option_native_fields import (
    audit_capture,
    audit_events,
    field,
    number,
    saved_rows,
)
from selective_options.events import make_event
from selective_options.store import DuckStore
from tests.test_option_coverage import control, fixture
from tests.test_option_projection import quote


def native_rows(instrument, ticker, **extra):
    return fixture(
        instrument,
        ticker
        | {
            "mark_price": 0.0205,
            "bid_iv": 54,
            "ask_iv": 56,
            "stats": {"volume": 0, "volume_usd": 0},
            "index_price": 100000,
            "underlying_index": "BTC-INDEX",
            **extra,
        },
    )


@pytest.mark.parametrize("value", [True, False, "12", [], {}, float("inf"), float("nan"), -1])
def test_finite_native_numbers_do_not_coerce(value):
    assert not number(value, 0)


def test_huge_native_integer_is_preserved_without_float_overflow():
    value = 10**400
    assert not number(value, 0)
    assert field({"volume": value}, "volume", number) == {"state": "INVALID", "value": value}


@pytest.mark.parametrize(
    "body,state,at",
    [
        ({}, "ABSENT", "stats"),
        ({"stats": {}}, "ABSENT", "stats.volume"),
        ({"stats": None}, "NULL_PARENT", "stats"),
        ({"stats": []}, "INVALID_PARENT", "stats"),
        ({"stats": {"volume": None}}, "NULL", None),
        ({"stats": {"volume": "0"}}, "INVALID", None),
        ({"stats": {"volume": 0}}, "VALID", None),
    ],
)
def test_exact_nested_absent_null_invalid_and_zero(body, state, at):
    result = field(body, "stats.volume", lambda v: number(v, 0))
    assert result["state"] == state
    if at:
        assert result["at"] == at
    if state == "ABSENT":
        assert "value" not in result
    else:
        assert (
            result["value"] == body["stats"]
            if state.endswith("PARENT")
            else result["value"] == body["stats"]["volume"]
        )


def test_native_exact_inventory_with_no_feature_or_unit_claim(instrument, ticker):
    rows = native_rows(
        instrument, ticker, stats={"volume": 9007199254740993, "unknown_native": "preserved"}
    )
    result = audit_events(rows, "BTC", 1500)
    assert result["full_native_chain_fresh"]
    member = result["members"][0]
    assert member["fields"]["stats.volume"]["value"] == 9007199254740993
    assert member["native_stats_keys"] == ["unknown_native", "volume"]
    assert member["ticker_event"]["payload_json"] == rows[-2].payload_json
    assert member["metadata_event"]["payload_json"] == rows[0].payload_json
    assert member["additional_volume_inventory"]["volume"]["state"] == "ABSENT"
    assert not result["milestone_2_complete"] and not result["trading_enabled"]


@pytest.mark.parametrize(
    "extra,path,state",
    [
        ({"stats": None}, "stats.volume", "NULL_PARENT"),
        ({"stats": []}, "stats.volume", "INVALID_PARENT"),
        ({"stats": {"volume": None}}, "stats.volume", "NULL"),
        ({"stats": {"volume": -1}}, "stats.volume", "INVALID"),
        ({"stats": {}}, "stats.volume", "ABSENT"),
        ({"mark_price": "0.1"}, "mark_price", "INVALID"),
        ({"mark_price": True}, "mark_price", "INVALID"),
        ({"mark_price": None}, "mark_price", "NULL"),
        ({"bid_iv": None}, "bid_iv", "NULL"),
    ],
)
def test_optional_native_fields_stay_incomplete(instrument, ticker, extra, path, state):
    result = audit_events(native_rows(instrument, ticker, **extra), "BTC", 1500)
    assert result["coverage"]["state"] == "SELECTED_FEEDS_OBSERVED"
    assert result["field_counts"][path] == {state: 1}
    assert not result["full_native_chain_fresh"]


def test_summary_is_never_ticker_replacement(instrument, ticker):
    rows = native_rows(instrument, ticker)
    rows = [row for row in rows if row.kind != "ticker"]
    rows.append(
        make_event(
            source="deribit",
            environment="testnet",
            asset="BTC",
            symbol=instrument["instrument_name"],
            kind="chain_summary",
            event_time_ms=1500,
            observed_at_ms=1500,
            timestamp_kind="receipt",
            payload={"mark_price": 123, "volume": 456, "creation_timestamp": 1},
        )
    )
    result = audit_events(rows, "BTC", 1500)
    assert result["members"][0]["fields"]["mark_price"] == {"state": "MISSING_TICKER"}
    assert not result["full_native_chain_fresh"]


def test_stale_book_blocks_native_pass_even_with_complete_ticker(instrument, ticker):
    result = audit_events(native_rows(instrument, ticker), "BTC", 1500, leg_age=199)
    assert result["all_selected_ticker_fields_valid"]
    assert not result["full_native_chain_fresh"]


@pytest.mark.parametrize(
    "asset,cutoff,budget",
    [
        ("DOGE", 1500, 100),
        ("BTC", True, 100),
        ("BTC", 1500, True),
        ("BTC", 1500, 200001),
        ("BTC", -1, 100),
    ],
)
def test_policy_preflight(asset, cutoff, budget, tmp_path):
    with pytest.raises(ValueError, match="policy"):
        saved_rows(tmp_path / "missing.duckdb", asset, cutoff, 30000, 3600000, budget)


@pytest.mark.parametrize("leg_age,universe_age", [(True, 3600000), (30000, True)])
def test_age_bools_rejected_before_database_open(tmp_path, leg_age, universe_age):
    with pytest.raises(ValueError, match="policy"):
        saved_rows(tmp_path / "missing.duckdb", "BTC", 1500, leg_age, universe_age, 100)


def test_conflicting_metadata_cannot_yield_fields(instrument, ticker):
    rows = native_rows(instrument, ticker)
    rows.append(rows[0].model_copy(update={"payload_json": "{}"}))
    result = audit_events(rows, "BTC", 1500)
    assert result["coverage"]["blocker"] == "OPTION_METADATA_MISSING_OR_CONFLICTING"
    assert not result["members"] and not result["full_native_chain_fresh"]


async def test_readonly_sql_latest_pair_both_clocks_session_ties_and_bytes(
    tmp_path, instrument, ticker
):
    rows = native_rows(instrument, ticker)
    rows += [quote(rows[-2], t, 1300, mark_price=0.01) for t in range(1, 100)]
    rows += [
        quote(rows[4], 1600, 1300, mark_price=0.011),
        quote(rows[4], 1400, 1600, mark_price=0.012),
        quote(rows[4], 1700, 1199, mark_price=0.013),
    ]
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(rows)
    finally:
        await store.close()
    original = path.read_bytes()
    selected = saved_rows(path, "BTC", 1500, 30000, 3600000, 6)
    assert len(selected) == 6
    assert audit_events(selected, "BTC", 1500) == audit_events(rows, "BTC", 1500)
    assert path.read_bytes() == original
    # Later receipts/source times become visible independently; exact latest ties block.
    conflicting = quote(rows[4], 1300, 1300, mark_price=0.014)
    store = await DuckStore.open(path)
    try:
        await store.append([conflicting])
    finally:
        await store.close()
    selected = saved_rows(path, "BTC", 1500, 30000, 3600000, 7)
    assert len([row for row in selected if row.kind == "ticker"]) == 2
    assert audit_events(selected, "BTC", 1500)["coverage"]["blocker"] == "OPTION_LEG_AMBIGUOUS"
    with pytest.raises(ValueError, match="budget"):
        saved_rows(path, "BTC", 1500, 30000, 3600000, 6)


def test_empty_and_disconnected_are_not_native_pass(instrument, ticker):
    rows = fixture(instrument, ticker, selected=False)
    result = audit_events(rows, "BTC", 1500)
    assert result["coverage"]["blocker"] == "OPTION_SELECTED_UNIVERSE_EMPTY"
    assert not result["members"] and not result["full_native_chain_fresh"]
    rows = native_rows(instrument, ticker) + [control("STREAM_DISCONNECTED", 1450)]
    assert audit_events(rows, "BTC", 1500)["coverage"]["blocker"] == "OPTION_STREAM_INACTIVE"


def test_application_budget_and_capture_fresh_output_preflight(instrument, ticker, tmp_path):
    with pytest.raises(ValueError, match="budget"):
        audit_events(native_rows(instrument, ticker), "BTC", 1500, max_rows=5)
    with pytest.raises(FileExistsError):
        audit_capture(tmp_path / "missing", ("BTC",), tmp_path)
    with pytest.raises(ValueError, match="assets"):
        audit_capture(tmp_path / "missing", ("BTC", "BTC"), tmp_path / "new")


async def test_capture_exact_saved_view_readonly_hashes_and_refuse_mismatch(
    tmp_path, instrument, ticker
):
    from selective_options.option_coverage import coverage_at

    rows = native_rows(instrument, ticker)
    capture = tmp_path / "capture"
    capture.mkdir()
    store = await DuckStore.open(capture / "events.duckdb")
    try:
        await store.append(rows)
    finally:
        await store.close()
    report = {
        "ended_at_ms": 1500,
        "option_stream_coverage": [coverage_at(rows, "BTC", 1500, 30000, 3600000)],
    }
    (capture / "report.json").write_text(json.dumps(report), encoding="utf-8")
    (capture / "config.sanitized.json").write_text(
        json.dumps(
            {"data": {"max_staleness_s": 30}, "ingestion": {"option_universe_max_age_s": 3600}}
        ),
        encoding="utf-8",
    )
    result = audit_capture(capture, ("BTC",), tmp_path / "audit")
    assert result["original_bytes_unchanged"] and result["saved_coverage_exact"]
    assert result["new_network_requests"] == 0
    report["option_stream_coverage"][0]["state"] = "invented"
    (capture / "report.json").write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ValueError, match="differs"):
        audit_capture(capture, ("BTC",), tmp_path / "mismatch")
    assert not (tmp_path / "mismatch").exists()
