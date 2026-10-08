"""Read-only finite clock explanations preserve canonical PIT, ties and blockers."""

import json

import duckdb
import pytest

from scripts.audit_option_freshness import (
    ProjectionReader,
    audit_capture,
    audit_database,
    read_json,
    seconds_ms,
)
from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore
from tests.test_option_coverage import control, fixture
from tests.test_option_projection import quote


async def database(tmp_path, rows):
    path = tmp_path / "events.duckdb"
    store = await DuckStore.open(path)
    try:
        await store.append(rows)
    finally:
        await store.close()
    return path


def leg(result, kind):
    return next(item for item in result["legs"] if item["kind"] == kind)


async def test_real_negative_clocks_native_provenance_and_readonly_bytes(
    tmp_path, instrument, ticker
):
    rows = fixture(instrument, ticker)
    rows[-2] = quote(rows[-2], 1450, 1300)
    path = await database(tmp_path, rows)
    before = path.read_bytes()
    result = audit_database(path, "BTC", 1500, leg_age=200)
    assert result["coverage"] == coverage_at(rows, "BTC", 1500, 200)
    assert path.read_bytes() == before
    assert result["projection_fetch_counts"] == [3, 1, 2]
    assert result["history_rows_scanned_for_statistics"] == 2
    details = leg(result, "ticker")["latest_pair_candidates"][0]
    assert details["source_age_ms"] == 50 and details["receipt_age_ms"] == 200
    assert details["receipt_minus_source_ms"] == -150
    assert details["negative_clock_difference"]
    assert details["native_timestamp_value"] == 1450
    assert details["native_timestamp_matches_event"]
    assert details["timestamp_kind"] == "source"
    assert leg(result, "ticker")["canonical_state"] == "OBSERVED"
    assert leg(result, "ticker")["selected_session_statistics"]["negative_difference_count"] == 1
    assert result["selected_session"]["resync_at_ms"] == 1200
    assert not result["trading_enabled"] and not result["milestone_2_complete"]
    assert result["new_network_requests"] == result["orders_submitted"] == 0


@pytest.mark.parametrize("cutoff", [1199, 1200, 1300, 1399, 1400, 1500, 1599, 1600, 1799, 1800])
async def test_independent_pit_and_selected_session_match_full_oracle(
    tmp_path, instrument, ticker, cutoff
):
    rows = fixture(instrument, ticker)
    rows += [
        quote(rows[-2], 1600, 1300, mark_iv=60),
        quote(rows[-1], 1300, 1800, change_id=2),
        quote(rows[-2], 1700, 1199, mark_iv=61),
    ]
    result = audit_database(await database(tmp_path, rows), "BTC", cutoff)
    assert result["coverage"] == coverage_at(rows, "BTC", cutoff, 30000)
    if cutoff < 1400:
        assert result["status"] == "CONTEXT_BLOCKED"
        assert not result["legs"]
    else:
        for item in result["legs"]:
            for chosen in item["latest_pair_candidates"]:
                assert chosen["event_time_ms"] <= cutoff
                assert 1200 <= chosen["observed_at_ms"] <= cutoff


async def test_stale_native_updates_and_recent_receipts_are_distinct(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    rows[-2] = quote(rows[-2], 1250, 1490)
    rows += [quote(rows[-2], 1240, 1495, mark_iv=61)]
    result = audit_database(await database(tmp_path, rows), "BTC", 1500, leg_age=100)
    assert result["coverage"] == coverage_at(rows, "BTC", 1500, 100)
    tick = leg(result, "ticker")
    assert tick["diagnosis"] == "RECENT_RECEIPTS_WITH_OLD_NATIVE_TIMESTAMPS"
    assert tick["canonical_state"] == "STALE"
    assert tick["latest_pair_candidates"][0]["observed_at_ms"] == 1490
    assert tick["selected_session_statistics"]["last_receipt_ms"] == 1495
    assert tick["selected_session_statistics"]["recent_receipts_with_old_source_count"] == 2
    assert tick["selected_session_statistics"]["distinct_native_timestamp_count"] == 2
    assert leg(result, "book")["diagnosis"] == "NO_RECENT_NATIVE_SOURCE_UPDATES_OR_RECEIPTS"


async def test_recent_native_source_can_have_stale_receipt(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    rows[-1] = quote(rows[-1], 1490, 1300)
    result = audit_database(await database(tmp_path, rows), "BTC", 1500, leg_age=100)
    assert leg(result, "book")["diagnosis"] == "RECENT_NATIVE_SOURCE_WITH_STALE_SELECTED_RECEIPT"
    assert leg(result, "book")["canonical_state"] == "STALE"


async def test_all_latest_pair_ties_block_without_arbitrary_identity(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    rows.append(quote(rows[-1], 1300, 1300, change_id=2))
    result = audit_database(await database(tmp_path, rows), "BTC", 1500)
    assert result["coverage"]["blocker"] == "OPTION_LEG_AMBIGUOUS"
    book = leg(result, "book")
    assert book["diagnosis"] == "AMBIGUOUS_LATEST_PAIR"
    assert book["latest_pair_candidate_count"] == 2
    assert {row["event_id"] for row in book["latest_pair_candidates"]} == {
        rows[-1].event_id,
        rows[-2].event_id,
    }
    assert book["canonical_state"] is None


async def test_missing_session_leg_and_no_history_materialization(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    rows = rows[:-1]
    rows += [quote(rows[-1], t, 1300, mark_iv=t) for t in range(1, 101)]
    path = await database(tmp_path, rows)
    result = audit_database(path, "BTC", 1500, max_rows=5)
    assert result["projection_fetch_counts"] == [3, 1, 1]
    assert result["history_rows_scanned_for_statistics"] == 101
    assert leg(result, "ticker")["selected_session_statistics"]["stored_observation_count"] == 101
    assert leg(result, "book")["diagnosis"] == "MISSING_SELECTED_SESSION_OBSERVATION"
    assert leg(result, "book")["canonical_state"] == "MISSING"
    assert leg(result, "book")["selected_session_statistics"] is None
    with pytest.raises(ValueError, match="history row budget"):
        audit_database(path, "BTC", 1500, max_history_rows=100)


@pytest.mark.parametrize("mutation", ["timestamp", "kind", "payload", "invalid_json", "control"])
async def test_corrupt_native_or_control_evidence_never_claims_freshness(
    tmp_path, instrument, ticker, mutation
):
    path = await database(tmp_path, fixture(instrument, ticker))
    with duckdb.connect(str(path)) as db:
        if mutation == "timestamp":
            db.execute("UPDATE events SET event_time_ms=1301 WHERE kind='book'")
        elif mutation == "kind":
            db.execute("UPDATE events SET timestamp_kind='receipt' WHERE kind='book'")
        elif mutation == "payload":
            db.execute("UPDATE events SET payload_json='{}' WHERE kind='book'")
        elif mutation == "invalid_json":
            db.execute("UPDATE events SET payload_json='BROKEN' WHERE kind='book'")
        else:
            db.execute("UPDATE events SET payload_json='[]' WHERE kind='status'")
    result = audit_database(path, "BTC", 1500)
    assert result["coverage"]["blocker"] is not None
    if mutation == "control":
        assert result["status"] == "CONTEXT_BLOCKED" and not result["legs"]
    else:
        assert result["status"] == "CANONICAL_LEG_BLOCKED"
        assert leg(result, "book")["diagnosis"] == "CANONICAL_LEG_EVIDENCE_BLOCKED"
        if mutation == "invalid_json":
            assert not leg(result, "book")["latest_pair_candidates"][0]["payload_json_object_valid"]


@pytest.mark.parametrize("blocker", ["empty", "universe_stale", "disconnected", "tie"])
async def test_universe_and_control_blockers_preserved(tmp_path, instrument, ticker, blocker):
    rows = fixture(instrument, ticker, selected=blocker != "empty")
    if blocker == "disconnected":
        rows.append(control("STREAM_DISCONNECTED", 1450))
    if blocker == "tie":
        rows.append(control("STREAM_DISCONNECTED", 1400))
    age = 399 if blocker == "universe_stale" else 3600000
    result = audit_database(await database(tmp_path, rows), "BTC", 1500, universe_age=age)
    assert result["coverage"] == coverage_at(rows, "BTC", 1500, 30000, age)
    assert result["status"] == "CONTEXT_BLOCKED" and not result["legs"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"asset": "DOGE"},
        {"cutoff": True},
        {"leg_age": True},
        {"universe_age": True},
        {"max_rows": True},
        {"max_rows": 200001},
        {"max_history_rows": True},
        {"max_history_rows": 0},
        {"max_history_rows": 2000001},
        {"timeout_s": True},
        {"timeout_s": float("inf")},
        {"timeout_s": float("nan")},
        {"timeout_s": 121},
        {"timeout_s": 0},
        {"timeout_s": 10**400},
    ],
)
def test_policy_refuses_before_opening_database(tmp_path, kwargs):
    params = {"asset": "BTC", "cutoff": 1500} | kwargs
    with pytest.raises(ValueError, match="policy"):
        audit_database(tmp_path / "absent.duckdb", **params)


@pytest.mark.parametrize(
    "value", [True, False, 0, -1, "30", float("nan"), float("inf"), 0.0001, 10**400]
)
def test_saved_policy_no_bool_coercion_or_fractional_ms(value):
    with pytest.raises(ValueError, match="policy"):
        seconds_ms(value)


def test_duplicate_json_and_root_refused(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"x":1,"x":2}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        read_json(path)
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="object"):
        read_json(path)
    path.write_text('{"x":NaN}', encoding="utf-8")
    with pytest.raises(ValueError, match="constant"):
        read_json(path)


async def test_finite_deadline_interrupts_real_duckdb_and_source_unchanged(
    tmp_path, instrument, ticker, monkeypatch
):
    path = await database(tmp_path, fixture(instrument, ticker))
    before = path.read_bytes()

    def expensive(reader, *args):
        reader._connection.execute("SELECT sum(sin(i)) FROM range(1000000000) t(i)").fetchone()
        raise AssertionError("deadline failed to interrupt finite oversized diagnostic")

    monkeypatch.setattr(ProjectionReader, "_option_coverage", expensive)
    with pytest.raises(TimeoutError, match="deadline"):
        audit_database(path, "BTC", 1500, timeout_s=0.01)
    assert path.read_bytes() == before


async def test_capture_exact_saved_view_hashes_zero_requests_and_refuse_mismatch(
    tmp_path, instrument, ticker
):
    rows = fixture(instrument, ticker)
    await database(tmp_path, rows)
    saved = {"ended_at_ms": 1500, "option_stream_coverage": [coverage_at(rows, "BTC", 1500, 30000)]}
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(saved), encoding="utf-8")
    (tmp_path / "config.sanitized.json").write_text(
        json.dumps(
            {"data": {"max_staleness_s": 30}, "ingestion": {"option_universe_max_age_s": 3600}}
        ),
        encoding="utf-8",
    )
    result = audit_capture(tmp_path, ("BTC",), tmp_path / "out")
    assert result["saved_coverage_exact"] and result["source_bytes_unchanged"]
    assert result["new_network_requests"] == 0
    assert len(result["source_hashes"]) == 3
    saved["option_stream_coverage"][0]["state"] = "INCORRECT"
    report_path.write_text(json.dumps(saved), encoding="utf-8")
    with pytest.raises(ValueError, match="differs"):
        audit_capture(tmp_path, ("BTC",), tmp_path / "refuse")
    assert not (tmp_path / "refuse").exists()


def test_capture_assets_and_existing_output_preflight(tmp_path):
    with pytest.raises(ValueError, match="assets"):
        audit_capture(tmp_path / "absent", ("BTC", "BTC"), tmp_path / "new")
    with pytest.raises(FileExistsError, match="fresh output"):
        audit_capture(tmp_path / "absent", ("BTC",), tmp_path)


def test_database_and_json_byte_budgets_before_content_or_query(tmp_path):
    path = tmp_path / "oversize.duckdb"
    with path.open("wb") as handle:
        handle.truncate(1024**3 + 1)
    with pytest.raises(ValueError, match="1 GiB"):
        audit_database(path, "BTC", 1500)
    json_path = tmp_path / "oversize.json"
    with json_path.open("wb") as handle:
        handle.truncate(8 * 1024**2 + 1)
    with pytest.raises(ValueError, match="8 MiB"):
        read_json(json_path)


async def test_projection_overflow_preserves_canonical_blocker(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    result = audit_database(await database(tmp_path, rows), "BTC", 1500, max_rows=3)
    assert result["coverage"]["blocker"] == "OPTION_COVERAGE_OVER_BUDGET"
    assert not result["legs"]
