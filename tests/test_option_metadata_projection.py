"""Exact manifest metadata generations, control audits and shared real-storage budgets."""

import duckdb
import pytest

from selective_options.option_coverage import coverage_at
from selective_options.store import DuckStore
from tests.test_option_coverage import control, fixture, metadata, replace_payload


def observe_reads(store):
    calls, select = [], store._select

    def observed(sql, params):
        values = select(sql, params)
        calls.append((sql, len(values)))
        return values

    store._select = observed
    return calls


async def test_obsolete_generations_do_not_materialize_but_stay_in_archive_and_outbox(
    tmp_path, instrument, ticker
):
    rows = fixture(instrument, ticker)
    # Same-symbol generations, retired symbols, and a newer unreferenced generation
    # cannot replace the exact metadata ID associated with the acknowledged manifest.
    history = [metadata(instrument, receipt) for receipt in range(10, 110)]
    history += [
        metadata(instrument | {"instrument_name": f"BTC-RETIRED-{i}"}, 900) for i in range(20)
    ]
    history.append(metadata(instrument | {"strike": 120001}, 1050))
    # An unreferenced corrupt instrument is preserved but is not manifest evidence.
    history.append(metadata(instrument, 950).model_copy(update={"payload_json": "{}"}))
    expected = coverage_at(rows, "BTC", 1500, max_rows=6)
    assert coverage_at([*rows, *history], "BTC", 1500, max_rows=6)["blocker"] == (
        "OPTION_COVERAGE_OVER_BUDGET"
    )
    path, archive = tmp_path / "metadata.duckdb", tmp_path / "metadata.parquet"
    store = await DuckStore.open(path)
    calls = observe_reads(store)
    try:
        await store.append([*rows, *history], exact=True)
        await store.append([*rows, *history], exact=True)
        assert await store.option_coverage("BTC", 1500, max_rows=6) == expected
        assert [count for _, count in calls] == [3, 1, 2]
        assert await store.export_parquet(archive) == len(rows) + len(history)
        assert await store.pending_delivery_count() == len(rows) + len(history)
    finally:
        await store.close()
    store = await DuckStore.open(path)
    try:
        assert await store.option_coverage("BTC", 1500, max_rows=6) == expected
        assert await store.pending_delivery_count() == len(rows) + len(history)
    finally:
        await store.close()
    with duckdb.connect(str(path), read_only=True) as db:
        assert (
            db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == db.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(archive)]
            ).fetchall()
        )


@pytest.mark.parametrize("scenario", ["missing", "identity", "payload", "symbol", "inactive"])
async def test_current_metadata_failure_never_queries_quotes(
    tmp_path, instrument, ticker, scenario
):
    rows = fixture(instrument, ticker)
    if scenario == "missing":
        rows.pop(0)
    elif scenario == "identity":
        rows[0] = rows[0].model_copy(update={"event_id": "wrong"})
    elif scenario == "payload":
        rows[0] = rows[0].model_copy(update={"payload_json": "{}"})
    elif scenario == "symbol":
        rows[0] = rows[0].model_copy(update={"symbol": "BTC-WRONG"})
    else:
        rows[0] = replace_payload(rows[0], is_active=False)
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append(rows)
        expected = coverage_at(rows, "BTC", 1500)
        assert expected["state"] == "BLOCKED"
        assert await store.option_coverage("BTC", 1500) == expected
        assert len(calls) == 2
        assert "kind='instrument'" in calls[1][0]
    finally:
        await store.close()


@pytest.mark.parametrize("clock", ["event_time_ms", "observed_at_ms"])
@pytest.mark.parametrize("cutoff", [1599, 1600])
async def test_exact_metadata_binding_has_two_independent_clock_boundaries(
    tmp_path, instrument, ticker, clock, cutoff
):
    rows = fixture(instrument, ticker)
    rows[0] = rows[0].model_copy(update={clock: 1600})
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append(rows)
        expected = coverage_at(rows, "BTC", cutoff)
        assert expected["blocker"] == (
            "OPTION_METADATA_MISSING_OR_CONFLICTING"
            if cutoff == 1599
            else "OPTION_METADATA_CANONICAL_OR_SCOPE"
        )
        assert await store.option_coverage("BTC", cutoff) == expected
        assert [count for _, count in calls] == [3, int(cutoff == 1600)]
    finally:
        await store.close()


@pytest.mark.parametrize("scenario", ["old_corrupt_control", "tied_control", "tied_resync"])
async def test_context_integrity_is_audited_before_metadata_selection(
    tmp_path, instrument, ticker, scenario
):
    rows = fixture(instrument, ticker)
    if scenario == "old_corrupt_control":
        rows.append(control("STREAM_DISCONNECTED", 500).model_copy(update={"event_id": "bad"}))
    elif scenario == "tied_control":
        rows.append(control("STREAM_DISCONNECTED", 1400, connection=1))
    else:
        rows.append(control("STREAM_RESYNC", 1199, connection=1))
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append(rows)
        expected = coverage_at(rows, "BTC", 1500)
        assert expected["state"] == "BLOCKED"
        assert await store.option_coverage("BTC", 1500) == expected
        assert len(calls) == 1
    finally:
        await store.close()


@pytest.mark.parametrize("budget", range(1, 9))
async def test_unselected_current_metadata_consumes_the_shared_application_budget(
    tmp_path, instrument, ticker, budget
):
    rows = fixture(instrument, ticker)
    unselected = metadata(instrument | {"instrument_name": "BTC-UNSELECTED", "strike": 120000})
    rows[1] = replace_payload(
        rows[1],
        available=2,
        coverage="PARTIAL_OR_ABSENT",
        instruments=[rows[0].symbol, unselected.symbol],
        metadata_event_ids=[rows[0].event_id, unselected.event_id],
    )
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    rows.append(unselected)
    history = [metadata(instrument, receipt) for receipt in range(10, 60)]
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append([*rows, *history])
        assert await store.option_coverage("BTC", 1500, max_rows=budget) == coverage_at(
            rows, "BTC", 1500, max_rows=budget
        )
        assert sum(count for _, count in calls) <= budget + 1
    finally:
        await store.close()


async def test_corrupt_unselected_current_metadata_is_not_omitted(tmp_path, instrument, ticker):
    rows = fixture(instrument, ticker)
    unselected = metadata(instrument | {"instrument_name": "BTC-UNSELECTED", "strike": 120000})
    rows[1] = replace_payload(
        rows[1],
        available=2,
        coverage="PARTIAL_OR_ABSENT",
        instruments=[rows[0].symbol, unselected.symbol],
        metadata_event_ids=[rows[0].event_id, unselected.event_id],
    )
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    rows.append(unselected.model_copy(update={"payload_json": "{}"}))
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append(rows)
        expected = coverage_at(rows, "BTC", 1500)
        assert expected["blocker"] == "OPTION_INVALID_EVIDENCE"
        assert await store.option_coverage("BTC", 1500) == expected
        assert [count for _, count in calls] == [3, 2]
    finally:
        await store.close()


@pytest.mark.parametrize("payload", ["invalid", "[]", "true", "null"])
async def test_corrupt_current_metadata_root_blocks_after_exact_lookup(
    tmp_path, instrument, ticker, payload
):
    rows = fixture(instrument, ticker)
    store = await DuckStore.open(tmp_path / "metadata.duckdb")
    calls = observe_reads(store)
    try:
        await store.append(rows)
        store._connection.execute(
            "UPDATE events SET payload_json=? WHERE event_id=?", [payload, rows[0].event_id]
        )
        corrupted = [rows[0].model_copy(update={"payload_json": payload}), *rows[1:]]
        expected = coverage_at(corrupted, "BTC", 1500)
        assert expected["blocker"] == "OPTION_INVALID_EVIDENCE"
        assert await store.option_coverage("BTC", 1500) == expected
        assert [count for _, count in calls] == [3, 1]
    finally:
        await store.close()
