"""Synthetic DFII10 CSV/transport fixtures; no availability or trading claims."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock

import aiohttp
import duckdb
import pytest

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore
from selective_options.tips_yield import (
    MAX_BYTES,
    PollHealth,
    TipsYieldCollector,
    request_url,
    tips_yield_health,
    tips_yield_quality,
    tips_yield_snapshot,
)

START = date(2026, 9, 28)
END = date(2026, 10, 2)
RECEIVED = int(datetime(2026, 10, 4, 12, tzinfo=UTC).timestamp() * 1000)
POLLED = RECEIVED + 10
CSV = b"observation_date,DFII10\n2026-09-28,2.30\n2026-09-29,-0.50\n2026-09-30,.\n2026-10-01,\n2026-10-02,2.40\n"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def workspace_temp():
    scratch = ROOT.parents[1] / "work/tips-yield-0339"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as temporary:
        path = Path(temporary).resolve()
        assert path.is_relative_to(scratch.resolve())
        yield path


def snapshot(body=CSV, received=RECEIVED):
    return tips_yield_snapshot(body, START, END, received)


def healthy(row, polled=POLLED, response=RECEIVED):
    return tips_yield_health(
        polled, snapshot_event_id=row.event_id, response_received_at_ms=response, http_status=200
    )


def quality(rows, at=POLLED, **kwargs):
    return tips_yield_quality(rows, as_of_ms=at, **kwargs)


def changed_payload(row, **updates):
    return row.model_copy(update={"payload_json": json.dumps({**row.payload(), **updates})})


def test_native_percent_negative_values_missing_kinds_and_receipt_semantics():
    row = snapshot()
    body = row.payload()
    assert body["raw_csv_hex"] == CSV.hex()
    assert body["units"] == "percent"
    assert body["rows"][0] == {
        "observation_date": "2026-09-28",
        "yield_percent_text": "2.30",
        "missing": "not_missing",
    }
    assert body["rows"][1]["yield_percent_text"] == "-0.50"
    assert body["rows"][2]["missing"] == "dot"
    assert body["rows"][3]["missing"] == "empty"
    assert row.timestamp_kind == "receipt"
    assert row.event_time_ms == row.observed_at_ms == RECEIVED
    assert (row.source, row.asset, row.symbol, row.kind) == (
        "fred_tips",
        "GLOBAL",
        "DFII10",
        "index",
    )
    result = quality([row, healthy(row)])
    assert result["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert result["latest_value_percent_text"] == "2.40"
    assert result["latest_value_date"] == "2026-10-02" and result["latest_value_date_age_days"] == 2
    assert result["missing_dot_rows"] == result["missing_empty_rows"] == 1
    assert all(
        result[field] is False
        for field in (
            "trading_approval",
            "continuous_history_verified",
            "complete_feed_available",
            "publication_time_available",
            "historical_vintage_available",
        )
    )


def test_entire_snapshot_revision_removal_and_missing_latest_value_retained():
    original = snapshot()
    revised = snapshot(b"observation_date,DFII10\n2026-09-28,2.31\n2026-10-02,.\n", RECEIVED + 1000)
    poll = healthy(revised, POLLED + 1000, RECEIVED + 1000)
    rows = [original, healthy(original), revised, poll]
    assert quality(rows, POLLED + 999)["snapshot_event_id"] == original.event_id
    result = quality(rows, POLLED + 1000)
    assert result["snapshot_event_id"] == revised.event_id
    assert result["returned_rows"] == 2 and result["latest_value_date"] == "2026-09-28"
    assert result["latest_value_percent_text"] == "2.31"
    assert result["latest_returned_date"] == "2026-10-02"
    assert original.payload()["rows"] != revised.payload()["rows"]
    repeated = snapshot(received=RECEIVED + 2000)
    assert repeated.event_id != original.event_id
    assert repeated.payload() == original.payload()


def test_exact_two_clock_poll_boundaries_outage_and_recovery():
    row = snapshot()
    poll = healthy(row)
    failure = tips_yield_health(POLLED + 100, error_type="FixtureOutage")
    recovered_row = snapshot(received=RECEIVED + 200)
    recovery = healthy(recovered_row, POLLED + 200, RECEIVED + 200)
    rows = [row, poll, failure, recovered_row, recovery]
    assert quality(rows, RECEIVED - 1)["blocker"] == "TIPS_HEALTH_MISSING"
    assert quality(rows, RECEIVED)["blocker"] == "TIPS_HEALTH_MISSING"
    assert quality(rows, POLLED)["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert quality(rows, POLLED + 99)["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert quality(rows, POLLED + 100)["blocker"] == "TIPS_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 199)["blocker"] == "TIPS_FEED_UNAVAILABLE"
    assert quality(rows, POLLED + 200)["snapshot_event_id"] == recovered_row.event_id
    assert quality([row, poll], RECEIVED + 7200000)["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert quality([row, poll], RECEIVED + 7200001)["blocker"] == "TIPS_POLL_STALE"


@pytest.mark.parametrize(
    "body", [b"observation_date,DFII10\n", b"observation_date,DFII10\n2026-10-02,.\n"]
)
def test_empty_and_only_missing_snapshots_prove_poll_only(body):
    row = snapshot(body)
    result = quality([row, healthy(row)])
    assert result["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert result["dated_value_state"] == "NO_REPORTED_VALUE"
    assert result["latest_value_date"] is None
    assert result["latest_value_percent_text"] is None
    assert result["complete_feed_available"] is False


@pytest.mark.parametrize(
    "body",
    [
        b"DATE,DFII10\n2026-10-02,2.40\n",
        b"observation_date,DGS10\n2026-10-02,2.40\n",
        b"<html>Error</html>",
        b"observation_date,DFII10,EXTRA\n",
        b"observation_date,DFII10\n2026-10-02,1,2\n",
        b"observation_date,DFII10\n2026-09-27,2.40\n",
        b"observation_date,DFII10\n2026-10-03,2.40\n",
        b"observation_date,DFII10\n20261002,2.40\n",
        b"observation_date,DFII10\n2026-10-02,NaN\n",
        b"observation_date,DFII10\n2026-10-02,inf\n",
        b"observation_date,DFII10\n2026-10-02,2e0\n",
        b"observation_date,DFII10\n2026-10-02, 2.40\n",
        b"observation_date,DFII10\n2026-10-02,2.40%\n",
        b"observation_date,DFII10\n2026-10-02,2.40\n2026-10-02,2.40\n",
        b"observation_date,DFII10\n2026-10-02,2.40\n2026-09-28,2.30\n",
        b"observation_date,DFII10\n\n",
        b"\xff",
    ],
)
def test_malformed_csv_and_unbounded_semantics_are_rejected(body):
    with pytest.raises((ValueError, IntegrityError)):
        snapshot(body)


def test_window_and_byte_bounds_are_strict_and_no_future_daily_date():
    with pytest.raises(IntegrityError, match="BYTE_BUDGET"):
        snapshot(b" " * (MAX_BYTES + 1))
    with pytest.raises(ValueError):
        request_url(date(2025, 1, 1), date(2026, 1, 2))
    with pytest.raises(ValueError):
        request_url(END, START)
    with pytest.raises(ValueError):
        snapshot(received=True)
    with pytest.raises(IntegrityError, match="FUTURE_WINDOW"):
        tips_yield_snapshot(
            CSV, START, END, int(datetime(2026, 10, 1, tzinfo=UTC).timestamp() * 1000)
        )
    assert (
        request_url(START, END)
        == "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFII10&cosd=2026-09-28&coed=2026-10-02"
    )


@pytest.mark.parametrize(
    "updates",
    [
        {"units": "fraction"},
        {"raw_csv_sha256": "0" * 64},
        {"series": "DXY"},
        {"raw_csv_hex": "00"},
        {"publication_time_available": True},
        {"extra": 1},
        {"window_start_date": "2026-09-29"},
        {"rows": []},
    ],
)
def test_payload_corruption_fails_canonical_snapshot_validation(updates):
    row = snapshot()
    assert quality([changed_payload(row, **updates), healthy(row)])["state"] == "BLOCKED"


@pytest.mark.parametrize(
    "updates",
    [
        {"ok": 1},
        {"ok": False},
        {"snapshot_event_ids": []},
        {"http_status": 201},
        {"response_received_at_ms": True},
        {"error_type": ""},
        {"extra": 1},
        {"scope": "complete history"},
    ],
)
def test_health_corruption_fails_closed(updates):
    row = snapshot()
    assert quality([row, changed_payload(healthy(row), **updates)])["state"] == "BLOCKED"


def test_health_ref_missing_conflicting_ambiguous_or_wrong_response_cannot_pass():
    row = snapshot()
    poll = healthy(row)
    assert quality([poll])["blocker"] == "TIPS_REFERENCED_SNAPSHOT_MISSING"
    assert (
        quality([row, healthy(row, response=RECEIVED - 1)])["blocker"]
        == "TIPS_SNAPSHOT_RESPONSE_RECEIPT_MISMATCH"
    )
    failure = tips_yield_health(POLLED, error_type="FixtureTie")
    assert quality([row, poll, failure])["blocker"] == "TIPS_HEALTH_AMBIGUOUS"
    assert quality([row, poll, poll])["state"] == "POLL_SNAPSHOT_VERIFIED"
    assert (
        quality([row, row.model_copy(update={"observed_at_ms": RECEIVED + 1}), poll])["blocker"]
        == "TIPS_EVENT_ID_CONFLICT"
    )
    assert (
        quality([row, poll.model_copy(update={"event_time_ms": POLLED - 1})])["blocker"]
        == "TIPS_HEALTH_CANONICAL"
    )


def test_future_and_other_scopes_never_supply_required_evidence():
    row = snapshot()
    poll = healthy(row)
    for field in ("event_time_ms", "observed_at_ms"):
        assert (
            quality([row, poll.model_copy(update={field: POLLED + 1})])["blocker"]
            == "TIPS_HEALTH_MISSING"
        )
        assert (
            quality([row.model_copy(update={field: POLLED + 1}), poll])["blocker"]
            == "TIPS_REFERENCED_SNAPSHOT_MISSING"
        )
    for field, value in (
        ("source", "other"),
        ("asset", "PAXG"),
        ("symbol", "DXY"),
        ("environment", "testnet"),
    ):
        assert (
            quality([row, poll.model_copy(update={field: value})])["blocker"]
            == "TIPS_HEALTH_MISSING"
        )
    assert quality([row, poll], max_rows=1)["blocker"] == "TIPS_QUALITY_OVER_BUDGET"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"as_of_ms": True},
        {"as_of_ms": 0},
        {"max_poll_age_ms": 0},
        {"max_rows": True},
        {"max_rows": 100001},
    ],
)
def test_strict_quality_policy(kwargs):
    with pytest.raises(ValueError):
        tips_yield_quality([], **{"as_of_ms": POLLED, **kwargs})


class Content:
    def __init__(self, body):
        self.body = body

    async def iter_chunked(self, size):
        assert size == 8192
        yield self.body


class Response:
    def __init__(self, body, status):
        self.content, self.status = Content(body), status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Session:
    def __init__(self, body=CSV, status=200, error=None):
        self.body, self.status, self.error = body, status, error
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return Response(self.body, self.status)


async def test_collector_binds_after_sink_and_default_window_no_redirect_or_tls_override(
    monkeypatch,
):
    clock = iter([RECEIVED - 100, RECEIVED, POLLED])
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: next(clock))
    sink = AsyncMock()
    session = Session()
    collector = TipsYieldCollector(AppConfig(), sink)
    await collector.poll(session, start_date=START, end_date=END)
    rows = [call.args[0] for call in sink.await_args_list]
    assert [row.kind for row in rows] == ["index", "feed_health"]
    status = PollHealth.model_validate(rows[1].payload())
    assert status.snapshot_event_ids == (rows[0].event_id,)
    assert status.response_received_at_ms == RECEIVED
    assert session.calls[0][1]["allow_redirects"] is False
    assert session.calls[0][1]["timeout"].total <= 30
    assert "ssl" not in session.calls[0][1]
    assert quality(rows)["state"] == "POLL_SNAPSHOT_VERIFIED"


@pytest.mark.parametrize(
    "session",
    [
        Session(status=302),
        Session(status=403),
        Session(body=b"<html/>"),
        Session(body=b" " * (MAX_BYTES + 1)),
        Session(error=aiohttp.ClientConnectionError("fixture")),
    ],
)
async def test_http_parse_budget_or_transport_failure_emits_only_failed_health(
    monkeypatch, session
):
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: RECEIVED)
    sink = AsyncMock()
    await TipsYieldCollector(AppConfig(), sink).poll(session, start_date=START, end_date=END)
    rows = [call.args[0] for call in sink.await_args_list]
    assert len(rows) == 1 and rows[0].kind == "feed_health"
    assert rows[0].payload()["ok"] is False and rows[0].payload()["snapshot_event_ids"] == []
    assert quality(rows, RECEIVED)["blocker"] == "TIPS_FEED_UNAVAILABLE"


async def test_invalid_window_no_http_and_sink_failure_never_claims_success(monkeypatch):
    monkeypatch.setattr("selective_options.tips_yield.now_ms", lambda: RECEIVED)
    session = Session()
    sink = AsyncMock()
    collector = TipsYieldCollector(AppConfig(), sink)
    with pytest.raises(ValueError):
        await collector.poll(session, start_date=START)
    with pytest.raises(ValueError):
        await collector.poll(session, start_date=START, end_date=date(2026, 10, 5))
    assert session.calls == []
    sink.side_effect = OSError("Fixture sink failure")
    with pytest.raises(OSError, match="sink failure"):
        await collector.poll(session, start_date=START, end_date=END)
    assert sink.await_count == 1 and sink.await_args.args[0].kind == "index"


async def test_real_store_revision_pit_outbox_parquet_and_closed_reload(workspace_temp):
    row = snapshot()
    poll = healthy(row)
    revised = snapshot(b"observation_date,DFII10\n2026-10-02,.\n", RECEIVED + 100)
    revised_poll = healthy(revised, POLLED + 100, RECEIVED + 100)
    store = await DuckStore.open(workspace_temp / "events.duckdb")
    try:
        await store.append([row, poll, revised, revised_poll], exact=True)
        assert await store.pending_delivery_count() == 4
        before = await store.snapshot("GLOBAL", POLLED - 1, "public_reference")
        assert quality(before.rows(), POLLED - 1)["blocker"] == "TIPS_HEALTH_MISSING"
        at = await store.snapshot("GLOBAL", POLLED, "public_reference")
        assert quality(at.rows())["latest_value_percent_text"] == "2.40"
        await store.export_parquet(workspace_temp / "events.parquet")
    finally:
        await store.close()
    with duckdb.connect(str(workspace_temp / "events.duckdb"), read_only=True) as connection:
        assert (
            connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(workspace_temp / "events.parquet")],
            ).fetchall()
        )
    reopened = await DuckStore.open(workspace_temp / "events.duckdb")
    try:
        assert await reopened.pending_delivery_count() == 4
        latest = await reopened.snapshot("GLOBAL", POLLED + 100, "public_reference")
        assert quality(latest.rows(), POLLED + 100)["dated_value_state"] == "NO_REPORTED_VALUE"
    finally:
        await reopened.close()
