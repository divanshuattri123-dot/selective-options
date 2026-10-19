"""Offline synthetic loader refusals plus the unchanged finite saved-public seed."""

import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event
from selective_options.solana_checkpoint import MAX_AGE_MS, load_solana_resume
from selective_options.solana_network import GENESIS, SolanaNetworkCollector, network_events
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore

AS_OF = 1_000_500
SCOPE = "performance and vote-account stake; sink accepted, no atomic commit claim"
PUBLIC_SOURCE = (
    Path(__file__).resolve().parents[1] / "evidence/solana-network-public-01/events.duckdb"
)


def pair(offset=0, *, seconds=None):
    return network_events(
        [
            {
                "slot": 1_000_000 + offset,
                "numTransactions": 9007199254740993,
                "numSlots": 100,
                "samplePeriodSecs": 60,
                "numNonVoteTransactions": 12,
            }
        ],
        seconds if seconds is not None else 1000 + offset // 1000,
        {
            "current": [
                {
                    "votePubkey": "1" * 31 + "2",
                    "nodePubkey": "1" * 32,
                    "activatedStake": 9007199254740993,
                    "epochVoteAccount": True,
                    "lastVote": 999,
                    "rootSlot": 998,
                    "commission": 5,
                }
            ],
            "delinquent": [],
        },
        tuple(1_000_000 + offset + increment for increment in (100, 200, 300, 400)),
        180_000,
    )


def health(events=(), *, at=AS_OF, ok=True, **changes):
    return make_event(
        source="solana_public",
        environment="public_reference",
        asset="SOL",
        symbol="SOL_NETWORK",
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=at,
        observed_at_ms=at,
        payload={
            "ok": ok,
            "reason": "NETWORK_SAMPLES_ACCEPTED" if ok else "RATE_LIMITED",
            "code": 0 if ok else 429,
            "error_type": None,
            "event_ids": [event.event_id for event in events],
            "scope": SCOPE,
            **changes,
        },
    )


def seed(offset=0):
    events = pair(offset)
    return (*events, health(events, at=AS_OF + offset))


async def database(path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(list(rows), exact=True)
    finally:
        await store.close()
    return path


def stored_size(rows):
    return sum(
        len(value.encode("utf-8")) if isinstance(value, str) else len(str(value))
        for event in rows
        for value in event.model_dump().values()
    )


async def test_exact_seed_is_stable_minimal_and_source_unchanged(tmp_path):
    old, current = seed(), seed(1000)
    unrelated = current[0].model_copy(update={"source": "other", "event_id": "a" * 64})
    path = await database(tmp_path / "events.duckdb", [*reversed(current), *old, unrelated])
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    actual = await load_solana_resume(path, AS_OF + 1000, max_rows=6)

    assert actual == current
    assert len(actual) == 3
    assert actual[0].payload()["transactions_per_second_numerator"] == "9007199254740993"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_saved_public_source_returns_exact_three_rows_without_mutation():
    source = PUBLIC_SOURCE
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    with duckdb.connect(str(source), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY observed_at_ms,event_time_ms,event_id"
        )
        columns = [column[0] for column in cursor.description]
        expected = [dict(zip(columns, values, strict=True)) for values in cursor.fetchall()]
    as_of = max(row["observed_at_ms"] for row in expected)

    actual = await load_solana_resume(
        source, as_of, max_rows=3, max_bytes=stored_size_models(expected)
    )

    assert [row.model_dump() for row in actual] == expected
    assert len(actual) == 3
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before


def stored_size_models(rows):
    return sum(
        len(value.encode("utf-8")) if isinstance(value, str) else len(str(value))
        for event in rows
        for value in event.values()
    )


async def test_latest_failed_health_blocks_and_preserves_earlier_as_of(tmp_path):
    original = seed()
    failure = health(at=AS_OF + 1000, ok=False)
    path = await database(tmp_path / "events.duckdb", [*original, failure])
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    assert await load_solana_resume(path, AS_OF + 999) == original
    with pytest.raises(IntegrityError, match="SOL_FEED_UNAVAILABLE"):
        await load_solana_resume(path, AS_OF + 1000)
    assert await load_solana_resume(path, AS_OF) == original
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_latest_tied_health_does_not_choose_or_fall_back(tmp_path):
    rows = seed()
    tied = health(at=AS_OF, ok=False)
    path = await database(tmp_path / "events.duckdb", [*rows, tied])

    with pytest.raises(IntegrityError, match="SOL_HEALTH_AMBIGUOUS"):
        await load_solana_resume(path, AS_OF)


@pytest.mark.parametrize("prefix_length", [1, 2])
async def test_unpublished_sink_prefix_is_not_hidden_by_older_healthy_view(tmp_path, prefix_length):
    original = seed()
    prefix = pair(1000)[:prefix_length]
    rows = (*original, *prefix)
    assert network_quality(rows, AS_OF + 1000)["state"] == "NETWORK_SAMPLE_VERIFIED"
    path = await database(tmp_path / "events.duckdb", rows)

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_UNBOUND_SNAPSHOT"):
        await load_solana_resume(path, AS_OF + 1000)


@pytest.mark.parametrize("prefix_length", [1, 2])
async def test_actual_collector_sink_interruption_leaves_rejected_durable_prefix(
    tmp_path, monkeypatch, prefix_length
):
    path = await database(tmp_path / "events.duckdb", seed())
    store = await DuckStore.open(path)
    new = pair(1000)
    calls = 0

    async def interrupted_sink(event):
        nonlocal calls
        if calls == prefix_length:
            raise OSError("synthetic sink publication interruption")
        await store.append([event], exact=True)
        calls += 1

    monkeypatch.setattr("selective_options.solana_network.asyncio.sleep", AsyncMock())
    monkeypatch.setattr("selective_options.solana_network.now_ms", lambda: AS_OF + 1000)
    collector = SolanaNetworkCollector(AppConfig(), interrupted_sink)
    collector.request = AsyncMock(
        side_effect=[
            (GENESIS, 1_001_100),
            ([new[0].payload()["sample"]], 1_001_200),
            (1001, 1_001_300),
            (new[1].payload()["vote_accounts"], 1_001_400),
        ]
    )
    try:
        with pytest.raises(OSError, match="synthetic sink publication interruption"):
            await collector.collect(None)
    finally:
        await store.close()
    assert calls == prefix_length
    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_UNBOUND_SNAPSHOT"):
        await load_solana_resume(path, AS_OF + 1000)


async def test_older_unbound_snapshot_is_not_hidden_by_newer_success(tmp_path):
    path = await database(tmp_path / "events.duckdb", [pair()[0], *seed(1000)])

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_UNBOUND_SNAPSHOT"):
        await load_solana_resume(path, AS_OF + 1000)


async def test_one_native_pair_cannot_be_bound_by_two_successful_health_rows(tmp_path):
    rows = seed()
    path = await database(tmp_path / "events.duckdb", [*rows, health(rows[:2], at=AS_OF + 1)])

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_AMBIGUOUS_SNAPSHOT_BINDING"):
        await load_solana_resume(path, AS_OF + 1)


@pytest.mark.parametrize("references", [[], ["missing"], ["missing", "other"], ["same", "same"]])
async def test_missing_or_duplicate_health_references_refuse(tmp_path, references):
    rows = seed()
    bad = health(rows[:2], event_ids=references)
    path = await database(tmp_path / "events.duckdb", [*rows[:2], bad])

    with pytest.raises(IntegrityError, match="SOL_HEALTH_REFERENCES|SOL_REFERENCED_SNAPSHOT"):
        await load_solana_resume(path, AS_OF)


async def test_missing_historical_reference_refuses_despite_valid_latest(tmp_path):
    old = seed()
    path = await database(tmp_path / "events.duckdb", [old[0], old[2], *seed(1000)])

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_MISSING_BOUND_SNAPSHOT"):
        await load_solana_resume(path, AS_OF + 1000)


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "x" * 64},
        {"observed_at_ms": 1_000_301},
        {"symbol": "SOL_VOTE_STAKE"},
        {"timestamp_kind": "receipt"},
        {"event_time_ms": 1_000_001},
        {"source": "wrong_source"},
        {"environment": "testnet"},
        {"asset": "ETH"},
    ],
)
async def test_mismatched_native_envelope_refuses(tmp_path, changes):
    rows = seed()
    altered = rows[0].model_copy(update=changes)
    path = await database(tmp_path / "events.duckdb", [altered, *rows[1:]])

    with pytest.raises(IntegrityError):
        await load_solana_resume(path, AS_OF)


@pytest.mark.parametrize(
    "payload_changes",
    [
        {"genesis_hash": "wrong"},
        {"source_url": "https://wrong.invalid"},
        {"sample_response_received_at_ms": True},
        {"transactions_per_second_numerator": "rounded"},
        {"block_time_seconds": "1000"},
        {"unrecognized": "extra"},
    ],
)
async def test_historical_canonical_payload_is_checked_before_minimal_seed(
    tmp_path, payload_changes
):
    rows = seed()
    bad = rows[0].model_copy(
        update={"payload_json": json.dumps({**rows[0].payload(), **payload_changes})}
    )
    path = await database(tmp_path / "events.duckdb", [bad, *rows[1:], *seed(1000)])

    with pytest.raises(IntegrityError):
        await load_solana_resume(path, AS_OF + 1000)


@pytest.mark.parametrize("field", ["event_id", "observed_at_ms", "timestamp_kind", "payload_json"])
async def test_noncanonical_historical_health_refuses(tmp_path, field):
    rows = seed()
    values = {
        "event_id": "x" * 64,
        "observed_at_ms": AS_OF + 1,
        "timestamp_kind": "source",
        "payload_json": json.dumps(rows[2].payload(), indent=2),
    }
    bad = rows[2].model_copy(update={field: values[field]})
    path = await database(tmp_path / "events.duckdb", [*rows[:2], bad, *seed(1000)])

    with pytest.raises(IntegrityError, match="SOL_HEALTH_CANONICAL"):
        await load_solana_resume(path, AS_OF + 1000)


@pytest.mark.parametrize("kind,symbol", [("status", "SOL_NETWORK"), ("on_chain", "UNKNOWN")])
async def test_unexpected_scoped_envelope_cannot_hide_behind_healthy_latest(tmp_path, kind, symbol):
    rows = seed()
    bad = rows[0].model_copy(update={"event_id": "x" * 64, "kind": kind, "symbol": symbol})
    path = await database(tmp_path / "events.duckdb", [*rows, bad])

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_SCOPE_MISMATCH"):
        await load_solana_resume(path, AS_OF)


async def test_native_receipt_visibility_before_and_at_boundary(tmp_path):
    original = seed()
    prefix = pair(1000)[0]
    path = await database(tmp_path / "events.duckdb", [*original, prefix])

    assert await load_solana_resume(path, prefix.observed_at_ms - 1) == original
    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_UNBOUND_SNAPSHOT"):
        await load_solana_resume(path, prefix.observed_at_ms)


async def test_native_source_visibility_before_and_at_boundary(tmp_path):
    original = seed(-1000)
    # This synthetic performance source clock is within the permitted two-second
    # RPC estimate skew, but its original receipt precedes that source timestamp.
    prefix = pair(seconds=1001)[0]
    path = await database(tmp_path / "events.duckdb", [*original, prefix])

    assert prefix.observed_at_ms < prefix.event_time_ms
    assert await load_solana_resume(path, prefix.event_time_ms - 1) == original
    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_UNBOUND_SNAPSHOT"):
        await load_solana_resume(path, prefix.event_time_ms)


async def test_health_receipt_becomes_visible_exactly_at_boundary(tmp_path):
    rows = seed()
    path = await database(tmp_path / "events.duckdb", rows)

    with pytest.raises(IntegrityError, match="SOL_HEALTH_MISSING"):
        await load_solana_resume(path, AS_OF - 1)
    assert await load_solana_resume(path, AS_OF) == rows


@pytest.mark.parametrize("clock", ["event_time_ms", "observed_at_ms"])
async def test_future_health_envelope_is_invisible_until_each_clock_boundary(tmp_path, clock):
    rows = seed()
    future = health(at=AS_OF + 1000, ok=False).model_copy(update={clock: AS_OF + 2000})
    path = await database(tmp_path / "events.duckdb", [*rows, future])

    assert await load_solana_resume(path, AS_OF + 1999) == rows
    with pytest.raises(IntegrityError, match="SOL_HEALTH_CANONICAL"):
        await load_solana_resume(path, AS_OF + 2000)


async def test_latest_freshness_receipt_and_sample_limits_are_independent(tmp_path):
    rows = seed()
    path = await database(tmp_path / "events.duckdb", rows)

    assert await load_solana_resume(path, AS_OF + 1, max_receipt_age_ms=401) == rows
    with pytest.raises(IntegrityError, match="SOL_HEALTH_STALE"):
        await load_solana_resume(path, AS_OF + 101, max_receipt_age_ms=100)
    with pytest.raises(IntegrityError, match="SOL_SNAPSHOT_RECEIPT_STALE"):
        await load_solana_resume(path, AS_OF + 1, max_receipt_age_ms=400)
    assert await load_solana_resume(path, AS_OF, max_sample_age_ms=500) == rows
    with pytest.raises(IntegrityError, match="SOL_PERFORMANCE_SOURCE_STALE"):
        await load_solana_resume(path, AS_OF, max_sample_age_ms=499)


@pytest.mark.parametrize("limit", ["rows", "bytes"])
async def test_full_selected_history_budget_is_checked_before_minimal_handoff(tmp_path, limit):
    rows = (*seed(), *seed(1000))
    path = await database(tmp_path / "events.duckdb", rows)
    options = {"max_rows": 5} if limit == "rows" else {"max_bytes": stored_size(rows) - 1}

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_OVER_BUDGET"):
        await load_solana_resume(path, AS_OF + 1000, **options)
    assert await load_solana_resume(
        path, AS_OF + 1000, max_rows=6, max_bytes=stored_size(rows)
    ) == seed(1000)


async def test_raw_utf8_stored_bytes_are_budgeted_before_json_decoding(tmp_path):
    rows = seed()
    # Store invalid but enormous raw JSON. A parser would fail; the byte check
    # must reject its stored bytes first, including the multibyte UTF-8 content.
    bad = rows[0].model_copy(update={"payload_json": "☀" * 300})
    path = await database(tmp_path / "events.duckdb", rows)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE events SET payload_json=? WHERE event_id=?", [bad.payload_json, bad.event_id]
        )
    selected = (bad, *rows[1:])
    chars = sum(len(str(value)) for row in selected for value in row.model_dump().values())
    assert chars < stored_size(selected)

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_OVER_BUDGET"):
        await load_solana_resume(path, AS_OF, max_bytes=chars)
    with pytest.raises(IntegrityError, match="SOL_INVALID_EVIDENCE"):
        await load_solana_resume(path, AS_OF, max_bytes=stored_size(selected))


@pytest.mark.parametrize("null_field", ["event_id", "kind"])
async def test_null_field_cannot_hide_oversized_payload_from_prefetch_budget(
    tmp_path, monkeypatch, null_field
):
    path = await database(tmp_path / "events.duckdb", seed())
    connect = duckdb.connect
    with connect(str(path)) as connection:
        # Remove constraints to represent a damaged/nonstandard source database.
        connection.execute("CREATE TABLE copied_events AS SELECT * FROM events")
        connection.execute("DROP TABLE events")
        connection.execute("ALTER TABLE copied_events RENAME TO events")
        connection.execute(
            f"UPDATE events SET {null_field}=NULL,payload_json=? WHERE symbol='SOL_PERFORMANCE'",
            ["x" * 20_000],
        )

    class BudgetOnlyConnection:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.connection.__enter__()
            return self

        def __exit__(self, *args):
            return self.connection.__exit__(*args)

        def execute(self, sql, *args):
            if sql.startswith("SELECT event_id"):
                pytest.fail("NULL selected values must refuse before row materialization")
            return self.connection.execute(sql, *args)

    monkeypatch.setattr(
        "selective_options.solana_checkpoint.duckdb.connect",
        lambda *args, **kwargs: BudgetOnlyConnection(connect(*args, **kwargs)),
    )
    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_INVALID_ROW"):
        await load_solana_resume(path, AS_OF, max_bytes=1000)


@pytest.mark.parametrize(
    "policy",
    [
        {"as_of_ms": True},
        {"as_of_ms": 2**63},
        {"max_receipt_age_ms": 0},
        {"max_receipt_age_ms": MAX_AGE_MS + 1},
        {"max_sample_age_ms": True},
        {"max_sample_age_ms": MAX_AGE_MS + 1},
        {"max_rows": True},
        {"max_rows": 100_001},
        {"max_bytes": 0},
        {"max_bytes": 64_000_001},
    ],
)
async def test_invalid_policy_is_rejected_before_database_open(tmp_path, monkeypatch, policy):
    monkeypatch.setattr(
        "selective_options.solana_checkpoint.duckdb.connect",
        lambda *args, **kwargs: pytest.fail("invalid policy must fail before opening"),
    )
    options = dict(policy)
    as_of = options.pop("as_of_ms", AS_OF)

    with pytest.raises(ValueError, match="SOL_CHECKPOINT_INVALID_POLICY"):
        await load_solana_resume(tmp_path / "absent.duckdb", as_of, **options)


async def test_missing_database_is_not_created(tmp_path):
    missing = tmp_path / "missing" / "absent.duckdb"

    with pytest.raises(FileNotFoundError, match="SOL_CHECKPOINT_DATABASE_MISSING"):
        await load_solana_resume(missing, AS_OF)
    assert not missing.parent.exists()


async def test_corrupt_database_refuses_and_remains_unchanged(tmp_path):
    path = tmp_path / "invalid.duckdb"
    path.write_bytes(b"not a database")

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_INVALID_DATABASE"):
        await load_solana_resume(path, AS_OF)
    assert path.read_bytes() == b"not a database"


async def test_duplicate_stored_event_ids_refuse_in_unconstrained_database(tmp_path):
    path = await database(tmp_path / "events.duckdb", seed())
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE copied_events AS SELECT * FROM events")
        connection.execute("DROP TABLE events")
        connection.execute("ALTER TABLE copied_events RENAME TO events")
        connection.execute("INSERT INTO events SELECT * FROM events LIMIT 1")

    with pytest.raises(IntegrityError, match="SOL_CHECKPOINT_DUPLICATE_EVENT"):
        await load_solana_resume(path, AS_OF)
