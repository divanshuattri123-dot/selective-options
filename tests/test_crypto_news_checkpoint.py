"""Offline tests for the bounded CoinDesk checkpoint selection."""

import hashlib
import itertools
import random
import shutil
from datetime import UTC, datetime
from email.utils import format_datetime
from pathlib import Path

import duckdb
import pytest

from selective_options.crypto_news import news_poll_health, parse_feed
from selective_options.crypto_news_checkpoint import load_crypto_news_checkpoint
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_SAMPLE = ROOT / "evidence/crypto-news-public-02/events.duckdb"
RECEIVED = 1_800_000_000_000


def headlines(received=RECEIVED, count=4):
    items = []
    published = format_datetime(datetime.fromtimestamp((received - 60_000) / 1000, UTC))
    for index in range(count):
        items.append(
            f"<item><title>Native title {index}</title>"
            f"<link>https://www.coindesk.com/markets/checkpoint-{index}</link>"
            f"<guid>checkpoint-{index}</guid><pubDate>{published}</pubDate></item>"
        )
    body = ("<rss><channel>" + "".join(items) + "</channel></rss>").encode()
    rows, rejected = parse_feed(body, received)
    assert rejected == 0 and len(rows) == count
    return rows


def health_at(received, accepted=(), *, error_type=None):
    return news_poll_health(
        received,
        accepted_event_ids=tuple(
            row.event_id if hasattr(row, "event_id") else row for row in accepted
        ),
        response_received_at_ms=None if error_type else received - 1,
        http_status=None if error_type else 200,
        error_type=error_type,
    )


async def save_database(path: Path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(list(rows), exact=True)
    finally:
        await store.close()
    return path


async def load(path, as_of=RECEIVED + 10_000, **policy):
    return await load_crypto_news_checkpoint(path, as_of, **policy)


async def test_saved_native_checkpoint_binds_all_metadata_and_is_read_only():
    before = hashlib.sha256(PUBLIC_SAMPLE.read_bytes()).hexdigest()
    with duckdb.connect(str(PUBLIC_SAMPLE), read_only=True) as connection:
        latest = connection.execute(
            "SELECT max(observed_at_ms) FROM events WHERE source='coindesk_rss'"
        ).fetchone()[0]
    selected = await load(PUBLIC_SAMPLE, as_of=latest)
    assert len(selected.metadata) == 25
    assert len(selected.health) == 1
    assert selected.health[0].event_id == selected.latest_health_event_id
    assert selected.latest_health_received_at_ms == latest
    assert len(selected.events) == 26
    assert all(value is False for _identity, value in selected.source_delivery)
    assert selected.metadata == tuple(
        sorted(
            selected.metadata, key=lambda row: (row.observed_at_ms, row.event_time_ms, row.event_id)
        )
    )
    assert hashlib.sha256(PUBLIC_SAMPLE.read_bytes()).hexdigest() == before
    archived = await load(
        PUBLIC_SAMPLE,
        as_of=latest,
        parquet_archive=PUBLIC_SAMPLE.with_suffix(".parquet"),
    )
    assert archived == selected


@pytest.mark.parametrize("failure", ["missing_archive", "tampered_archive", "open_wal"])
async def test_closed_archive_preflight_refuses_invalid_source_without_mutating_it(
    tmp_path, failure
):
    database = tmp_path / "events.duckdb"
    parquet = tmp_path / "events.parquet"
    shutil.copy2(PUBLIC_SAMPLE, database)
    shutil.copy2(PUBLIC_SAMPLE.with_suffix(".parquet"), parquet)
    shutil.copy2(
        PUBLIC_SAMPLE.with_suffix(".parquet.manifest.json"),
        parquet.with_suffix(parquet.suffix + ".manifest.json"),
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        latest = connection.execute(
            "SELECT max(observed_at_ms) FROM events WHERE source='coindesk_rss'"
        ).fetchone()[0]
    if failure == "missing_archive":
        parquet.unlink()
    elif failure == "tampered_archive":
        parquet.write_bytes(parquet.read_bytes() + b"tamper")
    else:
        database.with_suffix(database.suffix + ".wal").write_bytes(b"open")
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    with pytest.raises(IntegrityError):
        await load_crypto_news_checkpoint(
            database,
            as_of_ms=latest,
            parquet_archive=parquet,
        )
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


async def test_minimum_health_subset_preserves_every_first_receipt(tmp_path):
    data = headlines(count=4)
    a, b, c, d = data
    h0 = health_at(RECEIVED + 1, (a, b))
    h1 = health_at(RECEIVED + 2, (b, c))
    h2 = health_at(RECEIVED + 3, (c, d))
    latest = health_at(RECEIVED + 4, (a,))
    path = await save_database(tmp_path / "news.duckdb", (*data, h0, h1, h2, latest))

    selected = await load(path, as_of=RECEIVED + 5)

    assert {row.event_id for row in selected.metadata} == {row.event_id for row in data}
    assert selected.latest_health_event_id == latest.event_id
    assert len(selected.health) == 3
    assert latest in selected.health
    assert sum(len(row.payload()["accepted_event_ids"]) for row in selected.health) >= len(data)
    assert {
        identity for row in selected.health for identity in row.payload()["accepted_event_ids"]
    } == {row.event_id for row in data}


async def test_no_news_checkpoint_can_resume_from_empty_successful_poll(tmp_path):
    latest = health_at(RECEIVED + 1)
    path = await save_database(tmp_path / "empty.duckdb", [latest])
    selected = await load(path, as_of=RECEIVED + 2)
    assert selected.metadata == ()
    assert selected.health == (latest,)
    assert selected.events == (latest,)


@pytest.mark.parametrize("mode", ["failed", "stale"])
async def test_latest_failed_or_stale_health_refuses_checkpoint(tmp_path, mode):
    data = headlines(count=1)
    previous = health_at(RECEIVED + 1, data)
    later = (
        health_at(RECEIVED + 2, error_type="TimeoutError")
        if mode == "failed"
        else health_at(RECEIVED + 2, data)
    )
    as_of = RECEIVED + 3 if mode == "failed" else RECEIVED + 2 + 600_001
    reason = "LATEST_HEALTH_FAILED" if mode == "failed" else "LATEST_HEALTH_STALE"
    path = await save_database(tmp_path / f"{reason}.duckdb", (*data, previous, later))
    with pytest.raises(IntegrityError, match=reason):
        await load(path, as_of=as_of)


async def test_tied_latest_health_never_arbitrarily_selects_one(tmp_path):
    data = headlines(count=2)
    first = health_at(RECEIVED + 1, data)
    tied_a = health_at(RECEIVED + 2, (data[0],))
    tied_b = health_at(RECEIVED + 2, (data[1],))
    path = await save_database(tmp_path / "tied.duckdb", (*data, first, tied_a, tied_b))
    with pytest.raises(IntegrityError, match="LATEST_HEALTH_TIED"):
        await load(path, as_of=RECEIVED + 3)


async def test_missing_historical_binding_or_missing_metadata_blocks(tmp_path):
    data = headlines(count=1)
    latest_empty = health_at(RECEIVED + 1)
    path = await save_database(tmp_path / "unbound.duckdb", (*data, latest_empty))
    with pytest.raises(IntegrityError, match="UNBOUND_METADATA"):
        await load(path, as_of=RECEIVED + 2)

    missing_identity = "a" * 64
    status = health_at(RECEIVED + 1, (missing_identity,))
    path = await save_database(tmp_path / "missing.duckdb", [status])
    with pytest.raises(IntegrityError, match="MISSING_BOUND_METADATA"):
        await load(path, as_of=RECEIVED + 2)


async def test_future_event_and_receipt_clocks_are_refused(tmp_path):
    data = headlines(count=1)
    good = health_at(RECEIVED + 1, data)
    future = health_at(RECEIVED + 100, data)
    path = await save_database(tmp_path / "future-receipt.duckdb", (*data, good, future))
    with pytest.raises(IntegrityError, match="FUTURE_CLOCK"):
        await load(path, as_of=RECEIVED + 50)

    # A native news row whose source clock lies beyond the requested cutoff is
    # independently rejected even when its first receipt is at/before that cutoff.
    shifted = data[0].model_copy(update={"event_time_ms": RECEIVED + 50})
    path = await save_database(tmp_path / "future-source.duckdb", (shifted, good))
    with pytest.raises(IntegrityError, match="FUTURE_CLOCK"):
        await load(path, as_of=RECEIVED + 2)


async def test_missing_outbox_row_and_raw_limits_fail_closed(tmp_path):
    data = headlines(count=1)
    status = health_at(RECEIVED + 1, data)
    path = await save_database(tmp_path / "partial-outbox.duckdb", (*data, status))
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM delivery_outbox WHERE event_id=?", [data[0].event_id])
    with pytest.raises(IntegrityError, match="PARTIAL_SINK_METADATA"):
        await load(path, as_of=RECEIVED + 2)

    path = await save_database(tmp_path / "limited.duckdb", (*data, status))
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load(path, as_of=RECEIVED + 2, max_rows=1)
    with pytest.raises(IntegrityError, match="OVER_BUDGET"):
        await load(path, as_of=RECEIVED + 2, max_bytes=100)


async def test_noncanonical_stored_payload_is_rejected(tmp_path):
    data = headlines(count=1)
    status = health_at(RECEIVED + 1, data)
    path = await save_database(tmp_path / "corrupt.duckdb", (*data, status))
    payload = data[0].payload_json.replace("Native title 0", "tampered title")
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE events SET payload_json=? WHERE event_id=?",
            [payload, data[0].event_id],
        )
    with pytest.raises(IntegrityError, match="CRYPTO_NEWS_METADATA_CANONICAL"):
        await load(path, as_of=RECEIVED + 2)


@pytest.mark.parametrize(
    "policy",
    [
        {"as_of_ms": True},
        {"max_poll_age_ms": 0},
        {"max_poll_age_ms": -1},
        {"max_rows": 0},
        {"max_bytes": 64_000_001},
    ],
)
async def test_invalid_policy_rejects_before_opening_database(tmp_path, policy):
    database = tmp_path / "must-not-be-created.duckdb"
    values = {"as_of_ms": RECEIVED + 1, **policy}
    with pytest.raises(ValueError, match="INVALID_POLICY"):
        await load_crypto_news_checkpoint(database, **values)
    assert not database.exists()


async def test_minimum_cover_matches_bruteforce_on_seeded_families(tmp_path):
    rng = random.Random(811_2026)
    data = headlines(count=8)
    for trial in range(20):
        subsets = [{index} for index in range(len(data))]
        subsets.extend(
            {index for index in range(len(data)) if rng.random() < 0.4} for _ in range(12)
        )
        latest_subset = {0, 3}
        subsets.append(latest_subset)
        polls = [
            health_at(RECEIVED + index + 1, [data[item] for item in subset])
            for index, subset in enumerate(subsets)
        ]
        path = await save_database(tmp_path / f"cover-{trial}.duckdb", (*data, *polls))
        selected = await load(path, as_of=RECEIVED + len(polls) + 1)

        target = set(range(len(data))) - latest_subset
        candidates = [set(subset) & target for subset in subsets[:-1]]
        expected = len(candidates) + 1
        for width in range(1, len(candidates) + 1):
            if any(
                set().union(*choice) == target
                for choice in itertools.combinations(candidates, width)
            ):
                expected = width + 1
                break
        assert len(selected.health) == expected
