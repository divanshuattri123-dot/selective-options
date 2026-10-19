"""Finite CoinDesk checkpoint handoff and receipt-preserving replay tests."""

import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.cli import main, parser
from selective_options.config import AppConfig
from selective_options.crypto_news import CryptoNewsCollector, parse_feed
from selective_options.crypto_news_checkpoint import load_crypto_news_checkpoint
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DIR = ROOT / "evidence/crypto-news-public-02"
PUBLIC_DB = PUBLIC_DIR / "events.duckdb"
PUBLIC_PARQUET = PUBLIC_DIR / "events.parquet"
PUBLIC_MANIFEST = PUBLIC_DIR / "events.parquet.manifest.json"
PUBLIC_XML = PUBLIC_DIR / "response.xml"


class Content:
    def __init__(self, body: bytes):
        self.body = body

    async def iter_chunked(self, _size):
        yield self.body


class Response:
    def __init__(self, body: bytes, status=200):
        self.status = status
        self.headers = {}
        self.content = Content(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class Session:
    def __init__(self, body: bytes, status=200):
        self.body = body
        self.status = status

    def get(self, *_args, **_kwargs):
        return Response(self.body, self.status)


def config(*, crypto_news_enabled=True, **changes):
    return AppConfig(
        ingestion={
            "crypto_news_enabled": crypto_news_enabled,
            "crypto_news_poll_interval_s": 300,
            "reference_sources": [],
            "policy_news_sources": [],
            "spot_trades_enabled": False,
            "derivatives_enabled": False,
            "flush_interval_s": 0.001,
            **changes,
        }
    )


async def idle(*_args, **_kwargs):
    await asyncio.Event().wait()


def copy_public_checkpoint(parent: Path) -> Path:
    source_dir = parent / "source"
    source_dir.mkdir()
    database = source_dir / "events.duckdb"
    shutil.copy2(PUBLIC_DB, database)
    shutil.copy2(PUBLIC_PARQUET, source_dir / "events.parquet")
    shutil.copy2(PUBLIC_MANIFEST, source_dir / "events.parquet.manifest.json")
    return database


def latest_native_receipt(database: Path = PUBLIC_DB) -> int:
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(
            "SELECT max(observed_at_ms) FROM events WHERE source='coindesk_rss'"
        ).fetchone()[0]


def seed_rows(database: Path = PUBLIC_DB):
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(
            "SELECT * FROM events WHERE source='coindesk_rss' AND kind='news'"
        ).fetchall()


def patch_clock(monkeypatch, start):
    clock = [start]

    def now():
        clock[0] += 1
        return clock[0]

    monkeypatch.setattr("selective_options.ingestion.now_ms", now)
    monkeypatch.setattr("selective_options.crypto_news.now_ms", now)


def rss(items):
    xml = ["<rss><channel>"]
    for guid, title in items:
        xml.append(
            "<item><title>"
            + title
            + "</title><link>https://www.coindesk.com/markets/"
            + guid
            + "</link><guid>"
            + guid
            + "</guid><pubDate>Tue, 06 Oct 2026 12:00:00 +0000</pubDate></item>"
        )
    xml.append("</channel></rss>")
    return "".join(xml).encode()


async def test_replay_preserves_first_receipts_and_appends_revisions(monkeypatch):
    clock = [1_800_000_000_000]

    def now():
        clock[0] += 1
        return clock[0]

    monkeypatch.setattr("selective_options.crypto_news.now_ms", now)
    saved = []

    async def sink(row):
        saved.append(row)

    collector = CryptoNewsCollector(config(), sink)

    first_body = rss([("story-a", "A title"), ("story-b", "B title")])
    await collector.poll(Session(first_body))
    first_news = [row for row in saved if row.kind == "news"]
    first_receipts = {row.payload()["publisher_item_id"]: row.observed_at_ms for row in first_news}

    # A disappears; B is revised. The old B observation remains, and a new event
    # records the changed payload without overwriting its first receipt.
    saved.clear()
    await collector.poll(Session(rss([("story-b", "B revised")])))
    revision_rows = list(saved)
    revisions = [row for row in revision_rows if row.kind == "news"]
    assert len(revisions) == 1
    revised = revisions[0]
    assert revised.payload()["title"] == "B revised"
    assert revised.observed_at_ms > first_receipts["story-b"]

    # A reappears unchanged. Neither it nor the existing B revision gets a new
    # headline receipt; the health row binds the original/revised IDs.
    saved.clear()
    await collector.poll(Session(rss([("story-a", "A title"), ("story-b", "B revised")])))
    reappearance = list(saved)
    assert not [row for row in reappearance if row.kind == "news"]
    health = next(row for row in reappearance if row.kind == "feed_health")
    assert set(health.payload()["accepted_event_ids"]) == {
        first_news[0].event_id,
        revised.event_id,
    }
    assert first_receipts["story-a"] == first_news[0].observed_at_ms


async def test_failed_poll_does_not_forget_first_receipts_or_duplicate_on_recovery(monkeypatch):
    clock = [1_800_000_000_100]

    def now():
        clock[0] += 1
        return clock[0]

    monkeypatch.setattr("selective_options.crypto_news.now_ms", now)
    saved = []

    async def sink(row):
        saved.append(row)

    collector = CryptoNewsCollector(config(), sink)
    body = rss([("outage-story", "Original")])
    await collector.poll(Session(body))
    original = next(row for row in saved if row.kind == "news")
    saved.clear()

    await collector.poll(Session(b"", status=503))
    failed = saved[-1]
    assert failed.kind == "feed_health"
    assert failed.payload()["ok"] is False
    saved.clear()

    await collector.poll(Session(body))
    assert not [row for row in saved if row.kind == "news"]
    recovered_health = next(row for row in saved if row.kind == "feed_health")
    assert recovered_health.payload()["ok"] is True
    assert recovered_health.payload()["accepted_event_ids"] == [original.event_id]
    assert original.observed_at_ms < recovered_health.observed_at_ms


async def test_conflicting_duplicate_publisher_id_fails_poll_before_metadata_sink(monkeypatch):
    clock = [1_800_000_000_200]

    def now():
        clock[0] += 1
        return clock[0]

    monkeypatch.setattr("selective_options.crypto_news.now_ms", now)
    saved = []

    async def sink(row):
        saved.append(row)

    collector = CryptoNewsCollector(config(), sink)
    await collector.poll(
        Session(rss([("same-guid", "First payload"), ("same-guid", "Conflicting payload")]))
    )
    assert [row.kind for row in saved] == ["feed_health"]
    assert saved[0].payload()["ok"] is False


async def test_actual_capture_revalidates_closed_copy_before_collector_and_replays_saved_xml(
    tmp_path, monkeypatch
):
    source = copy_public_checkpoint(tmp_path)
    source_before = tuple(
        hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (
            source,
            source.with_suffix(".parquet"),
            source.with_suffix(".parquet.manifest.json"),
        )
    )
    output = tmp_path / "capture"
    replay_at = latest_native_receipt(source) + 2_000
    clock = [replay_at]

    def now():
        clock[0] += 1
        return clock[0]

    monkeypatch.setattr("selective_options.ingestion.now_ms", now)
    monkeypatch.setattr("selective_options.crypto_news.now_ms", now)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", idle)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", idle)
    stages = []
    original_loader = load_crypto_news_checkpoint

    async def observed_loader(database, as_of, **kwargs):
        result = await original_loader(database, as_of, **kwargs)
        if database == source:
            stages.append("source_archive_verified")
        elif database == output / "events.duckdb":
            with duckdb.connect(str(database), read_only=True) as connection:
                assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == len(
                    result.events
                )
                assert connection.execute(
                    "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
                ).fetchone()[0] == len(result.events)
            stages.append("closed_destination_revalidated")
        return result

    class OrderedCollector(CryptoNewsCollector):
        def __init__(self, *args, **kwargs):
            assert stages[-1] == "closed_destination_revalidated"
            stages.append("news_collector_constructed")
            super().__init__(*args, **kwargs)

        async def run(self):
            stages.append("news_producer_started")
            await self.poll(Session(PUBLIC_XML.read_bytes()))
            await idle()

    monkeypatch.setattr("selective_options.ingestion.load_crypto_news_checkpoint", observed_loader)
    monkeypatch.setattr("selective_options.crypto_news.CryptoNewsCollector", OrderedCollector)
    replayed, rejected = parse_feed(PUBLIC_XML.read_bytes(), replay_at + 10)
    assert rejected == 0
    original = await original_loader(
        source, replay_at + 10, parquet_archive=source.with_suffix(".parquet")
    )
    assert {row.event_id for row in replayed} == {row.event_id for row in original.metadata}

    report = await capture(config(), output, 0.03, resume_crypto_news_from=source)

    assert stages == [
        "source_archive_verified",
        "closed_destination_revalidated",
        "news_collector_constructed",
        "news_producer_started",
    ]
    handoff = report["crypto_news_resume"]
    assert handoff["status"] == "COMMITTED_AND_RELOADED"
    assert handoff["retained_metadata_rows"] == 25
    assert handoff["minimal_bound_health_rows"] == 1
    assert handoff["copied_outbox_pending"] is True
    assert handoff["history_complete"] is False
    assert report["trading_enabled"] is False

    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        rows = connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        assert (
            rows
            == connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(output / "events.parquet")],
            ).fetchall()
        )
        assert connection.execute(
            "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id"
        ).fetchall() == [(row[0], False) for row in rows]
        source_news = connection.execute(
            "SELECT * FROM events WHERE source='coindesk_rss' AND kind='news' ORDER BY event_id"
        ).fetchall()
        health_rows = connection.execute(
            "SELECT payload_json FROM events WHERE source='coindesk_rss' AND kind='feed_health'"
        ).fetchall()
    assert source_news == sorted(seed_rows(source), key=lambda row: row[0])
    assert len(health_rows) == 2
    assert report["rows"] == len(rows) == 27
    assert (
        tuple(
            hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                source,
                source.with_suffix(".parquet"),
                source.with_suffix(".parquet.manifest.json"),
            )
        )
        == source_before
    )


async def test_missing_or_tampered_source_archive_refuses_before_output_or_producers(
    tmp_path, monkeypatch
):
    source = copy_public_checkpoint(tmp_path)
    source.with_suffix(".parquet").write_bytes(
        source.with_suffix(".parquet").read_bytes() + b"tamper"
    )
    producer = AsyncMock(side_effect=AssertionError("producer must not start"))
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", producer)
    output = tmp_path / "refused"
    with pytest.raises(IntegrityError):
        await capture(config(), output, 0.03, resume_crypto_news_from=source)
    assert not output.exists()
    producer.assert_not_awaited()


async def test_destination_revalidation_failure_closes_store_and_prevents_producers(
    tmp_path, monkeypatch
):
    source = copy_public_checkpoint(tmp_path)
    producer = AsyncMock(side_effect=AssertionError("producer must not start"))
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)
    monkeypatch.setattr("selective_options.ingestion.poll_chains", producer)
    original_loader = load_crypto_news_checkpoint

    async def failing_destination(database, as_of, **kwargs):
        if database.name == "events.duckdb" and database.parent.name == "refused":
            raise IntegrityError("FIXTURE_DESTINATION_REVALIDATION_FAILURE")
        return await original_loader(database, as_of, **kwargs)

    monkeypatch.setattr(
        "selective_options.ingestion.load_crypto_news_checkpoint", failing_destination
    )
    output = tmp_path / "refused"
    patch_clock(monkeypatch, latest_native_receipt(source) + 2_000)
    with pytest.raises(IntegrityError, match="DESTINATION_REVALIDATION_FAILURE"):
        await capture(config(), output, 0.03, resume_crypto_news_from=source)
    producer.assert_not_awaited()
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM events").fetchone()[0] == 26
        assert (
            connection.execute(
                "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
            ).fetchone()[0]
            == 26
        )

    failure = json.loads((output / "crypto-news-resume-failure.json").read_text(encoding="utf-8"))
    assert failure["status"] == "FAILED"
    assert failure["closed_destination_validation_passed"] is False
    assert failure["news_producer_started"] is False
    assert failure["trading_enabled"] is False


async def test_crypto_news_checkpoint_requires_explicit_enabled_capture(tmp_path, monkeypatch):
    loader = AsyncMock(side_effect=AssertionError("invalid setup must refuse first"))
    monkeypatch.setattr("selective_options.ingestion.load_crypto_news_checkpoint", loader)
    output = tmp_path / "refused"
    with pytest.raises(ValueError, match="crypto-news capture enabled"):
        await capture(
            config(crypto_news_enabled=False),
            output,
            0.03,
            resume_crypto_news_from=tmp_path / "absent.duckdb",
        )
    assert not output.exists()
    loader.assert_not_awaited()


def test_explicit_cli_resume_option_and_default():
    assert parser().parse_args(["capture", "--output", "new"]).resume_crypto_news_from is None
    parsed = parser().parse_args(
        ["capture", "--output", "new", "--resume-crypto-news-from", "old/events.duckdb"]
    )
    assert parsed.resume_crypto_news_from == Path("old/events.duckdb")


def test_cli_forwards_explicit_crypto_news_resume(tmp_path, monkeypatch):
    operation = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", operation)
    monkeypatch.setattr("selective_options.cli.load_config", lambda *_args: config())
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--resume-crypto-news-from",
            str(tmp_path / "source/events.duckdb"),
        ],
    )
    assert main() == 0
    assert (
        operation.await_args.kwargs["resume_crypto_news_from"] == tmp_path / "source/events.duckdb"
    )
