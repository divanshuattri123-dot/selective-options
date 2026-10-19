"""Offline capture handoff proof using the fixed public CoinDesk sample only."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import duckdb
from aiohttp import ClientSession

from selective_options import __version__
from selective_options.archive import file_digest
from selective_options.collectors import DeribitCollector, Sink
from selective_options.config import AppConfig
from selective_options.crypto_news import CryptoNewsCollector, parse_feed
from selective_options.crypto_news_checkpoint import (
    CryptoNewsCheckpoint,
    load_crypto_news_checkpoint,
)
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.ingestion import capture
from selective_options.store import complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/crypto-news-public-02"
OUTPUT = ROOT / "evidence/crypto-news-resume-saved-native-01"
EXPECTED = {
    "events.duckdb": "76bf7ecb50cf560bff24338ee5b4c41345fcc11fcf3d05eea14a4031b187915f",
    "events.parquet": "df73462160a9bce48b0c6fe5c20fa11adaa170a46564cad6185536e71bea5a62",
    "events.parquet.manifest.json": "4a18f6569ccb84278233e869214fa1188d69b5b86a2d61f3a61f413c04ad4e9e",
    "response.xml": "cddc74945dcbe5aec3218cd2b5e339c2cb3e8c9ce888e51b4208e4f5194cfc86",
}


class Content:
    def __init__(self, body: bytes) -> None:
        self.body = body

    async def iter_chunked(self, _size: int) -> AsyncIterator[bytes]:
        yield self.body


class Response:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.status = status
        self.headers: dict[str, str] = {}
        self.content = Content(body)

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class Session:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def get(self, *_args: object, **_kwargs: object) -> Response:
        return Response(self.body)


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise IntegrityError(reason)


async def idle(*_args: object, **_kwargs: object) -> None:
    await asyncio.Event().wait()


def offline_config() -> AppConfig:
    return AppConfig(
        ingestion={
            "crypto_news_enabled": True,
            "crypto_news_poll_interval_s": 300,
            "reference_sources": [],
            "policy_news_sources": [],
            "spot_trades_enabled": False,
            "derivatives_enabled": False,
            "flush_interval_s": 0.001,
        }
    )


async def run(output: Path) -> dict[str, object]:
    resolved = await asyncio.to_thread(output.resolve)
    expected_output = OUTPUT.resolve()
    work = (ROOT / "work").resolve()
    if resolved != expected_output and not resolved.is_relative_to(work):
        raise ValueError("output must be the saved-native evidence target or inside work/")
    if await asyncio.to_thread(output.exists):
        raise FileExistsError("resume verification output must be new")
    paths = {name: SOURCE / name for name in EXPECTED}
    source_hashes = {name: file_digest(path) for name, path in paths.items()}
    require(source_hashes == EXPECTED, "CRYPTO_NEWS_RESUME_NATIVE_SOURCE_HASH")

    latest = await asyncio.to_thread(_latest_health_ms, paths["events.duckdb"])
    selection_as_of = latest + 2_000
    original = await load_crypto_news_checkpoint(
        paths["events.duckdb"],
        selection_as_of,
        parquet_archive=paths["events.parquet"],
    )
    fixture_xml = paths["response.xml"].read_bytes()
    replay, rejected = parse_feed(fixture_xml, selection_as_of + 10)
    require(rejected == 0, "CRYPTO_NEWS_RESUME_NATIVE_XML_REJECTED")
    require(
        {row.event_id for row in replay} == {row.event_id for row in original.metadata},
        "CRYPTO_NEWS_RESUME_REPLAY_MEMBERSHIP",
    )

    temporary_root = work / "crypto-news-resume-proof"
    temporary_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=temporary_root) as temporary:
        root = Path(temporary)
        source = root / "source/events.duckdb"
        await asyncio.to_thread(source.parent.mkdir)
        for name, path in paths.items():
            await asyncio.to_thread(shutil.copy2, path, source.parent / name)
        capture_output = root / "capture"
        clock = [selection_as_of]

        def now() -> int:
            clock[0] += 1
            return clock[0]

        stages: list[str] = []
        original_loader = load_crypto_news_checkpoint

        async def observe_loader(
            database: Path,
            as_of_ms: int,
            *,
            max_poll_age_ms: int = 600_000,
            max_rows: int = 10_000,
            max_bytes: int = 16_000_000,
            parquet_archive: Path | None = None,
        ) -> CryptoNewsCheckpoint:
            checkpoint = await original_loader(
                database,
                as_of_ms,
                max_poll_age_ms=max_poll_age_ms,
                max_rows=max_rows,
                max_bytes=max_bytes,
                parquet_archive=parquet_archive,
            )
            if database == source:
                stages.append("source_archive_verified")
            elif database == capture_output / "events.duckdb":
                stages.append("closed_destination_revalidated")
            return checkpoint

        class ReplayCollector(CryptoNewsCollector):
            def __init__(
                self,
                config: AppConfig,
                sink: Sink,
                *,
                initial_metadata: tuple[MarketEvent, ...] = (),
            ) -> None:
                require(
                    stages[-1:] == ["closed_destination_revalidated"],
                    "CRYPTO_NEWS_RESUME_COLLECTOR_CONSTRUCTED_EARLY",
                )
                stages.append("collector_constructed")
                super().__init__(config, sink, initial_metadata=initial_metadata)

            async def run(self) -> None:
                stages.append("producer_started")
                await self.poll(cast(ClientSession, Session(fixture_xml)))
                await idle()

        with (
            patch("selective_options.ingestion.now_ms", now),
            patch("selective_options.crypto_news.now_ms", now),
            patch.object(DeribitCollector, "run", idle),
            patch("selective_options.ingestion.poll_chains", idle),
            patch("selective_options.ingestion.load_crypto_news_checkpoint", observe_loader),
            patch("selective_options.crypto_news.CryptoNewsCollector", ReplayCollector),
        ):
            capture_report = await capture(
                offline_config(),
                capture_output,
                0.05,
                resume_crypto_news_from=source,
            )

        require(
            stages
            == [
                "source_archive_verified",
                "closed_destination_revalidated",
                "collector_constructed",
                "producer_started",
            ],
            "CRYPTO_NEWS_RESUME_STARTUP_ORDER",
        )
        reloaded = await load_crypto_news_checkpoint(
            capture_output / "events.duckdb",
            clock[0],
            parquet_archive=capture_output / "events.parquet",
        )
        require(reloaded.metadata == original.metadata, "CRYPTO_NEWS_RESUME_FIRST_RECEIPTS")
        require(len(reloaded.health) == 1, "CRYPTO_NEWS_RESUME_LATEST_HEALTH_COVER")
        require(
            reloaded.health[0].event_id != original.latest_health_event_id,
            "CRYPTO_NEWS_RESUME_LATER_HEALTH_NOT_RETAINED",
        )
        with duckdb.connect(str(capture_output / "events.duckdb"), read_only=True) as connection:
            sql_rows = connection.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            parquet_rows = connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id",
                [str(capture_output / "events.parquet")],
            ).fetchall()
            outbox = connection.execute(
                "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id"
            ).fetchall()
            copied_news = connection.execute(
                "SELECT * FROM events WHERE source='coindesk_rss' AND kind='news' ORDER BY event_id"
            ).fetchall()
            health_rows = connection.execute(
                "SELECT payload_json FROM events WHERE source='coindesk_rss' AND kind='feed_health'"
            ).fetchall()
        require(sql_rows == parquet_rows, "CRYPTO_NEWS_RESUME_SQL_PARQUET")
        require(outbox == [(row[0], False) for row in sql_rows], "CRYPTO_NEWS_RESUME_OUTBOX")
        require(
            copied_news
            == sorted(
                await asyncio.to_thread(_native_news_rows, paths["events.duckdb"]),
                key=lambda row: row[0],
            ),
            "CRYPTO_NEWS_RESUME_NATIVE_METADATA_CHANGED",
        )
        require(len(health_rows) == 2, "CRYPTO_NEWS_RESUME_HEALTH_HISTORY")
        require(capture_report["trading_enabled"] is False, "CRYPTO_NEWS_RESUME_TRADING_ENABLED")
        require(capture_report["orders_submitted"] == 0, "CRYPTO_NEWS_RESUME_ORDERS")
        handoff = capture_report["crypto_news_resume"]
        if not isinstance(handoff, dict):
            raise IntegrityError("CRYPTO_NEWS_RESUME_REPORT_MISSING")
        require(
            handoff.get("status") == "COMMITTED_AND_RELOADED",
            "CRYPTO_NEWS_RESUME_REPORT_STATUS",
        )
        report = {
            "version": __version__,
            "status": "SAVED_NATIVE_CHECKPOINT_RESUME_VERIFIED",
            "transport_provenance": "saved_public_xml_replayed_with_mocked_transport",
            "real_network_requests": 0,
            "source": "coindesk_rss/public_reference/GLOBAL/CRYPTO_NEWS",
            "selection_as_of_ms": selection_as_of,
            "source_sha256": source_hashes,
            "source_unchanged": {
                name: file_digest(path) == source_hashes[name] for name, path in paths.items()
            },
            "source_metadata_rows": len(original.metadata),
            "source_minimal_health_rows": len(original.health),
            "source_first_receipt_min_ms": min(row.observed_at_ms for row in original.metadata),
            "source_first_receipt_max_ms": max(row.observed_at_ms for row in original.metadata),
            "replayed_items": len(replay),
            "duplicate_headline_rows_republished": 0,
            "destination_health_rows": len(health_rows),
            "destination_rows": len(sql_rows),
            "duckdb_parquet_exact_match": True,
            "all_outbox_rows_pending": True,
            "closed_destination_revalidated_before_collector": True,
            "startup_order": stages,
            "first_receipts_preserved_exactly": True,
            "new_health_retained": True,
            "history_complete": False,
            "publication_time_independently_verified": False,
            "sentiment_or_classification_available": False,
            "milestone_2_complete": False,
            "ready_for_signals": False,
            "trading_enabled": False,
            "orders_submitted": 0,
        }

    if await asyncio.to_thread(output.exists):
        raise FileExistsError("verification output was created while proof ran")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    await complete_thread(
        output.joinpath("verification.json").write_text,
        json.dumps(report, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def _latest_health_ms(database: Path) -> int:
    with duckdb.connect(str(database), read_only=True) as connection:
        result = connection.execute(
            "SELECT max(observed_at_ms) FROM events "
            "WHERE source='coindesk_rss' AND kind='feed_health'"
        ).fetchone()
    latest = None if result is None else result[0]
    if type(latest) is not int:
        raise IntegrityError("CRYPTO_NEWS_RESUME_NATIVE_HEALTH_MISSING")
    return latest


def _native_news_rows(database: Path) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(
            "SELECT * FROM events WHERE source='coindesk_rss' AND kind='news' ORDER BY event_id"
        ).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.output)), indent=2))


if __name__ == "__main__":
    main()
