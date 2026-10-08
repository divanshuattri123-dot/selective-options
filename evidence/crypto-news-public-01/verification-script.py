"""One finite public CoinDesk RSS metadata request with independent native proof."""

import argparse
import asyncio
import hashlib
import io
import json
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Literal
from xml.dom import Node, minidom

import aiohttp
import duckdb
from defusedxml.common import DefusedXmlException
from defusedxml.minidom import parse

from selective_options import __version__
from selective_options.archive import file_digest, seal_archive
from selective_options.collectors import now_ms
from selective_options.config import AppConfig
from selective_options.crypto_news import (
    FEED_URL,
    MAX_BYTES,
    MAX_ITEMS,
    SOURCE,
    CryptoNewsCollector,
    crypto_news_quality,
)
from selective_options.crypto_news import (
    REFERENCES as SOURCE_REFERENCES,
)
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE_FILE = ROOT / "src/selective_options/crypto_news.py"
VERIFIER_FILE = Path(__file__).resolve()
REFERENCES = (*SOURCE_REFERENCES, FEED_URL)


def preflight(output: Path) -> tuple[Path, str]:
    output = output.resolve()
    scratch = (ROOT.parents[1] / "work/crypto-news-0340").resolve()
    if output != (ROOT / "evidence/crypto-news-public-01").resolve() and not output.is_relative_to(
        scratch
    ):
        raise ValueError("output must be crypto-news-public-01 or within workspace scratch")
    if output.exists():
        raise FileExistsError("refusing to overwrite crypto news proof evidence")
    return output, file_digest(SOURCE_FILE)


def _direct_elements(node: minidom.Node, name: str) -> list[minidom.Node]:
    return [
        child
        for child in node.childNodes
        if child.nodeType == Node.ELEMENT_NODE and (child.localName or child.nodeName) == name
    ]


def _text(node: minidom.Node) -> str:
    if node.nodeType in (Node.TEXT_NODE, Node.CDATA_SECTION_NODE):
        return node.nodeValue or ""
    return "".join(_text(child) for child in node.childNodes)


def independent_metadata(
    body: bytes, rows: list[MarketEvent], received_ms: int
) -> list[dict[str, str]]:
    """Compare an independent XML DOM's direct native metadata to accepted events.

    This deliberately does not call the production RSS parser or validators. It
    never resolves an article, embeds an article body, or follows a feed link.
    """
    if type(body) is not bytes or len(body) > MAX_BYTES:
        raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_BYTE_BUDGET")
    try:
        document = parse(
            io.BytesIO(body), forbid_dtd=True, forbid_entities=True, forbid_external=True
        )
    except DefusedXmlException as exc:
        raise IntegrityError("CRYPTO_NEWS_PROBE_UNSAFE_XML") from exc
    try:
        root = document.documentElement
        if root is None:
            raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_ROOT")
        channels = _direct_elements(root, "channel")
        if (root.localName or root.nodeName) != "rss" or len(channels) != 1:
            raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_ROOT")
        native: list[dict[str, str]] = []
        items = _direct_elements(channels[0], "item")
        if len(items) > MAX_ITEMS:
            raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_ITEM_BUDGET")
        for item in items:
            fields: dict[str, str] = {}
            for name in ("title", "link", "guid", "pubDate"):
                children = _direct_elements(item, name)
                if len(children) > 1 or (name != "guid" and len(children) != 1):
                    raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_ITEM_SHAPE")
                fields[name] = _text(children[0]) if children else ""
            published = parsedate_to_datetime(fields["pubDate"].strip())
            if published.tzinfo is None or published.utcoffset() is None:
                raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_DATE_ZONE")
            native.append(
                {
                    "native_title": fields["title"],
                    "native_link": fields["link"],
                    "native_guid": fields["guid"],
                    "native_pub_date": fields["pubDate"],
                    "title": fields["title"].strip(),
                    "url": fields["link"].strip(),
                    "publisher_item_id": fields["guid"].strip() or fields["link"].strip(),
                    "published_at": published.astimezone(UTC).isoformat(),
                }
            )
        # Exact duplicated RSS items are one native metadata observation.
        unique = {json.dumps(item, sort_keys=True): item for item in native}
        actual: dict[str, dict[str, str]] = {}
        for row in rows:
            payload = row.payload()
            values: dict[str, str] = {}
            for field in (
                "native_title",
                "native_link",
                "native_guid",
                "native_pub_date",
                "title",
                "url",
                "publisher_item_id",
                "published_at",
            ):
                value = payload.get(field)
                if not isinstance(value, str):
                    raise IntegrityError("CRYPTO_NEWS_PROBE_NATIVE_FIELD_MISSING")
                values[field] = value
            instant = parsedate_to_datetime(values["native_pub_date"].strip())
            if (
                row.source != SOURCE
                or row.environment != "public_reference"
                or row.asset != "GLOBAL"
                or row.symbol != "CRYPTO_NEWS"
                or row.kind != "news"
                or row.observed_at_ms != received_ms
                or row.event_time_ms != int(instant.timestamp() * 1000)
            ):
                raise IntegrityError("CRYPTO_NEWS_PROBE_NATIVE_CLOCK_OR_SCOPE")
            actual[json.dumps(values, sort_keys=True)] = values
        if actual != unique or len(actual) != len(rows):
            raise IntegrityError("CRYPTO_NEWS_PROBE_INDEPENDENT_NATIVE_MISMATCH")
        return list(unique.values())
    finally:
        document.unlink()


def closed_rows(database: Path, archive: Path) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events ORDER BY event_id")
        columns = [entry[0] for entry in cursor.description]
        rows = cursor.fetchall()
        if (
            rows
            != connection.execute(
                "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(archive)]
            ).fetchall()
        ):
            raise IntegrityError("CRYPTO_NEWS_PROBE_DATABASE_PARQUET_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True))) for row in rows
        )


class RecordingCollector(CryptoNewsCollector):
    def __init__(self, config: AppConfig, store: DuckStore) -> None:
        self.store = store
        self.rows: list[MarketEvent] = []
        self.entities: list[dict[str, object]] = []
        self.body: bytes | None = None
        self.requests_attempted = 0
        super().__init__(config, self.record)

    async def record(self, row: MarketEvent) -> None:
        await self.store.append([row], exact=True)
        self.rows.append(row)

    def request(self, session: aiohttp.ClientSession) -> aiohttp.client._RequestContextManager:
        self.requests_attempted += 1
        if self.requests_attempted != 1:
            raise IntegrityError("CRYPTO_NEWS_PROBE_REQUEST_BUDGET")
        return super().request(session)

    async def response_observed(self, body: bytes, received_ms: int, http_status: int) -> None:
        if self.entities:
            raise IntegrityError("CRYPTO_NEWS_PROBE_MORE_THAN_ONE_ENTITY")
        self.body = body
        self.entities.append(
            {
                "http_status": http_status,
                "received_at_ms": received_ms,
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
                "url": FEED_URL,
                "basis": "exact decoded HTTP entity bytes passed to production parser",
            }
        )


async def verify(
    output: Path,
    *,
    transport_provenance: Literal["public_http", "explicit_mock_fixture"] = "public_http",
) -> dict[str, object]:
    if transport_provenance not in ("public_http", "explicit_mock_fixture"):
        raise ValueError("explicit public or mocked fixture provenance required")
    output, source_before = await complete_thread(preflight, output)
    verifier_before = await complete_thread(file_digest, VERIFIER_FILE)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    config = AppConfig(ingestion={"crypto_news_enabled": True})
    store = await DuckStore.open(output / "events.duckdb")
    collector = RecordingCollector(config, store)
    native_rows: list[dict[str, str]] = []
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=min(config.venue.request_timeout_s, 30.0)),
            headers={
                "User-Agent": "SelectiveOptionsResearch/0.3 (finite public RSS metadata probe)"
            },
        ) as session:
            await collector.poll(session)
        if collector.requests_attempted != 1:
            raise IntegrityError("CRYPTO_NEWS_PROBE_REQUEST_COUNT")
        cutoff = now_ms()
        snapshot = await store.snapshot("GLOBAL", cutoff, "public_reference")
        if set(snapshot.rows()) != set(collector.rows):
            raise IntegrityError("CRYPTO_NEWS_PROBE_CURRENT_PIT_ROWS_MISMATCH")
        view = crypto_news_quality(snapshot.rows(), as_of_ms=cutoff)
        if await store.crypto_news_quality(as_of_ms=cutoff) != view:
            raise IntegrityError("CRYPTO_NEWS_PROBE_BOUNDED_STORE_QUALITY_MISMATCH")
        if view != crypto_news_quality(collector.rows, as_of_ms=cutoff):
            raise IntegrityError("CRYPTO_NEWS_PROBE_EXACT_QUALITY_MISMATCH")
        news = [row for row in collector.rows if row.kind == "news"]
        independently_compared = False
        if view["state"] == "POLL_OBSERVED_VERIFIED":
            if collector.body is None or len(collector.entities) != 1:
                raise IntegrityError("CRYPTO_NEWS_PROBE_ACCEPTED_ENTITY_MISSING")
            received = collector.entities[0]["received_at_ms"]
            if not isinstance(received, int):
                raise IntegrityError("CRYPTO_NEWS_PROBE_ENTITY_RECEIPT")
            native_rows = independent_metadata(collector.body, news, received)
            independently_compared = True
        for row in collector.rows:
            at = max(row.observed_at_ms, row.event_time_ms)
            before = await store.snapshot("GLOBAL", at - 1, "public_reference")
            available = await store.snapshot("GLOBAL", at, "public_reference")
            if row.event_id in {item.event_id for item in before.rows()} or row.event_id not in {
                item.event_id for item in available.rows()
            }:
                raise IntegrityError("CRYPTO_NEWS_PROBE_EXACT_PIT_BOUNDARY")
        if collector.body is not None:
            await asyncio.to_thread((output / "response.xml").write_bytes, collector.body)
            if (
                await complete_thread(file_digest, output / "response.xml")
                != collector.entities[0]["sha256"]
            ):
                raise IntegrityError("CRYPTO_NEWS_PROBE_SAVED_ENTITY_CHECKSUM")
        await store.append(collector.rows, exact=True)
        count = await store.export_parquet(output / "events.parquet")
        manifest = await seal_archive(output / "events.parquet", count)
        if count != len(collector.rows) or await store.pending_delivery_count() != count:
            raise IntegrityError("CRYPTO_NEWS_PROBE_OUTBOX_OR_ARCHIVE_COUNT")
    finally:
        await store.close()
    saved = await complete_thread(closed_rows, output / "events.duckdb", output / "events.parquet")
    if set(saved) != set(collector.rows):
        raise IntegrityError("CRYPTO_NEWS_PROBE_CLOSED_RELOAD_EXACT")
    reopened = await DuckStore.open(output / "events.duckdb")
    try:
        snapshot = await reopened.snapshot("GLOBAL", cutoff, "public_reference")
        if (
            crypto_news_quality(snapshot.rows(), as_of_ms=cutoff) != view
            or await reopened.pending_delivery_count() != count
            or await reopened.crypto_news_quality(as_of_ms=cutoff) != view
        ):
            raise IntegrityError("CRYPTO_NEWS_PROBE_REOPEN_VIEW_OR_OUTBOX")
    finally:
        await reopened.close()
    source_after = await complete_thread(file_digest, SOURCE_FILE)
    verifier_after = await complete_thread(file_digest, VERIFIER_FILE)
    if source_before != source_after:
        raise IntegrityError("CRYPTO_NEWS_PROBE_SOURCE_CHANGED")
    if verifier_before != verifier_after:
        raise IntegrityError("CRYPTO_NEWS_PROBE_VERIFIER_CHANGED")
    report: dict[str, object] = {
        "version": __version__,
        "status": (
            "PUBLIC_CRYPTO_NEWS_METADATA_VERIFIED"
            if transport_provenance == "public_http"
            else "FIXTURE_CRYPTO_NEWS_METADATA_VERIFIED"
        )
        if independently_compared
        else "SOURCE_UNAVAILABLE_OR_INVALID",
        "transport_provenance": transport_provenance,
        "real_public_requests_attempted": collector.requests_attempted
        if transport_provenance == "public_http"
        else 0,
        "request_attempts": collector.requests_attempted,
        "retries": 0,
        "source": SOURCE,
        "feed_url": FEED_URL,
        "entities": collector.entities,
        "native_rows": native_rows,
        "quality": view,
        "persisted_rows": count,
        "persisted_metadata_items": len(news),
        "independent_native_metadata_comparison": independently_compared,
        "pit_boundary_checks": 2 * len(collector.rows),
        "parquet_exact_match": True,
        "parquet_sha256": manifest.sha256,
        "closed_reload_exact_match": True,
        "outbox_pending_after_reopen": count,
        "redis_delivery_claim": False,
        "bounded_store_quality_matches": True,
        "source_code_sha256_before": source_before,
        "source_code_sha256_after": source_after,
        "source_code_unchanged": True,
        "verifier_code_sha256_before": verifier_before,
        "verifier_code_sha256_after": verifier_after,
        "verifier_code_unchanged": True,
        "source_references": list(REFERENCES),
        "publisher_feed_documentation_independently_linked": False,
        "redirects_allowed": False,
        "tls_verification": "default; no override",
        "network_timeout_s": min(config.venue.request_timeout_s, 30.0),
        "metadata_only": True,
        "article_bodies_fetched": 0,
        "first_publication_independently_verified": False,
        "sentiment_or_classification_available": False,
        "native_data_backdated": False,
        "historical_vintage_available": False,
        "continuous_history_verified": False,
        "complete_feed_available": False,
        "milestone_2_complete": False,
        "trading_approval": False,
        "orders_submitted": 0,
    }
    await asyncio.to_thread(
        (output / "report.json").write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/crypto-news-public-01")
    args = parser.parse_args()
    report = asyncio.run(verify(args.output))
    print(
        json.dumps(
            {
                field: report[field]
                for field in (
                    "status",
                    "real_public_requests_attempted",
                    "request_attempts",
                    "persisted_rows",
                    "parquet_exact_match",
                    "closed_reload_exact_match",
                    "source_code_unchanged",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
