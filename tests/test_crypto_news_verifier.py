"""Finite verification fixtures never claim public availability or first publication."""

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import aiohttp
import pytest

from selective_options.crypto_news import parse_feed
from selective_options.errors import IntegrityError

ROOT = Path(__file__).resolve().parents[1]
RECEIVED = int(datetime(2026, 10, 3, 12, 30, tzinfo=UTC).timestamp() * 1000)
BODY = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Explicit fixture publisher</title>
<item><title> Native &amp; title </title><link> https://www.coindesk.com/fixture/one </link>
<guid isPermaLink="false"> publisher-identity </guid><pubDate> Sat, 03 Oct 2026 12:00:00 GMT </pubDate>
<description>Ignored fixture article body</description></item>
<item><title><![CDATA[Second <native> title]]></title><link>https://coindesk.com/fixture/two</link>
<pubDate>Sat, 03 Oct 2026 11:00:00 +0000</pubDate></item></channel></rss>"""


class Content:
    def __init__(self, body):
        self.body = body

    async def iter_chunked(self, size):
        assert size <= 65536
        for offset in range(0, len(self.body), size):
            yield self.body[offset : offset + size]


class Response:
    def __init__(self, body, status=200, error=None, location=None):
        self.status = status
        self.headers = {} if location is None else {"Location": location}
        self.content = Content(body)
        self.error = error

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        return False


class Session:
    def __init__(self, body=BODY, status=200, error=None, location=None):
        self.body, self.status, self.error = body, status, error
        self.calls = []
        self.location = location

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        assert kwargs["allow_redirects"] is False
        assert kwargs["timeout"].total <= 30
        assert "ssl" not in kwargs
        return Response(self.body, self.status, self.error, self.location)


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_crypto_news")


@pytest.fixture
def output():
    scratch = ROOT.parents[1] / "work/crypto-news-0341"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="verifier-", dir=scratch) as temporary:
        yield Path(temporary) / "probe"


def test_preflight_restricts_workspace_scope_and_immutable_outputs(verifier, output, tmp_path):
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(tmp_path / "probe")
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(ROOT / "evidence/crypto-news-public-01")
    output.mkdir()
    sentinel = output / "sentinel"
    sentinel.write_bytes(b"keep")
    with pytest.raises(FileExistsError, match="overwrite"):
        verifier.preflight(output)
    assert sentinel.read_bytes() == b"keep"


@pytest.mark.parametrize(
    "mode",
    [
        "success",
        "empty",
        "http403",
        "redirect",
        "invalid_xml",
        "transport",
        "oversize",
        "item_budget",
    ],
)
async def test_mock_finite_http_archive_pit_reopen_outbox_and_bound_view(
    verifier, output, monkeypatch, mode
):
    body = BODY
    if mode == "empty":
        body = b"<rss><channel/></rss>"
    elif mode == "invalid_xml":
        body = b"<html>Explicit fixture failure</html>"
    elif mode == "oversize":
        body = b"x" * 2_097_153
    elif mode == "item_budget":
        body = b"<rss><channel>" + b"<item/>" * 101 + b"</channel></rss>"
    session = Session(
        body=body,
        status=403 if mode == "http403" else 302 if mode == "redirect" else 200,
        error=aiohttp.ClientConnectionError("Explicit mocked fixture")
        if mode == "transport"
        else None,
    )

    class ManagedSession:
        def __init__(self, **kwargs):
            assert kwargs["timeout"].total <= 30

        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(verifier.aiohttp, "ClientSession", ManagedSession)
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)
    report = await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert report == json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert report["real_public_requests_attempted"] == 0
    assert report["request_attempts"] == len(session.calls) == 1
    assert report["retries"] == report["orders_submitted"] == report["article_bodies_fetched"] == 0
    good = mode in ("success", "empty")
    count = 3 if mode == "success" else 1
    assert report["status"] == (
        "FIXTURE_CRYPTO_NEWS_METADATA_VERIFIED" if good else "SOURCE_UNAVAILABLE_OR_INVALID"
    )
    assert report["persisted_rows"] == report["outbox_pending_after_reopen"] == count
    assert report["pit_boundary_checks"] == count * 2
    assert report["parquet_exact_match"] and report["closed_reload_exact_match"]
    assert report["source_code_unchanged"] and report["bounded_store_quality_matches"]
    assert report["independent_native_metadata_comparison"] == good
    assert report["trading_approval"] is report["milestone_2_complete"] is False
    assert report["first_publication_independently_verified"] is False
    if mode not in ("transport", "oversize"):
        assert (output / "response.xml").read_bytes() == session.body
    else:
        assert report["entities"] == [] and not (output / "response.xml").exists()
    if mode == "success":
        assert report["native_rows"][0]["native_title"] == " Native & title "
        assert report["native_rows"][1]["native_guid"] == ""
        assert report["native_rows"][1]["publisher_item_id"] == "https://coindesk.com/fixture/two"
    if mode == "empty":
        assert report["native_rows"] == [] and report["quality"]["headline_state"] == "EMPTY_POLL"
    original = (output / "events.duckdb").read_bytes()
    with pytest.raises(FileExistsError):
        await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert len(session.calls) == 1 and (output / "events.duckdb").read_bytes() == original


def test_independent_native_metadata_detects_changes_and_missing_items(verifier):
    rows, rejected = parse_feed(BODY, RECEIVED)
    assert rejected == 0
    assert len(verifier.independent_metadata(BODY, rows, RECEIVED)) == 2
    changed = rows[0].model_copy(
        update={"payload_json": json.dumps({**rows[0].payload(), "native_title": "tampered"})}
    )
    with pytest.raises(IntegrityError, match="NATIVE_MISMATCH"):
        verifier.independent_metadata(BODY, [changed, rows[1]], RECEIVED)
    with pytest.raises(IntegrityError, match="NATIVE_MISMATCH"):
        verifier.independent_metadata(BODY, rows[:1], RECEIVED)
    with pytest.raises(IntegrityError, match="CLOCK_OR_SCOPE"):
        verifier.independent_metadata(BODY, rows, RECEIVED + 1)


@pytest.mark.parametrize(
    "body,reason",
    [
        (b"<!DOCTYPE rss><rss><channel/></rss>", "UNSAFE_XML"),
        (b"<rss><channel/><channel/></rss>", "INDEPENDENT_ROOT"),
        (
            b"<rss><channel><item><title>A</title><title>B</title></item></channel></rss>",
            "ITEM_SHAPE",
        ),
        (
            b"<rss><channel><item><title>A</title><link>https://coindesk.com/a</link><pubDate>Sat, 03 Oct 2026 12:00:00</pubDate></item></channel></rss>",
            "DATE_ZONE",
        ),
    ],
)
def test_independent_comparison_rejects_ambiguous_native_xml(verifier, body, reason):
    with pytest.raises(IntegrityError, match=reason):
        verifier.independent_metadata(body, [], RECEIVED)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16", "utf-16-le", "utf-16-be"])
def test_independent_dom_forbids_dtd_in_every_native_encoding(verifier, encoding):
    text = '<!DOCTYPE rss [<!ENTITY embedded "forbidden">]><rss><channel/></rss>'
    with pytest.raises(IntegrityError, match="UNSAFE_XML"):
        verifier.independent_metadata(text.encode(encoding), [], RECEIVED)


@pytest.mark.parametrize(
    "body,reason",
    [
        (b"x" * 2_097_153, "BYTE_BUDGET"),
        (b"<rss><channel>" + b"<item/>" * 101 + b"</channel></rss>", "ITEM_BUDGET"),
    ],
    ids=["byte_budget", "item_budget"],
)
def test_independent_dom_enforces_finite_entity_and_item_budgets(verifier, body, reason):
    with pytest.raises(IntegrityError, match=reason):
        verifier.independent_metadata(body, [], RECEIVED)


async def test_second_request_budget_before_transport(verifier, output):
    store = await verifier.DuckStore.open(output.parent / "budget.duckdb")
    try:
        collector = verifier.RecordingCollector(verifier.AppConfig(), store)
        session = Session()
        collector.request(session)
        with pytest.raises(IntegrityError, match="REQUEST_BUDGET"):
            collector.request(session)
        assert len(session.calls) == 1
    finally:
        await store.close()


async def test_invalid_provenance_before_files_or_transport(verifier, output):
    with pytest.raises(ValueError, match="provenance"):
        await verifier.verify(output, transport_provenance="invented")
    assert not output.exists()


async def test_failed_closed_reload_refuses_success_report(verifier, output, monkeypatch):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)

    def unavailable(self, session):
        raise aiohttp.ClientConnectionError("Explicit mock fixture")

    monkeypatch.setattr(verifier.CryptoNewsCollector, "request", unavailable)

    def failed_reload(*args):
        raise IntegrityError("Explicit fixture closed reload failure")

    monkeypatch.setattr(verifier, "closed_rows", failed_reload)
    with pytest.raises(IntegrityError, match="closed reload failure"):
        await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert (output / "events.duckdb").exists() and not (output / "report.json").exists()


@pytest.mark.parametrize("changed_file", ["SOURCE_FILE", "VERIFIER_FILE"])
async def test_changed_code_during_probe_refuses_verified_report(
    verifier, output, monkeypatch, changed_file
):
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)

    def unavailable(self, session):
        raise aiohttp.ClientConnectionError("Explicit mock fixture")

    monkeypatch.setattr(verifier.CryptoNewsCollector, "request", unavailable)
    original_digest = verifier.file_digest
    observed = 0

    def changed_digest(path):
        nonlocal observed
        digest = original_digest(path)
        if path == getattr(verifier, changed_file):
            observed += 1
            if observed == 2:
                return "f" * 64
        return digest

    monkeypatch.setattr(verifier, "file_digest", changed_digest)
    with pytest.raises(IntegrityError, match="CHANGED"):
        await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert observed == 2
    assert (output / "events.parquet").exists() and not (output / "report.json").exists()


@pytest.mark.parametrize(
    "value",
    [
        None,
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://coindesk.com/arc/outboundfeeds/rss?outputType=xml",
        "/arc/outboundfeeds/rss/?outputType=xml",
    ],
)
def test_native_location_header_is_exact_allowlisted_evidence_only(verifier, value):
    evidence = verifier.location_evidence(value)
    assert evidence["state"] == ("ABSENT" if value is None else "EXACT_SAFE_NATIVE_TEXT")
    assert evidence["value"] == value
    if value is not None:
        assert evidence["redirect_followed"] is False


@pytest.mark.parametrize(
    "value",
    [
        "https://secret:password@coindesk.com/feed",
        "https://coindesk.com/feed?token=secret",
        "https://coindesk.com/feed#secret",
        "https://unrelated.example/feed",
        "https://coindesk.com/private/credential",
        "http://coindesk.com/feed",
        "//coindesk.com/feed",
        "/feed%3Ftoken%3Dsecret",
        "https://coindesk.com:8443/feed",
        "https://coindesk.com/feed\r\nSecret: value",
        "https://coindesk.com/feed?outputType=xml&token=secret",
        "https://coindesk.com/feed?outputType=xml&broken",
        "x" * 4097,
        "",
    ],
)
def test_unsafe_location_header_never_archived_or_followed(verifier, value):
    evidence = verifier.location_evidence(value)
    assert evidence["state"] == "REFUSED_VALUE_HASH_ONLY"
    assert evidence["value"] is None and evidence["characters"] == len(value)
    assert len(evidence["sha256_utf8"]) == 64 and evidence["redirect_followed"] is False
    assert value not in json.dumps(evidence) if value else evidence["value"] is None


@pytest.mark.parametrize("status", [200, 308])
async def test_response_hook_captures_safe_header_and_body_before_status_refusal(
    verifier, output, monkeypatch, status
):
    location = "https://www.coindesk.com/arc/outboundfeeds/rss/"
    session = Session(BODY if status == 200 else b"Redirecting...\n", status, location=location)

    class ManagedSession:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return session

        async def __aexit__(self, *args):
            return False

    monkeypatch.setattr(verifier.aiohttp, "ClientSession", ManagedSession)
    monkeypatch.setattr("selective_options.crypto_news.now_ms", lambda: RECEIVED)
    monkeypatch.setattr(verifier, "now_ms", lambda: RECEIVED)
    report = await verifier.verify(output, transport_provenance="explicit_mock_fixture")
    assert len(session.calls) == report["request_attempts"] == 1
    assert report["redirects_followed"] == report["retries"] == 0
    assert (output / "response.xml").read_bytes() == session.body
    entity = report["entities"][0]
    assert entity["location_header"] == verifier.location_evidence(location)
    assert entity["http_status"] == status and entity["received_at_ms"] == RECEIVED
    assert report["persisted_metadata_items"] == (2 if status == 200 else 0)
    assert report["independent_native_metadata_comparison"] == (status == 200)
    assert report["quality"]["state"] == ("POLL_OBSERVED_VERIFIED" if status == 200 else "BLOCKED")


@pytest.mark.parametrize("damage", ["missing", "delivered"])
async def test_independent_closed_reader_refuses_count_preserving_outbox_damage(
    verifier, output, damage
):
    output.mkdir()
    rows, _ = parse_feed(BODY, RECEIVED)
    database, archive = output / "events.duckdb", output / "events.parquet"
    store = await verifier.DuckStore.open(database)
    try:
        await store.append(rows, exact=True)
        await store.export_parquet(archive)
    finally:
        await store.close()
    with verifier.duckdb.connect(str(database)) as connection:
        if damage == "missing":
            connection.execute(
                "UPDATE delivery_outbox SET event_id=? WHERE event_id=?",
                ["unrelated", rows[0].event_id],
            )
        else:
            connection.execute(
                "UPDATE delivery_outbox SET delivered=TRUE WHERE event_id=?", [rows[0].event_id]
            )
    with pytest.raises(IntegrityError, match="CLOSED_OUTBOX_EXACT_MISMATCH"):
        verifier.closed_rows(database, archive)
