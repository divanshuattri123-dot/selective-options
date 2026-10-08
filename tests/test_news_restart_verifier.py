"""Offline saved-public replay tests; temporary artifacts stay in the workspace."""

import importlib
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock
from xml.etree.ElementTree import ParseError, fromstring, tostring

import duckdb
import pytest

from selective_options import news
from selective_options.archive import file_digest
from selective_options.errors import IntegrityError
from selective_options.news import NewsPollHealth
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/news-quality-public-01/run"


@pytest.fixture
def workspace_temp():
    scratch = ROOT / "work/news-restart-0338"
    scratch.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="test-", dir=scratch) as temporary:
        path = Path(temporary).resolve()
        assert path.is_relative_to(scratch.resolve())
        yield path


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_news_restart")


def source_copy(verifier, destination):
    copied = destination / "source"
    copied.mkdir()
    for name in verifier.SOURCE_NAMES:
        shutil.copyfile(SOURCE / name, copied / name)
    return copied


async def test_actual_saved_entity_restarts_preserve_original_receipts_archive_outbox_and_pit(
    verifier,
    workspace_temp,
    monkeypatch,
):
    def no_network(*args, **kwargs):
        raise AssertionError("offline verifier must never create a public HTTP session")

    monkeypatch.setattr("aiohttp.ClientSession", no_network)
    before = verifier.hashes(SOURCE)
    output = workspace_temp / "verified"
    result = await verifier.verify(SOURCE, output)
    assert result == json.loads((output / "report.json").read_text(encoding="utf-8"))
    assert result["status"] == "OFFLINE_SAVED_PUBLIC_RESTART_VERIFIED"
    assert result["native_records"] == 16 and result["original_headlines"] == 15
    assert result["persisted_records"] == 19 and result["mocked_http_attempts"] == 4
    assert result["outbox_pending_before_failure"] == 16
    assert result["outbox_pending_after_final_reopen"] == 18
    assert result["exact_closed_reloads"] == 3 and result["three_archives_exact"]
    assert result["real_public_requests"] == 0 and result["redis_delivery_claim"] is False
    assert result["metadata_first_receipts_preserved"] and result["pit_checks"] == 32
    assert (
        result["source_hashes_before"]
        == result["source_hashes_after"]
        == before
        == verifier.hashes(SOURCE)
    )
    views = result["quality_scenarios"]
    assert views["before_original_response"]["historical_metadata_items"] == 0
    assert views["at_original_response"]["historical_metadata_items"] == 15
    assert views["at_original_response"]["blocker"] == "NEWS_HEALTH_MISSING"
    assert views["before_repeat_health"]["health_event_id"] == result["local_acknowledged_event_id"]
    assert views["reopened_repeat"] == views["at_repeat_health"]
    assert views["before_failure"]["state"] == "POLL_OBSERVED_VERIFIED"
    assert views["at_failure"]["blocker"] == "NEWS_FEED_UNAVAILABLE"
    assert views["reopened_failure"] == views["at_failure"]
    assert views["at_recovery_response_before_health"]["blocker"] == "NEWS_FEED_UNAVAILABLE"
    assert views["before_recovery_health"]["blocker"] == "NEWS_FEED_UNAVAILABLE"
    assert views["at_recovery_health"]["state"] == "POLL_OBSERVED_VERIFIED"
    assert views["reopened_recovery"] == views["at_recovery_health"]
    original = NewsPollHealth.model_validate(
        verifier.preflight(SOURCE, workspace_temp / "unused").health.payload()
    )
    assert (
        views["at_recovery_health"]["first_observed_at_ms"]
        == [original.response_received_at_ms] * 15
    )
    assert (
        views["at_recovery_health"]["latest_publication_time_ms"]
        == views["at_original_health"]["latest_publication_time_ms"]
    )
    for stage in ("repeat", "recovery"):
        recorded = NewsPollHealth.model_validate(
            json.loads(result["synthetic_health_records"][stage]["payload_json"])
        )
        assert recorded.accepted_event_ids == original.accepted_event_ids
    assert all(
        result[field] is False
        for field in (
            "all_news_complete",
            "milestone_2_complete",
            "first_publication_independently_verified",
            "trading_approval",
        )
    )


async def test_existing_output_refusal_precedes_any_poll_and_preserves_bytes(
    verifier, workspace_temp, monkeypatch
):
    output = workspace_temp / "existing"
    output.mkdir()
    marker = output / "marker.txt"
    marker.write_bytes(b"preserve me")
    mocked = AsyncMock(side_effect=AssertionError("preflight must refuse before replay"))
    monkeypatch.setattr(verifier, "replay_poll", mocked)
    with pytest.raises(FileExistsError):
        await verifier.verify(SOURCE, output)
    mocked.assert_not_awaited()
    assert marker.read_bytes() == b"preserve me"
    assert sorted(path.name for path in output.iterdir()) == ["marker.txt"]


def test_output_outside_authorized_workspace_refused_without_creation(verifier):
    output = ROOT / "work/news-restart-forbidden"
    assert not output.exists()
    with pytest.raises(ValueError, match="output must"):
        verifier.preflight(SOURCE, output)
    assert not output.exists()


@pytest.mark.parametrize("file", ["response.xml", "events.parquet", "events.duckdb"])
async def test_source_corruption_fails_before_output_or_poll(
    verifier, workspace_temp, monkeypatch, file
):
    source = source_copy(verifier, workspace_temp)
    if file == "events.duckdb":
        with duckdb.connect(str(source / file)) as connection:
            connection.execute(
                "UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE kind='news'"
            )
    else:
        data = bytearray((source / file).read_bytes())
        data[len(data) // 2] ^= 1
        (source / file).write_bytes(data)
    output = workspace_temp / "never-created"
    mocked = AsyncMock(side_effect=AssertionError("corrupt source must never reach collector"))
    monkeypatch.setattr(verifier, "replay_poll", mocked)
    with pytest.raises((IntegrityError, ParseError)):
        await verifier.verify(source, output)
    mocked.assert_not_awaited()
    assert not output.exists()


def test_changed_raw_headline_even_with_matching_entity_checksum_cannot_replace_native_record(
    verifier, workspace_temp
):
    source = source_copy(verifier, workspace_temp)
    root = fromstring((source / "response.xml").read_bytes())
    title = root.find("./channel/item/title")
    assert title is not None
    title.text = "Clearly labelled corrupt saved headline fixture"
    body = tostring(root, encoding="utf-8")
    (source / "response.xml").write_bytes(body)
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    report["response"]["sha256"] = file_digest(source / "response.xml")
    report["response"]["bytes"] = len(body)
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="RAW_ENTITY_OR_ORIGINAL_RECEIPT"):
        verifier.preflight(source, workspace_temp / "never-created")


@pytest.mark.parametrize("mutation", ["receipt", "quality", "provenance"])
def test_original_receipt_quality_or_provenance_mismatch_refused(
    verifier, workspace_temp, mutation
):
    source = source_copy(verifier, workspace_temp)
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if mutation == "receipt":
        report["response"]["observed_at_ms"] += 1
    elif mutation == "quality":
        report["quality"]["accepted_event_ids"] = []
    else:
        report["fixture_poll_health"] = True
    (source / "report.json").write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(IntegrityError, match="NEWS_RESTART_ORIGINAL"):
        verifier.preflight(source, workspace_temp / "never-created")


def test_source_row_and_file_budgets_are_preflight_bounds(verifier, workspace_temp, monkeypatch):
    monkeypatch.setattr(verifier, "MAX_SOURCE_ROWS", 18)
    with pytest.raises(IntegrityError, match="ROW_BUDGET"):
        verifier.preflight(SOURCE, workspace_temp / "never-created")
    monkeypatch.setattr(verifier, "MAX_SOURCE_BYTES", 1)
    with pytest.raises(ValueError, match="over-budget"):
        verifier.preflight(SOURCE, workspace_temp / "never-created")


@pytest.mark.parametrize("response,health", [(True, 10), (10, False), (0, 10), (11, 10)])
async def test_bad_fixture_receipt_clocks_never_reach_sink(verifier, response, health):
    sink = AsyncMock()
    with pytest.raises(ValueError, match="receipt clocks"):
        await verifier.replay_poll(b"<rss><channel/></rss>", sink, response, health)
    sink.assert_not_awaited()


async def test_mocked_transport_failure_is_bound_failure_with_no_metadata_or_public_request(
    verifier,
):
    sink = AsyncMock()
    original_clock = news.now_ms
    rows = await verifier.replay_poll(b"unused", sink, 1000, 1000, transport_failure=True)
    assert news.now_ms is original_clock
    assert len(rows) == 1 and rows[0].kind == "feed_health"
    health = NewsPollHealth.model_validate(rows[0].payload())
    assert health.ok is False and health.error_type == "ClientConnectionError"
    assert health.accepted_event_ids == () and health.accepted_items == 0
    assert health.response_received_at_ms is None
    assert rows[0].observed_at_ms == rows[0].event_time_ms == 1000


async def test_unexpected_malformed_entity_propagates_and_restores_clock(verifier):
    original_clock = news.now_ms
    sink = AsyncMock()
    with pytest.raises(ParseError):
        await verifier.replay_poll(b"<rss", sink, 1000, 1010)
    assert news.now_ms is original_clock
    sink.assert_not_awaited()


async def test_mid_replay_failure_closes_committed_database_and_never_publishes_success(
    verifier, workspace_temp, monkeypatch
):
    original_append = DuckStore.append

    async def failed_append(self, rows, *, exact=False):
        if any(row.kind == "feed_health" and row.payload()["ok"] is False for row in rows):
            raise OSError("labelled fixture failure to commit outage health")
        await original_append(self, rows, exact=exact)

    monkeypatch.setattr(DuckStore, "append", failed_append)
    output = workspace_temp / "interrupted"
    source_hashes = verifier.hashes(SOURCE)
    with pytest.raises(OSError, match="commit outage"):
        await verifier.verify(SOURCE, output)
    assert not (output / "report.json").exists()
    assert len(verifier.read_rows(output / "events.duckdb")) == 17
    assert verifier.hashes(SOURCE) == source_hashes
    with pytest.raises(FileExistsError):
        await verifier.verify(SOURCE, output)


async def test_source_changed_during_fixture_replay_fails_final_hash_audit(
    verifier, workspace_temp, monkeypatch
):
    source = source_copy(verifier, workspace_temp)
    real_replay = verifier.replay_poll
    attempts = 0

    async def change_source(*args, **kwargs):
        nonlocal attempts
        result = await real_replay(*args, **kwargs)
        attempts += 1
        if attempts == 4:
            path = source / "response.xml"
            path.write_bytes(path.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(verifier, "replay_poll", change_source)
    output = workspace_temp / "not-success"
    with pytest.raises(IntegrityError, match="SOURCE_CHANGED"):
        await verifier.verify(source, output)
    assert not (output / "report.json").exists()
