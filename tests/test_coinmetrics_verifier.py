"""Labelled mock transport proofs use the actual committed ingestion and recovery."""

import asyncio
import hashlib
import json
import os
import subprocess
from urllib.parse import urlencode

import pytest

from scripts import verify_coinmetrics as proof
from selective_options import coinmetrics
from selective_options.coinmetrics import canonical_coinmetrics, canonical_coinmetrics_health
from selective_options.errors import IntegrityError

LABEL = coinmetrics.native_time_ms("2026-10-05T00:00:00.000000000Z")
STARTED = LABEL + 2 * coinmetrics.DAY_MS + 100


def native(asset, date="2026-10-05", **changes):
    return {
        "asset": asset,
        "time": date + "T00:00:00.000000000Z",
        "CapMrktCurUSD": "1724754820477.82986243790963106",
        "CapMVRVCur": "1.596729720053423595",
        **changes,
    }


class Response:
    def __init__(self, body, status=200):
        self.body = body
        self.status = status
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def iter_chunked(self, size):
        # Deliberate non-JSON-aligned byte boundaries exercise original-byte hook.
        yield self.body[:17]
        yield self.body[17:]


def configure(tmp_path, monkeypatch, pages):
    root = tmp_path / "project/outputs/selective-options"
    output = tmp_path / "project/work/coinmetrics-0347/attempt"
    monkeypatch.setattr(proof, "ROOT", root)
    monkeypatch.setattr(proof, "code_hashes", lambda: {"fixture-source": "unchanged"})
    receipts = iter((STARTED, STARTED + 10, STARTED + 20, STARTED + 30))
    # collect start, one/two native response receipts, then poll finish.
    monkeypatch.setattr(coinmetrics, "now_ms", lambda: next(receipts))
    monkeypatch.setattr(proof, "now_ms", lambda: STARTED + 100)
    requests = []
    original_entities = []

    class Session:
        def __init__(self, **kwargs):
            assert kwargs["timeout"].total == 30

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def get(self, url, *, params, allow_redirects):
            assert url == coinmetrics.URL and allow_redirects is False
            assert params["assets"] == "btc,eth"
            assert params["metrics"] == "CapMrktCurUSD,CapMVRVCur"
            assert params["frequency"] == "1d"
            assert params["start_time"] == "2026-09-30"
            assert params["end_time"] == "2026-10-06"
            index = len(requests)
            assert index < len(pages), "unexpected retry or unbounded request"
            requests.append(params.copy())
            document, status = pages[index]
            if callable(document):
                document = document(params)
            body = (
                document if isinstance(document, bytes) else json.dumps(document, indent=2).encode()
            )
            original_entities.append((body, status))
            return Response(body, status)

    monkeypatch.setattr(proof.aiohttp, "ClientSession", Session)
    return output, requests, original_entities


def paginated(params):
    return {
        "data": [native("btc"), native("eth")],
        "next_page_token": "fixture-page-two",
        "next_page_url": coinmetrics.URL
        + "?"
        + urlencode({**params, "next_page_token": "fixture-page-two"}),
    }


def assert_native_entities(output, expected):
    document = json.loads((output / "native-entities.json").read_text())
    assert document["transport_provenance"] == "explicit_mock_fixture"
    assert len(document["entities"]) == len(expected)
    for entity, (body, status) in zip(document["entities"], expected, strict=True):
        assert bytes.fromhex(entity["body_hex"]) == body
        assert entity["sha256"] == hashlib.sha256(body).hexdigest()
        assert entity["bytes"] == len(body)
        assert entity["http_status"] == status
        assert entity["basis"] == "explicit_mock_fixture"
    return document["entities"]


async def test_two_page_native_bytes_real_commit_pit_quality_and_recovery(tmp_path, monkeypatch):
    output, requests, entities = configure(
        tmp_path,
        monkeypatch,
        [
            (paginated, 200),
            ({"data": [native("btc", "2026-10-06"), native("eth", "2026-10-06")]}, 200),
        ],
    )
    report = await proof.verify(output, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "MOCK_DAILY_VALUATION_POLL_VERIFIED"
    assert report["rows"] == report["persisted_rows"] == 6
    assert report["native_daily_rows"] == 4
    assert report["request_attempts"] == 2 and len(requests) == 2
    assert requests[1]["next_page_token"] == "fixture-page-two"
    assert report["real_public_requests_attempted"] == 0
    assert report["outbox_pending_after_reopen"] == report["restored_outbox_pending"] == 6
    original = proof.closed_rows(output / "events.duckdb", output / "events.parquet")
    assert proof.closed_rows(output / "restored/events.duckdb") == original
    native_rows = [row for row in original if row.kind == "on_chain"]
    expected = {
        ("BTC", LABEL): STARTED + 10,
        ("ETH", LABEL): STARTED + 10,
        ("BTC", LABEL + coinmetrics.DAY_MS): STARTED + 20,
        ("ETH", LABEL + coinmetrics.DAY_MS): STARTED + 20,
    }
    assert {(row.asset, row.event_time_ms): row.observed_at_ms for row in native_rows} == expected
    for row in native_rows:
        snapshot = canonical_coinmetrics(row)
        assert snapshot.native_row.CapMrktCurUSD == "1724754820477.82986243790963106"
        assert snapshot.native_row.CapMVRVCur == "1.596729720053423595"
        assert snapshot.response_received_at_ms == row.observed_at_ms
        assert snapshot.units == {"CapMrktCurUSD": "USD", "CapMVRVCur": "Dimensionless"}
    assert report["boundary_cutoffs_ms"] == [
        STARTED + 9,
        STARTED + 10,
        STARTED + 19,
        STARTED + 20,
        STARTED + 29,
        STARTED + 30,
    ]
    assert report["pit_boundary_checks"] == 12
    recorded = assert_native_entities(output, entities)
    assert [entry["received_at_ms"] for entry in recorded] == [STARTED + 10, STARTED + 20]
    assert all(view["state"] == "DAILY_REGIME_OBSERVED" for view in report["quality"].values())
    for key in (
        "history_complete",
        "publication_time_available",
        "provider_finality_independently_verified",
        "exchange_attribution_available",
        "milestone_2_complete",
        "ready_for_signals",
        "trading_enabled",
        "redis_delivery_claim",
    ):
        assert report[key] is False
    assert report["orders_submitted"] == report["historical_strategy_trials"] == 0
    saved = json.loads((output / "report.json").read_text())
    assert saved["source_sha256"] == proof.file_digest(output / "events.duckdb")
    assert saved["native_entities_sha256"] == proof.file_digest(output / "native-entities.json")


@pytest.mark.parametrize(
    ("status", "reason"), [(403, "ACCESS_DENIED"), (429, "RATE_LIMITED"), (500, "HTTP_UNAVAILABLE")]
)
async def test_http_failure_records_native_entity_and_failed_health_report(
    tmp_path, monkeypatch, status, reason
):
    output, requests, entities = configure(
        tmp_path, monkeypatch, [(b'{"error":"labelled fixture denial"}', status)]
    )
    report = await proof.verify(output, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "SOURCE_UNAVAILABLE_OR_INVALID"
    assert report["native_daily_rows"] == 0 and report["persisted_rows"] == 2
    assert report["request_attempts"] == 1 and len(requests) == 1
    rows = proof.closed_rows(output / "events.duckdb")
    assert all(row.kind == "feed_health" for row in rows)
    for row in rows:
        health = canonical_coinmetrics_health(row)
        assert health.reason == reason and health.code == status and not health.ok
        assert not health.accepted_observations
    assert all(view["state"] != "DAILY_REGIME_OBSERVED" for view in report["quality"].values())
    assert_native_entities(output, entities)
    assert proof.closed_rows(output / "restored/events.duckdb") == rows
    assert report["real_public_requests_attempted"] == 0 and report["trading_enabled"] is False


@pytest.mark.parametrize("late_failure", ["invalid-value", "future-period", "duplicate-json"])
async def test_later_invalid_page_never_commits_earlier_valid_data(
    tmp_path, monkeypatch, late_failure
):
    if late_failure == "duplicate-json":
        later = b'{"data":[],"data":[]}'
    else:
        row = (
            native("btc", "2026-10-06", CapMVRVCur="-1")
            if late_failure == "invalid-value"
            else native("btc", "2026-10-07")
        )
        later = {"data": [row]}
    output, requests, entities = configure(tmp_path, monkeypatch, [(paginated, 200), (later, 200)])
    report = await proof.verify(output, transport_provenance="explicit_mock_fixture")
    assert report["status"] == "SOURCE_UNAVAILABLE_OR_INVALID"
    assert report["native_daily_rows"] == 0 and report["persisted_rows"] == 2
    assert len(requests) == 2 and report["request_attempts"] == 2
    rows = proof.closed_rows(output / "events.duckdb")
    assert all(row.kind == "feed_health" for row in rows)
    assert all(canonical_coinmetrics_health(row).reason == "INVALID_DATA" for row in rows)
    assert_native_entities(output, entities)
    assert proof.closed_rows(output / "restored/events.duckdb") == rows


async def test_code_drift_refuses_success_after_preserving_native_store_and_archives(
    tmp_path, monkeypatch
):
    output, _, entities = configure(
        tmp_path, monkeypatch, [({"data": [native("btc"), native("eth")]}, 200)]
    )
    hashes = iter([{"source": "before"}, {"source": "after"}])
    monkeypatch.setattr(proof, "code_hashes", lambda: next(hashes))
    with pytest.raises(IntegrityError, match="COINMETRICS_PROOF_EXECUTED_CODE_CHANGED"):
        await proof.verify(output, transport_provenance="explicit_mock_fixture")
    assert not (output / "report.json").exists()
    assert_native_entities(output, entities)
    rows = proof.closed_rows(output / "events.duckdb", output / "events.parquet")
    assert len(rows) == 4
    assert (output / "events.parquet.manifest.json").exists()
    assert proof.closed_rows(output / "restored/events.duckdb") == rows


@pytest.mark.parametrize(
    "case", ["existing", "outside", "wrong-evidence-name", "invalid-provenance"]
)
async def test_preflight_refuses_before_transport_or_store(tmp_path, monkeypatch, case):
    output, requests, _ = configure(tmp_path, monkeypatch, [])
    provenance = "explicit_mock_fixture"
    expected = ValueError
    if case == "existing":
        output.mkdir(parents=True)
        (output / "original.txt").write_text("preserve")
        expected = FileExistsError
    elif case == "outside":
        output = tmp_path / "outside"
    elif case == "wrong-evidence-name":
        output = proof.ROOT / "evidence/wrong-name"
    else:
        provenance = "unlabelled"
    with pytest.raises(expected):
        await proof.verify(output, transport_provenance=provenance)
    assert requests == []
    assert not (output / "events.duckdb").exists()
    if case == "existing":
        assert (output / "original.txt").read_text() == "preserve"
    else:
        assert not output.exists()


async def test_output_symlink_component_refused_before_any_request(tmp_path, monkeypatch):
    output, requests, _ = configure(tmp_path, monkeypatch, [])
    destination = tmp_path / "target"
    destination.mkdir()
    output.parent.parent.mkdir(parents=True)
    try:
        os.symlink(destination, output.parent, target_is_directory=True)
    except OSError as exc:
        if os.name != "nt":
            pytest.skip(f"host cannot create symbolic links: {exc}")
        # Windows junctions are reparse points too and do not require the
        # symbolic-link privilege. Both targets stay inside this test's temp tree.
        created = await asyncio.to_thread(
            subprocess.run,
            ["cmd", "/c", "mklink", "/J", str(output.parent), str(destination)],
            capture_output=True,
            text=True,
            check=False,
        )
        if created.returncode:
            pytest.skip(f"host cannot create symlink or junction: {created.stderr}")
    with pytest.raises((IntegrityError, ValueError), match="(?i)reparse|symlink|symbolic|junction"):
        await proof.verify(output, transport_provenance="explicit_mock_fixture")
    assert requests == [] and list(destination.iterdir()) == []
