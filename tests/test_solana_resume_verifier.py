"""Verifier regressions use labelled local fixtures; no live SOL acceptance."""

import importlib
import json
from pathlib import Path

import duckdb
import pytest

from selective_options.store import DuckStore
from tests.test_solana_network import GENESIS, health, pair, responses

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def verifier(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "scripts"))
    return importlib.import_module("verify_solana_resume")


async def source_fixture(path, acknowledged=False):
    path.mkdir()
    performance, stake = pair()
    rows = (performance, stake, health((performance, stake)))
    store = await DuckStore.open(path / "events.duckdb")
    try:
        await store.append(list(rows))
        if acknowledged:
            await store.acknowledge_delivery([row.event_id for row in rows])
        await store.export_parquet(path / "events.parquet")
    finally:
        await store.close()
    saved = [
        {
            "method": method,
            "slot_argument": 1000 if method == "getBlockTime" else None,
            "observed_at_ms": receipt,
            "validated_result": value,
        }
        for method, (value, receipt) in zip(
            ("getGenesisHash", "getRecentPerformanceSamples", "getBlockTime", "getVoteAccounts"),
            responses(),
            strict=True,
        )
    ]
    (path / "results.json").write_text(json.dumps(saved), encoding="utf-8")
    (path / "README.md").write_text("Synthetic native-shaped test fixture.\n", encoding="utf-8")
    return rows


@pytest.mark.parametrize("acknowledged", [False, True])
async def test_full_offline_verifier_checks_real_capture_seed_and_pending_restore(
    tmp_path, verifier, acknowledged
):
    source = tmp_path / "source"
    rows = await source_fixture(source, acknowledged)
    before = verifier.tree_hashes(source)
    output = tmp_path / "proof"
    report = await verifier.verify(output, source)
    assert report == json.loads((output / "verification.json").read_text())
    assert report["status"] == "SAVED_NATIVE_SOL_RESUME_VERIFIED"
    assert report["source_pending_outbox_rows"] == (0 if acknowledged else 3)
    assert (
        report["destination_pending_outbox_rows"]
        == report["archive_restore_pending_outbox_rows"]
        == 3
    )
    assert report["sql_pit_boundary_checks"] == 12
    assert report["startup_order"][0] == "closed_destination_revalidated_with_exact_pending_outbox"
    assert report["startup_order"][1] == "mocked_sol_constructed"
    assert report["network_attempts"] == report["new_public_requests"] == 0
    assert report["source_tree_unchanged"] and verifier.tree_hashes(source) == before
    assert report["synthetic_cases"]["refusals"]["later_outage"] == "SOL_FEED_UNAVAILABLE"
    assert "UNBOUND" in report["synthetic_cases"]["refusals"]["unbound_prefix"]
    for scope in (
        "complete_history_copied",
        "transport_resumed_by_copy",
        "acknowledgements_restored",
        "seven_day_acceptance",
        "milestone_2_complete",
        "trading_enabled",
    ):
        assert report[scope] is False
    copied, pending = verifier.read_database(output / "capture/events.duckdb")
    assert copied == rows and pending == {row.event_id: False for row in rows}
    previous = verifier.tree_hashes(output)
    with pytest.raises(FileExistsError):
        await verifier.verify(output, source)
    assert verifier.tree_hashes(output) == previous


@pytest.mark.parametrize("case", ["quantity", "receipt", "genesis", "vote_member", "vote_stake"])
async def test_independent_saved_response_audit_refuses_altered_raw_evidence(
    tmp_path, verifier, case
):
    source = tmp_path / "source"
    rows = await source_fixture(source)
    path = source / "results.json"
    saved = json.loads(path.read_text())
    if case == "quantity":
        saved[1]["validated_result"][0]["numTransactions"] += 1
    if case == "receipt":
        saved[2]["observed_at_ms"] += 1
    if case == "genesis":
        saved[0]["validated_result"] = GENESIS + "x"
    if case == "vote_member":
        saved[3]["validated_result"]["current"][0]["nodePubkey"] = "2" * 32
    if case == "vote_stake":
        saved[3]["validated_result"]["delinquent"][0]["activatedStake"] += 1
    path.write_text(json.dumps(saved), encoding="utf-8")
    with pytest.raises(AssertionError):
        verifier.check_native(source, rows)


@pytest.mark.parametrize("case", ["identity", "payload_bytes", "source", "symbol"])
async def test_independent_native_audit_refuses_wrong_event_identity_or_exact_bytes(
    tmp_path, verifier, case
):
    source = tmp_path / "source"
    rows = await source_fixture(source)
    updates = {
        "identity": {"event_id": "0" * 64},
        "payload_bytes": {"payload_json": json.dumps(rows[0].payload(), indent=2)},
        "source": {"source": "other"},
        "symbol": {"symbol": "SOL_WRONG"},
    }
    changed = (rows[0].model_copy(update=updates[case]), *rows[1:])
    with pytest.raises((AssertionError, StopIteration)):
        verifier.check_native(source, changed)


async def test_failed_destination_parquet_audit_never_reports_success(
    tmp_path, verifier, monkeypatch
):
    source = tmp_path / "source"
    await source_fixture(source)
    original = verifier.compare_parquet

    def fail_destination(path, expected):
        if path.parent.name == "capture":
            raise OSError("synthetic destination read failure")
        original(path, expected)

    monkeypatch.setattr(verifier, "compare_parquet", fail_destination)
    output = tmp_path / "proof"
    with pytest.raises(OSError, match="synthetic destination read failure"):
        await verifier.verify(output, source)
    result = json.loads((output / "verification.json").read_text())
    assert result["status"] == "FAILED" and result["failure_type"] == "OSError"
    assert "archive_restore_exact" not in result


async def test_verifier_outbox_audit_refuses_missing_or_extra_members(tmp_path, verifier):
    source = tmp_path / "source"
    rows = await source_fixture(source)
    with duckdb.connect(str(source / "events.duckdb")) as db:
        db.execute("DELETE FROM delivery_outbox WHERE event_id=?", [rows[0].event_id])
    with pytest.raises(AssertionError):
        verifier.read_database(source / "events.duckdb")


async def test_verifier_rejects_output_within_historical_tree(tmp_path, verifier):
    source = tmp_path / "source"
    await source_fixture(source)
    before = verifier.tree_hashes(source)
    with pytest.raises(ValueError, match="separate trees"):
        await verifier.verify(source / "unsafe", source)
    assert verifier.tree_hashes(source) == before
