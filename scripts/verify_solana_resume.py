"""Offline saved-native SOL seed/capture proof; zero new public or private requests."""

import argparse
import asyncio
import hashlib
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import duckdb

from selective_options import __version__, ingestion
from selective_options.archive import restore_archive
from selective_options.collectors import DeribitCollector
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.solana_checkpoint import load_solana_resume
from selective_options.solana_network import GENESIS, URL, SolanaNetworkCollector, network_events
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "evidence/solana-network-public-01"
COLUMNS = tuple(MarketEvent.model_fields)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_hashes(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*")) if p.is_file()}


def runtime_hashes() -> dict[str, str]:
    paths = [
        ROOT / "src/selective_options" / name
        for name in (
            "__init__.py",
            "ingestion.py",
            "solana_checkpoint.py",
            "solana_network.py",
            "solana_quality.py",
            "events.py",
            "store.py",
            "archive.py",
        )
    ]
    paths.append(Path(__file__).resolve())
    return {str(path.relative_to(ROOT)): digest(path) for path in paths}


def read_database(path: Path) -> tuple[tuple[MarketEvent, ...], dict[str, bool]]:
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY observed_at_ms,event_time_ms,event_id")
        columns = tuple(c[0] for c in result.description)
        assert columns == COLUMNS, "event column order changed"
        values = result.fetchall()
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, value, strict=True))) for value in values
        )
        assert [tuple(row.model_dump().values()) for row in rows] == values
        outbox = {
            str(key): bool(delivered)
            for key, delivered in db.execute(
                "SELECT event_id,delivered FROM delivery_outbox ORDER BY event_id"
            ).fetchall()
        }
        assert len(outbox) == len(rows) and set(outbox) == {row.event_id for row in rows}
    return rows, outbox


def compare_parquet(path: Path, expected: tuple[MarketEvent, ...]) -> None:
    with duckdb.connect() as db:
        result = db.execute(
            "SELECT * FROM read_parquet(?,hive_partitioning=false) "
            "ORDER BY observed_at_ms,event_time_ms,event_id",
            [str(path)],
        )
        assert tuple(column[0] for column in result.description) == COLUMNS
        assert result.fetchall() == [tuple(row.model_dump().values()) for row in expected]


def check_native(source: Path, rows: tuple[MarketEvent, ...]) -> dict[str, object]:
    """Compare quantities, membership, receipt clocks and identities without producer helpers."""
    saved: list[dict[str, Any]] = json.loads((source / "results.json").read_text(encoding="utf-8"))
    assert len(saved) == 4 and len(rows) == 3
    methods = {entry["method"]: entry for entry in saved}
    assert set(methods) == {
        "getGenesisHash",
        "getRecentPerformanceSamples",
        "getBlockTime",
        "getVoteAccounts",
    }
    assert methods["getGenesisHash"]["validated_result"] == GENESIS
    assert all(type(entry["observed_at_ms"]) is int for entry in saved)
    assert len({entry["observed_at_ms"] for entry in saved}) == 4
    by_symbol = {row.symbol: row for row in rows}
    assert set(by_symbol) == {"SOL_PERFORMANCE", "SOL_VOTE_STAKE", "SOL_NETWORK"}
    performance, stake, health = (
        by_symbol[symbol] for symbol in ("SOL_PERFORMANCE", "SOL_VOTE_STAKE", "SOL_NETWORK")
    )
    for row in rows:
        assert (row.source, row.environment, row.asset) == (
            "solana_public",
            "public_reference",
            "SOL",
        )
        assert row.schema_version == 1
        body = cast(dict[str, Any], row.payload())
        assert row.payload_json == json.dumps(body, sort_keys=True, separators=(",", ":"))
        native = str(body["sample"]["slot"]) if row is performance else None
        identity = json.dumps(
            [
                row.source,
                row.environment,
                row.asset,
                row.symbol,
                row.kind,
                native,
                row.event_time_ms,
                row.payload_json,
            ],
            separators=(",", ":"),
        )
        assert hashlib.sha256(identity.encode()).hexdigest() == row.event_id
    p, s, h = (cast(dict[str, Any], event.payload()) for event in (performance, stake, health))
    sample = methods["getRecentPerformanceSamples"]["validated_result"]
    assert len(sample) == 1
    assert p["sample"] == {key: sample[0].get(key) for key in p["sample"]}
    assert methods["getBlockTime"]["slot_argument"] == sample[0]["slot"]
    assert performance.event_time_ms == methods["getBlockTime"]["validated_result"] * 1000
    assert (
        performance.observed_at_ms
        == p["block_time_response_received_at_ms"]
        == methods["getBlockTime"]["observed_at_ms"]
    )
    assert (
        p["sample_response_received_at_ms"]
        == methods["getRecentPerformanceSamples"]["observed_at_ms"]
    )
    assert (
        p["genesis_observed_at_ms"]
        == s["genesis_observed_at_ms"]
        == methods["getGenesisHash"]["observed_at_ms"]
    )
    assert (
        stake.event_time_ms
        == stake.observed_at_ms
        == s["snapshot_received_at_ms"]
        == methods["getVoteAccounts"]["observed_at_ms"]
    )
    assert performance.timestamp_kind == "source" and stake.timestamp_kind == "receipt"
    assert p["transactions_per_second_numerator"] == str(sample[0]["numTransactions"])
    assert p["transactions_per_second_denominator"] == sample[0]["samplePeriodSecs"]
    assert p["non_vote_transactions_per_second_numerator"] == (
        str(sample[0]["numNonVoteTransactions"])
        if sample[0].get("numNonVoteTransactions") is not None
        else None
    )
    votes = methods["getVoteAccounts"]["validated_result"]
    canonical = s["vote_accounts"]
    for partition in ("current", "delinquent"):
        accepted = canonical[partition]
        assert len(accepted) == len(votes[partition])
        for raw, record in zip(votes[partition], accepted, strict=True):
            assert record == {key: raw[key] for key in record}
        assert s[partition + "_activated_stake_lamports"] == str(
            sum(record["activatedStake"] for record in votes[partition])
        )
        assert s[partition + "_vote_accounts"] == len(votes[partition])
    assert s["total_visible_activated_stake_lamports"] == str(
        sum(record["activatedStake"] for partition in votes.values() for record in partition)
    )
    assert p["source_url"] == s["source_url"] == URL
    assert p["genesis_hash"] == s["genesis_hash"] == GENESIS
    assert p["trading_approval"] is s["trading_approval"] is False
    assert h["ok"] is True and h["event_ids"] == [performance.event_id, stake.event_id]
    assert health.event_time_ms == health.observed_at_ms >= stake.observed_at_ms
    assert health.timestamp_kind == "receipt"
    return {
        "current_vote_accounts": len(votes["current"]),
        "delinquent_vote_accounts": len(votes["delinquent"]),
        "total_visible_activated_stake_lamports": s["total_visible_activated_stake_lamports"],
        "raw_membership_quantities_receipts_and_identities_match": True,
    }


def pit_boundaries(path: Path, rows: tuple[MarketEvent, ...]) -> int:
    checks = 0
    with duckdb.connect(str(path), read_only=True) as db:
        for row in rows:
            for boundary in (row.event_time_ms, row.observed_at_ms):
                for cutoff in (boundary - 1, boundary):
                    expected = int(row.event_time_ms <= cutoff and row.observed_at_ms <= cutoff)
                    result = db.execute(
                        "SELECT count(*) FROM events WHERE event_id=? "
                        "AND event_time_ms<=? AND observed_at_ms<=?",
                        [row.event_id, cutoff, cutoff],
                    ).fetchone()
                    assert result == (expected,)
                    checks += 1
    return checks


async def write_fixture(path: Path, rows: tuple[MarketEvent, ...]) -> None:
    store = await DuckStore.open(path)
    try:
        await store.append(list(rows), exact=True)
    finally:
        await store.close()


def synthetic_health(at: int) -> MarketEvent:
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
            "ok": False,
            "reason": "RATE_LIMITED",
            "code": 429,
            "error_type": None,
            "event_ids": [],
            "scope": "performance and vote-account stake; sink accepted, no atomic commit claim",
        },
    )


async def verify(output: Path, source: Path = SOURCE) -> dict[str, object]:
    output = await complete_thread(output.resolve)
    source = await complete_thread(source.resolve)
    if output == source or source in output.parents or output in source.parents:
        raise ValueError("proof output and historical source must be separate trees")
    source_code = await complete_thread(runtime_hashes)
    before = await complete_thread(tree_hashes, source)
    rows, source_outbox = await complete_thread(read_database, source / "events.duckdb")
    native = await complete_thread(check_native, source, rows)
    await complete_thread(compare_parquet, source / "events.parquet", rows)
    health = next(row for row in rows if row.symbol == "SOL_NETWORK")
    at = health.observed_at_ms
    assert await load_solana_resume(source / "events.duckdb", at) == rows
    await complete_thread(output.mkdir, parents=True, exist_ok=False)
    report: dict[str, object] = {
        "status": "FAILED",
        "version": __version__,
        "scope": "offline finite saved-native seed",
        "new_public_requests": 0,
        "new_private_requests": 0,
        "orders_submitted": 0,
        "trading_enabled": False,
        "milestone_2_complete": False,
        "complete_history_copied": False,
        "transport_resumed_by_copy": False,
        "acknowledgements_restored": False,
        "seven_day_acceptance": False,
        "source_database_sha256": before["events.duckdb"],
        "source_files_sha256": before,
        "runtime_source_sha256": source_code,
        "selection_as_of_ms": at,
        "seed_event_ids": [row.event_id for row in rows],
        "seed_payload_sha256": {
            row.event_id: hashlib.sha256(row.payload_json.encode()).hexdigest() for row in rows
        },
        **native,
    }
    network_attempts: list[str] = []
    order: list[str] = []
    destination_revalidated = False
    real_loader = load_solana_resume
    real_init = SolanaNetworkCollector.__init__

    def deny_network(*args: object, **kwargs: object) -> None:
        network_attempts.append("transport_attempt")
        raise AssertionError("offline verifier attempted a network operation")

    async def idle(*args: object, **kwargs: object) -> None:
        assert destination_revalidated, "producer started before closed seed revalidation"
        order.append("mocked_producer_started")
        await asyncio.Event().wait()

    async def observed_loader(database: Path, clock: int, **policy: int) -> tuple[MarketEvent, ...]:
        nonlocal destination_revalidated
        selected = await real_loader(database, clock, **policy)
        if await complete_thread(database.resolve) == output / "capture/events.duckdb":
            copied, outbox = await complete_thread(read_database, database)
            assert copied == selected == rows
            assert outbox == {row.event_id: False for row in rows}
            destination_revalidated = True
            order.append("closed_destination_revalidated_with_exact_pending_outbox")
        return selected

    def observed_init(self: SolanaNetworkCollector, *args: Any, **kwargs: Any) -> None:
        assert destination_revalidated, "SOL constructed before closed seed revalidation"
        order.append("mocked_sol_constructed")
        real_init(self, *args, **kwargs)

    config = AppConfig.model_validate(
        {
            "system": {"assets": ["SOL"]},
            "ingestion": {
                "solana_network_enabled": True,
                "reference_sources": [],
                "derivatives_enabled": False,
                "spot_trades_enabled": False,
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    try:
        with ExitStack() as stack:
            stack.enter_context(patch("socket.getaddrinfo", deny_network))
            stack.enter_context(patch("socket.socket.connect", deny_network))
            stack.enter_context(patch.object(ingestion, "now_ms", lambda: at))
            stack.enter_context(patch.object(ingestion, "load_solana_resume", observed_loader))
            stack.enter_context(patch.object(DeribitCollector, "run", idle))
            stack.enter_context(patch.object(ingestion, "poll_chains", idle))
            stack.enter_context(patch.object(SolanaNetworkCollector, "__init__", observed_init))
            stack.enter_context(patch.object(SolanaNetworkCollector, "run", idle))
            captured = await ingestion.capture(
                config, output / "capture", 0.05, resume_solana_from=source / "events.duckdb"
            )
            assert captured["status"] == "CAPTURE_FINISHED"
            assert captured["trading_enabled"] is captured["milestone_2_complete"] is False
            assert destination_revalidated and len(order) == 5
            destination, pending = await complete_thread(
                read_database, output / "capture/events.duckdb"
            )
            assert destination == rows and pending == {row.event_id: False for row in rows}
            await complete_thread(compare_parquet, output / "capture/events.parquet", rows)
            restored = await restore_archive(output / "capture/events.parquet", output / "restored")
            restored_rows, restored_outbox = await complete_thread(
                read_database, output / "restored/events.duckdb"
            )
            assert restored_rows == rows and restored_outbox == pending
            assert restored["redis_delivery_pending"] == 3
            assert await load_solana_resume(output / "restored/events.duckdb", at) == rows
            refused: dict[str, str] = {}
            later = at + 1000
            outage = output / "synthetic-later-outage.duckdb"
            await write_fixture(outage, (*rows, synthetic_health(later)))
            assert await load_solana_resume(outage, at) == rows
            # Only the first canonical row of a new synthetic poll reaches the sink.
            performance = next(row for row in rows if row.symbol == "SOL_PERFORMANCE")
            stake = next(row for row in rows if row.symbol == "SOL_VOTE_STAKE")
            p, s = performance.payload(), stake.payload()
            receipts = tuple(
                cast(int, value) + 1000
                for value in (
                    p["genesis_observed_at_ms"],
                    p["sample_response_received_at_ms"],
                    p["block_time_response_received_at_ms"],
                    s["snapshot_received_at_ms"],
                )
            )
            prefix, _ = network_events(
                [p["sample"]],
                p["block_time_seconds"],
                s["vote_accounts"],
                (receipts[0], receipts[1], receipts[2], receipts[3]),
                780_000,
            )
            prefix_path = output / "synthetic-unbound-prefix.duckdb"
            await write_fixture(prefix_path, (*rows, prefix))
            for label, fixture in (("later_outage", outage), ("unbound_prefix", prefix_path)):
                stack.enter_context(patch.object(ingestion, "now_ms", lambda: later))
                start_count = len(order)
                blocked_output = output / ("refused-" + label)
                try:
                    await ingestion.capture(
                        config, blocked_output, 0.05, resume_solana_from=fixture
                    )
                except IntegrityError as exc:
                    refused[label] = str(exc)
                else:
                    raise AssertionError("invalid synthetic seed was accepted")
                assert not blocked_output.exists() and len(order) == start_count
            assert refused["later_outage"] == "SOL_FEED_UNAVAILABLE"
        assert not network_attempts
        assert await complete_thread(tree_hashes, source) == before
        assert await complete_thread(runtime_hashes) == source_code
        report.update(
            status="SAVED_NATIVE_SOL_RESUME_VERIFIED",
            copied_events=3,
            source_pending_outbox_rows=sum(not delivered for delivered in source_outbox.values()),
            destination_pending_outbox_rows=3,
            archive_restore_pending_outbox_rows=3,
            exact_sql_and_parquet_rows=True,
            closed_reload_verified=True,
            archive_restore_exact=True,
            source_tree_unchanged=True,
            sql_pit_boundary_checks=await complete_thread(
                pit_boundaries, source / "events.duckdb", rows
            ),
            startup_order=order,
            network_attempts=len(network_attempts),
            synthetic_cases={
                "provenance": "locally constructed fixtures; no actual source outage asserted",
                "refusals": refused,
                "earlier_saved_native_view_unchanged": True,
            },
            capture_resume=captured["solana_resume"],
        )
    except BaseException as exc:
        report["failure_type"] = type(exc).__name__
        raise
    finally:
        await complete_thread(
            (output / "verification.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=SOURCE)
    arguments = parser.parse_args()
    print(json.dumps(asyncio.run(verify(arguments.output, arguments.source)), indent=2))
