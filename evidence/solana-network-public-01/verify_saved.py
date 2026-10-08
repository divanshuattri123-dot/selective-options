"""Independent saved-public response checks and explicitly synthetic outage replay; zero HTTP."""

import asyncio
import hashlib
import json
from pathlib import Path

import duckdb

from selective_options import solana_network
from selective_options.archive import seal_archive
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.solana_quality import network_quality
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parent


def read_rows(path):
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events")
        columns = [c[0] for c in result.description]
        rows = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        outbox = db.execute(
            "SELECT count(*) FROM delivery_outbox WHERE delivered = FALSE"
        ).fetchone()[0]
    return rows, outbox


async def verify():
    if (ROOT / "fixture.duckdb").exists() or (ROOT / "fixture.parquet").exists():
        raise FileExistsError("Use a new sibling folder for saved-response fixture reruns")
    original_hash = hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest()
    rows, outbox = read_rows(ROOT / "events.duckdb")
    report = json.loads((ROOT / "report.json").read_text())
    saved = json.loads((ROOT / "results.json").read_text())
    assert len(saved) == 4 and len(rows) == outbox == 3
    by_method = {row["method"]: row for row in saved}
    assert by_method["getGenesisHash"]["validated_result"] == solana_network.GENESIS
    perf = next(row for row in rows if row.symbol == "SOL_PERFORMANCE")
    stake = next(row for row in rows if row.symbol == "SOL_VOTE_STAKE")
    health = next(row for row in rows if row.kind == "feed_health")
    sample = by_method["getRecentPerformanceSamples"]["validated_result"][0]
    p, s = perf.payload(), stake.payload()
    assert p["sample"] == {key: sample.get(key) for key in p["sample"]}
    assert perf.event_time_ms == by_method["getBlockTime"]["validated_result"] * 1000
    assert perf.observed_at_ms == by_method["getBlockTime"]["observed_at_ms"]
    assert (
        p["sample_response_received_at_ms"]
        == by_method["getRecentPerformanceSamples"]["observed_at_ms"]
    )
    assert (
        p["genesis_observed_at_ms"]
        == s["genesis_observed_at_ms"]
        == by_method["getGenesisHash"]["observed_at_ms"]
    )
    assert (
        stake.event_time_ms
        == stake.observed_at_ms
        == by_method["getVoteAccounts"]["observed_at_ms"]
    )
    assert p["transactions_per_second_numerator"] == str(sample["numTransactions"])
    assert p["transactions_per_second_denominator"] == sample["samplePeriodSecs"]
    votes = by_method["getVoteAccounts"]["validated_result"]
    for partition in ("current", "delinquent"):
        canonical = s["vote_accounts"][partition]
        assert len(canonical) == len(votes[partition])
        for raw, accepted in zip(votes[partition], canonical, strict=True):
            assert accepted == {key: raw[key] for key in accepted}
        assert s[partition + "_activated_stake_lamports"] == str(
            sum(row["activatedStake"] for row in votes[partition])
        )
        assert s[partition + "_vote_accounts"] == len(votes[partition])
    assert s["total_visible_activated_stake_lamports"] == str(
        sum(row["activatedStake"] for part in votes.values() for row in part)
    )
    assert set(health.payload()["event_ids"]) == {perf.event_id, stake.event_id}
    at = health.observed_at_ms
    original_quality = network_quality(rows, at)
    assert original_quality == report["quality"]
    with duckdb.connect(str(ROOT / "events.duckdb"), read_only=True) as db:
        for row in rows:
            visible = max(row.event_time_ms, row.observed_at_ms)
            for cutoff, expected in ((visible - 1, 0), (visible, 1)):
                count = db.execute(
                    "SELECT count(*) FROM events WHERE event_id = ? AND event_time_ms <= ? AND observed_at_ms <= ?",
                    [row.event_id, cutoff, cutoff],
                ).fetchone()[0]
                assert count == expected
    with duckdb.connect() as db:
        result = db.execute("SELECT * FROM read_parquet(?)", [str(ROOT / "events.parquet")])
        columns = [c[0] for c in result.description]
        archive = tuple(
            MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in result.fetchall()
        )
        assert set(rows) == set(archive)
    fixture = await DuckStore.open(ROOT / "fixture.duckdb")
    assert not (await fixture.snapshot("SOL", at + 1, "public_reference")).rows(), (
        "Use a new sibling folder for reruns"
    )
    emitted = []

    async def sink(event):
        await fixture.append([event])
        emitted.append(event)

    collector = solana_network.SolanaNetworkCollector(AppConfig(), sink)
    iterator = iter(saved)

    async def request(session, method, slot=None):
        entry = next(iterator)
        assert entry["method"] == method and entry["slot_argument"] == slot
        return entry["validated_result"], entry["observed_at_ms"]

    collector.request = request
    clock = solana_network.now_ms
    try:
        solana_network.now_ms = lambda: at
        assert await collector.collect(None)
        assert set(emitted) == set(rows)
        await fixture.append(emitted)

        async def refused(*args):
            raise FeedUnavailable("RATE_LIMITED", 429)

        collector.request = refused
        solana_network.now_ms = lambda: at + 1000
        assert not await collector.collect(None)
        before = (await fixture.snapshot("SOL", at, "public_reference")).rows()
        after = (await fixture.snapshot("SOL", at + 1000, "public_reference")).rows()
        assert network_quality(before, at) == original_quality
        assert network_quality(after, at + 1000)["blocker"] == "SOL_FEED_UNAVAILABLE"
        assert len(after) == 4
        count = await fixture.export_parquet(ROOT / "fixture.parquet")
        sealed = await seal_archive(ROOT / "fixture.parquet", count)
    finally:
        solana_network.now_ms = clock
        await fixture.close()
    restored, pending = read_rows(ROOT / "fixture.duckdb")
    assert set(restored) == set(after) and pending == 4
    assert hashlib.sha256((ROOT / "events.duckdb").read_bytes()).hexdigest() == original_hash
    proof = {
        "status": "SAVED_RESPONSE_AND_LABELLED_OUTAGE_REPLAY_VERIFIED",
        "new_public_requests": 0,
        "original_rows": 3,
        "original_pending_outbox_rows": outbox,
        "sql_pit_boundaries": 6,
        "raw_values_and_actual_receipts_match": True,
        "sealed_archive_exact_match": True,
        "saved_public_runtime_replay_exact": True,
        "synthetic_outage_fixture_rows": 4,
        "synthetic_outage_visible": True,
        "earlier_view_unchanged": True,
        "fixture_closed_reload_match": True,
        "fixture_parquet_sha256": sealed.sha256,
        "source_database_sha256": original_hash,
        "source_database_unchanged": True,
        "current_vote_accounts": len(votes["current"]),
        "delinquent_vote_accounts": len(votes["delinquent"]),
        "total_visible_activated_stake_lamports": s["total_visible_activated_stake_lamports"],
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "independent-proof.json").write_text(
        json.dumps(proof, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(proof, indent=2))


if __name__ == "__main__":
    asyncio.run(verify())
