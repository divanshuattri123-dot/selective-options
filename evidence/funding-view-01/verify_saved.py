"""Offline public archive projection plus clearly synthetic funding revisions/restore."""

import asyncio
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.archive import restore_archive, seal_archive
from selective_options.binance import Instrument
from selective_options.binance_funding_history import parse_page
from selective_options.binance_funding_view import funding_history_at
from selective_options.events import MarketEvent
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parent / "binance-funding-public-01"


def load(path):
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [c[0] for c in result.description]
        return [
            MarketEvent.model_validate(dict(zip(columns, r, strict=True)))
            for r in result.fetchall()
        ]


async def verify():
    source_path = SOURCE / "events.duckdb"
    source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
    public = load(source_path)
    report = json.loads((SOURCE / "report.json").read_text())
    policy = {
        "asset": "BTC",
        "symbol": "BTCUSDT",
        "start_ms": report["start_ms"],
        "end_ms": report["end_ms"],
    }
    receipts = sorted({row.observed_at_ms for row in public})
    assert len(public) == 6 and len(receipts) == 3
    assert funding_history_at(public, as_of_ms=receipts[0] - 1, **policy) == ()
    actual_checks = []
    for clock in receipts:
        expected = tuple(
            sorted((r for r in public if r.observed_at_ms <= clock), key=lambda r: r.event_time_ms)
        )
        assert funding_history_at(public, as_of_ms=clock, **policy) == expected
        actual_checks.append(
            {"original_receipt_ms": clock, "selected_public_charges": len(expected)}
        )
    saved = min(public, key=lambda r: r.event_time_ms)
    body = saved.payload()
    instrument = Instrument.model_validate(body["instrument"])
    base = receipts[-1] + 1000
    synthetic = []
    for offset, changed in [(0, False), (1000, True), (2000, False), (3000, False)]:
        raw = {
            "symbol": saved.symbol,
            "fundingTime": saved.event_time_ms,
            "fundingRate": format(Decimal(body["funding_rate"]) + Decimal("0.00001"), "f")
            if changed
            else body["funding_rate"],
            "markPrice": body["mark_price_at_charge"],
            "rateType": body["reported_rate_type"],
        }
        synthetic.extend(
            parse_page(
                instrument, [raw], base + offset, saved.event_time_ms, saved.event_time_ms, 1
            )
        )
    store = await DuckStore.open(ROOT / "mixed-fixture.duckdb")
    try:
        await store.append(public + synthetic + public + synthetic)
        assert await store.pending_delivery_count() == 10
        count = await store.export_parquet(ROOT / "mixed-fixture.parquet")
        assert count == 10
        seal = await seal_archive(ROOT / "mixed-fixture.parquet", count)
    finally:
        await store.close()
    restore = await restore_archive(ROOT / "mixed-fixture.parquet", ROOT / "restored", batch_size=3)
    restored = load(ROOT / "restored/events.duckdb")
    assert (
        restored
        == load(ROOT / "mixed-fixture.duckdb")
        == sorted(public + synthetic, key=lambda r: r.event_id)
    )
    assert funding_history_at(restored, as_of_ms=base - 1, **policy) == tuple(
        sorted(public, key=lambda r: r.event_time_ms)
    )
    simulated_checks = []
    for expected in synthetic:
        clock = expected.observed_at_ms
        selected = funding_history_at(restored, as_of_ms=clock, **policy)
        assert len(selected) == 6 and selected[0] == expected
        with duckdb.connect(str(ROOT / "restored/events.duckdb"), read_only=True) as db:
            sql = db.execute(
                "SELECT event_id FROM events WHERE source='binance_history' AND environment='public_reference' "
                "AND asset='BTC' AND symbol='BTCUSDT' AND kind='funding' AND event_time_ms BETWEEN ? AND ? "
                "AND event_time_ms<=? AND observed_at_ms<=? "
                "QUALIFY row_number() OVER (PARTITION BY event_time_ms ORDER BY observed_at_ms DESC)=1 ORDER BY event_time_ms",
                [policy["start_ms"], policy["end_ms"], clock, clock],
            ).fetchall()
        assert [r[0] for r in sql] == [r.event_id for r in selected]
        simulated_checks.append(
            {
                "synthetic_as_of_ms": clock,
                "selected_charges": 6,
                "revised_charge_event_id": expected.event_id,
            }
        )
    assert hashlib.sha256(source_path.read_bytes()).hexdigest() == source_hash
    proof = {
        "version": __version__,
        "status": "OFFLINE_FUNDING_REVISION_AND_RESTORE_VERIFIED",
        "network_requests": 0,
        "original_public_rows": 6,
        "original_public_receipt_checks": actual_checks,
        "original_public_database_sha256_unchanged": source_hash,
        "synthetic_rows": 4,
        "synthetic_event_ids": [r.event_id for r in synthetic],
        "synthetic_scope": "A -> B -> A and unchanged later poll; rates/receipts changed only as fixtures, not actual venue revisions",
        "synthetic_revision_checks": simulated_checks,
        "restored_rows": 10,
        "exact_poll_replay_deduplicated": True,
        "parquet_exact_match": True,
        "parquet_sha256": seal.sha256,
        "restore": restore,
        "historical_schedule_completeness": "NOT_PROVEN",
        "rate_aggregation_implemented": False,
        "historical_intervals_available": False,
        "finality_verified": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "report.json").write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": proof["status"],
                "original_public_rows": 6,
                "synthetic_rows": 4,
                "selected_charges": 6,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(verify())
