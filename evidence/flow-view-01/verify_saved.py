"""Offline saved-public projection and explicitly synthetic A -> B -> A archive proof."""

import asyncio
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.archive import restore_archive, seal_archive
from selective_options.binance import Instrument
from selective_options.binance_accounts import account_events
from selective_options.binance_taker import taker_events
from selective_options.events import MarketEvent
from selective_options.flow_view import flow_at
from selective_options.store import DuckStore

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path, kind=None):
    with duckdb.connect(str(path), read_only=True) as db:
        result = db.execute("SELECT * FROM events ORDER BY event_id")
        columns = [c[0] for c in result.description]
        return [
            event
            for values in result.fetchall()
            if (event := MarketEvent.model_validate(dict(zip(columns, values, strict=True))))
            and (kind is None or event.kind == kind)
        ]


def select(rows, kind, asset, clock):
    return flow_at(rows, kind=kind, asset=asset, symbol=asset + "USDT", as_of_ms=clock)


def synthetic(saved, receipt, changed):
    body = saved.payload()
    instrument = Instrument.model_validate(body["instrument"])
    if saved.kind == "taker_flow":
        raw = {
            "timestamp": body["period_start_ms"],
            "buyVol": str(Decimal(body["buy_volume"]) + Decimal("0.01"))
            if changed
            else body["buy_volume"],
            "sellVol": body["sell_volume"],
            "buySellRatio": body["buy_sell_ratio"],
        }
        return taker_events(instrument, [raw], receipt, receipt)[0]
    raw = {
        "symbol": saved.symbol,
        "timestamp": body["period_end_ms"],
        "longAccount": body["reported_longAccount"],
        "shortAccount": body["reported_shortAccount"],
        "longShortRatio": str(Decimal(body["reported_longShortRatio"]) + Decimal("0.01"))
        if changed
        else body["reported_longShortRatio"],
    }
    return account_events(instrument, [raw], receipt, receipt)[0]


async def verify():
    public = []
    fixtures = []
    hashes = {}
    for kind, directory in [
        ("taker_flow", "binance-taker-public-02"),
        ("account_ratio", "binance-accounts-public-01"),
    ]:
        path = ROOT.parent / directory / "events.duckdb"
        hashes[str(path.relative_to(ROOT.parent))] = digest(path)
        rows = load(path, kind)
        assert len(rows) == 12
        for asset in ["BTC", "ETH", "SOL", "PAXG"]:
            scoped = [r for r in rows if r.asset == asset]
            receipt = max(r.observed_at_ms for r in scoped)
            expected = tuple(sorted(scoped, key=lambda r: r.event_time_ms))
            assert len(expected) == 3
            assert select(rows, kind, asset, receipt) == expected
            assert select(rows, kind, asset, min(r.observed_at_ms for r in scoped) - 1) == ()
            public.append(
                {"kind": kind, "asset": asset, "periods": 3, "actual_receipt_ms": receipt}
            )
        saved = max((r for r in rows if r.asset == "BTC"), key=lambda r: r.event_time_ms)
        fixtures.extend(
            synthetic(saved, saved.observed_at_ms + offset, changed)
            for offset, changed in [(0, False), (1000, True), (2000, False)]
        )
    assert len({r.event_id for r in fixtures}) == 6
    store = await DuckStore.open(ROOT / "synthetic.duckdb")
    try:
        await store.append(fixtures + fixtures)
        assert await store.pending_delivery_count() == 6
        count = await store.export_parquet(ROOT / "synthetic.parquet")
        assert count == 6
        seal = await seal_archive(ROOT / "synthetic.parquet", count)
    finally:
        await store.close()
    restored = await restore_archive(ROOT / "synthetic.parquet", ROOT / "restored", batch_size=2)
    original = load(ROOT / "synthetic.duckdb")
    replay = load(ROOT / "restored" / "events.duckdb")
    assert original == replay == sorted(fixtures, key=lambda r: r.event_id)
    checks = []
    for kind in ["taker_flow", "account_ratio"]:
        ordered = sorted((r for r in fixtures if r.kind == kind), key=lambda r: r.observed_at_ms)
        assert select(replay, kind, "BTC", ordered[0].observed_at_ms - 1) == ()
        for expected in ordered:
            selected = select(replay, kind, "BTC", expected.observed_at_ms)
            assert selected == (expected,)
            # Independent SQL selection checks storage clocks and receipt ordering.
            with duckdb.connect(str(ROOT / "restored" / "events.duckdb"), read_only=True) as db:
                assert db.execute(
                    "SELECT event_id FROM events WHERE kind=? AND asset='BTC' "
                    "AND event_time_ms<=? AND observed_at_ms<=? ORDER BY observed_at_ms DESC LIMIT 1",
                    [kind, expected.observed_at_ms, expected.observed_at_ms],
                ).fetchone() == (expected.event_id,)
            checks.append(
                {
                    "kind": kind,
                    "synthetic_as_of_ms": expected.observed_at_ms,
                    "selected_id": expected.event_id,
                }
            )
    assert all(digest(ROOT.parent / path) == value for path, value in hashes.items())
    report = {
        "version": __version__,
        "status": "OFFLINE_FLOW_REVISION_AND_ARCHIVE_VERIFIED",
        "network_requests": 0,
        "saved_public_rows": 24,
        "saved_public_projection_checks": public,
        "saved_public_database_sha256_unchanged": hashes,
        "synthetic_scope": "A -> B -> A values and repeated exact poll; all changed receipts/revisions are synthetic, not public observations",
        "synthetic_rows": 6,
        "synthetic_revision_checks": checks,
        "synthetic_pre_receipt_checks": 2,
        "exact_poll_replay_deduplicated": True,
        "archive_exact_match": True,
        "synthetic_parquet_sha256": seal.sha256,
        "restore": restored,
        "legacy_limitation": "Pre-0.3.22 rows omit repeated poll receipts; historical reversions cannot be reconstructed",
        "continuous_history_verified": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }
    (ROOT / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "saved_public_rows": 24, "synthetic_rows": 6}))


if __name__ == "__main__":
    asyncio.run(verify())
