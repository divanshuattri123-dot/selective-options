"""Durable synthetic proof of actual capture supervision after ambiguous sink delivery."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb

from selective_options import ingestion
from selective_options.bus import EventBus
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent
from selective_options.spot_trades import KrakenTrade, SpotTradeCollector, trade_event

ROOT = Path.cwd()
OUTPUT = ROOT / "evidence/spot-sink-failure-fixture-0344"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes():
    return {p.relative_to(ROOT).as_posix(): sha(p) for p in (ROOT / "src").rglob("*.py")}


async def main():
    assert not OUTPUT.exists()
    before = source_hashes()
    received = 1_791_119_400_000
    wires = [
        {
            "symbol": "BTC/USD",
            "side": "buy",
            "price": 100 + i,
            "qty": 0.1,
            "trade_id": i,
            "timestamp": datetime.fromtimestamp((received - 1000 + i) / 1000, UTC).isoformat(),
        }
        for i in (1, 2)
    ]
    sockets = []
    holders = []
    cause = OSError("synthetic downstream failure after one queue publish")

    class Socket:
        def __init__(self):
            self.frames = [
                {
                    "method": "subscribe",
                    "success": True,
                    "req_id": 2,
                    "result": {"channel": "trade", "symbol": "BTC/USD"},
                },
                {"channel": "trade", "type": "snapshot", "data": wires},
            ]

        async def __aenter__(self):
            sockets.append(self)
            return self

        async def __aexit__(self, *args):
            pass

        async def send(self, raw):
            pass

        async def recv(self):
            if self.frames:
                return json.dumps(self.frames.pop(0))
            await asyncio.Event().wait()

    async def discover(self, socket):
        holders.append(self)
        self.symbols = {"BTC/USD": "BTC"}

    original_publish = EventBus.publish

    async def publish(self, event):
        if event.kind == "trade" and event.payload()["trade_id"] == 2:
            raise cause
        await original_publish(self, event)

    async def idle(*args):
        await asyncio.Event().wait()

    config = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
        },
    )
    with (
        patch("selective_options.spot_trades.connect", lambda *a, **k: Socket()),
        patch("selective_options.spot_trades.now_ms", lambda: received),
        patch.object(SpotTradeCollector, "_discover", discover),
        patch.object(EventBus, "publish", publish),
        patch.object(ingestion.DeribitCollector, "run", idle),
        patch.object(ingestion, "poll_chains", idle),
    ):
        try:
            await asyncio.wait_for(ingestion.capture(config, OUTPUT / "capture", 2), 10)
        except IntegrityError as exc:
            assert str(exc) == "SPOT_EMISSION_FAILED" and exc.__cause__ is cause
        else:
            raise AssertionError("failed sink returned capture success")
    report = json.loads((OUTPUT / "capture/report.json").read_text())
    assert report["status"] == "FAILED" and report["failure_type"] == "IntegrityError"
    assert report["milestone_2_complete"] is report["trading_enabled"] is False
    assert len(sockets) == len(holders) == holders[0].connections == 1
    collector = holders[0]
    assert collector.sequences == collector.bars == collector.last_events == {}
    assert collector.awaiting_snapshot == {"BTC/USD"}
    for call in (
        collector._trades({"channel": "trade", "type": "snapshot", "data": wires}, received),
        collector._flush(received + 300000),
        collector.run(),
    ):
        try:
            await call
        except IntegrityError as exc:
            assert str(exc) == "SPOT_EMISSION_OUTCOME_UNKNOWN"
        else:
            raise AssertionError("ambiguous collector reused")
    assert len(sockets) == 1
    path = OUTPUT / "capture/events.duckdb"
    digest = sha(path)
    conn = duckdb.connect(str(path), read_only=True)
    try:
        cursor = conn.execute("SELECT * FROM events ORDER BY event_id")
        names = [item[0] for item in cursor.description]
        rows = [
            MarketEvent.model_validate(dict(zip(names, r, strict=True)), strict=True)
            for r in cursor.fetchall()
        ]
        trades = [r for r in rows if r.kind == "trade"]
        expected = trade_event(KrakenTrade.model_validate(wires[0]), "BTC", received)
        assert trades == [expected] and not any(r.kind == "bar" for r in rows)
        assert any(
            r.payload().get("reason") == "SPOT_TRADE_HALTED"
            and r.payload().get("detail") == "SPOT_EMISSION_FAILED"
            for r in rows
        )
        assert not any(
            r.payload().get("reason") in {"SPOT_TRADE_DISCONNECTED", "TRADE_SNAPSHOT_INITIALIZED"}
            for r in rows
        )
        assert conn.execute(
            "SELECT count(*) FROM delivery_outbox WHERE NOT delivered"
        ).fetchone() == (len(rows),)
        assert conn.execute("SELECT count(*) FROM delivery_outbox").fetchone() == (len(rows),)
        assert (
            conn.execute("SELECT * FROM events ORDER BY event_id").fetchall()
            == conn.execute(
                "SELECT * FROM read_parquet(?,hive_partitioning=false) ORDER BY event_id",
                [str(OUTPUT / "capture/events.parquet")],
            ).fetchall()
        )
        pit = 0
        for row in rows:
            boundary = max(row.event_time_ms, row.observed_at_ms)
            for at, count in ((boundary - 1, 0), (boundary, 1)):
                assert conn.execute(
                    "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                    [row.event_id, at, at],
                ).fetchone() == (count,)
                pit += 1
    finally:
        conn.close()
    assert sha(path) == digest and source_hashes() == before
    result = {
        "status": "SYNTHETIC_CAPTURE_SINK_FAILSTOP_VERIFIED",
        "version": "0.3.44",
        "new_public_requests": 0,
        "synthetic_trade_inputs": 2,
        "delivered_trade_prefix": 1,
        "rejected_successor_trade_id": 2,
        "websocket_connections": 1,
        "collector_state_unchanged": True,
        "collector_reuse_refused": True,
        "capture_status": "FAILED",
        "capture_failure_type": "IntegrityError",
        "error_reason": "SPOT_EMISSION_FAILED",
        "queued_prefix_drained_and_archived": True,
        "rows": len(rows),
        "pending_outbox_rows": len(rows),
        "per_record_sql_pit_checks": pit,
        "database_sha256": digest,
        "tested_source_sha256": before,
        "verifier_sha256": sha(Path(__file__)),
        "milestone_2_complete": False,
        "trading_enabled": False,
        "fixture_receipt_ms": received,
        "scope": "Synthetic frame/sink failure; actual collector run, supervisor, queue, consumer, store and archive. No sink/storage batch transaction or live service continuity claim.",
    }
    (OUTPUT / "report.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    script_bytes = await asyncio.to_thread(Path(__file__).read_bytes)
    await asyncio.to_thread((OUTPUT / "verification-script.py").write_bytes, script_bytes)
    print(json.dumps({k: v for k, v in result.items() if k != "tested_source_sha256"}))


asyncio.run(main())
