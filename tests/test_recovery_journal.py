import asyncio
import json
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime

import duckdb
import pytest

from selective_options.archive import restore_archive, seal_archive
from selective_options.errors import IntegrityError
from selective_options.recovery_journal import (
    load_recovery,
    marker_for,
    publish_recovery,
    validate_plan,
)
from selective_options.spot_trades import KrakenTrade, trade_event
from selective_options.store import DuckStore
from selective_options.trade_backfill import RestTrade
from selective_options.trade_reconciliation import RecoveryBuffer


def anchor():
    return trade_event(
        KrakenTrade(
            symbol="BTC/USD",
            side="buy",
            price=100,
            qty=0.1,
            trade_id=3,
            timestamp=datetime.fromtimestamp(303, UTC),
        ),
        "BTC",
        500000,
    )


def plan():
    buffer = RecoveryBuffer(anchor(), 5)
    buffer.add(
        RestTrade(price="100", qty=".1", timestamp="304", side="buy", trade_id=4).event(
            anchor(), 600000
        )
    )
    buffer.add(
        trade_event(
            KrakenTrade(
                symbol="BTC/USD",
                side="buy",
                price=100,
                qty=0.1,
                trade_id=5,
                timestamp=datetime.fromtimestamp(305, UTC),
            ),
            "BTC",
            610000,
        )
    )
    return buffer.finish(620000)


async def test_publish_is_atomic_idempotent_and_reloadable(tmp_path):
    path = tmp_path / "state.duckdb"
    store = await DuckStore.open(path)
    marker = await publish_recovery(store, anchor(), plan())
    assert await publish_recovery(store, anchor(), plan()) == marker
    assert await store.pending_delivery_count() == 4
    await store.acknowledge_delivery([marker.event_id])
    await publish_recovery(store, anchor(), plan())
    assert await store.pending_delivery_count() == 3
    await store.close()
    recovered = await load_recovery(path, marker.event_id, 620000)
    assert recovered.selected == plan().selected and recovered.through_id == 5
    with pytest.raises(IntegrityError, match="NOT_YET_PUBLISHED"):
        await load_recovery(path, marker.event_id, 619999)


async def test_archive_round_trip_retains_mixed_source_plan(tmp_path):
    store = await DuckStore.open(tmp_path / "state.duckdb")
    marker = await publish_recovery(store, anchor(), plan())
    parquet = tmp_path / "events.parquet"
    count = await store.export_parquet(parquet)
    await seal_archive(parquet, count)
    await store.close()
    await restore_archive(parquet, tmp_path / "restored")
    recovered = await load_recovery(tmp_path / "restored/events.duckdb", marker.event_id, 620000)
    assert recovered.selected == plan().selected


async def test_existing_receipt_conflict_rolls_back_marker_and_new_rows(tmp_path):
    path = tmp_path / "state.duckdb"
    store = await DuckStore.open(path)
    await store.append([plan().selected[0].model_copy(update={"observed_at_ms": 599999})])
    with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
        await publish_recovery(store, anchor(), plan())
    assert sum(row["count"] for row in await store.counts()) == 1
    assert await store.pending_delivery_count() == 1
    await store.close()
    marker = marker_for(anchor(), validate_plan(anchor(), plan()))
    with pytest.raises(IntegrityError, match="MARKER_MISSING"):
        await load_recovery(path, marker.event_id, 620000)


async def test_conflicting_duplicate_within_exact_batch_rejected(tmp_path):
    store = await DuckStore.open(tmp_path / "state.duckdb")
    row = anchor()
    with pytest.raises(IntegrityError, match="EXACT_EVENT_CONFLICT"):
        await store.append([row, row.model_copy(update={"observed_at_ms": 500001})], exact=True)
    assert await store.counts() == []
    await store.close()


def test_raw_duplicate_receipts_and_anchor_are_canonicalized():
    original = plan()
    extra = (
        anchor().model_copy(update={"observed_at_ms": 610000}),
        original.raw[0].model_copy(update={"observed_at_ms": 619999}),
    )
    checked = validate_plan(anchor(), replace(original, raw=original.raw + extra))
    assert len(checked.raw) == 3 and anchor() in checked.raw
    assert checked.selected == original.selected


@pytest.mark.parametrize(
    "change", [{"anchor_id": 2}, {"selected": tuple(reversed(plan().selected))}]
)
def test_forged_plan_selection_rejected(change):
    with pytest.raises(IntegrityError, match="SELECTION_MISMATCH"):
        validate_plan(anchor(), replace(plan(), **change))


async def test_future_verification_cannot_be_published(tmp_path, monkeypatch):
    monkeypatch.setattr("selective_options.recovery_journal.now_ms", lambda: 619999)
    store = await DuckStore.open(tmp_path / "state.duckdb")
    with pytest.raises(IntegrityError, match="IN_FUTURE"):
        await publish_recovery(store, anchor(), plan())
    assert await store.counts() == []
    await store.close()


@pytest.mark.parametrize(
    "tamper,error",
    [
        ("missing", "EVIDENCE_MISSING"),
        ("receipt", "MARKER_OR_EVIDENCE_CHANGED"),
        ("digest", "MARKER_OR_EVIDENCE_CHANGED"),
        ("duplicate", "INVALID_REFERENCES"),
        ("source", "INVALID_MARKER"),
    ],
)
async def test_restart_revalidates_evidence_and_marker(tmp_path, tamper, error):
    path = tmp_path / "state.duckdb"
    store = await DuckStore.open(path)
    marker = await publish_recovery(store, anchor(), plan())
    await store.close()
    with duckdb.connect(str(path)) as db:
        identifier = plan().selected[0].event_id
        if tamper == "missing":
            db.execute("DELETE FROM events WHERE event_id=?", [identifier])
        elif tamper == "receipt":
            db.execute(
                "UPDATE events SET observed_at_ms=observed_at_ms+1 WHERE event_id=?", [identifier]
            )
        elif tamper == "source":
            db.execute("UPDATE events SET source='other' WHERE event_id=?", [marker.event_id])
        else:
            payload = marker.payload()
            if tamper == "digest":
                payload["evidence_sha256"] = "0" * 64
            else:
                payload["raw_event_ids"].append(payload["raw_event_ids"][0])
            db.execute(
                "UPDATE events SET payload_json=? WHERE event_id=?",
                [json.dumps(payload), marker.event_id],
            )
    with pytest.raises(IntegrityError, match=error):
        await load_recovery(path, marker.event_id, 620000)


async def test_missing_database_not_created(tmp_path):
    path = tmp_path / "absent.duckdb"
    with pytest.raises(FileNotFoundError):
        await load_recovery(path, "x", 620000)
    assert not path.exists()


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
async def test_abrupt_process_exit_at_publication_boundary(tmp_path, phase):
    path = tmp_path / "state.duckdb"
    source = tmp_path / "input.json"
    source.write_text(
        json.dumps(
            {"anchor": anchor().model_dump(), "raw": [row.model_dump() for row in plan().raw]}
        )
    )
    script = """
import asyncio,json,os,sys
from pathlib import Path
from selective_options.events import MarketEvent
from selective_options.store import DuckStore
from selective_options.trade_reconciliation import RecoveryBuffer
from selective_options.recovery_journal import publish_recovery
class Interrupted:
 def __init__(self,connection):self.connection=connection
 def execute(self,sql,*args):
  result=self.connection.execute(sql,*args)
  if sql.startswith('INSERT INTO delivery_outbox'):os._exit(23)
  return result
async def main():
 data=json.loads(Path(sys.argv[2]).read_text())
 anchor=MarketEvent.model_validate(data['anchor'])
 buffer=RecoveryBuffer(anchor,5)
 for row in data['raw']:buffer.add(MarketEvent.model_validate(row))
 store=await DuckStore.open(Path(sys.argv[1]))
 if sys.argv[3]=='before_commit':store._connection=Interrupted(store._connection)
 await publish_recovery(store,anchor,buffer.finish(620000))
 os._exit(23)
asyncio.run(main())
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script, str(path), str(source), phase],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 23, result.stderr.decode()
    marker = marker_for(anchor(), validate_plan(anchor(), plan()))
    if phase == "before_commit":
        with pytest.raises(IntegrityError, match="MARKER_MISSING"):
            await load_recovery(path, marker.event_id, 620000)
    else:
        assert (await load_recovery(path, marker.event_id, 620000)).selected == plan().selected
    store = await DuckStore.open(path)
    assert await store.pending_delivery_count() == (0 if phase == "before_commit" else 4)
    assert sum(row["count"] for row in await store.counts()) == (
        0 if phase == "before_commit" else 4
    )
    await store.close()
