import asyncio
from unittest.mock import AsyncMock

import pytest
from test_recovery_journal import anchor, plan
from test_trade_checkpoints import message, wire

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.ingestion import capture
from selective_options.recovery_handoff import RecoveredSequence, handoff_recovery
from selective_options.recovery_journal import load_recovery_evidence, publish_recovery
from selective_options.spot_trades import KrakenTrade, SpotTradeCollector
from selective_options.store import DuckStore


async def committed(path):
    store = await DuckStore.open(path)
    marker = await publish_recovery(store, anchor(), plan())
    await store.close()
    return marker.event_id


def test_mixed_overlap_successor_and_bounded_identity_cache():
    sequence = RecoveredSequence(plan(), capacity=2)
    assert not sequence.accept(KrakenTrade.model_validate(wire(4)))
    assert not sequence.accept(KrakenTrade.model_validate(wire(5)))
    assert sequence.accept(KrakenTrade.model_validate(wire(6)))
    assert list(sequence.seen) == list(sequence.identities) == [5, 6]
    with pytest.raises(IntegrityError, match="SEQUENCE_GAP"):
        sequence.accept(KrakenTrade.model_validate(wire(8)))
    assert sequence.last == 6
    with pytest.raises(ValueError, match="positive"):
        RecoveredSequence(plan(), capacity=0)


@pytest.mark.parametrize("identifier", [4, 5, 6])
@pytest.mark.parametrize("changes", [{"price": 101}, {"qty": 0.2}, {"side": "sell"}])
def test_recovered_and_new_duplicates_reject_changed_economics(identifier, changes):
    sequence = RecoveredSequence(plan())
    sequence.accept(KrakenTrade.model_validate(wire(6)))
    with pytest.raises(IntegrityError, match="CONFLICTING_TRADE_ID"):
        sequence.accept(KrakenTrade.model_validate(wire(identifier, **changes)))
    assert sequence.last == 6


def test_cross_transport_tolerance_does_not_relax_later_websocket_identity():
    sequence = RecoveredSequence(plan())
    assert not sequence.accept(
        KrakenTrade.model_validate(wire(4, timestamp="1970-01-01T00:05:04.000001+00:00"))
    )
    with pytest.raises(IntegrityError, match="CONFLICTING_TRADE_ID"):
        sequence.accept(KrakenTrade.model_validate(wire(4)))


async def test_handoff_snapshot_overlap_followed_by_live_update_keeps_partial_bars(tmp_path):
    path = tmp_path / "prior.duckdb"
    marker = await committed(path)
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink)
    await handoff_recovery(collector, path, marker)
    assert collector.awaiting_snapshot == {"BTC/USD"} and not collector.bars
    collector.symbols = {"BTC/USD": "BTC"}
    await collector._trades(message(3, 4, 5, 6), 306000)
    await collector._trades(message(7, kind="update"), 307000)
    await collector._flush(605000)
    rows = [call.args[0] for call in sink.await_args_list]
    assert [row.payload()["trade_id"] for row in rows if row.kind == "trade"] == [6, 7]
    assert all(row.source == "kraken" for row in rows)
    bars = [row for row in rows if row.kind == "bar"]
    assert bars and all(not row.payload()["complete"] for row in bars)
    assert bars[0].payload()["trade_count"] == 2
    assert not collector.awaiting_snapshot


@pytest.mark.parametrize(
    "snapshot,error",
    [
        (message(6, kind="update"), "UPDATE_BEFORE"),
        (message(4), "STALE"),
        (message(4, 5, 7), "SEQUENCE_GAP"),
    ],
)
async def test_bad_first_batch_cannot_advance_or_publish(tmp_path, snapshot, error):
    path = tmp_path / "prior.duckdb"
    marker = await committed(path)
    sink = AsyncMock()
    collector = SpotTradeCollector(AppConfig(), sink)
    await handoff_recovery(collector, path, marker)
    collector.symbols = {"BTC/USD": "BTC"}
    with pytest.raises(IntegrityError, match=error):
        await collector._trades(snapshot, 620000)
    assert collector.sequences["BTC/USD"].last == 5
    assert collector.awaiting_snapshot == {"BTC/USD"} and not collector.bars
    sink.assert_not_awaited()


async def test_invalid_marker_disabled_asset_and_repeated_handoff_do_not_activate(tmp_path):
    path = tmp_path / "prior.duckdb"
    marker = await committed(path)
    collector = SpotTradeCollector(AppConfig(), AsyncMock())
    with pytest.raises(IntegrityError):
        await handoff_recovery(collector, path, "missing")
    assert not collector.sequences and not collector.awaiting_snapshot
    disabled = SpotTradeCollector(
        AppConfig.model_validate({"system": {"assets": ["ETH"]}}), AsyncMock()
    )
    with pytest.raises(IntegrityError, match="ASSET_DISABLED"):
        await handoff_recovery(disabled, path, marker)
    assert not disabled.sequences
    await handoff_recovery(collector, path, marker)
    with pytest.raises(IntegrityError, match="FRESH_COLLECTOR"):
        await handoff_recovery(collector, path, marker)


async def test_cancellation_during_reload_leaves_no_runtime_state(tmp_path, monkeypatch):
    entered = asyncio.Event()

    async def blocked(*args):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("selective_options.recovery_handoff.load_recovery", blocked)
    collector = SpotTradeCollector(AppConfig(), AsyncMock())
    task = asyncio.create_task(handoff_recovery(collector, tmp_path / "unused", "marker"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not collector.sequences and not collector.awaiting_snapshot and not collector.bars


async def test_collector_started_during_reload_cannot_receive_handoff(tmp_path, monkeypatch):
    collector = SpotTradeCollector(AppConfig(), AsyncMock())

    async def started_during_load(*args):
        collector.connections = 1
        return plan()

    monkeypatch.setattr("selective_options.recovery_handoff.load_recovery", started_during_load)
    with pytest.raises(IntegrityError, match="FRESH_COLLECTOR"):
        await handoff_recovery(collector, tmp_path / "unused", "marker")
    assert not collector.sequences and not collector.awaiting_snapshot


async def test_capture_copies_and_reloads_marker_before_starting_producers(tmp_path, monkeypatch):
    source = tmp_path / "prior.duckdb"
    marker = await committed(source)

    async def idle(*args):
        await asyncio.Event().wait()

    async def spot_run(self):
        assert self.sequences["BTC/USD"].last == 5
        assert self.awaiting_snapshot == {"BTC/USD"}
        await idle()

    for name in ("DeribitCollector.run", "poll_chains"):
        monkeypatch.setattr("selective_options.ingestion." + name, idle)
    monkeypatch.setattr("selective_options.ingestion.SpotTradeCollector.run", spot_run)
    config = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "derivatives_enabled": False,
                "reference_sources": [],
                "policy_news_sources": [],
                "flush_interval_s": 0.001,
            },
        }
    )
    report = await capture(config, tmp_path / "next", 0.02, source, marker)
    assert report["recovery_handoff"] == "COMMITTED_AND_RELOADED"
    assert report["rows"] == 4 and report["recovery_selected_rows"] == 2
    assert report["trade_checkpoint_seed_rows"] == 0
    assert report["trading_enabled"] is report["milestone_2_complete"] is False
    assert await load_recovery_evidence(tmp_path / "next/events.duckdb", marker, 620000) == (
        await load_recovery_evidence(source, marker, 620000)
    )
    with pytest.raises(ValueError, match="source database"):
        await capture(config, tmp_path / "bad", 1, recovery_marker=marker)
    assert not (tmp_path / "bad").exists()


def test_cli_threads_explicit_marker(monkeypatch, tmp_path):
    from selective_options.cli import main

    mock = AsyncMock(return_value={"trading_enabled": False})
    monkeypatch.setattr("selective_options.cli.capture", mock)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "capture",
            "--output",
            str(tmp_path),
            "--resume-trades-from",
            "prior.duckdb",
            "--recovery-marker",
            "explicit-marker",
        ],
    )
    assert main() == 0
    assert mock.await_args.kwargs == {
        "recovery_marker": "explicit-marker",
        "resume_eth_gas_from": None,
        "resume_stablecoin_from": None,
    }


async def test_interruption_after_copy_leaves_restartable_evidence_no_producers(
    tmp_path, monkeypatch
):
    source = tmp_path / "prior.duckdb"
    marker = await committed(source)
    interrupted = AsyncMock(side_effect=asyncio.CancelledError)
    producer = AsyncMock()
    monkeypatch.setattr("selective_options.ingestion.handoff_recovery", interrupted)
    monkeypatch.setattr("selective_options.ingestion.DeribitCollector.run", producer)
    with pytest.raises(asyncio.CancelledError):
        await capture(AppConfig(), tmp_path / "interrupted", 1, source, marker)
    producer.assert_not_called()
    collector = SpotTradeCollector(AppConfig(), AsyncMock())
    await handoff_recovery(collector, tmp_path / "interrupted/events.duckdb", marker)
    assert collector.sequences["BTC/USD"].last == 5
    assert collector.awaiting_snapshot == {"BTC/USD"}
