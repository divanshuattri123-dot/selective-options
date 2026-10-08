"""Explicit saved seeds preserve progress checks, never current feed availability."""

from unittest.mock import AsyncMock

import pytest

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.stablecoin_supply import StablecoinSupplyCollector, supply_event
from tests.test_stablecoin_supply import POLLED, RECEIVED, results, snapshot, word


def test_valid_seed_keeps_exact_native_clocks_without_publishing(monkeypatch):
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED)
    row, sink = snapshot(), AsyncMock()
    collector = StablecoinSupplyCollector(AppConfig(), sink, initial_snapshot=row)
    assert collector.previous is row and row.observed_at_ms == RECEIVED
    sink.assert_not_called()
    assert StablecoinSupplyCollector(AppConfig(), sink).previous is None


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "changed"},
        {"source": "other"},
        {"environment": "testnet"},
        {"symbol": "other"},
        {"kind": "feed_health"},
        {"observed_at_ms": POLLED + 1},
        {"payload_json": "{}"},
    ],
)
def test_invalid_seed_refused_before_any_sink(monkeypatch, changes):
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED)
    sink = AsyncMock()
    with pytest.raises((ValueError, IntegrityError)):
        StablecoinSupplyCollector(
            AppConfig(), sink, initial_snapshot=snapshot().model_copy(update=changes)
        )
    sink.assert_not_called()


async def test_seeded_repeat_is_full_new_receipt_revision_and_health_binds_it(monkeypatch):
    old, current = snapshot(), snapshot(offset=1000)
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED + 1000)
    sink = AsyncMock()
    collector = StablecoinSupplyCollector(AppConfig(), sink, initial_snapshot=old)
    collector._collect = AsyncMock(return_value=current)
    assert await collector.collect(None)
    assert collector.previous is current
    assert sink.await_args_list[0].args[0] == current
    assert sink.await_args_list[1].args[0].payload()["accepted_event_id"] == current.event_id
    assert current.event_id != old.event_id and old.observed_at_ms == RECEIVED


@pytest.mark.parametrize("conflict", ["receipt", "same_block_supply"])
async def test_seeded_regression_or_conflict_leaves_seed_and_publishes_failed_health(
    monkeypatch, conflict
):
    old = snapshot()
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED + 1000)
    values = results(offset=-1 if conflict == "receipt" else 1000)
    if conflict == "same_block_supply":
        values[2] = word(1000000), values[2][1]
    current = supply_event(values, 1800000)
    sink = AsyncMock()
    collector = StablecoinSupplyCollector(AppConfig(), sink, initial_snapshot=old)
    collector._collect = AsyncMock(return_value=current)
    assert not await collector.collect(None)
    assert collector.previous is old
    assert sink.await_count == 1
    assert sink.await_args.args[0].kind == "feed_health"
    assert not sink.await_args.args[0].payload()["ok"]


async def test_seed_survives_failed_new_snapshot_sink(monkeypatch):
    old = snapshot()
    monkeypatch.setattr("selective_options.stablecoin_supply.now_ms", lambda: POLLED + 1000)
    sink = AsyncMock(side_effect=IntegrityError("FIXTURE_SINK_REJECTED"))
    collector = StablecoinSupplyCollector(AppConfig(), sink, initial_snapshot=old)
    collector._collect = AsyncMock(return_value=snapshot(offset=1000))
    with pytest.raises(IntegrityError, match="FIXTURE_SINK_REJECTED"):
        await collector.collect(None)
    assert collector.previous is old and sink.await_count == 1
