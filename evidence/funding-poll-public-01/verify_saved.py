"""Independent SQL/raw-response checks and labelled later-poll/outage replay; no network."""

import asyncio
import hashlib
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import duckdb

from selective_options.archive import seal_archive
from selective_options.bybit import FeedUnavailable
from selective_options.config import AppConfig
from selective_options.events import MarketEvent
from selective_options.funding_poll import BinanceFundingCollector
from selective_options.store import DuckStore

folder = Path(__file__).resolve().parent
source = folder / "events.duckdb"
source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
report = json.loads((folder / "report.json").read_text())
responses = json.loads((folder / "responses.json").read_text())
with duckdb.connect(str(source), read_only=True) as db:
    result = db.execute("SELECT * FROM events ORDER BY event_id")
    columns = [c[0] for c in result.description]
    original = tuple(
        MarketEvent.model_validate(dict(zip(columns, r, strict=True))) for r in result.fetchall()
    )
    archived = db.execute(
        "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(folder / "events.parquet")]
    ).fetchall()
    assert [tuple(e.model_dump().values()) for e in original] == archived
    assert db.execute("SELECT count(*) FROM delivery_outbox WHERE NOT delivered").fetchone()[0] == 7
    history = [e for e in original if e.kind == "funding"]
    health = next(e for e in original if e.kind == "feed_health")
    raw = [
        (item, response["observed_at_ms"])
        for response in responses
        if response["endpoint"] == "fundingRate"
        for item in response["response"]
    ]
    assert len(raw) == len(history) == 6
    for event in history:
        item, receipt = next(
            (item, receipt) for item, receipt in raw if item["fundingTime"] == event.event_time_ms
        )
        payload = event.payload()
        assert event.observed_at_ms == receipt == payload["observation_received_at_ms"]
        assert payload["funding_rate"] == item["fundingRate"]
        assert payload["mark_price_at_charge"] == item["markPrice"]
        assert payload["reported_rate_type"] == item.get("rateType")
        for at, expected in [(receipt - 1, 0), (receipt, 1)]:
            assert (
                db.execute(
                    "SELECT count(*) FROM events WHERE event_id=? AND event_time_ms<=? AND observed_at_ms<=?",
                    [event.event_id, at, at],
                ).fetchone()[0]
                == expected
            )
    assert health.payload() == report["health"] and health.payload()["ok"] is True
    assert health.observed_at_ms >= max(e.observed_at_ms for e in history)
    assert report["scheduler_started"] is False


async def main() -> None:
    fixture = folder / "fixture.duckdb"
    if fixture.exists():
        raise FileExistsError(
            "Preserve fixture proof; reproduce in a fresh sibling copy with saved evidence"
        )
    emitted: list[MarketEvent] = []

    async def sink(event: MarketEvent) -> None:
        emitted.append(event)

    cfg = AppConfig.model_validate(
        {
            "system": {"assets": ["BTC"]},
            "ingestion": {
                "binance_funding_history_enabled": True,
                "binance_funding_history_page_size": 2,
            },
        }
    )
    collector = BinanceFundingCollector(cfg, sink)
    end = int(str(health.payload()["end_ms"]))
    for offset in [0, 900_000]:
        collector.request = AsyncMock(
            side_effect=[(r["response"], r["observed_at_ms"] + offset) for r in responses]
        )
        with (
            patch(
                "selective_options.funding_poll.now_ms",
                side_effect=[
                    end + offset,
                    health.observed_at_ms + offset,
                    health.observed_at_ms + offset,
                ],
            ),
            patch("selective_options.binance_funding_history.now_ms", return_value=end + offset),
            patch("selective_options.binance_funding_history.asyncio.sleep", new=AsyncMock()),
        ):
            assert await collector.collect(None)
    assert set(emitted[:7]) == set(original)
    collector.request = AsyncMock(side_effect=FeedUnavailable("RATE_LIMITED", 429))
    with (
        patch("selective_options.funding_poll.now_ms", return_value=end + 1_800_000),
        patch("selective_options.binance_funding_history.now_ms", return_value=end + 1_800_000),
    ):
        assert not await collector.collect(None)
    assert len(emitted) == 15 and emitted[-1].kind == "feed_health"
    assert emitted[-1].payload()["ok"] is False and emitted[-1].payload()["code"] == 429
    collector.request.assert_awaited_once()
    store = await DuckStore.open(fixture)
    try:
        await store.append(emitted)
        await store.append(emitted)
        assert await store.pending_delivery_count() == 15
        count = await store.export_parquet(folder / "fixture.parquet")
        assert count == 15
        await seal_archive(folder / "fixture.parquet", count)
    finally:
        await store.close()
    with duckdb.connect(str(fixture), read_only=True) as db:
        rows = db.execute("SELECT * FROM events ORDER BY event_id").fetchall()
        restored = db.execute(
            "SELECT * FROM read_parquet(?) ORDER BY event_id", [str(folder / "fixture.parquet")]
        ).fetchall()
        assert rows == restored
        latest = db.execute(
            "SELECT event_id FROM events WHERE kind='funding' QUALIFY row_number() OVER (PARTITION BY event_time_ms ORDER BY observed_at_ms DESC)=1 ORDER BY event_time_ms"
        ).fetchall()
        expected = sorted(
            [e for e in emitted[7:14] if e.kind == "funding"], key=lambda e: e.event_time_ms
        )
        assert [r[0] for r in latest] == [e.event_id for e in expected]
        assert (
            db.execute(
                "SELECT count(*) FROM events WHERE event_time_ms<=? AND observed_at_ms<=?",
                [health.observed_at_ms, health.observed_at_ms],
            ).fetchone()[0]
            == 7
        )
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
    result = dict(
        source_sha256=source_hash,
        source_rows=7,
        public_funding_observations=6,
        independent_pit_boundaries=12,
        source_archive_exact=True,
        source_bytes_unchanged=True,
        exact_updated_runtime_saved_replay=True,
        synthetic_later_poll_and_outage_rows=8,
        fixture_rows=15,
        fixture_selected_charges=6,
        fixture_outage_visible=True,
        fixture_prior_view_unchanged=True,
        fixture_archive_exact=True,
        synthetic_event_ids=[e.event_id for e in emitted[7:]],
        new_network_requests=0,
        real_scheduler_or_soak_verified=False,
        trading_enabled=False,
    )
    (folder / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
