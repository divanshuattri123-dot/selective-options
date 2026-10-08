"""Replay committed events after a Redis outage using a durable outbox."""

from pathlib import Path

from .bus import RedisMirror
from .config import AppConfig
from .store import DuckStore


async def replay_pending(store: DuckStore, mirror: RedisMirror, batch_size: int) -> int:
    delivered = 0
    while rows := await store.pending_delivery(batch_size):
        await mirror.publish_batch(rows)
        # A crash after Redis commits but before this ACK may replay duplicates.
        # Consumers must deduplicate on event_id; this is at-least-once delivery.
        await store.acknowledge_delivery([row.event_id for row in rows])
        delivered += len(rows)
    return delivered


async def replay_store(config: AppConfig, database: Path) -> dict[str, object]:
    import asyncio

    if config.ingestion.redis_url is None:
        raise ValueError("configure the Redis URL before replay")
    if not await asyncio.to_thread(database.is_file):
        raise FileNotFoundError("replay requires an existing database")
    store = await DuckStore.open(database, int(config.data.max_clock_skew_s * 1000))
    mirror = RedisMirror(config.ingestion.redis_url)
    try:
        delivered = await replay_pending(store, mirror, config.ingestion.storage_batch_size)
        return {
            "status": "REPLAY_FINISHED",
            "events_delivered": delivered,
            "pending_events": await store.pending_delivery_count(),
            "delivery": "AT_LEAST_ONCE",
            "consumer_deduplication_key": "event_id",
            "trading_enabled": False,
        }
    finally:
        await store.close()
        await mirror.close()
