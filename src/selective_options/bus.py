"""Bounded in-process bus and optional Redis stream mirror for deployment."""

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass

from redis.asyncio import Redis

from .errors import IntegrityError
from .events import MarketEvent


@dataclass(frozen=True)
class CommitBarrier:
    completed: asyncio.Future[None]


class EventBus:
    def __init__(self, capacity: int, timeout_s: float) -> None:
        self.queue: asyncio.Queue[MarketEvent | CommitBarrier] = asyncio.Queue(maxsize=capacity)
        self.timeout_s = timeout_s

    async def publish(self, event: MarketEvent) -> None:
        try:
            async with asyncio.timeout(self.timeout_s):
                await self.queue.put(event)
        except TimeoutError as exc:
            raise IntegrityError("INGESTION_BACKPRESSURE: no event was silently discarded") from exc

    async def flush(self) -> None:
        """Wait for preceding FIFO events to pass storage and configured consumers."""
        completed = asyncio.get_running_loop().create_future()
        try:
            async with asyncio.timeout(self.timeout_s):
                await self.queue.put(CommitBarrier(completed))
                await completed
        except TimeoutError as exc:
            raise IntegrityError("INGESTION_COMMIT_BARRIER_TIMEOUT") from exc
        finally:
            # A timed-out/cancelled waiter cannot strand an unobserved future error.
            if not completed.done():
                completed.cancel()


class HotBuffer:
    def __init__(self, capacity: int) -> None:
        self._buffers: dict[tuple[str, str, str], deque[MarketEvent]] = defaultdict(
            lambda: deque(maxlen=capacity)
        )

    def append(self, event: MarketEvent) -> None:
        self._buffers[(event.source, event.asset, event.kind)].append(event)

    def recent(self, source: str, asset: str, kind: str) -> tuple[MarketEvent, ...]:
        return tuple(self._buffers.get((source, asset, kind), ()))


class RedisMirror:
    """No automatic trimming: durability policy must not discard unconsumed entries."""

    def __init__(self, url: str) -> None:
        self._redis = Redis.from_url(url, decode_responses=True)

    async def publish_batch(self, events: list[MarketEvent]) -> None:
        async with self._redis.pipeline(transaction=True) as pipe:
            for event in events:
                pipe.xadd("selective:events:v1", {"event": event.model_dump_json()})
            await pipe.execute()

    async def close(self) -> None:
        await self._redis.aclose()
