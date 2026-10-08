import asyncio
import math
import time
from collections.abc import Awaitable, Callable

from .config import BucketConfig, RateConfig


class TokenBucket:
    def __init__(
        self,
        config: BucketConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self._clock, self._sleep = clock, sleep
        self._tokens = config.capacity
        self._updated = clock()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float) -> None:
        if not math.isfinite(cost) or not 0 < cost <= self.config.capacity:
            raise ValueError("cost must be finite, positive and within bucket capacity")
        async with self._lock:
            while True:
                now = self._clock()
                self._tokens = min(
                    self.config.capacity,
                    self._tokens + max(0, now - self._updated) * self.config.refill_per_s,
                )
                self._updated = now
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                await self._sleep((cost - self._tokens) / self.config.refill_per_s)


class EndpointLimiter:
    def __init__(self, config: RateConfig) -> None:
        self._buckets = {
            name: TokenBucket(getattr(config, name))
            for name in ("discovery", "subscriptions", "ordinary")
        }

    async def acquire(self, method: str) -> None:
        name = (
            "discovery"
            if method == "public/get_instruments"
            else "subscriptions"
            if method.endswith("/subscribe")
            else "ordinary"
        )
        bucket = self._buckets[name]
        await bucket.acquire(bucket.config.cost)
