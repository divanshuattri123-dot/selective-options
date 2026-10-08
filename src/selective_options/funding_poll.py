"""Opt-in rolling funding observation polls; source staging precedes publication."""

import asyncio

import aiohttp
from pydantic import ValidationError

from .binance import BinanceCollector
from .binance_funding_history import BinanceFundingArchive, collect_binance_funding_history
from .binance_funding_view import funding_history_at
from .bybit import FeedUnavailable
from .collectors import now_ms
from .errors import IntegrityError
from .events import make_event


class BinanceFundingCollector(BinanceCollector):
    async def poll_health(
        self,
        asset: str,
        start: int,
        end: int,
        ok: bool,
        reason: str,
        code: int = 0,
        plan: BinanceFundingArchive | None = None,
    ) -> None:
        received = now_ms()
        if plan is not None and (
            received < end or any(row.observed_at_ms > received for row in plan.events)
        ):
            raise IntegrityError("BINANCE_FUNDING_POLL_HEALTH_CLOCK_REGRESSION")
        await self.sink(
            make_event(
                source="binance_history",
                environment="public_reference",
                asset=asset,
                symbol=asset + "USDT",
                kind="feed_health",
                timestamp_kind="receipt",
                event_time_ms=received,
                observed_at_ms=received,
                payload={
                    "ok": ok,
                    "reason": reason,
                    "code": code,
                    "start_ms": start,
                    "end_ms": end,
                    "pages": plan.pages if plan else None,
                    "termination": plan.termination if plan else None,
                    "observations": len(plan.events) if plan else 0,
                    "coverage": "bounded recent poll; complete historical schedule not proven",
                    "publication": "health emitted after staged observations reach the sink; no atomic sink/commit claim",
                    "trading_enabled": False,
                },
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        all_ok = True
        for asset in self.config.system.assets:
            end = now_ms()
            start = end - self.config.ingestion.binance_funding_history_hours * 3_600_000
            try:
                plan = await collect_binance_funding_history(
                    self,
                    session,
                    asset,
                    start_ms=start,
                    end_ms=end,
                    page_size=self.config.ingestion.binance_funding_history_page_size,
                    max_pages=self.config.ingestion.binance_funding_history_max_pages,
                    timeout_s=self.config.ingestion.binance_funding_history_timeout_s,
                )
                verified = now_ms()
                if verified < end or any(row.observed_at_ms > verified for row in plan.events):
                    raise IntegrityError("BINANCE_FUNDING_POLL_CLOCK_REGRESSION")
                selected = funding_history_at(
                    plan.events,
                    asset=asset,
                    symbol=asset + "USDT",
                    start_ms=start,
                    end_ms=end,
                    as_of_ms=verified,
                )
                if selected != plan.events or plan.start_ms != start or plan.end_ms != end:
                    raise IntegrityError("BINANCE_FUNDING_POLL_PLAN_SCOPE")
            except FeedUnavailable as exc:
                await self.poll_health(asset, start, end, False, exc.reason, exc.code)
                # The discovery/transport is shared: never retry a venue refusal via the next asset.
                return False
            except (aiohttp.ClientError, OSError, TimeoutError):
                await self.poll_health(asset, start, end, False, "TRANSPORT_UNAVAILABLE")
                return False
            except (ValidationError, IntegrityError):
                await self.poll_health(asset, start, end, False, "INVALID_DATA")
                all_ok = False
                continue
            # Consumer/storage errors and cancellation propagate. No false source outage or success.
            for row in plan.events:
                await self.sink(row)
            await self.poll_health(
                asset,
                start,
                end,
                bool(plan.events),
                "BOUNDED_HISTORY_ACCEPTED" if plan.events else "NO_SETTLEMENTS_RETURNED",
                plan=plan,
            )
            all_ok = all_ok and bool(plan.events)
        return all_ok

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            while True:
                await self.collect(session)
                await asyncio.sleep(self.config.ingestion.binance_funding_history_poll_interval_s)
