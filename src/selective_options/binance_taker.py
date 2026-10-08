"""Sampled public taker aggregates; source period and receipt both constrain availability."""

import asyncio
from typing import Annotated

import aiohttp
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictInt,
    TypeAdapter,
    ValidationError,
)

from .binance import BinanceCollector, Discovery, Endpoint, Instrument
from .bybit import FeedUnavailable
from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent, make_event

PERIOD_MS = 300_000
MAX_AGE_MS = 1_800_000
MAX_LATEST_END_AGE_MS = 600_000
DecimalText = Annotated[str, Field(strict=True, pattern=r"^(0|[1-9][0-9]{0,29})(\.[0-9]{1,18})?$")]


class TakerSample(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")
    buySellRatio: DecimalText
    buyVol: DecimalText
    sellVol: DecimalText
    timestamp: StrictInt = Field(gt=0)


def taker_events(
    instrument: Instrument,
    raw: JsonValue,
    received: int,
    discovery_received: int,
    *,
    check_latest: bool = True,
) -> list[MarketEvent]:
    if not (
        instrument.contractType == "PERPETUAL"
        and instrument.status == "TRADING"
        and instrument.quoteAsset == instrument.marginAsset == "USDT"
        and instrument.symbol == instrument.baseAsset + "USDT"
    ):
        raise IntegrityError("BINANCE_TAKER_INSTRUMENT_SCOPE")
    if not 0 < discovery_received <= received:
        raise IntegrityError("BINANCE_TAKER_RECEIPT_ORDER")
    samples = TypeAdapter(list[TakerSample]).validate_python(raw)
    if not 1 <= len(samples) <= 3:
        raise IntegrityError("BINANCE_TAKER_SAMPLE_COUNT")
    unique: dict[int, TakerSample] = {}
    for sample in samples:
        if not received - MAX_AGE_MS <= sample.timestamp <= received:
            raise IntegrityError("BINANCE_TAKER_STALE_OR_FUTURE")
        if sample.timestamp in unique and unique[sample.timestamp] != sample:
            raise IntegrityError("BINANCE_TAKER_CONFLICTING_PERIOD")
        unique[sample.timestamp] = sample
    events = [
        make_event(
            source="binance_taker",
            environment="public_reference",
            asset=instrument.baseAsset,
            symbol=instrument.symbol,
            kind="taker_flow",
            event_time_ms=sample.timestamp + PERIOD_MS,
            observed_at_ms=received,
            native_id=str(sample.timestamp),
            payload={
                "observation_received_at_ms": received,
                "period_start_ms": sample.timestamp,
                "period_ms": PERIOD_MS,
                "period_end_ms": sample.timestamp + PERIOD_MS,
                "buy_volume": sample.buyVol,
                "sell_volume": sample.sellVol,
                "buy_sell_ratio": sample.buySellRatio,
                "ratio_basis": "venue reported; rounding retained",
                "volume_units": "venue_native_volume",
                "normalization_status": "BLOCKED_PENDING_UNIT_VERIFICATION",
                "instrument": instrument.model_dump(mode="json"),
                "timestamp_basis": "period end derived from documented start; receipt also required",
                "publication_time_available": False,
                "revision_policy": "changed values retained as separate observations; no finality assumed",
                "coverage": "sampled latest five-minute aggregates; no continuous history claim",
            },
        )
        for _, sample in sorted(unique.items())
        if sample.timestamp + PERIOD_MS <= received
    ]
    if not events:
        raise FeedUnavailable("NO_ENDED_PERIOD", 0)
    if check_latest and received - events[-1].event_time_ms > MAX_LATEST_END_AGE_MS:
        raise IntegrityError("BINANCE_TAKER_LATEST_PERIOD_STALE")
    return events


class BinanceTakerCollector(BinanceCollector):
    source = "binance_taker"
    endpoint: Endpoint = "takerlongshortRatio"

    def parse_samples(
        self, instrument: Instrument, raw: JsonValue, received: int, discovered: int
    ) -> list[MarketEvent]:
        return taker_events(instrument, raw, received, discovered)

    def poll_interval(self) -> float:
        return self.config.ingestion.binance_taker_poll_interval_s

    async def health(self, asset: str, ok: bool, reason: str, code: int = 0) -> None:
        received = now_ms()
        await self.sink(
            make_event(
                source=self.source,
                environment="public_reference",
                asset=asset,
                symbol="USDT_PERPETUAL",
                kind="feed_health",
                event_time_ms=received,
                observed_at_ms=received,
                timestamp_kind="receipt",
                payload={"ok": ok, "reason": reason, "code": code},
            )
        )

    async def collect(self, session: aiohttp.ClientSession) -> bool:
        asset = "GLOBAL"
        groups: dict[str, list[MarketEvent]] = {}
        try:
            raw, discovered = await self.request(session, "exchangeInfo", {})
            discovery = Discovery.model_validate(raw)
            if len({r.symbol for r in discovery.symbols}) != len(discovery.symbols):
                raise IntegrityError("BINANCE_DUPLICATE_DISCOVERY")
            selected: dict[str, Instrument] = {}
            for row in discovery.symbols:
                if (
                    row.baseAsset in self.config.system.assets
                    and row.contractType == "PERPETUAL"
                    and row.status == "TRADING"
                    and row.quoteAsset == row.marginAsset == "USDT"
                ):
                    if row.baseAsset in selected:
                        raise IntegrityError("BINANCE_AMBIGUOUS_DISCOVERY")
                    selected[row.baseAsset] = row
            for asset in self.config.system.assets:
                if asset not in selected:
                    groups[asset] = []
                    continue
                instrument = selected[asset]
                await asyncio.sleep(0.25)
                raw, received = await self.request(
                    session,
                    self.endpoint,
                    {
                        "symbol": instrument.symbol,
                        "period": "5m",
                        "limit": "3",
                    },
                )
                groups[asset] = self.parse_samples(instrument, raw, received, discovered)
        except FeedUnavailable as exc:
            await self.health(asset, False, exc.reason, exc.code)
            return False
        except (aiohttp.ClientError, TimeoutError) as exc:
            await self.health(asset, False, type(exc).__name__)
            return False
        except (IntegrityError, ValidationError, ValueError):
            await self.health(asset, False, "INVALID_DATA")
            return False
        # Validate the full cycle before publishing; sink failures are never source health.
        for asset, events in groups.items():
            for event in events:
                await self.sink(event)
            await self.health(
                asset, bool(events), "AGGREGATES_ACCEPTED" if events else "INSTRUMENT_UNAVAILABLE"
            )
        return all(groups.values())

    async def run(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            while True:
                ok = await self.collect(session)
                delay = self.poll_interval()
                await asyncio.sleep(delay if ok else max(delay, 600))
