import time
from collections.abc import Callable
from typing import Protocol

from pydantic import TypeAdapter

from .errors import IntegrityError, RpcError
from .models import BookSummary, ChainRow, ChainSnapshot, DiscoveryResult, Instrument, Ticker
from .rpc import RpcClient


class VenueAdapter(Protocol):
    async def discover(self, asset: str) -> DiscoveryResult: ...
    async def option_chain(self, asset: str) -> ChainSnapshot: ...
    async def ticker(self, instrument_name: str) -> Ticker: ...


class DeribitAdapter:
    def __init__(self, rpc: RpcClient, *, clock: Callable[[], float] = time.time) -> None:
        self._rpc, self._clock = rpc, clock

    async def discover(self, asset: str) -> DiscoveryResult:
        currency, fallback = asset, False
        try:
            raw = await self._rpc.call(
                "public/get_instruments", {"currency": currency, "kind": "option", "expired": False}
            )
        except RpcError as exc:
            if exc.code != -32602 or exc.parameter != "currency":
                raise
            currency, fallback = "any", True
            raw = await self._rpc.call(
                "public/get_instruments", {"currency": currency, "kind": "option", "expired": False}
            )
        instruments = TypeAdapter(list[Instrument]).validate_python(raw)
        now_ms = int(self._clock() * 1000)
        selected = tuple(
            sorted(
                (
                    row
                    for row in instruments
                    if row.base_currency == asset
                    and row.is_active
                    and row.expiration_timestamp > now_ms
                ),
                key=lambda row: (
                    row.expiration_timestamp,
                    row.strike,
                    row.option_type,
                    row.instrument_name,
                ),
            )
        )
        if len({row.instrument_name for row in selected}) != len(selected):
            raise IntegrityError("duplicate instruments in discovery")
        return DiscoveryResult(
            asset=asset, query_currency=currency, fallback_used=fallback, instruments=selected
        )

    async def option_chain(self, asset: str) -> ChainSnapshot:
        discovery = await self.discover(asset)
        quotes: dict[str, BookSummary] = {}
        if discovery.instruments:
            raw = await self._rpc.call(
                "public/get_book_summary_by_currency",
                {"currency": discovery.query_currency, "kind": "option"},
            )
            for row in TypeAdapter(list[BookSummary]).validate_python(raw):
                if row.instrument_name in quotes:
                    raise IntegrityError("duplicate quote summaries")
                quotes[row.instrument_name] = row
        return ChainSnapshot(
            asset=asset,
            observed_at_ms=int(self._clock() * 1000),
            discovery=discovery,
            rows=tuple(
                ChainRow(instrument=row, summary=quotes.get(row.instrument_name))
                for row in discovery.instruments
            ),
        )

    async def ticker(self, instrument_name: str) -> Ticker:
        ticker = Ticker.model_validate(
            await self._rpc.call("public/ticker", {"instrument_name": instrument_name})
        )
        if ticker.instrument_name != instrument_name:
            raise IntegrityError("ticker instrument mismatch")
        return ticker
