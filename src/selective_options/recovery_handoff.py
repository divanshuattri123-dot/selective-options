"""Explicit startup handoff from durable mixed-source evidence to a fresh collector."""

from pathlib import Path

from .collectors import now_ms
from .errors import IntegrityError, TradeGap
from .recovery_journal import load_recovery
from .spot_trades import KrakenTrade, SpotTradeCollector, TradeSequence, trade_event
from .trade_reconciliation import RecoveryPlan, TradeIdentity


class RecoveredSequence(TradeSequence):
    def __init__(self, plan: RecoveryPlan, capacity: int = 2000) -> None:
        if capacity < 1:
            raise ValueError("sequence capacity must be positive")
        super().__init__(capacity)
        self.asset = plan.selected[-1].asset
        self.identities: dict[int, dict[str, TradeIdentity]] = {}
        for event in plan.selected[-capacity:]:
            identity = TradeIdentity.read(event)
            self.seen[identity.native_id] = event.event_id
            self.identities[identity.native_id] = {identity.transport: identity}
        for event in plan.raw:
            identity = TradeIdentity.read(event)
            if identity.native_id in self.identities:
                self.identities[identity.native_id][identity.transport] = identity
        self.last = plan.through_id

    def accept(self, trade: KrakenTrade) -> bool:
        identity = TradeIdentity.read(trade_event(trade, self.asset, now_ms()))
        known = self.identities.get(trade.trade_id)
        if known is not None:
            if any(not identity.matches(previous) for previous in known.values()):
                raise IntegrityError("CONFLICTING_TRADE_ID")
            known[identity.transport] = identity
            return False
        if self.last is not None and trade.trade_id != self.last + 1:
            raise TradeGap(trade.symbol, self.last + 1, trade.trade_id)
        self.last = trade.trade_id
        self.seen[trade.trade_id] = trade.model_dump_json()
        self.identities[trade.trade_id] = {identity.transport: identity}
        if len(self.seen) > self.capacity:
            oldest, _ = self.seen.popitem(last=False)
            del self.identities[oldest]
        return True


async def handoff_recovery(
    collector: SpotTradeCollector, database: Path, marker_id: str
) -> RecoveryPlan:
    if collector.connections or collector.sequences or collector.bars:
        raise IntegrityError("RECOVERY_HANDOFF_REQUIRES_FRESH_COLLECTOR")
    plan = await load_recovery(database, marker_id, now_ms())
    if collector.connections or collector.sequences or collector.bars:
        raise IntegrityError("RECOVERY_HANDOFF_REQUIRES_FRESH_COLLECTOR")
    final = plan.selected[-1]
    if final.asset not in collector.config.system.assets:
        raise IntegrityError("RECOVERY_HANDOFF_ASSET_DISABLED")
    sequence = RecoveredSequence(plan)
    # No await after validation: activate all startup state together. Raw REST
    # evidence remains in its original source and never enters WebSocket bars.
    collector.sequences[final.symbol] = sequence
    collector.checkpoint_assets[final.symbol] = final.asset
    collector.last_events[final.symbol] = final
    collector.awaiting_snapshot.add(final.symbol)
    return plan
