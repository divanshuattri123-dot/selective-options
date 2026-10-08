"""Fetch, buffer, commit and reload one gap; caller still owns runtime activation."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from .collectors import now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent
from .recovery_journal import load_committed_recovery, publish_recovery
from .store import DuckStore
from .trade_backfill import collect_range
from .trade_reconciliation import (
    RecoveryBuffer,
    RecoveryPlan,
    TradeIdentity,
    reconcile_while_receiving,
)


@dataclass(frozen=True)
class CommittedGap:
    marker: MarketEvent
    plan: RecoveryPlan


async def recover_trade_gap(
    config: AppConfig,
    store: DuckStore,
    anchor: MarketEvent,
    through_id: int,
    receive: Callable[[], Awaitable[MarketEvent]],
    output: Path,
    initial: tuple[MarketEvent, ...] = (),
) -> CommittedGap:
    """One serialized book attempt. Receive must preserve actual receipt timestamps.

    The caller must suspend this book's normal processing/bar flush and exclusively
    route its incoming events here. This function never advances a collector cursor.
    Socket frames arriving after the pump stops must be sequence-checked by the caller.
    """
    identity = TradeIdentity.read(anchor)
    settings = config.ingestion
    if anchor.asset not in config.system.assets:
        raise IntegrityError("RECOVERY_ASSET_DISABLED")
    if (
        not 0
        < through_id - identity.native_id
        <= (settings.kraken_backfill_max_pages * settings.kraken_backfill_page_size)
    ):
        raise ValueError("requested trade range exceeds the page budget or is not positive")
    buffer = RecoveryBuffer(
        anchor, through_id, max_skew_ms=int(config.data.max_clock_skew_s * 1000)
    )
    for event in initial:
        buffer.add(event)
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=False)
    report: dict[str, object] = {
        "status": "FAILED",
        "symbol": anchor.symbol,
        "anchor_event_id": anchor.event_id,
        "after_trade_id": identity.native_id,
        "requested_through_id": through_id,
        "started_at_ms": now_ms(),
        "stage": "FETCH_AND_BUFFER",
        "marker_id": None,
        "publication_outcome": "NOT_ATTEMPTED",
        "runtime_activated": False,
        "bars_rebuilt": False,
        "trading_enabled": False,
    }

    async def fetch() -> list[MarketEvent]:
        rows, pages = await collect_range(config, anchor, through_id, output)
        report["pages"] = pages
        return rows

    try:
        await asyncio.to_thread(
            (output / "anchor.json").write_text, anchor.model_dump_json(indent=2), encoding="utf-8"
        )
        plan = await reconcile_while_receiving(
            buffer, receive, fetch, timeout_s=settings.kraken_backfill_timeout_s
        )
        report.update(stage="PUBLISH", publication_outcome="UNKNOWN_UNTIL_RETURN")
        marker = await publish_recovery(store, anchor, plan)
        report.update(stage="RELOAD", marker_id=marker.event_id, publication_outcome="COMMITTED")
        recovered_anchor, committed = await load_committed_recovery(
            store, marker.event_id, now_ms()
        )
        if recovered_anchor != anchor or committed.selected != plan.selected:
            raise IntegrityError("RECOVERY_COMMITTED_SELECTION_CHANGED")
        report.update(
            status="COMMITTED_AND_RELOADED",
            stage="COMPLETE",
            selected_rows=len(committed.selected),
            raw_rows=len(committed.raw),
            through_id=committed.through_id,
            verified_at_ms=committed.verified_at_ms,
        )
        return CommittedGap(marker, committed)
    except BaseException as exc:
        report.update(
            failure_type=type(exc).__name__,
            failure_reason=str(exc) if isinstance(exc, IntegrityError) else "See failure type",
        )
        raise
    finally:
        report["ended_at_ms"] = now_ms()
        await asyncio.to_thread(
            (output / "report.json").write_text,
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
