"""Canonical, scoped as-of revision selection for sampled public flow observations."""

from collections.abc import Iterable
from typing import Literal

from pydantic import JsonValue

from .binance import Instrument
from .binance_accounts import account_events
from .binance_taker import taker_events
from .bybit import FeedUnavailable
from .errors import IntegrityError
from .events import MarketEvent, make_event

FlowKind = Literal["taker_flow", "account_ratio"]
SOURCES = {"taker_flow": "binance_taker", "account_ratio": "binance_accounts"}


def _validate(event: MarketEvent) -> None:
    try:
        payload = event.payload()
        instrument = Instrument.model_validate(payload["instrument"])
        if event.kind == "taker_flow":
            raw: JsonValue = [
                {
                    "timestamp": payload["period_start_ms"],
                    "buyVol": payload["buy_volume"],
                    "sellVol": payload["sell_volume"],
                    "buySellRatio": payload["buy_sell_ratio"],
                }
            ]
            canonical = taker_events(
                instrument, raw, event.observed_at_ms, event.observed_at_ms, check_latest=False
            )
        else:
            raw = [
                {
                    "symbol": event.symbol,
                    "timestamp": payload["period_end_ms"],
                    "longAccount": payload["reported_longAccount"],
                    "shortAccount": payload["reported_shortAccount"],
                    "longShortRatio": payload["reported_longShortRatio"],
                }
            ]
            canonical = account_events(
                instrument, raw, event.observed_at_ms, event.observed_at_ms, check_latest=False
            )
        if "observation_received_at_ms" not in payload:
            # Pre-0.3.22 archives retained only the first receipt of an identical value.
            body = canonical[0].payload()
            del body["observation_received_at_ms"]
            canonical = [
                make_event(
                    source=canonical[0].source,
                    environment="public_reference",
                    asset=canonical[0].asset,
                    symbol=canonical[0].symbol,
                    kind=canonical[0].kind,
                    event_time_ms=canonical[0].event_time_ms,
                    observed_at_ms=event.observed_at_ms,
                    native_id=str(
                        payload["period_start_ms"]
                        if event.kind == "taker_flow"
                        else payload["period_end_ms"]
                    ),
                    payload=body,
                )
            ]
        if canonical != [event]:
            raise IntegrityError("FLOW_OBSERVATION_IDENTITY_OR_PAYLOAD")
    except (KeyError, ValueError, IntegrityError, FeedUnavailable) as exc:
        raise IntegrityError("FLOW_OBSERVATION_INVALID") from exc


def flow_at(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    symbol: str,
    kind: FlowKind,
    as_of_ms: int,
    max_rows: int = 10_000,
) -> tuple[MarketEvent, ...]:
    """One latest *observed* revision per period; never infer first publication or finality."""
    if (
        kind not in SOURCES
        or type(as_of_ms) is not int
        or as_of_ms <= 0
        or type(max_rows) is not int
        or not 1 <= max_rows <= 100_000
        or not asset.strip()
        or not symbol.strip()
    ):
        raise ValueError("valid scope, positive as-of clock and row budget are required")
    unique: dict[str, MarketEvent] = {}
    inspected = 0
    for event in events:
        if (event.source, event.environment, event.asset, event.symbol, event.kind) != (
            SOURCES[kind],
            "public_reference",
            asset,
            symbol,
            kind,
        ) or max(event.event_time_ms, event.observed_at_ms) > as_of_ms:
            continue
        inspected += 1
        if inspected > max_rows:
            raise IntegrityError("FLOW_VIEW_OVER_BUDGET")
        _validate(event)
        prior = unique.get(event.event_id)
        if prior is None or event.observed_at_ms < prior.observed_at_ms:
            # Repeated sampling of an identical observation cannot become a later revision.
            unique[event.event_id] = event
    periods: dict[int, list[MarketEvent]] = {}
    for event in unique.values():
        periods.setdefault(event.event_time_ms, []).append(event)
    selected = []
    for _, rows in sorted(periods.items()):
        latest = max(e.observed_at_ms for e in rows)
        candidates = [e for e in rows if e.observed_at_ms == latest]
        if len(candidates) != 1:
            raise IntegrityError("FLOW_VIEW_AMBIGUOUS_REVISION")
        selected.append(candidates[0])
    return tuple(selected)


def flow_view_summary(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    symbol: str,
    kind: FlowKind,
    as_of_ms: int,
) -> dict[str, object]:
    """Projection integrity only; freshness, health, semantics and continuity remain separate."""
    selected: tuple[MarketEvent, ...] = ()
    blocker: str | None = None
    try:
        selected = flow_at(events, asset=asset, symbol=symbol, kind=kind, as_of_ms=as_of_ms)
    except IntegrityError as exc:
        blocker = str(exc)
    return {
        "source": SOURCES[kind],
        "asset": asset,
        "symbol": symbol,
        "kind": kind,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED" if blocker else "REVISION_VIEW_VERIFIED" if selected else "MISSING",
        "blocker": blocker,
        "selected_periods": len(selected),
        "latest_event_id": selected[-1].event_id if selected else None,
        "latest_period_end_ms": selected[-1].event_time_ms if selected else None,
        "latest_observed_at_ms": selected[-1].observed_at_ms if selected else None,
        "scope": "One latest observed revision per sampled period; health/freshness/semantics/continuity assessed separately",
        "legacy_limitation": "Pre-0.3.22 rows omit repeated poll receipts; historical reversions cannot be reconstructed",
        "trading_approval": False,
    }
