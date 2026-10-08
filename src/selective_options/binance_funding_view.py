"""Canonical as-of funding observations; one revision per reported charge in a window."""

from collections.abc import Iterable

from .binance import Instrument
from .binance_funding_history import parse_page
from .errors import IntegrityError
from .events import MarketEvent


def _validate(event: MarketEvent) -> None:
    try:
        body = event.payload()
        instrument = Instrument.model_validate(body["instrument"])
        canonical = parse_page(
            instrument,
            [
                {
                    "symbol": event.symbol,
                    "fundingTime": event.event_time_ms,
                    "fundingRate": body["funding_rate"],
                    "markPrice": body["mark_price_at_charge"],
                    "rateType": body["reported_rate_type"],
                }
            ],
            event.observed_at_ms,
            event.event_time_ms,
            event.event_time_ms,
            1,
        )
        if canonical != (event,):
            raise IntegrityError("BINANCE_FUNDING_OBSERVATION_IDENTITY_OR_PAYLOAD")
    except (ValueError, KeyError, IntegrityError) as exc:
        raise IntegrityError("BINANCE_FUNDING_OBSERVATION_INVALID") from exc


def funding_history_at(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    as_of_ms: int,
    max_rows: int = 10_000,
) -> tuple[MarketEvent, ...]:
    """Return latest observed revisions, filtering both clocks before strict validation."""
    if (
        not asset.strip()
        or not symbol.strip()
        or any(type(v) is not int or v <= 0 for v in (start_ms, end_ms, as_of_ms, max_rows))
        or start_ms > end_ms
        or max_rows > 100_000
    ):
        raise ValueError("positive integer clocks/budget, valid scope and ordered window required")
    charges: dict[int, dict[str, MarketEvent]] = {}
    inspected = 0
    for row in events:
        if (
            (row.source, row.environment, row.asset, row.symbol, row.kind)
            != ("binance_history", "public_reference", asset, symbol, "funding")
            or not start_ms <= row.event_time_ms <= end_ms
            or max(row.event_time_ms, row.observed_at_ms) > as_of_ms
        ):
            continue
        inspected += 1
        if inspected > max_rows:
            raise IntegrityError("BINANCE_FUNDING_VIEW_OVER_BUDGET")
        _validate(row)
        charges.setdefault(row.event_time_ms, {})[row.event_id] = row
    selected = []
    for _, revisions in sorted(charges.items()):
        receipt = max(row.observed_at_ms for row in revisions.values())
        candidates = [row for row in revisions.values() if row.observed_at_ms == receipt]
        if len(candidates) != 1:
            raise IntegrityError("BINANCE_FUNDING_VIEW_AMBIGUOUS_REVISION")
        selected.append(candidates[0])
    return tuple(selected)


def funding_view_summary(
    events: Iterable[MarketEvent],
    *,
    asset: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    as_of_ms: int,
    max_rows: int = 10_000,
) -> dict[str, object]:
    selected: tuple[MarketEvent, ...] = ()
    blocker: str | None = None
    try:
        selected = funding_history_at(
            events,
            asset=asset,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            as_of_ms=as_of_ms,
            max_rows=max_rows,
        )
    except IntegrityError as exc:
        blocker = str(exc)
    return {
        "source": "binance_history",
        "asset": asset,
        "symbol": symbol,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED" if blocker else "REVISION_VIEW_VERIFIED" if selected else "MISSING",
        "blocker": blocker,
        "selected_charges": len(selected),
        "selected_event_ids": [row.event_id for row in selected],
        "latest_funding_time_ms": selected[-1].event_time_ms if selected else None,
        "latest_selected_receipt_ms": max(row.observed_at_ms for row in selected)
        if selected
        else None,
        "scope": "One latest observed revision per reported charge timestamp; no rate summation, schedule completeness, historical intervals, first publication or finality inferred",
        "historical_schedule_completeness": "NOT_PROVEN",
        "trading_approval": False,
    }
