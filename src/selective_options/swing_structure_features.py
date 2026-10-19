"""Causally confirmed swing extrema over audited closed bars.

confirmed-swing-bars-v1 reports a bar only after its configured right-hand
confirmation bars have closed. Equal extrema select the rightmost bar in the
local window. The result is descriptive structure, never a direction, signal,
entry, position, or order.
"""

from typing import Literal

from pydantic import ConfigDict

from .events import MarketEvent, PointInTimeSnapshot
from .features import FeatureReport, _bar, _integer, compute_features
from .models import ExternalModel


class ConfirmedPivot(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    kind: Literal["SWING_HIGH", "SWING_LOW"]
    pivot_event_id: str
    pivot_close_ms: int
    pivot_price: float
    confirmation_event_id: str
    confirmation_close_ms: int
    left_window_event_ids: tuple[str, ...]
    right_confirmation_event_ids: tuple[str, ...]


class SwingStructureReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["confirmed-swing-bars-v1"] = "confirmed-swing-bars-v1"
    base: FeatureReport
    left_bars: int
    right_bars: int
    bars_required: int
    state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"]
    reason: str | None
    candidate_count: int
    pivots: tuple[ConfirmedPivot, ...]
    input_event_ids: tuple[str, ...]
    tie_policy: Literal["rightmost_equal_extreme"] = "rightmost_equal_extreme"
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def compute_swing_structure(
    snapshot: PointInTimeSnapshot,
    *,
    source: str,
    symbol: str,
    timeframe_s: int,
    left_bars: int = 2,
    right_bars: int = 2,
    max_bars: int = 2000,
    max_latest_age_ms: int | None = None,
) -> SwingStructureReport:
    """Report local extrema only once all right-side bars are closed and known."""
    _integer("left_bars", left_bars, 1, 1000)
    _integer("right_bars", right_bars, 1, 1000)
    base = compute_features(
        snapshot,
        source=source,
        symbol=symbol,
        timeframe_s=timeframe_s,
        max_bars=max_bars,
        max_latest_age_ms=max_latest_age_ms,
    )
    selected: dict[str, MarketEvent] = {}
    for row in snapshot.rows(source, "bar"):
        if row.symbol != symbol or _bar(row).timeframe_s != timeframe_s:
            continue
        previous = selected.get(row.event_id)
        if previous is not None and previous != row:
            raise ValueError("SWING_STRUCTURE_CONFLICTING_EVENT_ID")
        selected[row.event_id] = row
    rows = sorted(selected.values(), key=lambda row: (row.event_time_ms, row.event_id))
    ids = tuple(row.event_id for row in rows)
    required = left_bars + right_bars + 1
    candidates = max(0, len(rows) - left_bars - right_bars)
    pivots: list[ConfirmedPivot] = []
    state: Literal["AVAILABLE", "WARMUP", "UNAVAILABLE"] = "AVAILABLE"
    reason: str | None = None
    if base.blockers:
        state, reason = "UNAVAILABLE", base.blockers[0]
    elif len(rows) < required:
        state, reason = "WARMUP", "INSUFFICIENT_CLOSED_BARS"
    else:
        for index in range(left_bars, len(rows) - right_bars):
            try:
                bars = [_bar(row) for row in rows[index - left_bars : index + right_bars + 1]]
                current = bars[left_bars]
                left, right = bars[:left_bars], bars[left_bars + 1 :]
                left_ids = tuple(row.event_id for row in rows[index - left_bars : index])
                confirmation_rows = rows[index + 1 : index + right_bars + 1]
                right_ids = tuple(row.event_id for row in confirmation_rows)
                confirmation = confirmation_rows[-1]
                if current.high >= max(bar.high for bar in left) and current.high > max(
                    bar.high for bar in right
                ):
                    pivots.append(
                        ConfirmedPivot(
                            kind="SWING_HIGH",
                            pivot_event_id=rows[index].event_id,
                            pivot_close_ms=rows[index].event_time_ms,
                            pivot_price=current.high,
                            confirmation_event_id=confirmation.event_id,
                            confirmation_close_ms=confirmation.event_time_ms,
                            left_window_event_ids=left_ids,
                            right_confirmation_event_ids=right_ids,
                        )
                    )
                if current.low <= min(bar.low for bar in left) and current.low < min(
                    bar.low for bar in right
                ):
                    pivots.append(
                        ConfirmedPivot(
                            kind="SWING_LOW",
                            pivot_event_id=rows[index].event_id,
                            pivot_close_ms=rows[index].event_time_ms,
                            pivot_price=current.low,
                            confirmation_event_id=confirmation.event_id,
                            confirmation_close_ms=confirmation.event_time_ms,
                            left_window_event_ids=left_ids,
                            right_confirmation_event_ids=right_ids,
                        )
                    )
            except (ArithmeticError, OverflowError, ValueError):
                state, reason, pivots = "UNAVAILABLE", "NUMERIC_UNAVAILABLE", []
                break
    pivots.sort(key=lambda pivot: (pivot.confirmation_close_ms, pivot.pivot_close_ms, pivot.kind))
    return SwingStructureReport(
        base=base,
        left_bars=left_bars,
        right_bars=right_bars,
        bars_required=required,
        state=state,
        reason=reason,
        candidate_count=candidates if state != "UNAVAILABLE" or not base.blockers else 0,
        pivots=tuple(pivots),
        input_event_ids=ids if not base.blockers else (),
    )
