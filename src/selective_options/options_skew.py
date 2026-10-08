"""Causal, discrete native 25-delta call-minus-put implied-volatility research.

This is a selected mark-IV risk reversal, not a fitted surface, executable
price, signal, or proof that an entire live option chain was streamed. The
feature does not require a two-sided executable quote because it never prices
or simulates a fill; executable quality is explicitly unverified.
"""

from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal
from fractions import Fraction
from typing import Literal

from pydantic import ConfigDict, StrictInt, StrictStr

from .errors import IntegrityError
from .events import MarketEvent, PointInTimeSnapshot
from .features import _audited_rows
from .models import ExternalModel, Instrument, Ticker
from .option_coverage import Universe, status_canonical
from .options_features import _native, _native_clocks, _unique


class SkewSide(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    symbol: StrictStr
    delta: float
    delta_error: float
    mark_iv_points: float
    metadata_event_id: StrictStr
    metadata_observed_at_ms: StrictInt
    quote_event_id: StrictStr
    quote_source_time_ms: StrictInt
    quote_observed_at_ms: StrictInt
    underlying_price: float
    underlying_index: StrictStr
    quote_currency: StrictStr
    settlement_currency: StrictStr
    instrument_type: Literal["linear", "reversed"]


class SkewExpiry(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    expiration_timestamp: StrictInt
    state: Literal["AVAILABLE", "UNAVAILABLE"]
    reason: StrictStr | None
    members: tuple[StrictStr, ...]
    quote_coverage: int
    call: SkewSide | None = None
    put: SkewSide | None = None
    risk_reversal_points: float | None = None


class SkewReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["options-risk-reversal-v1"] = "options-risk-reversal-v1"
    asset: StrictStr
    environment: Literal["testnet"] = "testnet"
    as_of_ms: StrictInt
    target_absolute_delta: float
    maximum_delta_error: float
    max_quote_age_ms: StrictInt
    max_discovery_age_ms: StrictInt
    max_clock_skew_ms: StrictInt
    state: Literal["DISCRETE_RISK_REVERSAL_COMPUTED", "PARTIAL", "UNAVAILABLE"]
    blockers: tuple[StrictStr, ...]
    manifest_event_id: StrictStr | None
    manifest_observed_at_ms: StrictInt | None
    discovery_coverage: StrictStr | None
    discovered_members: StrictInt
    input_sha256: StrictStr
    input_event_ids: tuple[StrictStr, ...]
    expiries: tuple[SkewExpiry, ...]
    full_chain_verified: Literal[False] = False
    current_feed_health_verified: Literal[False] = False
    executable_quotes_verified: Literal[False] = False
    interpolated: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def _clock_policy(name: str, value: int) -> None:
    if type(value) is not int or not 1 <= value <= 86_400_000:
        raise ValueError(f"{name} must be an explicit bounded positive integer")


def _delta_fraction(value: float) -> Fraction:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise IntegrityError("OPTIONS_SKEW_DELTA_INVALID")
    return Fraction(str(value))


def _latest_quote(
    history: list[tuple[MarketEvent, Ticker]],
) -> tuple[MarketEvent, Ticker] | None:
    if not history:
        return None
    latest_source_time = max(row.event_time_ms for row, _ in history)
    candidates = [pair for pair in history if pair[0].event_time_ms == latest_source_time]
    if len(candidates) != 1:
        raise IntegrityError("OPTIONS_SKEW_QUOTE_SOURCE_TIME_AMBIGUOUS")
    return candidates[0]


def _select_side(
    candidates: list[tuple[MarketEvent, Instrument, MarketEvent, Ticker]],
    *,
    option_type: Literal["call", "put"],
    target_absolute_delta: Fraction,
    maximum_delta_error: Fraction,
) -> tuple[MarketEvent, Instrument, MarketEvent, Ticker] | None:
    target = target_absolute_delta if option_type == "call" else -target_absolute_delta
    ranked = sorted(
        candidates,
        key=lambda pair: (
            abs(_delta_fraction(pair[3].greeks.delta) - target),
            pair[1].instrument_name,
        ),
    )
    eligible = [
        pair
        for pair in ranked
        if abs(_delta_fraction(pair[3].greeks.delta) - target) <= maximum_delta_error
    ]
    if not eligible:
        return None
    best_error = abs(_delta_fraction(eligible[0][3].greeks.delta) - target)
    if (
        len(eligible) > 1
        and abs(_delta_fraction(eligible[1][3].greeks.delta) - target) == best_error
    ):
        raise IntegrityError("OPTIONS_SKEW_DELTA_SELECTION_AMBIGUOUS")
    return eligible[0]


def compute_risk_reversal(
    snapshot: PointInTimeSnapshot,
    discovery: PointInTimeSnapshot,
    *,
    target_absolute_delta: float = 0.25,
    maximum_delta_error: float = 0.05,
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_underlying_mismatch_fraction: float = 0.01,
    max_rows: int = 100_000,
    max_expiries: int = 128,
) -> SkewReport:
    """Compute per-expiry call-IV minus put-IV at a discrete delta target.

    Both snapshots must be same-cutoff Deribit testnet views. The latest quote
    for every discovered active member must be present, fresh and unambiguous;
    incomplete coverage blocks skew because it can hide a nearer-delta strike.
    No interpolation, delta conversion or executable fill inference is made.
    """
    if (
        type(target_absolute_delta) not in (int, float)
        or not math.isfinite(target_absolute_delta)
        or not 0 < target_absolute_delta < 1
    ):
        raise ValueError("target_absolute_delta must be finite and strictly between zero and one")
    if (
        type(maximum_delta_error) not in (int, float)
        or not math.isfinite(maximum_delta_error)
        or not 0 <= maximum_delta_error < 1
    ):
        raise ValueError("maximum_delta_error must be finite and in [0, 1)")
    if (
        type(max_underlying_mismatch_fraction) not in (int, float)
        or not math.isfinite(max_underlying_mismatch_fraction)
        or max_underlying_mismatch_fraction < 0
    ):
        raise ValueError("max_underlying_mismatch_fraction must be finite and nonnegative")
    _clock_policy("max_quote_age_ms", max_quote_age_ms)
    _clock_policy("max_discovery_age_ms", max_discovery_age_ms)
    _clock_policy("max_clock_skew_ms", max_clock_skew_ms)
    if type(max_rows) is not int or not 1 <= max_rows <= 200_000:
        raise ValueError("max_rows must be an explicit bounded positive integer")
    if type(max_expiries) is not int or not 1 <= max_expiries <= 1000:
        raise ValueError("max_expiries must be an explicit bounded positive integer")
    if (
        snapshot.environment != "testnet"
        or discovery.environment != "testnet"
        or snapshot.asset not in {"BTC", "ETH", "SOL", "PAXG"}
        or discovery.asset != "SYSTEM"
        or snapshot.as_of_ms != discovery.as_of_ms
    ):
        raise IntegrityError("OPTIONS_SKEW_SNAPSHOT_SCOPE")

    raw, controls = _audited_rows(snapshot), _audited_rows(discovery)
    if len(raw) + len(controls) > max_rows:
        raise IntegrityError("OPTIONS_SKEW_INPUT_BUDGET")
    all_rows = _unique((*raw, *controls))
    for row in all_rows.values():
        _native_clocks(row.payload(), snapshot.as_of_ms)
    identities = tuple(sorted(all_rows))
    digest = hashlib.sha256(
        json.dumps(
            [all_rows[key].model_dump(mode="json") for key in identities],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()

    manifests = []
    for row in _unique(controls).values():
        if (
            row.source == "deribit"
            and row.kind == "status"
            and row.payload().get("reason") == "UNIVERSE_DISCOVERED"
            and row.payload().get("asset") == snapshot.asset
        ):
            status_canonical(row)
            manifests.append(row)
    blockers: list[str] = []
    manifest: MarketEvent | None = None
    universe: Universe | None = None
    if not manifests:
        blockers.append("DISCOVERY_MISSING")
    else:
        latest = max(row.observed_at_ms for row in manifests)
        choices = [row for row in manifests if row.observed_at_ms == latest]
        if len(choices) != 1:
            blockers.append("DISCOVERY_AMBIGUOUS")
        else:
            manifest = choices[0]
            try:
                universe = Universe.model_validate(manifest.payload())
            except (ValueError, TypeError) as exc:
                raise IntegrityError("OPTIONS_SKEW_DISCOVERY_INVALID") from exc
            names, selected = set(universe.instruments), set(universe.selected_instruments)
            if (
                universe.model_dump(mode="json") != manifest.payload()
                or len(names) != universe.available
                or len(selected) != universe.selected
                or len(universe.metadata_event_ids) != universe.available
                or len(set(universe.metadata_event_ids)) != universe.available
                or not selected <= names
                or universe.coverage
                != ("FULL" if names and names == selected else "PARTIAL_OR_ABSENT")
            ):
                raise IntegrityError("OPTIONS_SKEW_DISCOVERY_COUNTS_OR_CANONICAL")
            if universe.coverage != "FULL":
                blockers.append("DISCOVERY_COVERAGE_NOT_FULL")
            if snapshot.as_of_ms - manifest.observed_at_ms > max_discovery_age_ms:
                blockers.append("DISCOVERY_STALE")

    metadata: dict[str, tuple[MarketEvent, Instrument]] = {}
    quotes: dict[str, list[tuple[MarketEvent, Ticker]]] = {}
    for row in _unique(raw).values():
        if row.source != "deribit" or row.kind not in {"instrument", "ticker"}:
            continue
        parsed = _native(row, snapshot.asset)
        if isinstance(parsed, Instrument):
            metadata[row.event_id] = (row, parsed)
        else:
            quotes.setdefault(row.symbol, []).append((row, parsed))

    by_expiry: dict[int, list[tuple[MarketEvent, Instrument]]] = {}
    if universe is not None and manifest is not None and not blockers:
        for name, identity in zip(universe.instruments, universe.metadata_event_ids, strict=True):
            pair = metadata.get(identity)
            if pair is None:
                blockers.append("DISCOVERY_METADATA_MISSING")
                break
            meta, instrument = pair
            if (
                instrument.instrument_name != name
                or instrument.base_currency != snapshot.asset
                or meta.observed_at_ms > manifest.observed_at_ms
                or not instrument.is_active
            ):
                raise IntegrityError("OPTIONS_SKEW_METADATA_DISCOVERY_SCOPE")
            if instrument.expiration_timestamp <= snapshot.as_of_ms:
                blockers.append("DISCOVERY_EXPIRED_REDISCOVERY_REQUIRED")
                break
            by_expiry.setdefault(instrument.expiration_timestamp, []).append((meta, instrument))
    if len(by_expiry) > max_expiries:
        blockers.append("EXPIRY_BUDGET_EXCEEDED")

    expiry_rows: list[SkewExpiry] = []
    target = Fraction(str(target_absolute_delta))
    max_error = Fraction(str(maximum_delta_error))
    if not blockers:
        assert manifest is not None
        for expiry, members in sorted(by_expiry.items()):
            candidates: dict[str, list[tuple[MarketEvent, Instrument, MarketEvent, Ticker]]] = {
                "call": [],
                "put": [],
            }
            reason: str | None = None
            for meta, instrument in sorted(members, key=lambda pair: pair[1].instrument_name):
                latest_quote = _latest_quote(quotes.get(instrument.instrument_name, []))
                if latest_quote is None:
                    reason = reason or "EXPIRY_QUOTE_MISSING"
                    continue
                row, quote = latest_quote
                if (
                    snapshot.as_of_ms - row.event_time_ms > max_quote_age_ms
                    or snapshot.as_of_ms - row.observed_at_ms > max_quote_age_ms
                ):
                    reason = reason or "EXPIRY_QUOTE_STALE"
                    continue
                if row.observed_at_ms < manifest.observed_at_ms:
                    reason = reason or "QUOTE_PREDATES_DISCOVERY"
                    continue
                if abs(row.event_time_ms - row.observed_at_ms) > max_clock_skew_ms:
                    reason = reason or "QUOTE_CLOCK_SKEW_EXCEEDED"
                    continue
                if quote.mark_iv <= 0:
                    reason = reason or "MARK_IV_UNAVAILABLE"
                    continue
                delta = quote.greeks.delta
                if instrument.option_type == "call" and delta <= 0:
                    reason = reason or "CALL_DELTA_SIGN_INVALID"
                    continue
                if instrument.option_type == "put" and delta >= 0:
                    reason = reason or "PUT_DELTA_SIGN_INVALID"
                    continue
                candidates[instrument.option_type].append((meta, instrument, row, quote))
            if reason:
                expiry_rows.append(
                    SkewExpiry(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE",
                        reason=reason,
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    )
                )
                continue
            calls = _select_side(
                candidates["call"],
                option_type="call",
                target_absolute_delta=target,
                maximum_delta_error=max_error,
            )
            puts = _select_side(
                candidates["put"],
                option_type="put",
                target_absolute_delta=target,
                maximum_delta_error=max_error,
            )
            if calls is None or puts is None:
                expiry_rows.append(
                    SkewExpiry(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE",
                        reason="DELTA_TARGET_UNAVAILABLE",
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    )
                )
                continue
            call_meta, call_inst, call_row, call = calls
            put_meta, put_inst, put_row, put = puts
            refs = {
                call_row.payload().get("underlying_index"),
                put_row.payload().get("underlying_index"),
            }
            units = {
                (
                    call_inst.quote_currency,
                    call_inst.settlement_currency,
                    call_inst.instrument_type,
                ),
                (put_inst.quote_currency, put_inst.settlement_currency, put_inst.instrument_type),
            }
            mismatch = math.log(max(call.underlying_price, put.underlying_price)) - math.log(
                min(call.underlying_price, put.underlying_price)
            )
            if len(refs) != 1 or not isinstance(next(iter(refs)), str) or not next(iter(refs)):
                expiry_rows.append(
                    SkewExpiry(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE",
                        reason="UNDERLYING_REFERENCE_MISMATCH",
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    )
                )
                continue
            if len(units) != 1:
                expiry_rows.append(
                    SkewExpiry(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE",
                        reason="OPTION_CONTRACT_UNIT_MISMATCH",
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    )
                )
                continue
            if mismatch > math.log1p(max_underlying_mismatch_fraction):
                expiry_rows.append(
                    SkewExpiry(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE",
                        reason="UNDERLYING_PRICE_MISMATCH",
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    )
                )
                continue
            reference = next(iter(refs))
            call_side = SkewSide(
                symbol=call_inst.instrument_name,
                delta=call.greeks.delta,
                delta_error=float(abs(_delta_fraction(call.greeks.delta) - target)),
                mark_iv_points=call.mark_iv,
                metadata_event_id=call_meta.event_id,
                metadata_observed_at_ms=call_meta.observed_at_ms,
                quote_event_id=call_row.event_id,
                quote_source_time_ms=call_row.event_time_ms,
                quote_observed_at_ms=call_row.observed_at_ms,
                underlying_price=call.underlying_price,
                underlying_index=reference,
                quote_currency=call_inst.quote_currency,
                settlement_currency=call_inst.settlement_currency,
                instrument_type=call_inst.instrument_type,
            )
            put_side = SkewSide(
                symbol=put_inst.instrument_name,
                delta=put.greeks.delta,
                delta_error=float(abs(_delta_fraction(put.greeks.delta) + target)),
                mark_iv_points=put.mark_iv,
                metadata_event_id=put_meta.event_id,
                metadata_observed_at_ms=put_meta.observed_at_ms,
                quote_event_id=put_row.event_id,
                quote_source_time_ms=put_row.event_time_ms,
                quote_observed_at_ms=put_row.observed_at_ms,
                underlying_price=put.underlying_price,
                underlying_index=reference,
                quote_currency=put_inst.quote_currency,
                settlement_currency=put_inst.settlement_currency,
                instrument_type=put_inst.instrument_type,
            )
            rr = float(Decimal(str(call.mark_iv)) - Decimal(str(put.mark_iv)))
            expiry_rows.append(
                SkewExpiry(
                    expiration_timestamp=expiry,
                    state="AVAILABLE",
                    reason=None,
                    members=tuple(sorted(pair[1].instrument_name for pair in members)),
                    quote_coverage=len(candidates["call"]) + len(candidates["put"]),
                    call=call_side,
                    put=put_side,
                    risk_reversal_points=rr,
                )
            )
    if not by_expiry and not blockers:
        blockers.append("NO_DISCOVERED_EXPIRIES")
    if expiry_rows and any(row.state != "AVAILABLE" for row in expiry_rows):
        blockers.append("INCOMPLETE_EXPIRY_RISK_REVERSALS")
    available_expiries = sum(row.state == "AVAILABLE" for row in expiry_rows)
    state: Literal["DISCRETE_RISK_REVERSAL_COMPUTED", "PARTIAL", "UNAVAILABLE"]
    if not blockers:
        state = "DISCRETE_RISK_REVERSAL_COMPUTED"
    elif available_expiries:
        state = "PARTIAL"
    else:
        state = "UNAVAILABLE"
    return SkewReport(
        asset=snapshot.asset,
        as_of_ms=snapshot.as_of_ms,
        target_absolute_delta=target_absolute_delta,
        maximum_delta_error=maximum_delta_error,
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_clock_skew_ms=max_clock_skew_ms,
        state=state,
        blockers=tuple(blockers),
        manifest_event_id=None if manifest is None else manifest.event_id,
        manifest_observed_at_ms=None if manifest is None else manifest.observed_at_ms,
        discovery_coverage=None if universe is None else universe.coverage,
        discovered_members=0 if universe is None else universe.available,
        input_sha256=digest,
        input_event_ids=identities,
        expiries=tuple(expiry_rows),
    )
