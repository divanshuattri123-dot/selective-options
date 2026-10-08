"""Causal native-base-amount option put/call ratios, not contract-count ratios.

Deribit documents option open_interest and ticker stats.volume in the underlying
base currency. Volume is the native rolling 24-hour statistic, not a calendar
day count; volume_usd is never used. Amounts are neither multiplied by contract
size nor converted through a price. Each metric aggregates only a complete
discovered member cohort with compatible metadata denomination/reference.
Missing volume can withhold volume while preserving independently complete OI.
Source: https://docs.deribit.com/api-reference/market-data/public-ticker
"""

import hashlib
import json
import math
from fractions import Fraction
from typing import Literal

from pydantic import ConfigDict, StrictInt, StrictStr

from .errors import IntegrityError
from .events import MarketEvent, PointInTimeSnapshot
from .features import _audited_rows, _integer
from .models import ExternalModel, Instrument, Ticker
from .option_coverage import Universe, status_canonical
from .options_features import _native, _native_clocks, _unique


class NativeAmount(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    numerator: StrictStr
    denominator: StrictStr


class PutCallRatio(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    state: Literal["AVAILABLE", "UNAVAILABLE"]
    reason: StrictStr | None
    unit: Literal["native_base_currency"] = "native_base_currency"
    base_currency: StrictStr
    input_complete: bool
    call_total: NativeAmount | None = None
    put_total: NativeAmount | None = None
    ratio_exact: NativeAmount | None = None
    value: float | None = None


class FlowMember(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    symbol: StrictStr
    option_type: Literal["call", "put"]
    metadata_event_id: StrictStr
    metadata_observed_at_ms: StrictInt
    quote_event_id: StrictStr | None = None
    quote_source_time_ms: StrictInt | None = None
    quote_observed_at_ms: StrictInt | None = None
    quote_reason: StrictStr | None = None
    open_interest: NativeAmount | None = None
    volume_24h: NativeAmount | None = None
    volume_reason: StrictStr | None = None
    quote_currency: StrictStr
    settlement_currency: StrictStr
    instrument_type: Literal["linear", "reversed"]
    price_index: StrictStr
    contract_size: float


class FlowExpiry(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    expiration_timestamp: StrictInt
    members: tuple[FlowMember, ...]
    quote_coverage: StrictInt
    blockers: tuple[StrictStr, ...]
    open_interest: PutCallRatio
    volume: PutCallRatio


class PutCallReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["options-put-call-v1"] = "options-put-call-v1"
    asset: StrictStr
    environment: Literal["testnet"] = "testnet"
    as_of_ms: StrictInt
    amount_unit_policy: Literal["deribit_option_base_currency"]
    volume_field: Literal["stats.volume"]
    volume_window: Literal["native_rolling_24h"] = "native_rolling_24h"
    quote_order: Literal["latest_source_timestamp_ties_refuse"] = (
        "latest_source_timestamp_ties_refuse"
    )
    max_quote_age_ms: StrictInt
    max_discovery_age_ms: StrictInt
    max_clock_skew_ms: StrictInt
    max_rows: StrictInt
    max_expiries: StrictInt
    state: Literal["PUT_CALL_RATIOS_COMPUTED", "PARTIAL", "UNAVAILABLE"]
    blockers: tuple[StrictStr, ...]
    manifest_event_id: StrictStr | None
    manifest_observed_at_ms: StrictInt | None
    discovery_coverage: StrictStr | None
    discovered_members: StrictInt
    input_sha256: StrictStr
    input_event_ids: tuple[StrictStr, ...]
    expiries: tuple[FlowExpiry, ...]
    aggregate_open_interest: PutCallRatio
    aggregate_volume: PutCallRatio
    full_chain_verified: Literal[False] = False
    current_feed_health_verified: Literal[False] = False
    executable_quotes_verified: Literal[False] = False
    simultaneous_quote_snapshot_verified: Literal[False] = False
    contract_count_ratio: Literal[False] = False
    interpolated: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def _amount(value: Fraction) -> NativeAmount:
    return NativeAmount(numerator=str(value.numerator), denominator=str(value.denominator))


def _native_amount(value: object) -> Fraction:
    if type(value) not in (int, float):
        raise IntegrityError("OPTIONS_FLOW_NATIVE_AMOUNT_INVALID")
    assert isinstance(value, (int, float))
    if value < 0 or isinstance(value, float) and not math.isfinite(value):
        raise IntegrityError("OPTIONS_FLOW_NATIVE_AMOUNT_INVALID")
    return Fraction(str(value))


def _volume(row: MarketEvent) -> tuple[Fraction | None, str | None]:
    body = row.payload()
    if "stats" not in body:
        return None, "VOLUME_STATS_MISSING"
    stats = body["stats"]
    if stats is None:
        return None, "VOLUME_STATS_NULL"
    if not isinstance(stats, dict):
        raise IntegrityError("OPTIONS_FLOW_VOLUME_STATS_INVALID")
    if "volume" not in stats:
        return None, "VOLUME_FIELD_MISSING"
    if stats["volume"] is None:
        return None, "VOLUME_FIELD_NULL"
    return _native_amount(stats["volume"]), None


def _unavailable(asset: str, reason: str) -> PutCallRatio:
    return PutCallRatio(
        state="UNAVAILABLE", reason=reason, base_currency=asset, input_complete=False
    )


def _ratio(asset: str, calls: Fraction, puts: Fraction) -> PutCallRatio:
    values = {"base_currency": asset, "input_complete": True}
    if calls == 0:
        return PutCallRatio(
            state="UNAVAILABLE",
            reason="CALL_DENOMINATOR_ZERO",
            call_total=_amount(calls),
            put_total=_amount(puts),
            **values,
        )
    exact = puts / calls
    try:
        value = float(exact)
    except OverflowError:
        value = math.inf
    if not math.isfinite(value) or value == 0 and exact != 0:
        return PutCallRatio(
            state="UNAVAILABLE",
            reason="RATIO_FLOAT_UNREPRESENTABLE",
            call_total=_amount(calls),
            put_total=_amount(puts),
            ratio_exact=_amount(exact),
            **values,
        )
    return PutCallRatio(
        state="AVAILABLE",
        reason=None,
        call_total=_amount(calls),
        put_total=_amount(puts),
        ratio_exact=_amount(exact),
        value=value,
        **values,
    )


def _sum_members(asset: str, members: list[FlowMember], *, volume: bool) -> PutCallRatio:
    if not members:
        return _unavailable(asset, "NO_DISCOVERED_MEMBERS")
    units = {
        (row.quote_currency, row.settlement_currency, row.instrument_type, row.price_index)
        for row in members
    }
    if len(units) != 1 or any(not part for unit in units for part in unit):
        return _unavailable(asset, "OPTION_UNIT_OR_REFERENCE_MISMATCH")
    if {row.option_type for row in members} != {"call", "put"}:
        return _unavailable(asset, "DISCOVERED_OPTION_SIDE_MISSING")
    if any(row.quote_reason is not None for row in members):
        return _unavailable(asset, "INCOMPLETE_MEMBER_QUOTES")
    if volume and any(row.volume_reason is not None for row in members):
        return _unavailable(asset, "INCOMPLETE_MEMBER_VOLUME")
    calls, puts = Fraction(0), Fraction(0)
    for row in members:
        native = row.volume_24h if volume else row.open_interest
        if native is None:
            raise IntegrityError("OPTIONS_FLOW_COMPLETE_MEMBER_AMOUNT_MISSING")
        amount = Fraction(int(native.numerator), int(native.denominator))
        if row.option_type == "call":
            calls += amount
        else:
            puts += amount
    return _ratio(asset, calls, puts)


def _member(
    meta: MarketEvent,
    instrument: Instrument,
    history: list[tuple[MarketEvent, Ticker]],
    *,
    as_of_ms: int,
    manifest_ms: int,
    max_quote_age_ms: int,
    max_clock_skew_ms: int,
) -> FlowMember:
    row: MarketEvent | None = None
    reason: str | None = None
    oi: Fraction | None = None
    volume: Fraction | None = None
    volume_reason: str | None = None
    if not history:
        reason = "EXPIRY_QUOTE_MISSING"
    else:
        latest_time = max(pair[0].event_time_ms for pair in history)
        candidates = [pair for pair in history if pair[0].event_time_ms == latest_time]
        if len(candidates) != 1:
            raise IntegrityError("OPTIONS_FLOW_QUOTE_SOURCE_TIME_AMBIGUOUS")
        row, _ = candidates[0]
        oi = _native_amount(row.payload()["open_interest"])
        volume, volume_reason = _volume(row)
        if (
            as_of_ms - row.event_time_ms > max_quote_age_ms
            or as_of_ms - row.observed_at_ms > max_quote_age_ms
        ):
            reason = "EXPIRY_QUOTE_STALE"
        elif row.observed_at_ms < manifest_ms:
            reason = "QUOTE_PREDATES_DISCOVERY"
        elif abs(row.event_time_ms - row.observed_at_ms) > max_clock_skew_ms:
            reason = "QUOTE_CLOCK_SKEW_EXCEEDED"
    return FlowMember(
        symbol=instrument.instrument_name,
        option_type=instrument.option_type,
        metadata_event_id=meta.event_id,
        metadata_observed_at_ms=meta.observed_at_ms,
        quote_event_id=None if row is None else row.event_id,
        quote_source_time_ms=None if row is None else row.event_time_ms,
        quote_observed_at_ms=None if row is None else row.observed_at_ms,
        quote_reason=reason,
        open_interest=None if oi is None else _amount(oi),
        volume_24h=None if volume is None else _amount(volume),
        volume_reason=volume_reason,
        quote_currency=instrument.quote_currency,
        settlement_currency=instrument.settlement_currency,
        instrument_type=instrument.instrument_type,
        price_index=instrument.price_index,
        contract_size=instrument.contract_size,
    )


def compute_put_call_ratios(
    snapshot: PointInTimeSnapshot,
    discovery: PointInTimeSnapshot,
    *,
    amount_unit_policy: Literal["deribit_option_base_currency"],
    volume_field: Literal["stats.volume"],
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_rows: int = 100_000,
    max_expiries: int = 128,
) -> PutCallReport:
    """Compute sums(put native amount)/sums(call native amount), with no fallback.

    All supplied rows are audited; each selected discovery member needs its
    latest source-time quote. Equal source-time observations refuse rather than
    using receipt order. Aggregate metrics use every discovered expiry, never
    just the available expiry ratios, and report zero denominators explicitly.
    Fractions bind the exact canonical JSON numeric values after decoding; they
    do not claim information beyond the precision already saved by ingestion.
    """
    if (
        type(amount_unit_policy) is not str
        or amount_unit_policy != "deribit_option_base_currency"
        or type(volume_field) is not str
        or volume_field != "stats.volume"
    ):
        raise ValueError("explicit supported native base amount and volume field required")
    for name, value, maximum in (
        ("max_quote_age_ms", max_quote_age_ms, 86_400_000),
        ("max_discovery_age_ms", max_discovery_age_ms, 86_400_000),
        ("max_clock_skew_ms", max_clock_skew_ms, 86_400_000),
        ("max_rows", max_rows, 200_000),
        ("max_expiries", max_expiries, 1000),
    ):
        _integer(name, value, 1, maximum)
    raw, controls = _audited_rows(snapshot), _audited_rows(discovery)
    if (
        snapshot.environment != "testnet"
        or discovery.environment != "testnet"
        or snapshot.asset not in {"BTC", "ETH", "SOL", "PAXG"}
        or discovery.asset != "SYSTEM"
        or snapshot.as_of_ms != discovery.as_of_ms
    ):
        raise IntegrityError("OPTIONS_FLOW_SNAPSHOT_SCOPE")
    if len(raw) + len(controls) > max_rows:
        raise IntegrityError("OPTIONS_FLOW_INPUT_BUDGET")
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
    metadata: dict[str, tuple[MarketEvent, Instrument]] = {}
    quotes: dict[str, list[tuple[MarketEvent, Ticker]]] = {}
    for row in _unique(raw).values():
        if row.source != "deribit" or row.kind not in {"instrument", "ticker"}:
            continue
        parsed = _native(row, snapshot.asset)
        if isinstance(parsed, Instrument):
            metadata[row.event_id] = row, parsed
        else:
            _native_amount(row.payload()["open_interest"])
            _volume(row)
            quotes.setdefault(row.symbol, []).append((row, parsed))
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
                raise IntegrityError("OPTIONS_FLOW_DISCOVERY_INVALID") from exc
            names, selected = set(universe.instruments), set(universe.selected_instruments)
            if (
                universe.model_dump(mode="json") != manifest.payload()
                or len(universe.instruments) != universe.available
                or len(universe.selected_instruments) != universe.selected
                or len(names) != universe.available
                or len(selected) != universe.selected
                or len(universe.metadata_event_ids) != universe.available
                or len(set(universe.metadata_event_ids)) != universe.available
                or not selected <= names
                or universe.coverage
                != ("FULL" if names and names == selected else "PARTIAL_OR_ABSENT")
            ):
                raise IntegrityError("OPTIONS_FLOW_DISCOVERY_COUNTS_OR_CANONICAL")
            if universe.coverage != "FULL":
                blockers.append("DISCOVERY_COVERAGE_NOT_FULL")
            if snapshot.as_of_ms - manifest.observed_at_ms > max_discovery_age_ms:
                blockers.append("DISCOVERY_STALE")
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
                raise IntegrityError("OPTIONS_FLOW_METADATA_DISCOVERY_SCOPE")
            if instrument.expiration_timestamp <= snapshot.as_of_ms:
                blockers.append("DISCOVERY_EXPIRED_REDISCOVERY_REQUIRED")
                break
            by_expiry.setdefault(instrument.expiration_timestamp, []).append(pair)
    if len(by_expiry) > max_expiries:
        blockers.append("EXPIRY_BUDGET_EXCEEDED")
    if not by_expiry and not blockers:
        blockers.append("NO_DISCOVERED_EXPIRIES")
    expiries: list[FlowExpiry] = []
    all_members: list[FlowMember] = []
    if not blockers:
        assert manifest is not None
        for expiry, cohort in sorted(by_expiry.items()):
            members = [
                _member(
                    meta,
                    instrument,
                    quotes.get(instrument.instrument_name, []),
                    as_of_ms=snapshot.as_of_ms,
                    manifest_ms=manifest.observed_at_ms,
                    max_quote_age_ms=max_quote_age_ms,
                    max_clock_skew_ms=max_clock_skew_ms,
                )
                for meta, instrument in sorted(cohort, key=lambda pair: pair[1].instrument_name)
            ]
            oi = _sum_members(snapshot.asset, members, volume=False)
            volume = _sum_members(snapshot.asset, members, volume=True)
            expiry_blockers = {
                reason
                for member in members
                for reason in (member.quote_reason, member.volume_reason)
                if reason is not None
            }
            expiry_blockers.update(
                metric.reason for metric in (oi, volume) if metric.reason is not None
            )
            expiries.append(
                FlowExpiry(
                    expiration_timestamp=expiry,
                    members=tuple(members),
                    quote_coverage=sum(member.quote_reason is None for member in members),
                    blockers=tuple(sorted(expiry_blockers)),
                    open_interest=oi,
                    volume=volume,
                )
            )
            all_members.extend(members)
    if blockers:
        aggregate_oi = _unavailable(snapshot.asset, "DISCOVERY_UNAVAILABLE")
        aggregate_volume = _unavailable(snapshot.asset, "DISCOVERY_UNAVAILABLE")
    else:
        aggregate_oi = _sum_members(snapshot.asset, all_members, volume=False)
        aggregate_volume = _sum_members(snapshot.asset, all_members, volume=True)
    if any(row.open_interest.state != "AVAILABLE" for row in expiries):
        blockers.append("INCOMPLETE_EXPIRY_OPEN_INTEREST_RATIOS")
    if any(row.volume.state != "AVAILABLE" for row in expiries):
        blockers.append("INCOMPLETE_EXPIRY_VOLUME_RATIOS")
    if aggregate_oi.state != "AVAILABLE":
        blockers.append("AGGREGATE_OPEN_INTEREST_UNAVAILABLE")
    if aggregate_volume.state != "AVAILABLE":
        blockers.append("AGGREGATE_VOLUME_UNAVAILABLE")
    any_available = aggregate_oi.state == "AVAILABLE" or aggregate_volume.state == "AVAILABLE"
    any_available = any_available or any(
        row.open_interest.state == "AVAILABLE" or row.volume.state == "AVAILABLE"
        for row in expiries
    )
    state: Literal["PUT_CALL_RATIOS_COMPUTED", "PARTIAL", "UNAVAILABLE"]
    state = (
        "PUT_CALL_RATIOS_COMPUTED"
        if not blockers
        else "PARTIAL"
        if any_available
        else "UNAVAILABLE"
    )
    return PutCallReport(
        asset=snapshot.asset,
        as_of_ms=snapshot.as_of_ms,
        amount_unit_policy=amount_unit_policy,
        volume_field=volume_field,
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_clock_skew_ms=max_clock_skew_ms,
        max_rows=max_rows,
        max_expiries=max_expiries,
        state=state,
        blockers=tuple(blockers),
        manifest_event_id=None if manifest is None else manifest.event_id,
        manifest_observed_at_ms=None if manifest is None else manifest.observed_at_ms,
        discovery_coverage=None if universe is None else universe.coverage,
        discovered_members=0 if universe is None else universe.available,
        input_sha256=digest,
        input_event_ids=identities,
        expiries=tuple(expiries),
        aggregate_open_interest=aggregate_oi,
        aggregate_volume=aggregate_volume,
    )
