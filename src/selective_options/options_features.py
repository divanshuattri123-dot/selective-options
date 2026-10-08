"""options-term-v1: discrete same-type ATM marks from an explicit discovered universe.

ATM minimizes abs(log(strike)-log(ticker.underlying_price)); this is the venue's
native underlying reference, NOT an assumed spot or forward. underlying_index
must be explicit and identical within expiry; it may name a future/synthetic or
index_price. No interpolation or averaging of calls/puts occurs. Exact distance
ties are unavailable. Ordering and ties compare exact folded strike/underlying
ratios of the native binary floats, avoiding cancellation in logarithms. EVERY
discovered same-type member of an expiry must have a
fresh quote, so missing nearer strikes cannot cause a farther-strike fallback.
mark_iv remains Deribit percentage points. Adjacent-expiry differences compared
with caller flat_threshold_iv_points classify discrete selected marks only.
This is not a fitted surface, complete chain, 25-delta skew, or strategy signal.
Native field contract: https://docs.deribit.com/#ticker-instrument_name-interval
"""

import hashlib
import json
import math
from fractions import Fraction
from typing import Literal

from pydantic import ConfigDict

from .errors import IntegrityError
from .events import MarketEvent, PointInTimeSnapshot, make_event
from .features import _audited_rows, _integer
from .models import ExternalModel, Instrument, Ticker
from .option_coverage import Universe, status_canonical


class OptionsPolicy(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    option_type: Literal["call", "put"]
    moneyness_method: Literal["native_underlying_log_distance"] = "native_underlying_log_distance"
    tie_policy: Literal["exact_distance_refuse"] = "exact_distance_refuse"
    quote_order: Literal["source_timestamp_ties_refuse"] = "source_timestamp_ties_refuse"
    max_quote_age_ms: int
    max_discovery_age_ms: int
    max_atm_log_moneyness: float
    max_underlying_mismatch_fraction: float
    flat_threshold_iv_points: float
    max_rows: int
    max_expiries: int


class OptionExpiry(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    expiration_timestamp: int
    state: Literal["AVAILABLE", "UNAVAILABLE"]
    reason: str | None
    members: tuple[str, ...]
    eligible_quotes: int
    symbol: str | None = None
    strike: float | None = None
    mark_iv_points: float | None = None
    underlying_price: float | None = None
    underlying_index: str | None = None
    log_moneyness_distance: float | None = None
    metadata_event_id: str | None = None
    quote_event_id: str | None = None
    quote_source_time_ms: int | None = None
    quote_observed_at_ms: int | None = None
    metadata_observed_at_ms: int | None = None
    quote_currency: str | None = None
    settlement_currency: str | None = None
    instrument_type: str | None = None


class OptionsFeatureReport(ExternalModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    formula_version: Literal["options-term-v1"] = "options-term-v1"
    asset: str
    environment: Literal["testnet"] = "testnet"
    as_of_ms: int
    policy: OptionsPolicy
    state: Literal["DISCRETE_TERM_COMPUTED", "UNAVAILABLE"]
    blockers: tuple[str, ...]
    manifest_event_id: str | None
    manifest_observed_at_ms: int | None
    discovery_declared_coverage: str | None
    discovered_members: int
    input_sha256: str
    input_event_ids: tuple[str, ...]
    expiries: tuple[OptionExpiry, ...]
    term_shape: Literal["CONTANGO", "FLAT", "BACKWARDATION", "MIXED", "UNAVAILABLE"]
    adjacent_iv_differences_points: tuple[float, ...]
    full_chain_verified: Literal[False] = False
    current_feed_health_verified: Literal[False] = False
    interpolated: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def _unique(rows: tuple[MarketEvent, ...]) -> dict[str, MarketEvent]:
    result: dict[str, MarketEvent] = {}
    for row in rows:
        if row.event_id in result and result[row.event_id] != row:
            raise IntegrityError("OPTIONS_CONFLICTING_IDENTITY")
        result[row.event_id] = row
    return result


def _folded_ratio(strike: float, underlying: float) -> Fraction:
    ratio = Fraction(strike) / Fraction(underlying)
    return max(ratio, 1 / ratio)


def _native(row: MarketEvent, asset: str) -> Instrument | Ticker:
    try:
        kind: Literal["source", "receipt"]
        if row.kind == "instrument":
            parsed: Instrument | Ticker = Instrument.model_validate_json(
                row.payload_json, strict=True
            )
            body = parsed.model_dump(mode="json")
            if body != row.payload():
                raise IntegrityError("OPTIONS_METADATA_PAYLOAD_NONCANONICAL")
            source_time, kind = row.observed_at_ms, "receipt"
        else:
            parsed = Ticker.model_validate_json(row.payload_json, strict=True)
            body = row.payload()
            source_time, kind = parsed.timestamp, "source"
        expected = make_event(
            source="deribit",
            environment="testnet",
            asset=asset,
            symbol=parsed.instrument_name,
            kind=row.kind,
            event_time_ms=source_time,
            observed_at_ms=row.observed_at_ms,
            timestamp_kind=kind,
            payload=body,
        )
        if expected != row:
            raise IntegrityError("OPTIONS_NATIVE_ENVELOPE_NONCANONICAL")
        return parsed
    except (ValueError, TypeError, AttributeError) as exc:
        raise IntegrityError("OPTIONS_INVALID_NATIVE_PAYLOAD") from exc


def _native_clocks(body: object, cutoff: int) -> None:
    if isinstance(body, dict):
        for key, value in body.items():
            if key in {"timestamp", "creation_timestamp", "event_time_ms", "observed_at_ms"}:
                if type(value) is not int or not 0 < value <= cutoff:
                    raise IntegrityError("OPTIONS_NESTED_CLOCK_INVALID_OR_FUTURE")
            _native_clocks(value, cutoff)
    elif isinstance(body, list):
        for value in body:
            _native_clocks(value, cutoff)


def compute_options_features(
    snapshot: PointInTimeSnapshot,
    discovery: PointInTimeSnapshot,
    *,
    option_type: Literal["call", "put"],
    moneyness_method: Literal["native_underlying_log_distance"],
    tie_policy: Literal["exact_distance_refuse"],
    max_quote_age_ms: int,
    max_discovery_age_ms: int,
    max_atm_log_moneyness: float,
    max_underlying_mismatch_fraction: float,
    flat_threshold_iv_points: float,
    max_rows: int = 100000,
    max_expiries: int = 128,
) -> OptionsFeatureReport:
    """Two audited same-cutoff snapshots; SYSTEM discovery binds asset metadata."""
    if option_type not in ("call", "put") or type(option_type) is not str:
        raise ValueError("explicit call/put option type required")
    if (
        type(moneyness_method) is not str
        or type(tie_policy) is not str
        or moneyness_method != "native_underlying_log_distance"
        or tie_policy != "exact_distance_refuse"
    ):
        raise ValueError("unsupported explicit moneyness method or tie policy")
    for name, value, maximum in (
        ("max_quote_age_ms", max_quote_age_ms, 86400000),
        ("max_discovery_age_ms", max_discovery_age_ms, 86400000),
        ("max_rows", max_rows, 200000),
        ("max_expiries", max_expiries, 1000),
    ):
        _integer(name, value, 1, maximum)
    for name, value in (
        ("max_atm_log_moneyness", max_atm_log_moneyness),
        ("max_underlying_mismatch_fraction", max_underlying_mismatch_fraction),
        ("flat_threshold_iv_points", flat_threshold_iv_points),
    ):
        try:
            valid = type(value) in (int, float) and math.isfinite(value) and value >= 0
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError(f"{name} requires an explicit finite nonnegative number")
    raw, controls = _audited_rows(snapshot), _audited_rows(discovery)
    if (
        snapshot.environment != "testnet"
        or discovery.environment != "testnet"
        or discovery.asset != "SYSTEM"
        or snapshot.asset not in {"BTC", "ETH", "SOL", "PAXG"}
        or snapshot.as_of_ms != discovery.as_of_ms
    ):
        raise IntegrityError("OPTIONS_SNAPSHOT_SCOPE")
    if len(raw) + len(controls) > max_rows:
        raise IntegrityError("OPTIONS_INPUT_BUDGET")
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
    policy = OptionsPolicy(
        option_type=option_type,
        max_quote_age_ms=max_quote_age_ms,
        max_discovery_age_ms=max_discovery_age_ms,
        max_atm_log_moneyness=max_atm_log_moneyness,
        max_underlying_mismatch_fraction=max_underlying_mismatch_fraction,
        flat_threshold_iv_points=flat_threshold_iv_points,
        max_rows=max_rows,
        max_expiries=max_expiries,
    )
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
    manifest = None
    universe = None
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
                raise IntegrityError("OPTIONS_INVALID_DISCOVERY_PAYLOAD") from exc
            symbols = set(universe.instruments)
            selected = set(universe.selected_instruments)
            if (
                universe.model_dump(mode="json") != manifest.payload()
                or len(symbols) != universe.available
                or len(universe.instruments) != universe.available
                or len(selected) != universe.selected
                or len(universe.selected_instruments) != universe.selected
                or not selected <= symbols
                or len(universe.metadata_event_ids) != universe.available
                or len(set(universe.metadata_event_ids)) != universe.available
                or universe.coverage
                != ("FULL" if selected == symbols and symbols else "PARTIAL_OR_ABSENT")
            ):
                raise IntegrityError("OPTIONS_DISCOVERY_COUNTS_OR_CANONICAL")
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
        for symbol, identity in zip(universe.instruments, universe.metadata_event_ids, strict=True):
            if identity not in metadata:
                blockers.append("DISCOVERY_METADATA_MISSING")
                break
            row, instrument = metadata[identity]
            if (
                instrument.instrument_name != symbol
                or instrument.base_currency != snapshot.asset
                or row.observed_at_ms > manifest.observed_at_ms
                or not instrument.is_active
            ):
                raise IntegrityError("OPTIONS_METADATA_DISCOVERY_SCOPE")
            if instrument.expiration_timestamp <= snapshot.as_of_ms:
                blockers.append("DISCOVERY_EXPIRED_REDISCOVERY_REQUIRED")
                break
            if instrument.option_type == option_type:
                by_expiry.setdefault(instrument.expiration_timestamp, []).append((row, instrument))
    if len(by_expiry) > max_expiries:
        blockers.append("EXPIRY_BUDGET_EXCEEDED")
    expiries: list[OptionExpiry] = []
    if not blockers:
        for expiry, members in sorted(by_expiry.items()):
            candidates: list[tuple[MarketEvent, Instrument, MarketEvent, Ticker, str]] = []
            reason = None
            for meta, instrument in sorted(members, key=lambda pair: pair[1].instrument_name):
                history = quotes.get(instrument.instrument_name, [])
                if not history:
                    reason = reason or "EXPIRY_QUOTE_MISSING"
                    continue
                key = max(row.event_time_ms for row, _ in history)
                choices = [pair for pair in history if pair[0].event_time_ms == key]
                if len(choices) != 1:
                    reason = reason or "EXPIRY_QUOTE_AMBIGUOUS"
                    continue
                row, quote = choices[0]
                reference = row.payload().get("underlying_index")
                if (
                    snapshot.as_of_ms - row.event_time_ms > max_quote_age_ms
                    or snapshot.as_of_ms - row.observed_at_ms > max_quote_age_ms
                ):
                    reason = reason or "EXPIRY_QUOTE_STALE"
                elif row.observed_at_ms < manifest.observed_at_ms:
                    reason = reason or "QUOTE_PREDATES_DISCOVERY"
                elif type(reference) is not str or not reference.strip():
                    reason = reason or "UNDERLYING_REFERENCE_MISSING"
                elif quote.mark_iv <= 0:
                    reason = reason or "MARK_IV_UNAVAILABLE"
                else:
                    candidates.append((meta, instrument, row, quote, reference))
            fields: dict[str, object] = {}
            if not reason and candidates:
                references = {pair[4] for pair in candidates}
                underlying = [pair[3].underlying_price for pair in candidates]
                units = {
                    (pair[1].quote_currency, pair[1].settlement_currency, pair[1].instrument_type)
                    for pair in candidates
                }
                if len(units) != 1:
                    reason = "OPTION_CONTRACT_UNIT_MISMATCH"
                elif len(references) != 1:
                    reason = "UNDERLYING_REFERENCE_MISMATCH"
                elif math.log(max(underlying)) - math.log(min(underlying)) > math.log1p(
                    max_underlying_mismatch_fraction
                ):
                    reason = "UNDERLYING_PRICE_MISMATCH"
                else:
                    ranked = sorted(
                        candidates,
                        key=lambda pair: (
                            _folded_ratio(pair[1].strike, pair[3].underlying_price),
                            pair[1].instrument_name,
                        ),
                    )
                    chosen = ranked[0]
                    distance = abs(
                        math.log(chosen[1].strike) - math.log(chosen[3].underlying_price)
                    )
                    if len(ranked) > 1 and _folded_ratio(
                        chosen[1].strike, chosen[3].underlying_price
                    ) == _folded_ratio(ranked[1][1].strike, ranked[1][3].underlying_price):
                        reason = "ATM_DISTANCE_AMBIGUOUS"
                    elif distance > max_atm_log_moneyness:
                        reason = "ATM_DISTANCE_EXCEEDED"
                    else:
                        meta, instrument, row, quote, reference = chosen
                        fields = dict(
                            symbol=instrument.instrument_name,
                            strike=instrument.strike,
                            mark_iv_points=quote.mark_iv,
                            underlying_price=quote.underlying_price,
                            underlying_index=reference,
                            log_moneyness_distance=distance,
                            metadata_event_id=meta.event_id,
                            quote_event_id=row.event_id,
                            quote_source_time_ms=row.event_time_ms,
                            quote_observed_at_ms=row.observed_at_ms,
                            metadata_observed_at_ms=meta.observed_at_ms,
                            quote_currency=instrument.quote_currency,
                            settlement_currency=instrument.settlement_currency,
                            instrument_type=instrument.instrument_type,
                        )
            expiries.append(
                OptionExpiry.model_validate(
                    dict(
                        expiration_timestamp=expiry,
                        state="UNAVAILABLE" if reason else "AVAILABLE",
                        reason=reason,
                        members=tuple(sorted(pair[1].instrument_name for pair in members)),
                        eligible_quotes=len(candidates),
                        **fields,
                    )
                )
            )
    if not expiries and not blockers:
        blockers.append("NO_MATCHING_EXPIRIES")
    if any(row.state != "AVAILABLE" for row in expiries):
        blockers.append("INCOMPLETE_EXPIRY_MARKS")
    if len(expiries) < 2 and not blockers:
        blockers.append("TERM_REQUIRES_TWO_EXPIRIES")
    differences: tuple[float, ...] = ()
    shape: Literal["CONTANGO", "FLAT", "BACKWARDATION", "MIXED", "UNAVAILABLE"] = "UNAVAILABLE"
    if not blockers:
        ivs = [row.mark_iv_points for row in expiries]
        assert all(value is not None for value in ivs)
        differences = tuple(
            float(right) - float(left)
            for left, right in zip(ivs, ivs[1:], strict=False)
            if left is not None and right is not None
        )
        if all(abs(value) <= flat_threshold_iv_points for value in differences):
            shape = "FLAT"
        elif all(value > flat_threshold_iv_points for value in differences):
            shape = "CONTANGO"
        elif all(value < -flat_threshold_iv_points for value in differences):
            shape = "BACKWARDATION"
        else:
            shape = "MIXED"
    return OptionsFeatureReport(
        asset=snapshot.asset,
        as_of_ms=snapshot.as_of_ms,
        policy=policy,
        state="UNAVAILABLE" if blockers else "DISCRETE_TERM_COMPUTED",
        blockers=tuple(blockers),
        manifest_event_id=None if manifest is None else manifest.event_id,
        manifest_observed_at_ms=None if manifest is None else manifest.observed_at_ms,
        discovery_declared_coverage=None if universe is None else universe.coverage,
        discovered_members=0 if universe is None else universe.available,
        input_sha256=digest,
        input_event_ids=identities,
        expiries=tuple(expiries),
        term_shape=shape,
        adjacent_iv_differences_points=differences,
    )
