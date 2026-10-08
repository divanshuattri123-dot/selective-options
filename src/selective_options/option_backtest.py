"""Read-only saved-option quote bridge into the bounded long-premium simulator.

This module only accepts an explicit unit profile and a discovered, active
Deribit option. It models a closed-before-expiry two-order round trip; it is not
an order path, a full option valuation model, or Gate 2 certification.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
)

from .backtest import (
    FeePolicy,
    HistoricQuote,
    InstrumentSpec,
    SimulatedOrder,
    SimulationPolicy,
    SimulationResult,
    simulate,
)
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .models import Instrument, Ticker
from .option_coverage import Universe, status_canonical
from .options_research import load_options_snapshots
from .research import _hash, _path
from .trials import run_registered, trial_counts

_JSON = TypeAdapter(dict[str, JsonValue])
_ASSETS = {"BTC", "ETH", "SOL", "PAXG"}


class OptionExecutionProfile(BaseModel):
    """Explicitly bounded option-unit profile; values are never inferred from a mark.

    For supported Deribit options, displayed depth is native base-currency amount,
    contract size is base currency per contract, and premium is quote currency per
    base-currency unit. The multiplier must therefore match the native contract
    size. This narrow research model supports premium/fee cash flow only, with
    quote and settlement currency equal and no conversion.
    """

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", allow_inf_nan=False)

    profile_id: str = Field(min_length=1, max_length=128)
    asset: Literal["BTC", "ETH", "SOL", "PAXG"]
    quote_currency: str = Field(min_length=1, max_length=16)
    settlement_currency: str = Field(min_length=1, max_length=16)
    instrument_type: Literal["linear", "reversed"]
    expected_contract_size: Decimal = Field(gt=0)
    contract_multiplier: Decimal = Field(gt=0)
    displayed_amount_unit: Literal["base_currency_amount"]
    quote_price_unit: Literal["currency_per_underlying_unit"]

    @field_validator("expected_contract_size", "contract_multiplier")
    @classmethod
    def bounded_decimal(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value > Decimal("1e18"):
            raise ValueError("profile Decimal must be finite and bounded")
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int) or exponent < -18:
            raise ValueError("profile Decimal precision must not exceed 18 places")
        return value


@dataclass(frozen=True, slots=True)
class SavedOptionQuotes:
    """Validated instrument and quote cohort selected from a closed source."""

    database: Path
    source_sha256: str
    asset: str
    symbol: str
    as_of_ms: int
    manifest_event_id: str
    manifest_observed_at_ms: int
    metadata_event_id: str
    instrument: Instrument
    instrument_spec: InstrumentSpec
    quotes: tuple[HistoricQuote, ...]
    max_discovery_age_ms: int
    max_clock_skew_ms: int
    max_rows: int


def _validate_scope(asset: str, symbol: str, as_of_ms: int, max_rows: int) -> None:
    if type(asset) is not str or asset not in _ASSETS:
        raise ValueError("OPTION_BACKTEST_ASSET_INVALID")
    if type(symbol) is not str or not symbol or len(symbol) > 128:
        raise ValueError("OPTION_BACKTEST_SYMBOL_INVALID")
    if type(as_of_ms) is not int or not 0 < as_of_ms <= 2**63 - 1:
        raise ValueError("OPTION_BACKTEST_CUTOFF_INVALID")
    if type(max_rows) is not int or not 1 <= max_rows <= 100000:
        raise ValueError("OPTION_BACKTEST_ROW_BUDGET_INVALID")


def _native_instrument(row: MarketEvent, asset: str) -> Instrument:
    try:
        if (
            row.source != "deribit"
            or row.environment != "testnet"
            or row.asset != asset
            or row.kind != "instrument"
            or row.timestamp_kind != "receipt"
        ):
            raise IntegrityError("OPTION_BACKTEST_METADATA_SCOPE")
        value = Instrument.model_validate_json(row.payload_json, strict=True)
        if value.model_dump(mode="json") != row.payload():
            raise IntegrityError("OPTION_BACKTEST_METADATA_NONCANONICAL")
        expected = make_event(
            source="deribit",
            environment="testnet",
            asset=asset,
            symbol=value.instrument_name,
            kind="instrument",
            event_time_ms=row.observed_at_ms,
            observed_at_ms=row.observed_at_ms,
            timestamp_kind="receipt",
            payload=value.model_dump(mode="json"),
        )
        if expected != row or value.instrument_name != row.symbol:
            raise IntegrityError("OPTION_BACKTEST_METADATA_IDENTITY")
        return value
    except IntegrityError:
        raise
    except (ValueError, TypeError, AttributeError) as exc:
        raise IntegrityError("OPTION_BACKTEST_METADATA_INVALID") from exc


def _native_ticker(
    row: MarketEvent, asset: str, symbol: str, max_clock_skew_ms: int
) -> tuple[Ticker, dict[str, JsonValue]]:
    try:
        if (
            row.source != "deribit"
            or row.environment != "testnet"
            or row.asset != asset
            or row.kind != "ticker"
            or row.symbol != symbol
            or row.timestamp_kind != "source"
        ):
            raise IntegrityError("OPTION_BACKTEST_QUOTE_SCOPE")
        body = row.payload()
        ticker = Ticker.model_validate_json(row.payload_json, strict=True)
        if (
            ticker.instrument_name != symbol
            or ticker.timestamp != row.event_time_ms
            or abs(ticker.timestamp - row.observed_at_ms) > max_clock_skew_ms
        ):
            raise IntegrityError("OPTION_BACKTEST_QUOTE_CLOCK_SKEW_OR_IDENTITY")
        expected = make_event(
            source="deribit",
            environment="testnet",
            asset=asset,
            symbol=symbol,
            kind="ticker",
            event_time_ms=ticker.timestamp,
            observed_at_ms=row.observed_at_ms,
            timestamp_kind="source",
            payload=body,
        )
        if expected != row:
            raise IntegrityError("OPTION_BACKTEST_QUOTE_EVENT_ID")
        # Preserve a bounded source-ahead observation for audit, but the shared
        # simulator rejects it as QUOTE_CLOCK_ORDER before any fill can occur.
        return ticker, body
    except IntegrityError:
        raise
    except (ValueError, TypeError, AttributeError) as exc:
        raise IntegrityError("OPTION_BACKTEST_QUOTE_INVALID") from exc


def _check_profile(
    instrument: Instrument, asset: str, profile: OptionExecutionProfile, as_of_ms: int
) -> InstrumentSpec:
    profile = OptionExecutionProfile.model_validate(profile.model_dump(), strict=True)
    if (
        profile.asset != asset
        or instrument.base_currency != asset
        or instrument.kind != "option"
        or not instrument.is_active
    ):
        raise IntegrityError("OPTION_BACKTEST_INSTRUMENT_INACTIVE_OR_WRONG_ASSET")
    if instrument.expiration_timestamp <= as_of_ms:
        raise IntegrityError("OPTION_BACKTEST_EXPIRED_OR_EXPIRING_AT_CUTOFF")
    if (
        instrument.quote_currency != instrument.settlement_currency
        or profile.quote_currency != instrument.quote_currency
        or profile.settlement_currency != instrument.settlement_currency
        or profile.instrument_type != instrument.instrument_type
    ):
        raise IntegrityError("OPTION_BACKTEST_UNSUPPORTED_CURRENCY_OR_TYPE")
    native_size = Decimal(str(instrument.contract_size))
    if native_size != profile.expected_contract_size:
        raise IntegrityError("OPTION_BACKTEST_CONTRACT_SIZE_PROFILE_MISMATCH")
    if profile.contract_multiplier != native_size:
        raise IntegrityError("OPTION_BACKTEST_MULTIPLIER_MUST_MATCH_NATIVE_CONTRACT_SIZE")
    try:
        return InstrumentSpec(
            instrument_id=instrument.instrument_name,
            currency=profile.settlement_currency,
            quantity_unit="contracts",
            quote_price_unit=profile.quote_price_unit,
            contract_multiplier=profile.contract_multiplier,
            accounting="single_instrument_long_only_linear_premium",
            funding_required=False,
            settlement_required=False,
            transfer_tax_required=False,
        )
    except ValueError as exc:
        raise IntegrityError("OPTION_BACKTEST_UNSUPPORTED_EXECUTION_PROFILE") from exc


def _decimal_field(body: dict[str, JsonValue], name: str, *, allow_zero: bool) -> Decimal | None:
    value = body.get(name)
    if value is None:
        return None
    if type(value) not in (int, float):
        raise IntegrityError("OPTION_BACKTEST_NATIVE_NUMBER_INVALID")
    try:
        number = Decimal(str(value))
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise IntegrityError("OPTION_BACKTEST_NATIVE_NUMBER_INVALID") from exc
    if not number.is_finite() or number < 0 or (number == 0 and not allow_zero):
        raise IntegrityError("OPTION_BACKTEST_NATIVE_NUMBER_INVALID")
    return number


def _tick_for_price(instrument: Instrument, price: Decimal) -> Decimal:
    base_tick = Decimal(str(instrument.tick_size))
    steps = sorted(
        (
            Decimal(str(step.above_price)),
            Decimal(str(step.tick_size)),
        )
        for step in instrument.tick_size_steps
    )
    if len({threshold for threshold, _ in steps}) != len(steps) or any(
        step_tick % base_tick != 0 for _, step_tick in steps
    ):
        raise IntegrityError("OPTION_BACKTEST_TICK_SCHEDULE_INVALID")
    applicable = base_tick
    for threshold, step_tick in steps:
        if price > threshold:
            applicable = step_tick
        else:
            break
    return applicable


def _require_tick_price(instrument: Instrument, price: Decimal) -> None:
    if not price.is_finite() or price <= 0:
        raise IntegrityError("OPTION_BACKTEST_PRICE_INVALID")
    if price % _tick_for_price(instrument, price) != 0:
        raise IntegrityError("OPTION_BACKTEST_PRICE_OFF_TICK")


def option_ticker_to_quote(
    event: MarketEvent,
    *,
    asset: str,
    symbol: str,
    instrument: Instrument,
    profile: OptionExecutionProfile,
    max_clock_skew_ms: int,
) -> HistoricQuote:
    """Map native executable best bid/ask only; IV and marks never set fills."""
    if type(asset) is not str or type(symbol) is not str:
        raise ValueError("OPTION_BACKTEST_SCOPE_INVALID")
    _check_profile(instrument, asset, profile, event.event_time_ms)
    if symbol != instrument.instrument_name:
        raise IntegrityError("OPTION_BACKTEST_INSTRUMENT_SYMBOL_MISMATCH")
    if type(max_clock_skew_ms) is not int or not 0 <= max_clock_skew_ms <= 2**63 - 1:
        raise ValueError("OPTION_BACKTEST_CLOCK_SKEW_POLICY_INVALID")
    _, body = _native_ticker(event, asset, symbol, max_clock_skew_ms)
    bid_value = _decimal_field(body, "best_bid_price", allow_zero=True)
    ask_value = _decimal_field(body, "best_ask_price", allow_zero=True)
    bid = None if bid_value == 0 else bid_value
    ask = None if ask_value == 0 else ask_value
    if bid is not None:
        _require_tick_price(instrument, bid)
    if ask is not None:
        _require_tick_price(instrument, ask)
    bid_amount = _decimal_field(body, "best_bid_amount", allow_zero=True)
    ask_amount = _decimal_field(body, "best_ask_amount", allow_zero=True)
    if bid_amount is None or ask_amount is None:
        raise IntegrityError("OPTION_BACKTEST_DISPLAYED_SIZE_MISSING")
    liquidity_id = hashlib.sha256(f"{symbol}\0{event.event_time_ms}".encode()).hexdigest()
    contract_size = Decimal(str(instrument.contract_size))
    if contract_size <= 0:
        raise IntegrityError("OPTION_BACKTEST_CONTRACT_SIZE_INVALID")
    return HistoricQuote(
        quote_id=event.event_id,
        liquidity_id=liquidity_id,
        instrument_id=symbol,
        source_time_ms=event.event_time_ms,
        observed_at_ms=event.observed_at_ms,
        health="available",
        bid=bid,
        ask=ask,
        # Deribit option quote amounts are native base-currency quantity; its
        # contract_size converts that amount to contract count.
        bid_contracts=bid_amount / contract_size,
        ask_contracts=ask_amount / contract_size,
    )


def _discovery_manifest(
    discovery_rows: tuple[MarketEvent, ...],
    asset: str,
    symbol: str,
    as_of_ms: int,
    max_discovery_age_ms: int,
) -> tuple[MarketEvent, Universe, str]:
    manifests: list[tuple[MarketEvent, Universe]] = []
    for row in discovery_rows:
        body = row.payload()
        if body.get("reason") != "UNIVERSE_DISCOVERED" or body.get("asset") != asset:
            continue
        status_canonical(row)
        try:
            # Validate the canonical JSON representation.  Strict Python-mode
            # validation rejects JSON arrays for tuple fields even though the
            # serialized Pydantic model emits arrays for those tuples.
            universe = Universe.model_validate_json(row.payload_json, strict=True)
        except (ValueError, TypeError) as exc:
            raise IntegrityError("OPTION_BACKTEST_DISCOVERY_INVALID") from exc
        symbols = set(universe.instruments)
        selected = set(universe.selected_instruments)
        if (
            universe.model_dump(mode="json") != body
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
            raise IntegrityError("OPTION_BACKTEST_DISCOVERY_COUNTS_OR_CANONICAL")
        manifests.append((row, universe))
    if not manifests:
        raise IntegrityError("OPTION_BACKTEST_DISCOVERY_MISSING")
    latest_at = max(row.observed_at_ms for row, _ in manifests)
    latest = [(row, universe) for row, universe in manifests if row.observed_at_ms == latest_at]
    if len(latest) != 1:
        raise IntegrityError("OPTION_BACKTEST_DISCOVERY_AMBIGUOUS")
    row, universe = latest[0]
    if as_of_ms - row.observed_at_ms > max_discovery_age_ms:
        raise IntegrityError("OPTION_BACKTEST_DISCOVERY_STALE")
    if symbol not in universe.instruments:
        raise IntegrityError("OPTION_BACKTEST_SYMBOL_NOT_DISCOVERED")
    if symbol not in universe.selected_instruments:
        raise IntegrityError("OPTION_BACKTEST_SYMBOL_NOT_SELECTED")
    metadata_id = dict(zip(universe.instruments, universe.metadata_event_ids, strict=True))[symbol]
    return row, universe, metadata_id


def load_saved_option_quotes(
    database: Path,
    *,
    asset: str,
    symbol: str,
    as_of_ms: int,
    profile: OptionExecutionProfile,
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_rows: int = 100000,
) -> SavedOptionQuotes:
    """Load one discovered option's complete bounded quote history read-only."""
    _validate_scope(asset, symbol, as_of_ms, max_rows)
    if type(max_discovery_age_ms) is not int or not 0 <= max_discovery_age_ms <= 2**63 - 1:
        raise ValueError("OPTION_BACKTEST_DISCOVERY_AGE_POLICY_INVALID")
    if type(max_clock_skew_ms) is not int or not 0 <= max_clock_skew_ms <= 2**63 - 1:
        raise ValueError("OPTION_BACKTEST_CLOCK_SKEW_POLICY_INVALID")
    profile = OptionExecutionProfile.model_validate(profile.model_dump(), strict=True)
    # The shared loader enforces a closed source, byte bound, both clocks before
    # its overflow sentinel, and a before/after source digest.
    snapshot, discovery, digest = load_options_snapshots(
        database, asset=asset, as_of_ms=as_of_ms, max_rows=max_rows
    )
    database = _path(database)
    instruments: dict[str, tuple[MarketEvent, Instrument]] = {}
    quote_events: list[MarketEvent] = []
    for row in snapshot.rows(source="deribit"):
        if row.kind == "instrument" and row.symbol == symbol:
            instrument = _native_instrument(row, asset)
            instruments[row.event_id] = (row, instrument)
        elif row.kind == "ticker" and row.symbol == symbol:
            quote_events.append(row)
    manifest, universe, metadata_id = _discovery_manifest(
        discovery.rows(source="deribit", kind="status"),
        asset,
        symbol,
        as_of_ms,
        max_discovery_age_ms,
    )
    if metadata_id not in instruments:
        raise IntegrityError("OPTION_BACKTEST_BOUND_METADATA_MISSING")
    metadata_event, instrument = instruments[metadata_id]
    if (
        metadata_event.observed_at_ms > manifest.observed_at_ms
        or not instrument.is_active
        or instrument.instrument_name != symbol
        or instrument.base_currency != asset
    ):
        raise IntegrityError("OPTION_BACKTEST_BOUND_METADATA_SCOPE")
    instrument_spec = _check_profile(instrument, asset, profile, as_of_ms)
    selected: list[tuple[MarketEvent, HistoricQuote]] = []
    for event in quote_events:
        ticker, _ = _native_ticker(event, asset, symbol, max_clock_skew_ms)
        if (
            ticker.timestamp < manifest.observed_at_ms
            or event.observed_at_ms < manifest.observed_at_ms
        ):
            continue
        quote = option_ticker_to_quote(
            event,
            asset=asset,
            symbol=symbol,
            instrument=instrument,
            profile=profile,
            max_clock_skew_ms=max_clock_skew_ms,
        )
        selected.append((event, quote))
    if not selected:
        raise IntegrityError("OPTION_BACKTEST_QUOTES_MISSING_AFTER_DISCOVERY")
    source_times = [event.event_time_ms for event, _ in selected]
    if len(source_times) != len(set(source_times)):
        raise IntegrityError("OPTION_BACKTEST_QUOTE_SOURCE_TIME_TIE")
    selected.sort(
        key=lambda pair: (pair[1].source_time_ms, pair[1].observed_at_ms, pair[1].quote_id)
    )
    if _hash(database) != digest or database.with_suffix(database.suffix + ".wal").exists():
        raise IntegrityError("RESEARCH_SOURCE_CHANGED")
    # Confirm the metadata ID still occupies its exact slot in the selected
    # discovery generation; later unreferenced metadata cannot replace it.
    if (
        dict(zip(universe.instruments, universe.metadata_event_ids, strict=True))[symbol]
        != metadata_id
    ):
        raise IntegrityError("OPTION_BACKTEST_METADATA_GENERATION_CHANGED")
    return SavedOptionQuotes(
        database=database,
        source_sha256=digest,
        asset=asset,
        symbol=symbol,
        as_of_ms=as_of_ms,
        manifest_event_id=manifest.event_id,
        manifest_observed_at_ms=manifest.observed_at_ms,
        metadata_event_id=metadata_id,
        instrument=instrument,
        instrument_spec=instrument_spec,
        quotes=tuple(quote for _, quote in selected),
        max_discovery_age_ms=max_discovery_age_ms,
        max_clock_skew_ms=max_clock_skew_ms,
        max_rows=max_rows,
    )


def _validate_roundtrip_orders(
    orders: tuple[SimulatedOrder, ...],
    symbol: str,
    as_of_ms: int,
    expiration_ms: int,
    discovery_observed_at_ms: int,
    instrument: Instrument,
) -> None:
    if not isinstance(orders, tuple) or len(orders) != 2:
        raise ValueError("OPTION_BACKTEST_REQUIRES_ONE_BUY_AND_ONE_SELL")
    buy, sell = orders
    if not isinstance(buy, SimulatedOrder) or not isinstance(sell, SimulatedOrder):
        raise ValueError("OPTION_BACKTEST_TYPED_ORDERS_REQUIRED")
    if (
        buy.side != "buy"
        or sell.side != "sell"
        or buy.instrument_id != symbol
        or sell.instrument_id != symbol
        or buy.contracts != sell.contracts
        or buy.submitted_at_ms >= sell.submitted_at_ms
        or buy.order_type != "passive_limit"
        or sell.order_type != "passive_limit"
        or buy.cancelled_at_ms is not None
        or sell.cancelled_at_ms is not None
    ):
        raise ValueError("OPTION_BACKTEST_ROUNDTRIP_ORDER_SHAPE_INVALID")
    if as_of_ms >= expiration_ms or max(buy.submitted_at_ms, sell.submitted_at_ms) >= expiration_ms:
        raise ValueError("OPTION_BACKTEST_MUST_CLOSE_BEFORE_EXPIRY")
    if buy.submitted_at_ms <= discovery_observed_at_ms:
        raise ValueError("OPTION_BACKTEST_ORDER_NOT_AFTER_DISCOVERY")
    contract_size = Decimal(str(instrument.contract_size))
    minimum_amount = Decimal(str(instrument.min_trade_amount))
    for order in orders:
        amount = order.contracts * contract_size
        if amount < minimum_amount:
            raise ValueError("OPTION_BACKTEST_ORDER_AMOUNT_BELOW_MINIMUM")
        assert order.limit_price is not None
        _require_tick_price(instrument, order.limit_price)


def _summary(result: SimulationResult, input_sha256: str) -> dict[str, JsonValue]:
    return {
        "simulation_id": result.simulation_id,
        "input_sha256": input_sha256,
        "fills": len(result.fills),
        "closed_roundtrip_net_pnl": str(result.closed_roundtrip_net_pnl)
        if result.closed_roundtrip_net_pnl is not None
        else None,
        "net_cash_flow": str(result.net_cash_flow),
        "inventory_contracts": str(result.inventory_contracts),
        "gate2_complete": False,
        "trading_enabled": False,
    }


def research_saved_option_roundtrip(
    database: Path,
    output: Path,
    registry: Path,
    *,
    asset: str,
    symbol: str,
    as_of_ms: int,
    profile: OptionExecutionProfile,
    orders: tuple[SimulatedOrder, ...],
    fees: FeePolicy,
    policy: SimulationPolicy,
    data_kind: Literal["synthetic_fixture", "historical_research"],
    max_discovery_age_ms: int,
    max_clock_skew_ms: int,
    max_rows: int = 100000,
) -> dict[str, JsonValue]:
    """Register and publish one closed-before-expiry saved-option round trip."""
    if data_kind not in ("synthetic_fixture", "historical_research"):
        raise ValueError("OPTION_BACKTEST_DATA_KIND_INVALID")
    output = _path(output, must_exist=False)
    registry = _path(registry, must_exist=False)
    loaded = load_saved_option_quotes(
        database,
        asset=asset,
        symbol=symbol,
        as_of_ms=as_of_ms,
        profile=profile,
        max_discovery_age_ms=max_discovery_age_ms,
        max_clock_skew_ms=max_clock_skew_ms,
        max_rows=max_rows,
    )
    report_path = output / "option-backtest.json"
    if (
        output.exists()
        or loaded.database == registry
        or loaded.database == output
        or registry == output
        or registry == report_path
        or output in registry.parents
    ):
        raise ValueError("OPTION_BACKTEST_PATHS_MUST_BE_DISTINCT_AND_OUTPUT_NEW")
    _validate_roundtrip_orders(
        orders,
        symbol,
        as_of_ms,
        loaded.instrument.expiration_timestamp,
        loaded.manifest_observed_at_ms,
        loaded.instrument,
    )
    if not isinstance(fees, FeePolicy) or not isinstance(policy, SimulationPolicy):
        raise ValueError("OPTION_BACKTEST_TYPED_POLICIES_REQUIRED")
    fees = FeePolicy.model_validate(fees.model_dump(), strict=True)
    policy = SimulationPolicy.model_validate(policy.model_dump(), strict=True)

    parameters = _JSON.validate_python(
        {
            "data_kind": data_kind,
            "asset": asset,
            "symbol": symbol,
            "as_of_ms": as_of_ms,
            "source_database_sha256": loaded.source_sha256,
            "manifest_event_id": loaded.manifest_event_id,
            "manifest_observed_at_ms": loaded.manifest_observed_at_ms,
            "metadata_event_id": loaded.metadata_event_id,
            "quote_event_ids": [quote.quote_id for quote in loaded.quotes],
            "profile": profile.model_dump(mode="json"),
            "orders": [order.model_dump(mode="json") for order in orders],
            "fees": fees.model_dump(mode="json"),
            "policy": policy.model_dump(mode="json"),
            "max_discovery_age_ms": max_discovery_age_ms,
            "max_clock_skew_ms": max_clock_skew_ms,
            "max_materialized_rows": max_rows,
        }
    )
    provenance = _JSON.validate_python(
        {
            "source": "deribit",
            "environment": "testnet",
            "data_kind": data_kind,
            "source_database_sha256": loaded.source_sha256,
            "historical_data_independently_verified": False,
            "manifest_event_id": loaded.manifest_event_id,
            "metadata_event_id": loaded.metadata_event_id,
            "quote_event_ids": [quote.quote_id for quote in loaded.quotes],
            "source_scope": {
                "asset": asset,
                "symbol": symbol,
                "as_of_ms": as_of_ms,
                "max_discovery_age_ms": max_discovery_age_ms,
                "max_clock_skew_ms": max_clock_skew_ms,
                "max_materialized_rows": max_rows,
            },
        }
    )

    def runner() -> SimulationResult:
        if (
            _hash(loaded.database) != loaded.source_sha256
            or loaded.database.with_suffix(loaded.database.suffix + ".wal").exists()
        ):
            raise IntegrityError("RESEARCH_SOURCE_CHANGED")
        result = simulate(
            orders=orders,
            quotes=loaded.quotes,
            instrument=loaded.instrument_spec,
            fees=fees,
            policy=policy,
            as_of_ms=as_of_ms,
        )
        if (
            result.inventory_contracts != 0
            or result.closed_roundtrip_net_pnl is None
            or any(order.status != "filled" for order in result.orders)
        ):
            raise ValueError("OPTION_BACKTEST_ROUNDTRIP_NOT_CLOSED")
        if len(result.fills) < 2 or any(
            fill.filled_at_ms >= loaded.instrument.expiration_timestamp for fill in result.fills
        ):
            raise ValueError("OPTION_BACKTEST_ROUNDTRIP_NOT_FILLED_BEFORE_EXPIRY")
        if (
            _hash(loaded.database) != loaded.source_sha256
            or loaded.database.with_suffix(loaded.database.suffix + ".wal").exists()
        ):
            raise IntegrityError("RESEARCH_SOURCE_CHANGED")
        return result

    handle, result = run_registered(
        registry,
        parameters,
        provenance,
        runner,
        lambda value: _summary(value, loaded.source_sha256),
    )
    if (
        _hash(loaded.database) != loaded.source_sha256
        or loaded.database.with_suffix(loaded.database.suffix + ".wal").exists()
    ):
        raise IntegrityError("RESEARCH_SOURCE_CHANGED")
    value = _JSON.validate_python(
        {
            "trial_id": handle.trial_id,
            "source_database_sha256": loaded.source_sha256,
            "data_kind": data_kind,
            "source_scope_and_cutoff": {
                "asset": asset,
                "symbol": symbol,
                "as_of_ms": as_of_ms,
                "manifest_event_id": loaded.manifest_event_id,
                "manifest_observed_at_ms": loaded.manifest_observed_at_ms,
                "metadata_event_id": loaded.metadata_event_id,
                "expiration_timestamp": loaded.instrument.expiration_timestamp,
                "quote_event_ids": [quote.quote_id for quote in loaded.quotes],
                "quote_source_times_ms": [quote.source_time_ms for quote in loaded.quotes],
                "quote_receipt_times_ms": [quote.observed_at_ms for quote in loaded.quotes],
                "materialized_quotes": len(loaded.quotes),
                "full_history_or_universe_verified": False,
            },
            "execution_profile": profile.model_dump(mode="json"),
            "registry_counts": trial_counts(registry),
            "simulation": result.model_dump(mode="json"),
            "gate2_complete": False,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
    )
    output.mkdir(parents=True, exist_ok=False)
    with report_path.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
    return value
