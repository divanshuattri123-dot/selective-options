"""Offline, conservative quote-based linear-premium accounting; no execution API.

A displayed quote is not a trade confirmation. Fills below are explicit research
assumptions, including strictly crossed passive limits charged as taker fills.
This bounded foundation does not satisfy all specification Gate 2 requirements.
"""

from __future__ import annotations

import hashlib
import json
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ZERO = Decimal("0")
ONE = Decimal("1")


class FrozenInput(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", allow_inf_nan=False)

    @field_validator("*", mode="after")
    @classmethod
    def bounded_decimal(cls, value: object) -> object:
        if isinstance(value, Decimal):
            if not value.is_finite() or value.copy_abs() > Decimal("1e18"):
                raise ValueError("finite Decimal magnitude must be at most 1e18")
            exponent = value.as_tuple().exponent
            if not isinstance(exponent, int) or exponent < -18:
                raise ValueError("Decimal precision must not exceed 18 fractional places")
        return value


class InstrumentSpec(FrozenInput):
    instrument_id: str = Field(min_length=1, max_length=128)
    currency: str = Field(min_length=1, max_length=16)
    quantity_unit: Literal["contracts"]
    quote_price_unit: Literal["currency_per_underlying_unit"]
    contract_multiplier: Decimal = Field(gt=0)
    accounting: Literal["single_instrument_long_only_linear_premium"]
    funding_required: bool
    settlement_required: bool
    transfer_tax_required: bool


class FeePolicy(FrozenInput):
    """Caller-supplied research policy, never a venue fee schedule default."""

    policy_id: str = Field(min_length=1, max_length=128)
    maker_rate: Decimal = Field(ge=0, le=1)
    taker_rate: Decimal = Field(ge=0, le=1)
    basis: Literal["executed_premium_notional"]


class SimulationPolicy(FrozenInput):
    latency_ms: int = Field(ge=0, le=86_400_000)
    max_quote_age_ms: int = Field(ge=0, le=86_400_000)
    min_displayed_contracts: Decimal = Field(gt=0)
    adverse_slippage_rate: Decimal = Field(ge=0, le=1)
    pessimistic: bool
    passive_model: Literal["strict_later_executable_crossing_taker_assumption"]


class HistoricQuote(FrozenInput):
    quote_id: str = Field(min_length=1, max_length=128)
    liquidity_id: str = Field(min_length=1, max_length=128)
    instrument_id: str = Field(min_length=1, max_length=128)
    source_time_ms: int = Field(gt=0, le=2**63 - 1)
    observed_at_ms: int = Field(gt=0, le=2**63 - 1)
    health: Literal["available", "failed", "unavailable"]
    bid: Decimal | None = Field(default=None, gt=0)
    ask: Decimal | None = Field(default=None, gt=0)
    bid_contracts: Decimal | None = Field(default=None, ge=0)
    ask_contracts: Decimal | None = Field(default=None, ge=0)


class SimulatedOrder(FrozenInput):
    order_id: str = Field(min_length=1, max_length=128)
    instrument_id: str = Field(min_length=1, max_length=128)
    side: Literal["buy", "sell"]
    order_type: Literal["market", "passive_limit"]
    contracts: Decimal = Field(gt=0)
    submitted_at_ms: int = Field(gt=0, le=2**63 - 1)
    limit_price: Decimal | None = Field(default=None, gt=0)
    cancelled_at_ms: int | None = Field(default=None, gt=0, le=2**63 - 1)

    @model_validator(mode="after")
    def order_shape(self) -> Self:
        if (self.order_type == "passive_limit") != (self.limit_price is not None):
            raise ValueError("only passive_limit orders must supply a limit price")
        if self.cancelled_at_ms is not None and self.cancelled_at_ms < self.submitted_at_ms:
            raise ValueError("cancellation cannot precede submission")
        return self


class FrozenResult(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", allow_inf_nan=False)


class Fill(FrozenResult):
    fill_id: str
    order_id: str
    quote_id: str
    liquidity_id: str
    instrument_id: str
    side: Literal["buy", "sell"]
    filled_at_ms: int
    quote_source_time_ms: int
    quote_observed_at_ms: int
    contracts: Decimal
    executable_touch: Decimal
    execution_price: Decimal
    premium_notional: Decimal
    fee: Decimal
    slippage_cost: Decimal
    cash_delta: Decimal
    liquidity_assumption: Literal["displayed_executable_quote_not_confirmed_execution"]
    fee_role: Literal["taker"]
    fee_policy_id: str


class NoFill(FrozenResult):
    order_id: str
    quote_id: str
    at_ms: int
    reason: str


class OrderOutcome(FrozenResult):
    order_id: str
    filled_contracts: Decimal
    remaining_contracts: Decimal
    status: Literal["filled", "cancelled", "partially_filled", "unfilled", "not_submitted"]


class SimulationResult(FrozenResult):
    simulation_id: str
    instrument: InstrumentSpec
    fee_policy: FeePolicy
    policy: SimulationPolicy
    decimal_precision: Literal[160] = 160
    schema_version: Literal[1] = 1
    as_of_ms: int
    fills: tuple[Fill, ...]
    no_fills: tuple[NoFill, ...]
    orders: tuple[OrderOutcome, ...]
    inventory_contracts: Decimal
    premium_cost_basis: Decimal
    entry_fee_cost_basis: Decimal
    net_cash_flow: Decimal
    total_fees: Decimal
    total_slippage: Decimal
    gross_realized_pnl: Decimal
    net_realized_pnl: Decimal
    closed_roundtrip_net_pnl: Decimal | None
    funding_cashflow: Decimal | None
    assumptions: tuple[str, ...]
    funding_supported: Literal[False] = False
    maker_execution_supported: Literal[False] = False
    history_complete: Literal[False] = False
    gate2_complete: Literal[False] = False
    ready_for_signals: Literal[False] = False
    trading_enabled: Literal[False] = False


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _quote_reason(quote: HistoricQuote, policy: SimulationPolicy, at: int) -> str | None:
    if quote.health != "available":
        return "QUOTE_UNAVAILABLE"
    if quote.source_time_ms > quote.observed_at_ms:
        return "QUOTE_CLOCK_ORDER"
    if at - quote.source_time_ms > policy.max_quote_age_ms:
        return "STALE_QUOTE"
    if (
        quote.bid is None
        or quote.ask is None
        or quote.bid_contracts is None
        or quote.ask_contracts is None
    ):
        return "MISSING_TWO_SIDED_QUOTE"
    if quote.bid > quote.ask:
        return "CROSSED_QUOTE"
    if min(quote.bid_contracts, quote.ask_contracts) < policy.min_displayed_contracts:
        return "THIN_QUOTE"
    return None


def simulate(
    *,
    orders: tuple[SimulatedOrder, ...],
    quotes: tuple[HistoricQuote, ...],
    instrument: InstrumentSpec,
    fees: FeePolicy,
    policy: SimulationPolicy,
    as_of_ms: int,
) -> SimulationResult:
    """Pure bounded simulation. Caller must register every actual research run.

    Both quote clocks must be >= order submission + fixed latency. At equal
    times cancellations precede quotes; orders consume liquidity by submission
    time then lexical ID. A liquidity_id denotes one displayed native allocation
    across repeated observations. No fresh depth is inferred from a new receipt.
    Budgets: 1000 orders, 10000 quotes, 100000 eligible order-quote decisions.
    Unsupported funding/settlement/tax requirements refuse the whole simulation.
    Orders persist until filled, cancelled or the as-of cutoff; IOC/FOK behavior
    and short-sale execution are outside this model. A sell with no inventory
    remains unfilled and can execute only if later long inventory exists.
    Open inventory has no mark-based valuation or claimed roundtrip P&L.
    """
    if type(as_of_ms) is not int or not 0 < as_of_ms <= 2**63 - 1:
        raise ValueError("AS_OF_INVALID")
    if not isinstance(orders, tuple) or not isinstance(quotes, tuple):
        raise ValueError("IMMUTABLE_INPUT_TUPLES_REQUIRED")
    if len(orders) > 1000 or len(quotes) > 10000:
        raise ValueError("SIMULATION_BUDGET_EXCEEDED")
    if (
        not isinstance(instrument, InstrumentSpec)
        or not isinstance(fees, FeePolicy)
        or not isinstance(policy, SimulationPolicy)
    ):
        raise ValueError("TYPED_POLICY_INPUTS_REQUIRED")
    instrument = InstrumentSpec.model_validate(instrument.model_dump(), strict=True)
    fees = FeePolicy.model_validate(fees.model_dump(), strict=True)
    policy = SimulationPolicy.model_validate(policy.model_dump(), strict=True)
    if (
        instrument.funding_required
        or instrument.settlement_required
        or instrument.transfer_tax_required
    ):
        raise ValueError("UNSUPPORTED_FUNDING_SETTLEMENT_OR_TRANSFER_TAX")
    if any(not isinstance(order, SimulatedOrder) for order in orders) or any(
        not isinstance(quote, HistoricQuote) for quote in quotes
    ):
        raise ValueError("TYPED_INPUTS_REQUIRED")
    orders = tuple(
        SimulatedOrder.model_validate(order.model_dump(), strict=True) for order in orders
    )
    quotes = tuple(
        HistoricQuote.model_validate(quote.model_dump(), strict=True) for quote in quotes
    )
    if any(order.instrument_id != instrument.instrument_id for order in orders) or any(
        quote.instrument_id != instrument.instrument_id for quote in quotes
    ):
        raise ValueError("UNSUPPORTED_MULTIPLE_INSTRUMENTS")
    if len({order.order_id for order in orders}) != len(orders):
        raise ValueError("DUPLICATE_ORDER_ID")
    if len({quote.quote_id for quote in quotes}) != len(quotes):
        raise ValueError("DUPLICATE_QUOTE_ID")
    eligible = sorted(
        (
            quote
            for quote in quotes
            if quote.source_time_ms <= as_of_ms and quote.observed_at_ms <= as_of_ms
        ),
        key=lambda quote: (max(quote.source_time_ms, quote.observed_at_ms), quote.quote_id),
    )
    if len(orders) * len(eligible) > 100_000:
        raise ValueError("SIMULATION_DECISION_BUDGET_EXCEEDED")
    liquidity_identity: dict[str, tuple[object, ...]] = {}
    for quote in eligible:
        signature = (
            quote.instrument_id,
            quote.source_time_ms,
            quote.health,
            quote.bid,
            quote.ask,
            quote.bid_contracts,
            quote.ask_contracts,
        )
        if (
            quote.liquidity_id in liquidity_identity
            and liquidity_identity[quote.liquidity_id] != signature
        ):
            raise ValueError("CONFLICTING_NATIVE_LIQUIDITY_ID")
        liquidity_identity[quote.liquidity_id] = signature
    ordered = sorted(orders, key=lambda order: (order.submitted_at_ms, order.order_id))
    identity = _digest(
        {
            "orders": [order.model_dump(mode="json") for order in ordered],
            "eligible_quotes": [quote.model_dump(mode="json") for quote in eligible],
            "instrument": instrument.model_dump(mode="json"),
            "fees": fees.model_dump(mode="json"),
            "policy": policy.model_dump(mode="json"),
            "as_of_ms": as_of_ms,
        }
    )
    with localcontext(Context(prec=160, rounding=ROUND_HALF_EVEN)):
        return _simulate(ordered, eligible, instrument, fees, policy, as_of_ms, identity)


def _simulate(
    orders: list[SimulatedOrder],
    quotes: list[HistoricQuote],
    instrument: InstrumentSpec,
    fees: FeePolicy,
    policy: SimulationPolicy,
    as_of_ms: int,
    identity: str,
) -> SimulationResult:
    remaining = {order.order_id: order.contracts for order in orders}
    used: dict[tuple[str, str], Decimal] = {}
    original_depth: dict[tuple[str, str], Decimal] = {}
    fills: list[Fill] = []
    rejected: list[NoFill] = []
    inventory = cash = total_fees = slippage = premium_basis = fee_basis = gross_realized = (
        net_realized
    ) = ZERO
    for quote in quotes:
        at = max(quote.source_time_ms, quote.observed_at_ms)
        quote_reason = _quote_reason(quote, policy, at)
        for order in orders:
            if remaining[order.order_id] == ZERO or order.submitted_at_ms > at:
                continue
            if order.cancelled_at_ms is not None and order.cancelled_at_ms <= at:
                continue
            reason = quote_reason
            ready = order.submitted_at_ms + policy.latency_ms
            if min(quote.source_time_ms, quote.observed_at_ms) < ready:
                reason = "BEFORE_SUBMISSION_OR_LATENCY"
            if reason is not None:
                rejected.append(
                    NoFill(
                        order_id=order.order_id, quote_id=quote.quote_id, at_ms=at, reason=reason
                    )
                )
                continue
            assert quote.ask is not None and quote.bid is not None
            assert quote.ask_contracts is not None and quote.bid_contracts is not None
            touch = quote.ask if order.side == "buy" else quote.bid
            if order.order_type == "passive_limit":
                assert order.limit_price is not None
                if (
                    min(quote.source_time_ms, quote.observed_at_ms) <= ready
                    or (order.side == "buy" and touch >= order.limit_price)
                    or (order.side == "sell" and touch <= order.limit_price)
                ):
                    rejected.append(
                        NoFill(
                            order_id=order.order_id,
                            quote_id=quote.quote_id,
                            at_ms=at,
                            reason="PASSIVE_TOUCH_NOT_EXECUTION",
                        )
                    )
                    continue
            price = touch * (
                ONE + policy.adverse_slippage_rate
                if order.side == "buy"
                else ONE - policy.adverse_slippage_rate
            )
            if price <= ZERO or (
                order.limit_price is not None
                and (
                    (order.side == "buy" and price > order.limit_price)
                    or (order.side == "sell" and price < order.limit_price)
                )
            ):
                rejected.append(
                    NoFill(
                        order_id=order.order_id,
                        quote_id=quote.quote_id,
                        at_ms=at,
                        reason="SLIPPAGE_OUTSIDE_LIMIT",
                    )
                )
                continue
            side_key = (quote.liquidity_id, order.side)
            depth = quote.ask_contracts if order.side == "buy" else quote.bid_contracts
            original_depth.setdefault(side_key, depth)
            available = max(ZERO, min(depth, original_depth[side_key]) - used.get(side_key, ZERO))
            quantity = min(remaining[order.order_id], available)
            if order.side == "sell":
                quantity = min(quantity, inventory)
            if quantity == ZERO:
                rejected.append(
                    NoFill(
                        order_id=order.order_id,
                        quote_id=quote.quote_id,
                        at_ms=at,
                        reason="LIQUIDITY_EXHAUSTED_OR_NO_LONG_INVENTORY",
                    )
                )
                continue
            notional = price * quantity * instrument.contract_multiplier
            fee = notional * fees.taker_rate
            cost = abs(price - touch) * quantity * instrument.contract_multiplier
            delta = -notional - fee if order.side == "buy" else notional - fee
            if order.side == "buy":
                inventory += quantity
                premium_basis += notional
                fee_basis += fee
            else:
                allocated_premium = (
                    premium_basis if quantity == inventory else premium_basis * quantity / inventory
                )
                allocated_fee = (
                    fee_basis if quantity == inventory else fee_basis * quantity / inventory
                )
                gross_realized += notional - allocated_premium
                net_realized += notional - allocated_premium - allocated_fee - fee
                premium_basis -= allocated_premium
                fee_basis -= allocated_fee
                inventory -= quantity
            remaining[order.order_id] -= quantity
            used[side_key] = used.get(side_key, ZERO) + quantity
            cash += delta
            total_fees += fee
            slippage += cost
            fills.append(
                Fill(
                    fill_id=_digest([identity, order.order_id, quote.quote_id, len(fills)]),
                    order_id=order.order_id,
                    quote_id=quote.quote_id,
                    liquidity_id=quote.liquidity_id,
                    instrument_id=instrument.instrument_id,
                    side=order.side,
                    filled_at_ms=at,
                    quote_source_time_ms=quote.source_time_ms,
                    quote_observed_at_ms=quote.observed_at_ms,
                    contracts=quantity,
                    executable_touch=touch,
                    execution_price=price,
                    premium_notional=notional,
                    fee=fee,
                    slippage_cost=cost,
                    cash_delta=delta,
                    liquidity_assumption="displayed_executable_quote_not_confirmed_execution",
                    fee_role="taker",
                    fee_policy_id=fees.policy_id,
                )
            )
    outcomes: list[OrderOutcome] = []
    for order in orders:
        left = remaining[order.order_id]
        status: Literal["filled", "cancelled", "partially_filled", "unfilled", "not_submitted"]
        if order.submitted_at_ms > as_of_ms:
            status = "not_submitted"
        elif left == ZERO:
            status = "filled"
        elif order.cancelled_at_ms is not None and order.cancelled_at_ms <= as_of_ms:
            status = "cancelled"
        elif left < order.contracts:
            status = "partially_filled"
        else:
            status = "unfilled"
        outcomes.append(
            OrderOutcome(
                order_id=order.order_id,
                filled_contracts=order.contracts - left,
                remaining_contracts=left,
                status=status,
            )
        )
    if inventory == ZERO:
        net_realized = cash
        gross_realized = cash + total_fees
    return SimulationResult(
        simulation_id=identity,
        instrument=instrument,
        fee_policy=fees,
        policy=policy,
        as_of_ms=as_of_ms,
        fills=tuple(fills),
        no_fills=tuple(rejected),
        orders=tuple(outcomes),
        inventory_contracts=inventory,
        premium_cost_basis=premium_basis,
        entry_fee_cost_basis=fee_basis,
        net_cash_flow=cash,
        total_fees=total_fees,
        total_slippage=slippage,
        gross_realized_pnl=gross_realized,
        net_realized_pnl=net_realized,
        closed_roundtrip_net_pnl=cash if inventory == ZERO else None,
        funding_cashflow=None,
        assumptions=(
            "OFFLINE_RESEARCH_REQUIRES_EXTERNAL_TRIAL_REGISTRATION",
            "single instrument, long-only, linear premium; no settlement/funding/transfer tax support",
            "both quote clocks after latency; cancellation before equal-time quote",
            "orders persist until filled, cancelled or as-of; no IOC/FOK/short execution model",
            "native liquidity_id shared across receipts and orders; depth consumption never resets",
            "displayed touch is assumed executable, not a confirmed fill",
            "passive equality is unfilled; strictly later crossing is a taker assumption",
            "both modes deliberately use the same conservative far touch; flag does not create an optimistic baseline",
            "equal availability timestamps sorted by quote_id are a deterministic assumption, not native chronology",
            "caller supplied fees and adverse slippage; no venue fee or currency rounding claim",
            "160-digit ROUND_HALF_EVEN Decimal arithmetic independent of caller context; no mark/mid valuation, capital/margin model or completeness claim",
        ),
    )
