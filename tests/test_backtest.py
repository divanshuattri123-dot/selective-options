"""Independent arithmetic and no-optimistic-fill tests for the offline foundation."""

from decimal import ROUND_UP, Decimal, Inexact, getcontext, setcontext

import pytest
from pydantic import ValidationError

from selective_options.backtest import (
    FeePolicy,
    HistoricQuote,
    InstrumentSpec,
    SimulatedOrder,
    SimulationPolicy,
    simulate,
)

D = Decimal


def instrument(**changes):
    return InstrumentSpec(
        **dict(
            instrument_id="X",
            currency="USD",
            quantity_unit="contracts",
            quote_price_unit="currency_per_underlying_unit",
            contract_multiplier=D("10"),
            accounting="single_instrument_long_only_linear_premium",
            funding_required=False,
            settlement_required=False,
            transfer_tax_required=False,
            **changes,
        )
    )


def policy(**changes):
    values = dict(
        latency_ms=10,
        max_quote_age_ms=50,
        min_displayed_contracts=D("0.1"),
        adverse_slippage_rate=D("0"),
        pessimistic=True,
        passive_model="strict_later_executable_crossing_taker_assumption",
    )
    values.update(changes)
    return SimulationPolicy(**values)


def fee(**changes):
    values = dict(
        policy_id="fixture-explicit-not-venue",
        maker_rate=D("0.0005"),
        taker_rate=D("0.001"),
        basis="executed_premium_notional",
    )
    values.update(changes)
    return FeePolicy(**values)


def quote(quote_id="q", **changes):
    values = dict(
        quote_id=quote_id,
        liquidity_id=quote_id,
        instrument_id="X",
        source_time_ms=110,
        observed_at_ms=110,
        health="available",
        bid=D("99"),
        ask=D("101"),
        bid_contracts=D("2"),
        ask_contracts=D("2"),
    )
    values.update(changes)
    return HistoricQuote(**values)


def order(order_id="buy", **changes):
    values = dict(
        order_id=order_id,
        instrument_id="X",
        side="buy",
        order_type="market",
        contracts=D("2"),
        submitted_at_ms=100,
    )
    values.update(changes)
    return SimulatedOrder(**values)


def run(orders=None, quotes=None, **changes):
    values = dict(
        orders=(order(),) if orders is None else orders,
        quotes=(quote(),) if quotes is None else quotes,
        instrument=instrument(),
        fees=fee(),
        policy=policy(),
        as_of_ms=200,
    )
    values.update(changes)
    return simulate(**values)


def test_known_bad_roundtrip_exact_spread_and_fee_loss():
    result = run(
        orders=(order(), order("sell", side="sell", submitted_at_ms=120)),
        quotes=(quote(), quote("exit", source_time_ms=130, observed_at_ms=130)),
    )
    assert [fill.execution_price for fill in result.fills] == [D("101"), D("99")]
    assert [fill.premium_notional for fill in result.fills] == [D("2020"), D("1980")]
    assert [fill.fee for fill in result.fills] == [D("2.020"), D("1.980")]
    assert result.gross_realized_pnl == D("-40")
    assert result.total_fees == D("4")
    assert (
        result.net_realized_pnl
        == result.net_cash_flow
        == result.closed_roundtrip_net_pnl
        == D("-44")
    )
    assert result.inventory_contracts == 0 and result.total_slippage == 0
    assert result.funding_cashflow is None
    assert not result.gate2_complete and not result.trading_enabled


@pytest.mark.parametrize("changes", [dict(source_time_ms=201), dict(observed_at_ms=201)])
def test_each_future_clock_physically_excluded_and_identity_unchanged(changes):
    baseline = run(quotes=())
    result = run(quotes=(quote(**changes),))
    assert result == baseline


@pytest.mark.parametrize(
    "changes",
    [
        dict(source_time_ms=109),
        dict(observed_at_ms=109, source_time_ms=109),
        dict(source_time_ms=99, observed_at_ms=110),
    ],
)
def test_no_pre_submission_or_pre_latency_fill(changes):
    result = run(quotes=(quote(**changes),))
    assert not result.fills
    assert result.no_fills[0].reason == "BEFORE_SUBMISSION_OR_LATENCY"


@pytest.mark.parametrize(
    "changes,reason",
    [
        (dict(health="failed"), "QUOTE_UNAVAILABLE"),
        (dict(health="unavailable"), "QUOTE_UNAVAILABLE"),
        (dict(ask=None), "MISSING_TWO_SIDED_QUOTE"),
        (dict(bid_contracts=None), "MISSING_TWO_SIDED_QUOTE"),
        (dict(bid=D("102")), "CROSSED_QUOTE"),
        (dict(ask_contracts=D("0.01")), "THIN_QUOTE"),
        (dict(bid_contracts=D("0")), "THIN_QUOTE"),
        (dict(source_time_ms=110, observed_at_ms=170), "STALE_QUOTE"),
        (dict(source_time_ms=120, observed_at_ms=110), "QUOTE_CLOCK_ORDER"),
    ],
)
def test_bad_quotes_reject_without_fill(changes, reason):
    result = run(quotes=(quote(**changes),))
    assert not result.fills and result.net_cash_flow == 0
    assert result.no_fills[0].reason == reason


def test_shared_liquidity_partial_fill_and_receipt_replay_not_refresh():
    orders = (order("a", contracts=D("1.5")), order("b", contracts=D("1.5")))
    result = run(
        orders=orders,
        quotes=(
            quote("q1", liquidity_id="same"),
            quote("q2", liquidity_id="same", source_time_ms=110, observed_at_ms=120),
        ),
    )
    assert [fill.contracts for fill in result.fills] == [D("1.5"), D("0.5")]
    assert [outcome.remaining_contracts for outcome in result.orders] == [D("0"), D("1")]
    assert result.orders[1].status == "partially_filled"
    assert sum(fill.contracts for fill in result.fills) == 2


def test_independent_native_liquidity_allocation_can_fill_remaining():
    result = run(
        orders=(order(contracts=D("3")),),
        quotes=(quote(), quote("q2", source_time_ms=120, observed_at_ms=120)),
    )
    assert [fill.contracts for fill in result.fills] == [D("2"), D("1")]


def test_passive_touch_unfilled_then_strict_later_crossing_taker():
    limit = order(order_type="passive_limit", limit_price=D("101"))
    result = run(
        orders=(limit,),
        quotes=(
            quote(),
            quote("equal", source_time_ms=120, observed_at_ms=120),
            quote("cross", source_time_ms=130, observed_at_ms=130, ask=D("100")),
        ),
    )
    assert [fill.quote_id for fill in result.fills] == ["cross"]
    assert result.fills[0].fee_role == "taker"
    assert result.fills[0].fee == D("2")
    assert [item.reason for item in result.no_fills] == ["PASSIVE_TOUCH_NOT_EXECUTION"] * 2


def test_initial_cross_at_ready_is_not_passive_execution():
    result = run(orders=(order(order_type="passive_limit", limit_price=D("102")),))
    assert not result.fills


def test_adverse_slippage_separate_fees_no_mid_or_mark():
    result = run(policy=policy(adverse_slippage_rate=D("0.01")))
    fill = result.fills[0]
    assert fill.executable_touch == D("101") and fill.execution_price == D("102.01")
    assert fill.slippage_cost == D("20.20")
    assert fill.fee == D("2.0402")
    assert fill.cash_delta == D("-2042.2402")
    assert result.closed_roundtrip_net_pnl is None


def test_slippage_does_not_violate_limit():
    result = run(
        orders=(order(order_type="passive_limit", limit_price=D("101.5")),),
        quotes=(quote(source_time_ms=120, observed_at_ms=120),),
        policy=policy(adverse_slippage_rate=D("0.01")),
    )
    assert not result.fills
    assert result.no_fills[0].reason == "SLIPPAGE_OUTSIDE_LIMIT"


def test_sell_cannot_create_short_and_caps_to_actual_long_inventory():
    result = run(
        orders=(
            order("sell-first", side="sell", submitted_at_ms=90),
            order("buy", contracts=D("1")),
            order("sell-later", side="sell", submitted_at_ms=120),
        ),
        quotes=(quote(), quote("q2", source_time_ms=130, observed_at_ms=130)),
    )
    assert [fill.order_id for fill in result.fills] == ["buy", "sell-first"]
    assert result.inventory_contracts == 0
    assert result.orders[0].status == "partially_filled"


def test_equal_time_cancellation_before_quote_and_partial_cancel():
    result = run(orders=(order(cancelled_at_ms=110),))
    assert not result.fills and result.orders[0].status == "cancelled"
    partial = run(
        orders=(order(contracts=D("3"), cancelled_at_ms=120),),
        quotes=(quote(), quote("q2", source_time_ms=120, observed_at_ms=120)),
    )
    assert partial.orders[0].status == "cancelled" and partial.orders[0].filled_contracts == 2


def test_deterministic_sorting_and_decimal_context_isolation():
    orders = (order("b"), order("a"))
    quotes = (quote("later", source_time_ms=120, observed_at_ms=120), quote())
    baseline = run(orders=orders, quotes=quotes)
    previous = getcontext().prec
    try:
        getcontext().prec = 6
        assert run(orders=tuple(reversed(orders)), quotes=tuple(reversed(quotes))) == baseline
    finally:
        getcontext().prec = previous
    assert len({fill.fill_id for fill in baseline.fills}) == len(baseline.fills)


@pytest.mark.parametrize(
    "field", ["funding_required", "settlement_required", "transfer_tax_required"]
)
def test_required_unsupported_costs_refuse_not_silent_zero(field):
    values = instrument().model_dump()
    values[field] = True
    with pytest.raises(ValueError, match="UNSUPPORTED"):
        run(instrument=InstrumentSpec(**values))


def test_multi_instrument_and_duplicate_identity_refuse():
    with pytest.raises(ValueError, match="MULTIPLE_INSTRUMENTS"):
        run(quotes=(quote(instrument_id="Y"),))
    with pytest.raises(ValueError, match="DUPLICATE_ORDER_ID"):
        run(orders=(order(), order()))
    with pytest.raises(ValueError, match="DUPLICATE_QUOTE_ID"):
        run(quotes=(quote(), quote()))


@pytest.mark.parametrize(
    "value", [1.0, "1", D("NaN"), D("Infinity"), D("0"), D("1e19"), D("0.0000000000000000001")]
)
def test_strict_finite_decimal_positive_quantity(value):
    with pytest.raises(ValidationError):
        order(contracts=value)


def test_frozen_models_unknown_mark_field_and_shape_refused():
    with pytest.raises(ValidationError):
        quote(mark=D("100"))
    with pytest.raises(ValidationError):
        order(limit_price=D("100"))
    with pytest.raises(ValidationError):
        order(order_type="passive_limit")
    with pytest.raises(ValidationError):
        order(cancelled_at_ms=90)
    with pytest.raises(ValidationError):
        policy(latency_ms=True)
    with pytest.raises(ValidationError):
        order().contracts = D("3")


def test_not_submitted_asof_and_tuple_scope():
    result = run(orders=(order(submitted_at_ms=300),))
    assert result.orders[0].status == "not_submitted"
    with pytest.raises(ValueError, match="IMMUTABLE"):
        run(orders=[order()])
    with pytest.raises(ValueError, match="AS_OF"):
        run(as_of_ms=True)


def test_multiple_entry_partial_exit_basis_exact_final_cash():
    result = run(
        orders=(
            order("a", contracts=D("1")),
            order("b", contracts=D("1"), submitted_at_ms=120),
            order("c", side="sell", contracts=D("0.5"), submitted_at_ms=140),
            order("d", side="sell", contracts=D("1.5"), submitted_at_ms=160),
        ),
        quotes=(
            quote(),
            quote("b", source_time_ms=130, observed_at_ms=130, ask=D("103")),
            quote("c", source_time_ms=150, observed_at_ms=150, bid=D("104"), ask=D("105")),
            quote("d", source_time_ms=170, observed_at_ms=170, bid=D("104"), ask=D("105")),
        ),
    )
    assert result.gross_realized_pnl == D("40")
    assert result.total_fees == D("4.120")
    assert result.net_realized_pnl == result.closed_roundtrip_net_pnl == D("35.880")


def test_both_modes_use_far_touch_never_optimistic_baseline():
    assert run(policy=policy(pessimistic=False)).fills[0].execution_price == D("101")
    assert run(policy=policy(pessimistic=True)).fills[0].execution_price == D("101")


@pytest.mark.parametrize(
    "change",
    [
        dict(source_time_ms=120),
        dict(ask=D("102")),
        dict(ask_contracts=D("20")),
        dict(health="failed"),
    ],
)
def test_reused_liquidity_conflicting_economics_refuse(change):
    with pytest.raises(ValueError, match="CONFLICTING_NATIVE_LIQUIDITY"):
        run(
            quotes=(
                quote("a", liquidity_id="native"),
                quote("b", liquidity_id="native", observed_at_ms=120, **change),
            )
        )


@pytest.mark.parametrize(
    "target,change",
    [
        ("order", dict(contracts=D("-1"))),
        ("order", dict(submitted_at_ms=True)),
        ("quote", dict(ask=D("NaN"))),
        ("quote", dict(observed_at_ms=-10)),
        ("fees", dict(taker_rate=D("-1"))),
        ("instrument", dict(contract_multiplier=D("-1"))),
        ("policy", dict(latency_ms=True)),
    ],
)
def test_forged_frozen_models_revalidated(target, change):
    values = dict(
        orders=(order(),), quotes=(quote(),), fees=fee(), instrument=instrument(), policy=policy()
    )
    if target == "order":
        values["orders"] = (order().model_copy(update=change),)
    elif target == "quote":
        values["quotes"] = (quote().model_copy(update=change),)
    else:
        values[target] = values[target].model_copy(update=change)
    with pytest.raises(ValidationError):
        run(**values)


def test_model_construct_missing_fields_refused():
    with pytest.raises(ValidationError):
        run(orders=(SimulatedOrder.model_construct(order_id="forged"),))


def test_order_quote_product_budget_refuses_without_truncation():
    orders = tuple(order(f"o-{i}") for i in range(101))
    quotes = tuple(quote(f"q-{i}") for i in range(1000))
    with pytest.raises(ValueError, match="DECISION_BUDGET"):
        run(orders=orders, quotes=quotes)


def test_owned_fixture_is_model_only_no_native_evidence_or_readiness():
    result = run()
    assert result.funding_cashflow is None
    assert result.funding_supported is False
    assert result.maker_execution_supported is False
    assert result.history_complete is False
    assert result.ready_for_signals is False
    assert "OFFLINE_RESEARCH_REQUIRES_EXTERNAL_TRIAL_REGISTRATION" in result.assumptions


def test_recurring_partial_basis_independent_of_rounding_and_inexact_traps():
    orders = (
        order("buy1", contracts=D("1")),
        order("buy2", contracts=D("2"), submitted_at_ms=120),
        order("sell", side="sell", contracts=D("1"), submitted_at_ms=140),
    )
    quotes = (
        quote(),
        quote("entry2", source_time_ms=130, observed_at_ms=130, ask=D("102")),
        quote("exit", source_time_ms=150, observed_at_ms=150),
    )
    baseline = run(orders=orders, quotes=quotes)
    previous = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_UP
        getcontext().traps[Inexact] = True
        assert run(orders=orders, quotes=quotes) == baseline
    finally:
        setcontext(previous)
    assert D("2033") < baseline.premium_cost_basis < D("2034")
    assert D("2.033") < baseline.entry_fee_cost_basis < D("2.034")


def test_audit_result_carries_caller_units_multiplier_and_fee_policies():
    result = run()
    assert result.instrument == instrument()
    assert result.fee_policy == fee()
    assert result.policy == policy()
    assert result.decimal_precision == 160


def test_passive_sell_touch_unfilled_strict_later_crossing_and_adverse_price():
    result = run(
        orders=(
            order("buy", contracts=D("1")),
            order(
                "sell",
                side="sell",
                contracts=D("1"),
                submitted_at_ms=120,
                order_type="passive_limit",
                limit_price=D("104"),
            ),
        ),
        quotes=(
            quote(),
            quote("touch", source_time_ms=140, observed_at_ms=140, bid=D("104"), ask=D("106")),
            quote("cross", source_time_ms=150, observed_at_ms=150, bid=D("105"), ask=D("106")),
        ),
        policy=policy(adverse_slippage_rate=D("0.001")),
    )
    assert [fill.quote_id for fill in result.fills] == ["q", "cross"]
    assert result.fills[1].execution_price == D("104.895")
    assert result.fills[1].fee == D("1.04895")
    assert result.fills[1].slippage_cost == D("1.05")
    assert any(item.reason == "PASSIVE_TOUCH_NOT_EXECUTION" for item in result.no_fills)


def test_partial_recurring_basis_full_exit_reconciles_exact_cash_and_fees():
    result = run(
        orders=(
            order("a", contracts=D("1")),
            order("b", submitted_at_ms=120),
            order("c", side="sell", contracts=D("1"), submitted_at_ms=140),
            order("d", side="sell", submitted_at_ms=160),
        ),
        quotes=(
            quote(),
            quote("b", source_time_ms=130, observed_at_ms=130, ask=D("102")),
            quote("c", source_time_ms=150, observed_at_ms=150),
            quote("d", source_time_ms=170, observed_at_ms=170),
        ),
    )
    assert (
        result.inventory_contracts == result.premium_cost_basis == result.entry_fee_cost_basis == 0
    )
    assert result.gross_realized_pnl == D("-80")
    assert result.total_fees == D("6.02")
    assert result.closed_roundtrip_net_pnl == result.net_realized_pnl == D("-86.02")


def test_maximum_bounded_economics_preserve_exact_fee_products():
    result = run(
        orders=(order(contracts=D("1000000000000000000")),),
        quotes=(
            quote(
                ask=D("999999999999999999.999999999999999999"),
                bid=D("999999999999999999"),
                ask_contracts=D("1000000000000000000"),
                bid_contracts=D("1000000000000000000"),
            ),
        ),
        policy=policy(adverse_slippage_rate=D("0.999999999999999999")),
    )
    fill = result.fills[0]
    assert fill.fee.as_tuple().exponent >= -160
    assert fill.fee > 0 and result.net_cash_flow < 0
    assert result.inventory_contracts == D("1000000000000000000")
