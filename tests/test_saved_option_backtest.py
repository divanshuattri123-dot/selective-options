"""Fixture-only tests for the read-only saved-option simulator bridge."""

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from selective_options.backtest import (
    FeePolicy,
    InstrumentSpec,
    SimulatedOrder,
    SimulationPolicy,
    simulate,
)
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.models import Instrument
from selective_options.option_backtest import (
    OptionExecutionProfile,
    load_saved_option_quotes,
    option_ticker_to_quote,
    research_saved_option_roundtrip,
)
from selective_options.store import DuckStore
from selective_options.trials import read_trials, trial_counts

D = Decimal
SYMBOL = "BTC-SYNTHETIC-EXPIRY-C"
AS_OF_MS = 140

PROFILE = OptionExecutionProfile(
    profile_id="fixture-btc-premium-cashflows-v1",
    asset="BTC",
    quote_currency="BTC",
    settlement_currency="BTC",
    instrument_type="reversed",
    expected_contract_size=D("1"),
    contract_multiplier=D("1"),
    displayed_amount_unit="base_currency_amount",
    quote_price_unit="currency_per_underlying_unit",
)
FEES = FeePolicy(
    policy_id="synthetic-0.1pct-taker",
    maker_rate=D("0.0005"),
    taker_rate=D("0.001"),
    basis="executed_premium_notional",
)
POLICY = SimulationPolicy(
    latency_ms=10,
    max_quote_age_ms=50,
    min_displayed_contracts=D("0.1"),
    adverse_slippage_rate=D("0"),
    pessimistic=True,
    passive_model="strict_later_executable_crossing_taker_assumption",
)
ORDERS = (
    SimulatedOrder(
        order_id="entry",
        instrument_id=SYMBOL,
        side="buy",
        order_type="passive_limit",
        contracts=D("2"),
        submitted_at_ms=100,
        limit_price=D("0.022"),
    ),
    SimulatedOrder(
        order_id="exit",
        instrument_id=SYMBOL,
        side="sell",
        order_type="passive_limit",
        contracts=D("2"),
        submitted_at_ms=120,
        limit_price=D("0.019"),
    ),
)


def instrument_event(
    *,
    symbol: str = SYMBOL,
    expiry: int = 1000,
    contract_size: float = 1.0,
    tick_size: float = 0.0001,
    tick_size_steps: tuple[tuple[float, float], ...] = (),
) -> MarketEvent:
    native = Instrument.model_validate(
        {
            "instrument_name": symbol,
            "base_currency": "BTC",
            "quote_currency": "BTC",
            "settlement_currency": "BTC",
            "kind": "option",
            "option_type": "call",
            "strike": 100000.0,
            "expiration_timestamp": expiry,
            "is_active": True,
            "contract_size": contract_size,
            "min_trade_amount": 0.1,
            "tick_size": tick_size,
            "tick_size_steps": [
                {"above_price": above_price, "tick_size": step_size}
                for above_price, step_size in tick_size_steps
            ],
            "price_index": "btc_usd",
            "instrument_type": "reversed",
            "maker_commission": 0.0003,
            "taker_commission": 0.0003,
        }
    )
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=symbol,
        kind="instrument",
        event_time_ms=90,
        observed_at_ms=90,
        timestamp_kind="receipt",
        payload=native.model_dump(mode="json"),
    )


def ticker_event(
    *,
    source: int,
    receipt: int,
    symbol: str = SYMBOL,
    bid: float | None = 0.020,
    ask: float | None = 0.021,
    bid_amount: float = 5.0,
    ask_amount: float = 5.0,
    mark_iv: float = 55.0,
) -> MarketEvent:
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=symbol,
        kind="ticker",
        event_time_ms=source,
        observed_at_ms=receipt,
        timestamp_kind="source",
        payload={
            "instrument_name": symbol,
            "timestamp": source,
            "best_bid_price": bid,
            "best_ask_price": ask,
            "best_bid_amount": bid_amount,
            "best_ask_amount": ask_amount,
            "mark_iv": mark_iv,
            "underlying_price": 100000.0,
            "underlying_index": "BTC-INDEX",
            "open_interest": 100.0,
            "greeks": {
                "delta": 0.3,
                "gamma": 0.0001,
                "vega": 50.0,
                "theta": -25.0,
                "rho": 3.0,
            },
        },
    )


def manifest_event(metadata: MarketEvent, *, observed: int = 95) -> MarketEvent:
    return make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        event_time_ms=observed,
        observed_at_ms=observed,
        timestamp_kind="receipt",
        payload={
            "reason": "UNIVERSE_DISCOVERED",
            "asset": "BTC",
            "available": 1,
            "selected": 1,
            "coverage": "FULL",
            "instruments": [metadata.symbol],
            "selected_instruments": [metadata.symbol],
            "metadata_event_ids": [metadata.event_id],
        },
    )


def fixture_rows(
    *,
    entry_size: float = 5.0,
    exit_size: float = 5.0,
    extra_quotes: tuple[MarketEvent, ...] = (),
) -> list[MarketEvent]:
    metadata = instrument_event()
    return [
        metadata,
        manifest_event(metadata),
        ticker_event(source=111, receipt=112, ask_amount=entry_size),
        ticker_event(source=131, receipt=132, bid_amount=exit_size, ask_amount=exit_size),
        *extra_quotes,
    ]


async def save_database(path: Path, events: list[MarketEvent]) -> Path:
    store = await DuckStore.open(path)
    try:
        await store.append(events, exact=True)
    finally:
        await store.close()
    return path


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_fixture(path: Path, **kwargs: object):
    return load_saved_option_quotes(
        path,
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=PROFILE,
        max_discovery_age_ms=100,
        max_clock_skew_ms=5000,
        **kwargs,
    )


async def test_closed_source_roundtrip_uses_executable_touches_and_registers(tmp_path):
    database = await save_database(tmp_path / "events.duckdb", fixture_rows())
    before = file_digest(database)
    report = research_saved_option_roundtrip(
        database,
        tmp_path / "result",
        tmp_path / "trials.jsonl",
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=PROFILE,
        orders=ORDERS,
        fees=FEES,
        policy=POLICY,
        data_kind="synthetic_fixture",
        max_discovery_age_ms=100,
        max_clock_skew_ms=5000,
        max_rows=10,
    )

    simulation = report["simulation"]
    assert isinstance(simulation, dict)
    assert [row["execution_price"] for row in simulation["fills"]] == ["0.021", "0.02"]
    assert [row["contracts"] for row in simulation["fills"]] == ["2", "2"]
    assert D(simulation["gross_realized_pnl"]) == D("-0.002")
    assert D(simulation["total_fees"]) == D("0.000082")
    assert D(simulation["closed_roundtrip_net_pnl"]) == D("-0.002082")
    assert simulation["fills"][0]["quote_source_time_ms"] == 111
    assert simulation["fills"][0]["quote_observed_at_ms"] == 112
    assert simulation["fills"][1]["quote_source_time_ms"] == 131
    assert simulation["fills"][1]["quote_observed_at_ms"] == 132
    assert report["gate2_complete"] is False and report["trading_enabled"] is False
    assert report["registry_counts"] == {
        "started": 1,
        "completed": 1,
        "failed": 0,
        "unresolved": 0,
        "trial_count_N": 1,
    }
    assert file_digest(database) == before
    assert json.loads((tmp_path / "result/option-backtest.json").read_text()) == report
    records = read_trials(tmp_path / "trials.jsonl")
    assert [record.phase for record in records] == ["STARTED", "COMPLETED"]
    assert records[0].parameters["source_database_sha256"] == before


async def test_orders_must_follow_selection_and_obey_native_tick_and_minimum_size(tmp_path):
    metadata = instrument_event(tick_size_steps=((0.01, 0.0005),))
    database = await save_database(
        tmp_path / "events.duckdb",
        [
            metadata,
            manifest_event(metadata),
            ticker_event(source=111, receipt=112),
            ticker_event(source=131, receipt=132),
        ],
    )
    early_buy = ORDERS[0].model_copy(update={"submitted_at_ms": 94})
    with pytest.raises(ValueError, match="NOT_AFTER_DISCOVERY"):
        research_saved_option_roundtrip(
            database,
            tmp_path / "early",
            tmp_path / "early-trials.jsonl",
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=PROFILE,
            orders=(early_buy, ORDERS[1]),
            fees=FEES,
            policy=POLICY,
            data_kind="synthetic_fixture",
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
            max_rows=10,
        )
    tied_buy = ORDERS[0].model_copy(update={"submitted_at_ms": 95})
    with pytest.raises(ValueError, match="NOT_AFTER_DISCOVERY"):
        research_saved_option_roundtrip(
            database,
            tmp_path / "tied",
            tmp_path / "tied-trials.jsonl",
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=PROFILE,
            orders=(tied_buy, ORDERS[1]),
            fees=FEES,
            policy=POLICY,
            data_kind="synthetic_fixture",
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
            max_rows=10,
        )

    off_tick_buy = ORDERS[0].model_copy(update={"limit_price": D("0.0211")})
    with pytest.raises(IntegrityError, match="PRICE_OFF_TICK"):
        research_saved_option_roundtrip(
            database,
            tmp_path / "off-tick",
            tmp_path / "off-tick-trials.jsonl",
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=PROFILE,
            orders=(off_tick_buy, ORDERS[1]),
            fees=FEES,
            policy=POLICY,
            data_kind="synthetic_fixture",
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
            max_rows=10,
        )

    off_lot_buy = ORDERS[0].model_copy(update={"contracts": D("0.05")})
    off_lot_sell = ORDERS[1].model_copy(update={"contracts": D("0.05")})
    with pytest.raises(ValueError, match="ORDER_AMOUNT_BELOW_MINIMUM"):
        research_saved_option_roundtrip(
            database,
            tmp_path / "off-lot",
            tmp_path / "off-lot-trials.jsonl",
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=PROFILE,
            orders=(off_lot_buy, off_lot_sell),
            fees=FEES,
            policy=POLICY,
            data_kind="synthetic_fixture",
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
            max_rows=10,
        )

    # Deribit min_trade_amount is a minimum, not a documented quantity step.
    fractional_buy = ORDERS[0].model_copy(update={"contracts": D("0.15")})
    fractional_sell = ORDERS[1].model_copy(update={"contracts": D("0.15")})
    report = research_saved_option_roundtrip(
        database,
        tmp_path / "fractional-minimum-valid",
        tmp_path / "fractional-minimum-valid-trials.jsonl",
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=PROFILE,
        orders=(fractional_buy, fractional_sell),
        fees=FEES,
        policy=POLICY,
        data_kind="synthetic_fixture",
        max_discovery_age_ms=100,
        max_clock_skew_ms=5000,
        max_rows=10,
    )
    assert report["gate2_complete"] is False


def test_tick_step_threshold_uses_base_tick_at_exact_boundary():
    metadata = instrument_event(tick_size_steps=((0.0052, 0.0005),))
    instrument = Instrument.model_validate_json(metadata.payload_json, strict=True)
    quote = option_ticker_to_quote(
        ticker_event(source=111, receipt=112, bid=0.0052, ask=0.0055),
        asset="BTC",
        symbol=SYMBOL,
        instrument=instrument,
        profile=PROFILE,
        max_clock_skew_ms=5000,
    )
    assert quote.bid == D("0.0052")
    assert quote.ask == D("0.0055")


async def test_saved_native_amount_depth_converts_to_contracts(tmp_path):
    metadata = instrument_event(contract_size=2.0)
    database = await save_database(
        tmp_path / "events.duckdb",
        [metadata, manifest_event(metadata), ticker_event(source=111, receipt=112)],
    )
    profile = PROFILE.model_copy(
        update={"expected_contract_size": D("2"), "contract_multiplier": D("2")}
    )
    loaded = load_saved_option_quotes(
        database,
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=profile,
        max_discovery_age_ms=100,
        max_clock_skew_ms=5000,
    )
    assert loaded.quotes[0].bid_contracts == D("2.5")
    assert loaded.quotes[0].ask_contracts == D("2.5")


@pytest.mark.parametrize(
    "future",
    [ticker_event(source=141, receipt=142), ticker_event(source=139, receipt=141)],
)
async def test_both_clocks_filter_future_rows_before_budget(tmp_path, future):
    database = await save_database(tmp_path / "events.duckdb", fixture_rows(extra_quotes=(future,)))
    loaded = load_fixture(database, max_rows=4)
    assert [quote.source_time_ms for quote in loaded.quotes] == [111, 131]
    assert future.event_id not in {quote.quote_id for quote in loaded.quotes}


async def test_source_ahead_of_receipt_is_preserved_but_non_executable(tmp_path):
    source_ahead = ticker_event(source=113, receipt=112)
    database = await save_database(
        tmp_path / "source-ahead.duckdb",
        [instrument_event(), manifest_event(instrument_event()), source_ahead],
    )
    loaded = load_fixture(database)
    assert len(loaded.quotes) == 1
    assert loaded.quotes[0].source_time_ms > loaded.quotes[0].observed_at_ms
    result = simulate(
        orders=(ORDERS[0],),
        quotes=loaded.quotes,
        instrument=loaded.instrument_spec,
        fees=FEES,
        policy=POLICY,
        as_of_ms=AS_OF_MS,
    )
    assert not result.fills
    assert [item.reason for item in result.no_fills] == ["QUOTE_CLOCK_ORDER"]


async def test_missing_side_stays_missing_even_with_mark_iv_and_cannot_fill(tmp_path):
    missing_bid = ticker_event(source=111, receipt=112, bid=None)
    database = await save_database(
        tmp_path / "events.duckdb",
        [instrument_event(), manifest_event(instrument_event()), missing_bid],
    )
    loaded = load_fixture(database)
    quote = loaded.quotes[0]
    assert quote.bid is None and quote.ask == D("0.021")
    result = simulate(
        orders=(ORDERS[0],),
        quotes=(quote,),
        instrument=loaded.instrument_spec,
        fees=FEES,
        policy=POLICY,
        as_of_ms=AS_OF_MS,
    )
    assert not result.fills
    assert result.no_fills[0].reason == "MISSING_TWO_SIDED_QUOTE"


def test_displayed_depth_caps_fill_and_reused_liquidity_does_not_replenish():
    metadata = instrument_event()
    first = ticker_event(source=111, receipt=112, ask_amount=1.5)
    replay = ticker_event(source=111, receipt=113, ask_amount=1.5, mark_iv=56.0)
    quote_a = option_ticker_to_quote(
        first,
        asset="BTC",
        symbol=SYMBOL,
        instrument=Instrument.model_validate_json(metadata.payload_json, strict=True),
        profile=PROFILE,
        max_clock_skew_ms=5000,
    )
    quote_b = option_ticker_to_quote(
        replay,
        asset="BTC",
        symbol=SYMBOL,
        instrument=Instrument.model_validate_json(metadata.payload_json, strict=True),
        profile=PROFILE,
        max_clock_skew_ms=5000,
    )
    assert quote_a.liquidity_id == quote_b.liquidity_id
    order = ORDERS[0].model_copy(update={"contracts": D("3")})
    result = simulate(
        orders=(order,),
        quotes=(quote_a, quote_b),
        instrument=InstrumentSpec(
            instrument_id=SYMBOL,
            currency="BTC",
            quantity_unit="contracts",
            quote_price_unit="currency_per_underlying_unit",
            contract_multiplier=D("1"),
            accounting="single_instrument_long_only_linear_premium",
            funding_required=False,
            settlement_required=False,
            transfer_tax_required=False,
        ),
        fees=FEES,
        policy=POLICY,
        as_of_ms=AS_OF_MS,
    )
    assert sum(fill.contracts for fill in result.fills) == D("1.5")
    assert result.orders[0].status == "partially_filled"


async def test_discovery_bound_metadata_and_expiry_are_required(tmp_path):
    metadata = instrument_event()
    rows = fixture_rows()
    rows.append(instrument_event(contract_size=2.0))
    database = await save_database(tmp_path / "events.duckdb", rows)
    loaded = load_fixture(database)
    assert loaded.metadata_event_id == metadata.event_id
    with pytest.raises(IntegrityError, match="CONTRACT_SIZE_PROFILE_MISMATCH"):
        bad_profile = PROFILE.model_copy(update={"expected_contract_size": D("2")})
        load_saved_option_quotes(
            database,
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=bad_profile,
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
        )


async def test_same_source_time_conflict_refuses_and_wal_refuses(tmp_path):
    conflict = ticker_event(source=111, receipt=113, mark_iv=56.0)
    database = await save_database(tmp_path / "tie.duckdb", fixture_rows(extra_quotes=(conflict,)))
    with pytest.raises(IntegrityError, match="QUOTE_SOURCE_TIME_TIE"):
        load_fixture(database)

    wal_database = await save_database(tmp_path / "wal.duckdb", fixture_rows())
    wal_database.with_suffix(".duckdb.wal").write_bytes(b"fixture-wal-marker")
    with pytest.raises(IntegrityError, match="MUST_BE_CLOSED"):
        load_fixture(wal_database)


async def test_partial_roundtrip_is_registered_failed_and_not_published(tmp_path):
    database = await save_database(
        tmp_path / "events.duckdb", fixture_rows(entry_size=1.5, exit_size=1.5)
    )
    registry = tmp_path / "trials.jsonl"
    with pytest.raises(ValueError, match="ROUNDTRIP_NOT_CLOSED"):
        research_saved_option_roundtrip(
            database,
            tmp_path / "result",
            registry,
            asset="BTC",
            symbol=SYMBOL,
            as_of_ms=AS_OF_MS,
            profile=PROFILE,
            orders=ORDERS,
            fees=FEES,
            policy=POLICY,
            data_kind="synthetic_fixture",
            max_discovery_age_ms=100,
            max_clock_skew_ms=5000,
            max_rows=10,
        )
    assert trial_counts(registry)["failed"] == 1
    assert not (tmp_path / "result/option-backtest.json").exists()


async def test_source_mutation_during_simulation_registers_failure(tmp_path):
    database = await save_database(tmp_path / "events.duckdb", fixture_rows())
    registry = tmp_path / "trials.jsonl"
    original = simulate

    def mutate_after_simulation(**kwargs):
        result = original(**kwargs)
        with database.open("ab") as stream:
            stream.write(b"EXPLICIT_TEST_ONLY_SOURCE_MUTATION")
        return result

    with patch("selective_options.option_backtest.simulate", mutate_after_simulation):
        with pytest.raises(IntegrityError, match="RESEARCH_SOURCE_CHANGED"):
            research_saved_option_roundtrip(
                database,
                tmp_path / "result",
                registry,
                asset="BTC",
                symbol=SYMBOL,
                as_of_ms=AS_OF_MS,
                profile=PROFILE,
                orders=ORDERS,
                fees=FEES,
                policy=POLICY,
                data_kind="synthetic_fixture",
                max_discovery_age_ms=100,
                max_clock_skew_ms=5000,
                max_rows=10,
            )
    assert trial_counts(registry)["failed"] == 1
    assert not (tmp_path / "result/option-backtest.json").exists()


async def test_closed_roundtrip_can_use_multiple_executable_partial_fills(tmp_path):
    database = await save_database(
        tmp_path / "events.duckdb",
        fixture_rows(
            exit_size=1.5,
            extra_quotes=(ticker_event(source=133, receipt=134, bid_amount=0.5),),
        ),
    )
    report = research_saved_option_roundtrip(
        database,
        tmp_path / "result",
        tmp_path / "trials.jsonl",
        asset="BTC",
        symbol=SYMBOL,
        as_of_ms=AS_OF_MS,
        profile=PROFILE,
        orders=ORDERS,
        fees=FEES,
        policy=POLICY,
        data_kind="synthetic_fixture",
        max_discovery_age_ms=100,
        max_clock_skew_ms=5000,
        max_rows=10,
    )
    simulation = report["simulation"]
    assert isinstance(simulation, dict)
    assert len(simulation["fills"]) == 3
    assert [row["side"] for row in simulation["fills"]] == ["buy", "sell", "sell"]
    assert report["gate2_complete"] is False and report["trading_enabled"] is False
