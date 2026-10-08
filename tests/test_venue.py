import pytest
from pydantic import ValidationError

from selective_options.errors import IntegrityError, RpcError
from selective_options.venue import DeribitAdapter


async def test_dynamic_metadata_not_names(fake_rpc, instrument):
    # Symbol spelling is not used to infer economics or base currency.
    instrument["instrument_name"] = "venue-evolved-symbol"
    chain = await DeribitAdapter(fake_rpc([[instrument]]), clock=lambda: 1).discover("BTC")
    assert chain.instruments[0].instrument_name == "venue-evolved-symbol"
    assert chain.instruments[0].quote_currency == "BTC"


async def test_currency_fallback_is_explicit_and_filtered(fake_rpc, instrument):
    other = dict(instrument, base_currency="ETH", instrument_name="OTHER")
    rpc = fake_rpc([RpcError("public/get_instruments", -32602, "currency"), [instrument, other]])
    result = await DeribitAdapter(rpc, clock=lambda: 1).discover("BTC")
    assert result.fallback_used and len(result.instruments) == 1
    assert rpc.calls[1][1]["currency"] == "any"


@pytest.mark.parametrize(
    "error",
    [RpcError("public/get_instruments", 10028), RpcError("public/get_instruments", -32602, "kind")],
)
async def test_other_api_errors_not_swallowed(fake_rpc, error):
    with pytest.raises(RpcError):
        await DeribitAdapter(fake_rpc([error])).discover("PAXG")


async def test_inactive_expired_and_wrong_base_removed(fake_rpc, instrument):
    rows = [
        dict(instrument, is_active=False),
        dict(instrument, expiration_timestamp=1),
        dict(instrument, base_currency="ETH"),
    ]
    assert not (await DeribitAdapter(fake_rpc([rows]), clock=lambda: 1).discover("BTC")).instruments


async def test_no_paxg_means_empty_not_synthetic(fake_rpc):
    rpc = fake_rpc([[]])
    result = await DeribitAdapter(rpc).option_chain("PAXG")
    assert result.rows == () and len(rpc.calls) == 1


async def test_chain_preserves_missing_and_one_sided_quotes(fake_rpc, instrument, summary):
    second = dict(instrument, instrument_name="second")
    summary["ask_price"] = None
    result = await DeribitAdapter(fake_rpc([[instrument, second], [summary]])).option_chain("BTC")
    assert result.rows[0].summary.ask_price is None
    assert result.rows[1].summary is None
    assert result.rows[0].summary.bid_price == 0.02  # Never substitutes mark_price=999.


@pytest.mark.parametrize("target", ["instruments", "quotes"])
async def test_duplicate_rows_fail(fake_rpc, instrument, summary, target):
    results = (
        [[instrument, instrument]]
        if target == "instruments"
        else [[instrument], [summary, summary]]
    )
    with pytest.raises(IntegrityError):
        await DeribitAdapter(fake_rpc(results)).option_chain("BTC")


async def test_ticker_identity_and_schema(fake_rpc, ticker):
    with pytest.raises(IntegrityError):
        await DeribitAdapter(fake_rpc([ticker])).ticker("different")
    ticker["greeks"]["delta"] = float("nan")
    with pytest.raises(ValidationError):
        await DeribitAdapter(fake_rpc([ticker])).ticker(ticker["instrument_name"])
