import os
import time
from collections import deque

import pytest


@pytest.fixture(autouse=True)
def isolate_environment(monkeypatch):
    for key in os.environ:
        if key.startswith(("SELECTIVE_", "DERIBIT_")):
            monkeypatch.delenv(key)


@pytest.fixture
def instrument():
    return {
        "instrument_name": "BTC-28JUN30-100000-C",
        "base_currency": "BTC",
        "quote_currency": "BTC",
        "settlement_currency": "BTC",
        "kind": "option",
        "option_type": "call",
        "strike": 100_000,
        "expiration_timestamp": 1_908_864_000_000,
        "is_active": True,
        "contract_size": 1,
        "min_trade_amount": 0.1,
        "tick_size": 0.0001,
        "price_index": "btc_usd",
        "instrument_type": "reversed",
        "maker_commission": 0.0003,
        "taker_commission": 0.0003,
    }


@pytest.fixture
def summary(instrument):
    return {
        "instrument_name": instrument["instrument_name"],
        "bid_price": 0.02,
        "ask_price": 0.021,
        "mark_price": 999,
        "open_interest": 100,
        "volume": 30,
        "creation_timestamp": int(time.time() * 1000),
    }


@pytest.fixture
def ticker(instrument):
    return {
        "instrument_name": instrument["instrument_name"],
        "timestamp": int(time.time() * 1000),
        "best_bid_price": 0.02,
        "best_ask_price": 0.021,
        "best_bid_amount": 5,
        "best_ask_amount": 5,
        "mark_iv": 55,
        "underlying_price": 100_000,
        "open_interest": 100,
        "greeks": {"delta": 0.3, "gamma": 0.0001, "vega": 50, "theta": -25, "rho": 3},
    }


class FakeRpc:
    def __init__(self, results):
        self.results = deque(results)
        self.calls = []

    async def call(self, method, params):
        self.calls.append((method, params))
        value = self.results.popleft()
        if isinstance(value, Exception):
            raise value
        return value


@pytest.fixture
def fake_rpc():
    return FakeRpc
