"""Shared compiled schemas retain fresh values, exact contracts and failure isolation."""

import json
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest
from pydantic import JsonValue, TypeAdapter, ValidationError

from selective_options.collectors import ChannelRegistry, DeribitCollector, Notification
from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.events import make_event


def event():
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol="A",
        kind="spot",
        event_time_ms=1000,
        observed_at_ms=1100,
        payload={
            "integer": 10,
            "native_decimal": "1.23400000000000000001",
            "nested": {"items": [1, "2", None, True], "empty": {}},
            "float": 1.25,
        },
    )


def test_payload_reuse_retains_exact_values_and_recursive_detachment():
    row = event()
    original = row.payload_json
    for _ in range(4):
        decoded = row.payload()
        assert decoded == TypeAdapter(dict[str, JsonValue]).validate_json(original)
        assert type(decoded["integer"]) is int and type(decoded["native_decimal"]) is str
        decoded["nested"]["items"].append("changed")
        decoded["nested"]["empty"]["changed"] = True
        assert row.payload_json == original
        assert row.payload()["nested"]["items"] == [1, "2", None, True]


def test_concurrent_payload_reads_do_not_share_output_or_failed_validation_state():
    row = event()
    bad = row.model_copy(update={"payload_json": "{broken"})
    expected = row.payload()

    def read(index):
        if index % 5 == 0:
            with pytest.raises(ValidationError):
                bad.payload()
        result = row.payload()
        assert result == expected
        result["nested"]["items"].append(index)
        return result

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(read, range(80)))
    assert len({id(value) for value in results}) == 80
    assert row.payload() == expected


@pytest.mark.parametrize("raw", ["[]", "null", "7", "{bad"])
def test_invalid_payload_then_valid_call_still_checks_dictionary_contract(raw):
    row = event()
    with pytest.raises(ValidationError):
        row.model_copy(update={"payload_json": raw}).payload()
    assert row.payload()["native_decimal"] == "1.23400000000000000001"


async def test_native_frame_hot_path_needs_no_new_schema_construction(monkeypatch, ticker):
    import selective_options.collectors as collectors
    import selective_options.events as events

    # Fail schema construction after import: valid and invalid frames still use full validators.
    def forbidden(*args, **kwargs):
        raise AssertionError("schema rebuilt inside hot path")

    monkeypatch.setattr(collectors, "TypeAdapter", forbidden)
    monkeypatch.setattr(events, "TypeAdapter", forbidden)
    monkeypatch.setattr(collectors, "now_ms", lambda: 1200)
    symbol = ticker["instrument_name"]
    registry = ChannelRegistry()
    registry.add_instrument("BTC", symbol, "btc_usd")
    data = ticker | {"timestamp": 1000, "extra_native": {"decimal": "0.0100001"}}
    message = {
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {"channel": f"ticker.{symbol}.100ms", "data": data},
    }
    sink = AsyncMock()
    collector = DeribitCollector(AppConfig(), sink)
    await collector._receive(AsyncMock(recv=AsyncMock(return_value=json.dumps(message))), registry)
    actual = sink.await_args.args[0]
    expected = make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=symbol,
        kind="ticker",
        event_time_ms=1000,
        observed_at_ms=1200,
        payload=data,
    )
    assert actual == expected
    decoded = actual.payload()
    decoded["extra_native"]["decimal"] = "mutated"
    assert actual.payload()["extra_native"]["decimal"] == "0.0100001"
    wrong = message | {"params": {"channel": "unrequested", "data": data}}
    with pytest.raises(IntegrityError, match="unrequested"):
        await collector._receive(
            AsyncMock(recv=AsyncMock(return_value=json.dumps(wrong))), registry
        )
    assert sink.await_count == 1


@pytest.mark.parametrize("case", ["crossed", "negative", "wrong_symbol", "predecessor"])
def test_book_integrity_survives_shared_adapter_and_previous_failures(case):
    registry = ChannelRegistry()
    registry.add_instrument("BTC", "A", "btc_usd")
    base = {
        "instrument_name": "A",
        "timestamp": 1000,
        "change_id": 1,
        "bids": [[99, 2]],
        "asks": [[101, 2]],
    }

    def receive(data):
        return registry.normalize(
            Notification.model_validate(
                {
                    "jsonrpc": "2.0",
                    "method": "subscription",
                    "params": {"channel": "book.A.none.20.100ms", "data": data},
                }
            ),
            1100,
        )

    receive(base)
    bad = base | {"change_id": 2}
    if case == "crossed":
        bad["bids"] = [[102, 2]]
    elif case == "negative":
        bad["asks"] = [[101, -1]]
    elif case == "wrong_symbol":
        bad["instrument_name"] = "B"
    else:
        bad["prev_change_id"] = 99
    with pytest.raises(IntegrityError):
        receive(bad)
    # A subsequent full snapshot remains fully validated and does not reuse decoded data.
    actual = receive(base | {"change_id": 3})[0]
    assert actual.observed_at_ms == 1100 and actual.event_time_ms == 1000
    assert actual.payload()["bids"] == [[99, 2]]
