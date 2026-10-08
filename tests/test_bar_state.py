"""Finite local codec tests. Serialization does not establish committed provenance."""

import json
from copy import deepcopy
from decimal import Decimal

import pytest
from pydantic import ValidationError

from selective_options.bar_state import (
    BarState,
    decode_bar_state,
    encode_bar_state,
    restore_bar_state,
)
from selective_options.bars import TradeBars
from selective_options.errors import IntegrityError
from selective_options.events import make_event


def tick(timestamp=301000, identifier=1, price=100.0, qty=0.1, receipt=None):
    return make_event(
        source="kraken",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="trade",
        event_time_ms=timestamp,
        observed_at_ms=receipt or timestamp + 100,
        native_id=str(identifier),
        payload={"price": price, "qty": qty, "trade_id": identifier, "native_extra": "001.2300"},
    )


def builder():
    bars = TradeBars((300, 900), 300000, 5000)
    # Arrival order differs from source order: prototype remains the first arrival.
    bars.add(tick(303000, 3, 110.0, 0.1))
    bars.add(tick(301000, 1, 90.0, 0.2))
    bars.add(tick(302000, 2, 95.0, 0.3))
    bars.close_ready(304000)
    return bars


def restore(data, now=305000, **policy):
    return restore_bar_state(data, now, timeframes_s=(300, 900), lateness_ms=5000, **policy)


def altered(data, change):
    value = json.loads(data)
    change(value)
    return json.dumps(value, allow_nan=True, separators=(",", ":")).encode()


def test_roundtrip_keeps_native_clocks_decimal_keys_and_independent_incomplete_bars():
    original = builder()
    before = deepcopy(vars(original))
    data = encode_bar_state(original, 304000)
    state = decode_bar_state(data)
    recovered = restore(data)
    assert state.format_version == 1 and state.as_of_ms == 304000
    assert recovered.timeframes == original.timeframes and recovered.lateness_ms == 5000
    assert recovered.watermark_ms == 299000 and recovered.coverage_start_ms == 305000
    assert recovered.identity == original.identity
    assert vars(original) == before
    for key, source in original.buckets.items():
        saved = recovered.buckets[key]
        assert saved is not source and saved.prototype is not source.prototype
        assert saved.prototype == source.prototype
        assert saved.prototype.observed_at_ms == 303100
        assert saved.first == (301000, 1) and saved.last == (303000, 3)
        assert (
            saved.volume == Decimal("0.6") and saved.volume.as_tuple() == source.volume.as_tuple()
        )
        assert saved.complete is False
    recovered.add(tick(306000, 4, 115.0, 0.4))
    assert vars(original) == before
    closed = recovered.close_ready(905000)
    assert len(closed) == 2 and all(event.payload()["complete"] is False for event in closed)
    assert all(event.observed_at_ms == 905000 for event in closed)
    recovered.add(tick(901000, 5))
    new = recovered.close_ready(1805000)
    assert len(new) == 2 and all(event.payload()["complete"] is True for event in new)
    assert recovered.close_ready(2705000) == []


def test_exact_decimal_representation_not_binary_float_or_normalized_text():
    original = builder()
    for bar in original.buckets.values():
        bar.volume = Decimal("0.6000000000000000000000000000000000000000")
    recovered = restore(encode_bar_state(original, 304000))
    for bar in recovered.buckets.values():
        assert (
            bar.volume.as_tuple()
            == Decimal("0.6000000000000000000000000000000000000000").as_tuple()
        )


def test_encoding_is_deterministic_despite_dictionary_insertion_order():
    original = builder()
    reordered = deepcopy(original)
    reordered.buckets = dict(reversed(list(reordered.buckets.items())))
    first = encode_bar_state(original, 304000)
    assert first == encode_bar_state(reordered, 304000) == encode_bar_state(original, 304000)
    assert json.loads(first)["buckets"][0]["interval_ms"] == 300000
    assert "native_extra" in decode_bar_state(first).buckets[0].prototype.payload_json


def test_preserved_watermark_refuses_late_and_backwards_revision():
    recovered = restore(encode_bar_state(builder(), 304000))
    with pytest.raises(IntegrityError, match="LATE_TRADE"):
        recovered.add(tick(298999, 9))
    with pytest.raises(IntegrityError, match="backwards"):
        recovered.close_ready(303999)


@pytest.mark.parametrize("scope", [None, ("kraken", "public_reference", "BTC", "BTC/USD")])
def test_empty_state_creates_no_quiet_bars_and_preserves_existing_scope(scope):
    original = TradeBars((300, 900), 300000, 5000)
    original.identity = scope
    recovered = restore(encode_bar_state(original, 304000))
    assert recovered.identity == scope and recovered.buckets == {}
    assert recovered.close_ready(905000) == []


def test_restart_configuration_and_explicit_cutoff_match():
    data = encode_bar_state(builder(), 304000)
    with pytest.raises(IntegrityError, match="RESTART_BEFORE_CUTOFF"):
        restore(data, 303999)
    with pytest.raises(IntegrityError, match="CONFIGURATION_MISMATCH"):
        restore_bar_state(data, 305000, timeframes_s=(300,), lateness_ms=5000)
    with pytest.raises(IntegrityError, match="CONFIGURATION_MISMATCH"):
        restore_bar_state(data, 305000, timeframes_s=(300, 900), lateness_ms=5001)
    assert (
        restore_bar_state(data, 304000, timeframes_s=(900, 300), lateness_ms=5000).coverage_start_ms
        == 304000
    )


@pytest.mark.parametrize(
    "name,value",
    [
        (name, value)
        for name in ("max_buckets", "max_bytes")
        for value in (True, 0, -1, 1.0, "1", None)
    ]
    + [("max_buckets", 100001), ("max_bytes", 64 * 1024 * 1024 + 1)],
)
def test_invalid_budgets_refused_for_all_codec_entrypoints(name, value):
    policy = {name: value}
    with pytest.raises(ValueError):
        encode_bar_state(builder(), 304000, **policy)
    with pytest.raises(ValueError):
        decode_bar_state(b"not parsed", **policy)
    with pytest.raises(ValueError):
        restore(b"not parsed", **policy)


@pytest.mark.parametrize("clock", [True, 0, -1, 1.0, "1", None, 1 << 63])
def test_explicit_clocks_reject_coercion_and_overflow(clock):
    with pytest.raises(ValueError):
        encode_bar_state(builder(), clock)
    with pytest.raises(ValueError):
        restore(b"not parsed", clock)


@pytest.mark.parametrize(
    "frames,late",
    [
        ((), 0),
        ([300, 900], 5000),
        ((300, 300), 5000),
        ((True,), 5000),
        ((1.0,), 5000),
        ((0,), 5000),
        ((86401,), 5000),
        ((300,), True),
        ((300,), -1),
        ((300,), 86400001),
    ],
)
def test_restart_policy_is_strict_and_finite(frames, late):
    with pytest.raises(ValueError):
        restore_bar_state(b"not parsed", 305000, timeframes_s=frames, lateness_ms=late)


def test_bucket_and_byte_budgets_fail_without_truncation_or_builder_mutation():
    original = builder()
    before = deepcopy(vars(original))
    data = encode_bar_state(original, 304000)
    for operation in (
        lambda: encode_bar_state(original, 304000, max_buckets=1),
        lambda: decode_bar_state(data, max_buckets=1),
        lambda: restore(data, max_buckets=1),
    ):
        with pytest.raises(IntegrityError, match="BUCKET_BUDGET"):
            operation()
    for operation in (
        lambda: encode_bar_state(original, 304000, max_bytes=len(data) - 1),
        lambda: decode_bar_state(data, max_bytes=len(data) - 1),
        lambda: restore(data, max_bytes=len(data) - 1),
    ):
        with pytest.raises(IntegrityError, match="BYTE_BUDGET"):
            operation()
    assert encode_bar_state(original, 304000, max_bytes=len(data)) == data
    assert vars(original) == before


@pytest.mark.parametrize(
    "case",
    [
        "future_source",
        "future_receipt",
        "future_first",
        "future_last",
        "future_coverage",
        "future_watermark",
        "scope",
        "prototype_scope",
        "prototype_kind",
        "prototype_id",
        "prototype_version",
        "bool_clock",
        "bool_count",
        "bool_key",
        "bool_complete",
        "float_version",
        "unknown",
        "nested_unknown",
        "duplicate_bucket",
        "unordered",
        "unconfigured_interval",
        "alignment",
        "finished",
        "ohlc",
        "negative_volume",
        "nan_volume",
        "overflow_volume",
        "whitespace_decimal",
        "non_decimal_qty",
        "first_after_last",
        "outside_interval",
        "prototype_outside_keys",
        "prototype_open_conflict",
        "count_one",
        "complete_before_coverage",
        "duplicate_timeframe",
        "bool_timeframe",
        "payload_duplicate",
    ],
)
def test_corrupt_or_unavailable_state_is_never_restored(case):
    data = encode_bar_state(builder(), 304000)

    def change(value):
        bucket = value["buckets"][0]
        prototype = bucket["prototype"]
        if case == "future_source":
            prototype["event_time_ms"] = 305000
        elif case == "future_receipt":
            prototype["observed_at_ms"] = 305000
        elif case == "future_first":
            bucket["first"] = [305000, 1]
        elif case == "future_last":
            bucket["last"] = [305000, 4]
        elif case == "future_coverage":
            value["coverage_start_ms"] = 305000
        elif case == "future_watermark":
            value["watermark_ms"] = 305000
        elif case == "scope":
            value["scope"] = None
        elif case == "prototype_scope":
            prototype["asset"] = "ETH"
        elif case == "prototype_kind":
            prototype["kind"] = "spot"
        elif case == "prototype_id":
            prototype["event_id"] = "invalid"
        elif case == "prototype_version":
            prototype["schema_version"] = True
        elif case == "bool_clock":
            value["as_of_ms"] = True
        elif case == "bool_count":
            bucket["count"] = True
        elif case == "bool_key":
            bucket["first"][1] = True
        elif case == "bool_complete":
            bucket["complete"] = 1
        elif case == "float_version":
            value["format_version"] = 1.0
        elif case == "unknown":
            value["extra"] = "refuse"
        elif case == "nested_unknown":
            bucket["extra"] = "refuse"
        elif case == "duplicate_bucket":
            value["buckets"].append(deepcopy(bucket))
        elif case == "unordered":
            value["buckets"].reverse()
        elif case == "unconfigured_interval":
            value["timeframes_ms"] = [900000]
        elif case == "alignment":
            bucket["opening_ms"] += 1
        elif case == "finished":
            value["as_of_ms"] = 600000
            value["watermark_ms"] = 600000
        elif case == "ohlc":
            bucket["high"] = 80.0
        elif case == "negative_volume":
            bucket["volume_decimal"] = "-0.6"
        elif case == "nan_volume":
            bucket["volume_decimal"] = "NaN"
        elif case == "overflow_volume":
            bucket["volume_decimal"] = "1E+10000"
        elif case == "whitespace_decimal":
            bucket["volume_decimal"] = " 0.6 "
        elif case == "non_decimal_qty":
            bucket["volume_decimal"] = "0.01"
        elif case == "first_after_last":
            bucket["first"] = [303001, 4]
        elif case == "outside_interval":
            bucket["last"] = [600000, 4]
        elif case == "prototype_outside_keys":
            bucket["last"] = [302000, 2]
        elif case == "prototype_open_conflict":
            bucket["first"] = [303000, 3]
            bucket["open"] = 109.0
        elif case == "count_one":
            bucket["count"] = 1
        elif case == "complete_before_coverage":
            bucket["complete"] = True
            value["coverage_start_ms"] = 301000
        elif case == "duplicate_timeframe":
            value["timeframes_ms"] = [300000, 300000]
        elif case == "bool_timeframe":
            value["timeframes_ms"] = [True]
        else:
            prototype["payload_json"] = '{"price":110.0,"qty":0.1,"trade_id":3,"trade_id":3}'

    with pytest.raises(IntegrityError):
        restore(altered(data, change), now=600000)


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"[]",
        b"{",
        b"\xff",
        b'{"x":NaN}',
        b'{"format_version":1,"format_version":1}',
        b'{"buckets":{}}',
    ],
)
def test_malformed_unicode_json_nonfinite_and_duplicate_fields_refused(data):
    with pytest.raises(IntegrityError, match="BAR_STATE_INVALID"):
        decode_bar_state(data)


@pytest.mark.parametrize("data", ["{}", bytearray(b"{}"), None])
def test_only_bytes_are_accepted(data):
    with pytest.raises(ValueError):
        decode_bar_state(data)


def test_models_are_frozen_and_restored_mutation_is_independent():
    data = encode_bar_state(builder(), 304000)
    state = decode_bar_state(data)
    with pytest.raises(ValidationError):
        state.as_of_ms = 305000
    with pytest.raises(ValidationError):
        state.buckets[0].count = 9
    first = restore(data)
    second = restore(data)
    first.buckets[(300000, 300000)].count = 99
    first.buckets[(300000, 300000)].volume = Decimal("9")
    first.buckets[(300000, 300000)].prototype.payload()["qty"] = 99
    assert second.buckets[(300000, 300000)].count == 3
    assert decode_bar_state(data) == state and isinstance(state, BarState)


@pytest.mark.parametrize(
    "case",
    [
        "wrong_key",
        "bad_scope_shape",
        "float_volume",
        "future_receipt",
        "nonfinite_price",
        "missing_scope",
    ],
)
def test_encoding_refuses_live_builder_corruption_without_mutating_it(case):
    original = builder()
    bucket = original.buckets[(300000, 300000)]
    if case == "wrong_key":
        original.buckets[(300000, 300001)] = original.buckets.pop((300000, 300000))
    elif case == "bad_scope_shape":
        original.identity = (*original.identity, "extra")
    elif case == "float_volume":
        bucket.volume = 0.6
    elif case == "future_receipt":
        bucket.prototype = bucket.prototype.model_copy(update={"observed_at_ms": 305000})
    elif case == "nonfinite_price":
        bucket.high = float("inf")
    else:
        original.identity = None
    before = deepcopy(vars(original))
    with pytest.raises(IntegrityError, match="BAR_STATE_INVALID"):
        encode_bar_state(original, 304000)
    assert vars(original) == before


@pytest.mark.parametrize(
    "clock_field",
    [
        "event_time_ms",
        "observed_at_ms",
        "available_at_ms",
        "coverage_start_ms",
        "watermark_ms",
        "source_timestamp",
    ],
)
def test_any_nested_explicit_availability_clock_cannot_leak_after_cutoff(clock_field):
    data = encode_bar_state(builder(), 304000)

    def change(value):
        for bucket in value["buckets"]:
            payload = json.loads(bucket["prototype"]["payload_json"])
            payload["native_extra"] = {
                "nested": [
                    {
                        clock_field: "1970-01-01T00:05:05+00:00"
                        if clock_field == "source_timestamp"
                        else 305000
                    }
                ]
            }
            bucket["prototype"]["payload_json"] = json.dumps(payload)

    with pytest.raises(IntegrityError, match="BAR_STATE_INVALID"):
        decode_bar_state(altered(data, change))


@pytest.mark.parametrize("bad", [True, 1.0, "304000", -1])
def test_nested_clock_types_are_not_coerced(bad):
    data = encode_bar_state(builder(), 304000)

    def change(value):
        payload = json.loads(value["buckets"][0]["prototype"]["payload_json"])
        payload["native_extra"] = {"observed_at_ms": bad}
        value["buckets"][0]["prototype"]["payload_json"] = json.dumps(payload)

    with pytest.raises(IntegrityError):
        decode_bar_state(altered(data, change))


def test_known_duration_fields_are_not_misrepresented_as_absolute_clocks():
    original = builder()
    for bucket in original.buckets.values():
        payload = bucket.prototype.payload()
        payload["native_extra"] = {
            "observed_at_ms": 304000,
            "interval_ms": 86400000,
            "lateness_ms": 500000,
        }
        bucket.prototype = bucket.prototype.model_copy(update={"payload_json": json.dumps(payload)})
    encoded = encode_bar_state(original, 304000)
    assert decode_bar_state(encoded).as_of_ms == 304000


@pytest.mark.parametrize(
    "case", ["nonfinite_extra", "naive_source_time", "invalid_source_time", "nested_depth"]
)
def test_prototype_extra_payload_is_finite_and_bounded(case):
    data = encode_bar_state(builder(), 304000)

    def change(value):
        payload = json.loads(value["buckets"][0]["prototype"]["payload_json"])
        if case == "nonfinite_extra":
            payload["native_extra"] = {"value": float("inf")}
        elif case == "naive_source_time":
            payload["source_timestamp"] = "1970-01-01T00:05:03"
        elif case == "invalid_source_time":
            payload["source_timestamp"] = "not an ISO timestamp"
        else:
            nested = {"value": "terminal"}
            for _ in range(65):
                nested = {"nested": nested}
            payload["native_extra"] = nested
        value["buckets"][0]["prototype"]["payload_json"] = json.dumps(payload)

    with pytest.raises(IntegrityError):
        decode_bar_state(altered(data, change))


def test_generic_trade_bars_configs_beyond_codec_policy_are_explicitly_refused():
    original = TradeBars((172800,), 300000, 5000)
    with pytest.raises(IntegrityError):
        encode_bar_state(original, 304000)
    original = TradeBars((300,), 300000, 86400001)
    with pytest.raises(IntegrityError):
        encode_bar_state(original, 304000)


def test_single_trade_bucket_consistency_and_scientific_decimal_volume():
    original = TradeBars((300,), 300000, 5000)
    original.add(tick(qty=1e-7))
    encoded = encode_bar_state(original, 304000)
    recovered = restore_bar_state(encoded, 305000, timeframes_s=(300,), lateness_ms=5000)
    assert recovered.buckets[(300000, 300000)].volume.as_tuple() == Decimal("1E-7").as_tuple()
