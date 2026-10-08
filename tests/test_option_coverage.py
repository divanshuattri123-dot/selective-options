"""Bound membership/receipt fixtures; no public capacity or historical continuity claim."""

import json
from unittest.mock import AsyncMock

import pytest

from selective_options.collectors import ChannelRegistry, Notification, discover_and_record
from selective_options.config import AppConfig
from selective_options.events import make_event
from selective_options.models import (
    BookSummary,
    ChainRow,
    ChainSnapshot,
    DiscoveryResult,
    Instrument,
)
from selective_options.option_coverage import coverage_at
from selective_options.rate_limit import EndpointLimiter
from selective_options.store import DuckStore


def control(reason, receipt, **body):
    return make_event(
        source="deribit",
        environment="testnet",
        asset="SYSTEM",
        symbol="SYSTEM",
        kind="status",
        timestamp_kind="receipt",
        event_time_ms=receipt,
        observed_at_ms=receipt,
        payload={"reason": reason, **body},
    )


def metadata(instrument, receipt=1000):
    parsed = Instrument.model_validate(instrument)
    return make_event(
        source="deribit",
        environment="testnet",
        asset="BTC",
        symbol=parsed.instrument_name,
        kind="instrument",
        timestamp_kind="receipt",
        event_time_ms=receipt,
        observed_at_ms=receipt,
        payload=parsed.model_dump(mode="json"),
    )


def fixture(instrument, ticker, selected=True):
    meta = metadata(instrument)
    symbol = meta.symbol
    manifest = control(
        "UNIVERSE_DISCOVERED",
        1100,
        asset="BTC",
        available=1,
        selected=int(selected),
        coverage="FULL" if selected else "PARTIAL_OR_ABSENT",
        instruments=[symbol],
        selected_instruments=[symbol] if selected else [],
        metadata_event_ids=[meta.event_id],
    )
    start = control("STREAM_RESYNC", 1200, connection=1)
    ack = control(
        "STREAM_SUBSCRIBED",
        1400,
        connection=1,
        channels=4,
        instruments=[{"asset": "BTC", "symbol": symbol}] if selected else [],
        universe_event_ids=[manifest.event_id],
    )
    registry = ChannelRegistry()
    registry.add_instrument("BTC", symbol, "btc_usd")
    quotes = []
    for channel, data in [
        (f"ticker.{symbol}.100ms", ticker | {"timestamp": 1300}),
        (
            f"book.{symbol}.none.20.100ms",
            {
                "instrument_name": symbol,
                "timestamp": 1300,
                "change_id": 1,
                "bids": [[99, 2]],
                "asks": [[101, 2]],
            },
        ),
    ]:
        quotes.extend(
            registry.normalize(
                Notification.model_validate(
                    {
                        "jsonrpc": "2.0",
                        "method": "subscription",
                        "params": {"channel": channel, "data": data},
                    }
                ),
                1300,
            )
        )
    return [meta, manifest, start, ack, *quotes]


def replace_payload(row, **changes):
    body = row.payload() | changes
    if row.kind == "status":
        reason = body.pop("reason")
        return control(reason, row.observed_at_ms, **body)
    return row.model_copy(update={"payload_json": json.dumps(body)})


def test_exact_full_manifest_and_leg_coverage(instrument, ticker):
    rows = fixture(instrument, ticker)
    result = coverage_at(rows, "BTC", 1500)
    assert result["state"] == "SELECTED_FEEDS_OBSERVED"
    assert result["full_discovered_membership"] is True
    assert result["observed_leg_observations"] == result["expected_leg_observations"] == 2
    assert result["trade_history_continuity_verified"] is False
    assert result["all_options_native_fields_available"] is False
    assert result["trading_approval"] is False
    assert coverage_at([*rows, *rows], "BTC", 1500) == result


async def test_real_storage_pit_export_and_replay(instrument, ticker, tmp_path):
    rows = fixture(instrument, ticker)
    store = await DuckStore.open(tmp_path / "events.duckdb")
    try:
        await store.append(rows)
        await store.append(rows)

        async def view(cutoff):
            return coverage_at(
                [
                    *(await store.snapshot("SYSTEM", cutoff)).rows(),
                    *(await store.snapshot("BTC", cutoff)).rows(),
                ],
                "BTC",
                cutoff,
            )

        assert (await view(1399))["blocker"] == "OPTION_STREAM_INACTIVE"
        assert (await view(1400))["state"] == "SELECTED_FEEDS_OBSERVED"
        assert await store.export_parquet(tmp_path / "events.parquet") == len(rows)
    finally:
        await store.close()


def test_capped_membership_does_not_become_full(instrument, ticker):
    rows = fixture(instrument, ticker)
    second = metadata(instrument | {"instrument_name": "BTC-OTHER", "strike": 120000})
    rows.append(second)
    manifest = replace_payload(
        rows[1],
        available=2,
        coverage="PARTIAL_OR_ABSENT",
        instruments=[rows[0].symbol, second.symbol],
        metadata_event_ids=[rows[0].event_id, second.event_id],
    )
    rows[1] = manifest
    rows[3] = replace_payload(rows[3], universe_event_ids=[manifest.event_id])
    result = coverage_at(rows, "BTC", 1500)
    assert (
        result["state"] == "SELECTED_FEEDS_OBSERVED"
        and result["full_discovered_membership"] is False
    )


@pytest.mark.parametrize("reason", ["STREAM_DISCONNECTED", "STREAM_REFRESH_DUE", "STREAM_RESYNC"])
def test_latest_inactive_control_overrides_busy_recent_quotes(instrument, ticker, reason):
    rows = fixture(instrument, ticker)
    rows.append(control(reason, 1501, connection=1))
    assert coverage_at(rows, "BTC", 1501)["blocker"] == "OPTION_STREAM_INACTIVE"
    assert coverage_at(rows, "BTC", 1500)["state"] == "SELECTED_FEEDS_OBSERVED"


def test_no_controls_legacy_and_tied_controls(instrument, ticker):
    rows = fixture(instrument, ticker)
    assert coverage_at(rows[:2], "BTC", 1500)["blocker"] == "OPTION_STREAM_CONTROL_MISSING"
    old = rows[3].payload()
    old.pop("universe_event_ids")
    reason = old.pop("reason")
    rows[3] = control(reason, 1400, **old)
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_LEGACY_MANIFEST_BINDING_MISSING"
    assert (
        coverage_at([*rows, control("STREAM_REFRESH_DUE", 1400, connection=1)], "BTC", 1500)[
            "blocker"
        ]
        == "OPTION_STREAM_CONTROL_AMBIGUOUS"
    )


@pytest.mark.parametrize(
    "changes,blocker",
    [
        ({"connection": True}, "OPTION_INVALID_EVIDENCE"),
        ({"channels": 2}, "OPTION_CHANNEL_COUNT"),
        ({"universe_event_ids": ["x", "x"]}, "OPTION_SUBSCRIPTION_DUPLICATES"),
        ({"unexpected": True}, "OPTION_SUBSCRIPTION_CANONICAL"),
        ({"universe_event_ids": ["missing"]}, "OPTION_SCOPED_MANIFEST_MISSING_OR_AMBIGUOUS"),
        ({"connection": 2}, "OPTION_RESYNC_MISSING_OR_AMBIGUOUS"),
    ],
)
def test_bad_ack_binding(instrument, ticker, changes, blocker):
    rows = fixture(instrument, ticker)
    rows[3] = replace_payload(rows[3], **changes)
    assert coverage_at(rows, "BTC", 1500)["blocker"] == blocker


@pytest.mark.parametrize(
    "changes,blocker",
    [
        ({"available": True}, "OPTION_INVALID_EVIDENCE"),
        ({"available": 2}, "OPTION_MANIFEST_SCOPE_OR_COUNTS"),
        ({"selected": 0}, "OPTION_MANIFEST_SCOPE_OR_COUNTS"),
        ({"coverage": "PARTIAL_OR_ABSENT"}, "OPTION_MANIFEST_SCOPE_OR_COUNTS"),
        ({"selected_instruments": ["wrong"]}, "OPTION_MANIFEST_SCOPE_OR_COUNTS"),
        ({"instruments": ["wrong"]}, "OPTION_METADATA_CANONICAL_OR_SCOPE"),
        ({"metadata_event_ids": ["missing"]}, "OPTION_METADATA_MISSING_OR_CONFLICTING"),
        ({"unexpected": True}, "OPTION_MANIFEST_CANONICAL"),
    ],
)
def test_bad_manifest(instrument, ticker, changes, blocker):
    rows = fixture(instrument, ticker)
    rows[1] = replace_payload(rows[1], **changes)
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    # A wrong whole universe also changes selected membership if it is to reach metadata validation.
    if changes == {"instruments": ["wrong"]}:
        rows[1] = replace_payload(rows[1], selected_instruments=["wrong"])
        rows[3] = replace_payload(
            rows[3],
            instruments=[{"asset": "BTC", "symbol": "wrong"}],
            universe_event_ids=[rows[1].event_id],
        )
    assert coverage_at(rows, "BTC", 1500)["blocker"] == blocker


def test_manifest_reference_cannot_be_changed_to_future_snapshot(instrument, ticker):
    rows = fixture(instrument, ticker)
    body = rows[1].payload()
    reason = body.pop("reason")
    rows[1] = control(reason, 1250, **body)
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_MANIFEST_SCOPE_OR_COUNTS"


@pytest.mark.parametrize(
    "change", [{"event_id": "bad"}, {"observed_at_ms": 1001}, {"timestamp_kind": "source"}]
)
def test_metadata_tampering(instrument, ticker, change):
    rows = fixture(instrument, ticker)
    rows[0] = rows[0].model_copy(update=change)
    rows[1] = replace_payload(rows[1], metadata_event_ids=[rows[0].event_id])
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_METADATA_CANONICAL_OR_SCOPE"


def test_manifest_age_and_expiry_are_separate(instrument, ticker):
    rows = fixture(instrument, ticker)
    assert (
        coverage_at(rows, "BTC", 1500, max_universe_age_ms=399)["blocker"]
        == "OPTION_UNIVERSE_STALE"
    )
    rows = fixture(instrument | {"expiration_timestamp": 1500}, ticker)
    assert coverage_at(rows, "BTC", 1499)["state"] == "SELECTED_FEEDS_OBSERVED"
    assert (
        coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_UNIVERSE_EXPIRED_REDISCOVERY_REQUIRED"
    )


def test_missing_stale_and_old_session_legs(instrument, ticker):
    rows = fixture(instrument, ticker)
    assert coverage_at(rows[:-1], "BTC", 1500)["observed_leg_observations"] == 1
    result = coverage_at(rows, "BTC", 1501, max_leg_age_ms=200)
    assert (
        result["state"] == "INCOMPLETE_LEG_OBSERVATIONS"
        and result["observed_leg_observations"] == 0
    )
    rows[-1] = rows[-1].model_copy(update={"observed_at_ms": 1199})
    result = coverage_at(rows, "BTC", 1500)
    assert (
        result["state"] == "INCOMPLETE_LEG_OBSERVATIONS"
        and result["observed_leg_observations"] == 1
    )


def test_latest_leg_conflict_and_canonical_integrity(instrument, ticker):
    rows = fixture(instrument, ticker)
    assert (
        coverage_at([*rows, rows[-1].model_copy(update={"event_id": "other"})], "BTC", 1500)[
            "blocker"
        ]
        == "OPTION_LEG_AMBIGUOUS"
    )
    rows[-1] = rows[-1].model_copy(update={"event_id": "other"})
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_LEG_CANONICAL"
    rows[-1] = rows[-1].model_copy(update={"payload_json": "{}"})
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_INVALID_EVIDENCE"


def test_future_other_scope_and_eligible_budget(instrument, ticker):
    rows = fixture(instrument, ticker)
    future = rows[-1].model_copy(update={"observed_at_ms": 1501, "payload_json": "{}"})
    other = rows[-1].model_copy(update={"asset": "ETH", "payload_json": "{}"})
    assert (
        coverage_at([*rows, *([future, other] * 5)], "BTC", 1500, max_rows=6)["state"]
        == "SELECTED_FEEDS_OBSERVED"
    )
    assert coverage_at(rows, "BTC", 1500, max_rows=5)["blocker"] == "OPTION_COVERAGE_OVER_BUDGET"


@pytest.mark.parametrize(
    "policy",
    [
        {"asset": "unknown"},
        {"as_of_ms": True},
        {"max_leg_age_ms": 0},
        {"max_universe_age_ms": -1},
        {"max_rows": 200001},
    ],
)
def test_policy_preflight(policy):
    with pytest.raises(ValueError):
        coverage_at([], **{"asset": "BTC", "as_of_ms": 1500, **policy})


async def test_runtime_discovery_binds_exact_metadata_to_acknowledged_selection(
    monkeypatch, instrument, summary
):
    parsed = Instrument.model_validate(instrument)
    chain = ChainSnapshot(
        asset="BTC",
        observed_at_ms=1000,
        discovery=DiscoveryResult(
            asset="BTC", query_currency="BTC", fallback_used=False, instruments=(parsed,)
        ),
        rows=(ChainRow(instrument=parsed, summary=BookSummary.model_validate(summary)),),
    )

    class Connection:
        def __init__(self, *args):
            pass

        async def __aenter__(self):
            return None

        async def __aexit__(self, *args):
            pass

    monkeypatch.setattr("selective_options.collectors.WebSocketRpc", Connection)
    monkeypatch.setattr(
        "selective_options.collectors.DeribitAdapter.option_chain", AsyncMock(return_value=chain)
    )
    sink = AsyncMock()
    cfg = AppConfig.model_validate(
        {"system": {"assets": ["BTC"]}, "ingestion": {"max_stream_instruments_per_asset": None}}
    )
    registry = await discover_and_record(cfg, sink, EndpointLimiter(cfg.rate_limits))
    rows = [call.args[0] for call in sink.await_args_list]
    manifest = next(row for row in rows if row.kind == "status")
    assert registry.universe_event_ids == [manifest.event_id]
    assert manifest.payload()["metadata_event_ids"] == [
        next(row.event_id for row in rows if row.kind == "instrument")
    ]
    assert (
        manifest.payload()["selected_instruments"]
        == manifest.payload()["instruments"]
        == [parsed.instrument_name]
    )


def test_empty_selection_duplicate_members_and_resync_integrity(instrument, ticker):
    assert (
        coverage_at(fixture(instrument, ticker, selected=False), "BTC", 1500)["blocker"]
        == "OPTION_SELECTED_UNIVERSE_EMPTY"
    )
    rows = fixture(instrument, ticker)
    rows[3] = replace_payload(rows[3], instruments=rows[3].payload()["instruments"] * 2)
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_SUBSCRIPTION_DUPLICATES"
    rows = fixture(instrument, ticker)
    rows[2] = replace_payload(rows[2], connection=True)
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_RESYNC_MISSING_OR_AMBIGUOUS"
    rows = fixture(instrument, ticker)
    assert (
        coverage_at([*rows, control("STREAM_RESYNC", 1201, connection=1)], "BTC", 1500)["blocker"]
        == "OPTION_RESYNC_MISSING_OR_AMBIGUOUS"
    )


@pytest.mark.parametrize("field", ["instruments", "selected_instruments", "metadata_event_ids"])
def test_duplicate_manifest_membership_is_not_full_coverage(instrument, ticker, field):
    rows = fixture(instrument, ticker)
    rows[1] = replace_payload(rows[1], **{field: rows[1].payload()[field] * 2})
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_MANIFEST_SCOPE_OR_COUNTS"


@pytest.mark.parametrize("change", [{"is_active": False}, {"base_currency": "ETH"}])
def test_inactive_or_wrong_native_asset_metadata_blocks(instrument, ticker, change):
    rows = fixture(instrument, ticker)
    rows[0] = metadata(instrument | change)
    rows[1] = replace_payload(rows[1], metadata_event_ids=[rows[0].event_id])
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_METADATA_CANONICAL_OR_SCOPE"


def test_metadata_revision_and_control_hash_conflicts_block(instrument, ticker):
    rows = fixture(instrument, ticker)
    assert (
        coverage_at([*rows, rows[0].model_copy(update={"observed_at_ms": 1001})], "BTC", 1500)[
            "blocker"
        ]
        == "OPTION_METADATA_MISSING_OR_CONFLICTING"
    )
    rows[0] = metadata(instrument, receipt=1150)
    rows[1] = replace_payload(rows[1], metadata_event_ids=[rows[0].event_id])
    rows[3] = replace_payload(rows[3], universe_event_ids=[rows[1].event_id])
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_METADATA_CANONICAL_OR_SCOPE"
    rows = fixture(instrument, ticker)
    rows[2] = rows[2].model_copy(update={"event_id": "tampered"})
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_STATUS_CANONICAL"


@pytest.mark.parametrize("age", [59, 86401, float("nan"), float("inf")])
def test_universe_age_config_has_independent_finite_bounds(age):
    with pytest.raises(ValueError):
        AppConfig.model_validate({"ingestion": {"option_universe_max_age_s": age}})


def test_two_symbols_require_exact_single_index_channel_count(instrument, ticker):
    rows = fixture(instrument, ticker)
    second = metadata(instrument | {"instrument_name": "BTC-OTHER", "strike": 120000})
    rows.append(second)
    names = [rows[0].symbol, second.symbol]
    rows[1] = replace_payload(
        rows[1],
        available=2,
        selected=2,
        instruments=names,
        selected_instruments=names,
        metadata_event_ids=[rows[0].event_id, second.event_id],
    )
    rows[3] = replace_payload(
        rows[3],
        channels=8,
        instruments=[{"asset": "BTC", "symbol": name} for name in names],
        universe_event_ids=[rows[1].event_id],
    )
    assert coverage_at(rows, "BTC", 1500)["blocker"] == "OPTION_CHANNEL_COUNT"
    rows[3] = replace_payload(rows[3], channels=7)
    assert coverage_at(rows, "BTC", 1500)["state"] == "INCOMPLETE_LEG_OBSERVATIONS"
