"""The DuckStore rows below are synthetic fixtures; no provider requests are made."""

import hashlib
import json

import pytest

from selective_options.coinmetrics import (
    DAY_MS,
    AcceptedObservation,
    DailyRow,
    PollHealth,
    coinmetrics_poll_health,
    native_time_ms,
    normalize_row,
)
from selective_options.coinmetrics_checkpoint import load_coinmetrics_resume
from selective_options.errors import IntegrityError
from selective_options.store import DuckStore

LABEL = native_time_ms("2026-10-05T00:00:00.000000000Z")
START = LABEL + 2 * DAY_MS + 100
RECEIPT = START + 10
FINISH = START + 20


def observation(asset: str, *, receipt: int = RECEIPT, started: int = START, **changes):
    return normalize_row(
        DailyRow.model_validate(
            {
                "asset": asset.lower(),
                "time": "2026-10-05T00:00:00.000000000Z",
                "CapMrktCurUSD": "1724754820477.82986243790963106",
                "CapMVRVCur": "1.596729720053423595",
                **changes,
            }
        ),
        receipt,
        started,
    )


def healthy(*events, started: int = START, finished: int = FINISH):
    assert events
    periods = [event.event_time_ms for event in events]
    return coinmetrics_poll_health(
        events[0].asset,
        PollHealth(
            frequency="1d",
            window_start_ms=min(periods),
            window_end_ms=max(periods),
            ok=True,
            reason="DAILY_REGIME_OBSERVED",
            poll_started_at_ms=started,
            poll_finished_at_ms=finished,
            response_received_at_ms=max(event.observed_at_ms for event in events),
            accepted_observations=tuple(
                AcceptedObservation(
                    event_id=event.event_id,
                    observed_at_ms=event.observed_at_ms,
                    period_label_ms=event.event_time_ms,
                )
                for event in events
            ),
        ),
    )


def failed(asset: str, *, started: int, finished: int):
    return coinmetrics_poll_health(
        asset,
        PollHealth(
            frequency="1d",
            window_start_ms=LABEL,
            window_end_ms=LABEL,
            ok=False,
            reason="ACCESS_DENIED",
            code=403,
            poll_started_at_ms=started,
            poll_finished_at_ms=finished,
            response_received_at_ms=finished,
        ),
    )


def valid_pair():
    btc, eth = observation("BTC"), observation("ETH")
    return btc, healthy(btc), eth, healthy(eth)


async def database(path, rows):
    store = await DuckStore.open(path)
    try:
        await store.append(list(rows), exact=True)
    finally:
        await store.close()
    return path


async def load(path, as_of_ms=FINISH, **changes):
    return await load_coinmetrics_resume(
        path,
        as_of_ms,
        max_poll_age_ms=changes.pop("max_poll_age_ms", DAY_MS),
        max_data_age_ms=changes.pop("max_data_age_ms", 3 * DAY_MS),
        **changes,
    )


async def test_loads_all_exact_scope_events_in_stable_order_and_read_only(tmp_path):
    btc, btc_health, eth, eth_health = valid_pair()
    unrelated = btc.model_copy(update={"event_id": "a" * 64, "source": "other_source"})
    future_receipt = observation("BTC", receipt=FINISH + 1, started=START + 1)
    future_source = btc.model_copy(update={"event_id": "b" * 64, "event_time_ms": FINISH + 1})
    path = await database(
        tmp_path / "events.duckdb",
        [future_source, unrelated, eth_health, future_receipt, btc, eth, btc_health],
    )
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    actual = await load(path, max_rows=4)
    expected = tuple(
        sorted(
            (btc, btc_health, eth, eth_health),
            key=lambda event: (event.observed_at_ms, event.event_time_ms, event.event_id),
        )
    )
    assert actual == expected
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_history_preserves_each_bound_revision_and_original_receipt(tmp_path):
    old_start = START - 100
    old_receipt = RECEIPT - 50
    old_btc = observation("BTC", receipt=old_receipt, started=old_start)
    latest_btc = observation("BTC", receipt=RECEIPT, started=START, CapMVRVCur="1.7")
    old_eth = observation("ETH", receipt=old_receipt, started=old_start)
    latest_eth = observation("ETH", receipt=RECEIPT, started=START, CapMVRVCur="1.8")
    rows = (
        old_btc,
        healthy(old_btc, started=old_start, finished=FINISH - 50),
        latest_btc,
        healthy(latest_btc),
        old_eth,
        healthy(old_eth, started=old_start, finished=FINISH - 50),
        latest_eth,
        healthy(latest_eth),
    )
    path = await database(tmp_path / "events.duckdb", rows)

    selected = await load(path, max_rows=8)
    assert selected == tuple(
        sorted(rows, key=lambda event: (event.observed_at_ms, event.event_time_ms, event.event_id))
    )
    assert {
        event.event_id: event.observed_at_ms for event in selected if event.kind == "on_chain"
    } == {
        old_btc.event_id: old_receipt,
        latest_btc.event_id: RECEIPT,
        old_eth.event_id: old_receipt,
        latest_eth.event_id: RECEIPT,
    }


async def test_latest_failed_poll_blocks_previous_success(tmp_path):
    btc, btc_health, eth, eth_health = valid_pair()
    latest_failure = failed("BTC", started=START + 100, finished=FINISH + 100)
    path = await database(
        tmp_path / "events.duckdb", [btc, btc_health, eth, eth_health, latest_failure]
    )

    with pytest.raises(IntegrityError, match="COINMETRICS_LATEST_POLL_FAILED"):
        await load(path, FINISH + 100)


async def test_tied_latest_health_is_ambiguous(tmp_path):
    btc, btc_health, eth, eth_health = valid_pair()
    tied_failure = failed("BTC", started=START + 1, finished=FINISH)
    path = await database(
        tmp_path / "events.duckdb", [btc, btc_health, eth, eth_health, tied_failure]
    )

    with pytest.raises(IntegrityError, match="COINMETRICS_AMBIGUOUS_LATEST_HEALTH"):
        await load(path)


async def test_stale_poll_and_daily_period_are_checked_separately(tmp_path):
    rows = valid_pair()
    path = await database(tmp_path / "events.duckdb", rows)

    with pytest.raises(IntegrityError, match="COINMETRICS_STALE_POLL_OR_RESPONSE"):
        await load(path, FINISH + 101, max_poll_age_ms=100, max_data_age_ms=3 * DAY_MS)
    with pytest.raises(IntegrityError, match="COINMETRICS_STALE_DAILY_PERIOD"):
        await load(path, FINISH, max_poll_age_ms=DAY_MS, max_data_age_ms=DAY_MS)


async def test_unbound_older_observation_is_not_hidden_by_newer_healthy_poll(tmp_path):
    old = observation("BTC", receipt=RECEIPT - 50, started=START - 100)
    btc, btc_health, eth, eth_health = valid_pair()
    path = await database(tmp_path / "events.duckdb", [old, btc, btc_health, eth, eth_health])

    with pytest.raises(IntegrityError, match="COINMETRICS_CHECKPOINT_UNBOUND_OBSERVATION"):
        await load(path)


async def test_mismatched_historical_health_reference_is_rejected(tmp_path):
    btc, _, eth, eth_health = valid_pair()
    wrong_reference = AcceptedObservation(
        event_id=btc.event_id,
        observed_at_ms=btc.observed_at_ms + 1,
        period_label_ms=btc.event_time_ms,
    )
    mismatched_health = coinmetrics_poll_health(
        "BTC",
        PollHealth(
            frequency="1d",
            window_start_ms=btc.event_time_ms,
            window_end_ms=btc.event_time_ms,
            ok=True,
            reason="DAILY_REGIME_OBSERVED",
            poll_started_at_ms=START,
            poll_finished_at_ms=FINISH,
            response_received_at_ms=RECEIPT + 1,
            accepted_observations=(wrong_reference,),
        ),
    )
    path = await database(tmp_path / "events.duckdb", [btc, mismatched_health, eth, eth_health])

    with pytest.raises(IntegrityError, match="COINMETRICS_BOUND_OBSERVATION_MISMATCH"):
        await load(path)


async def test_native_metric_schema_mismatch_is_rejected(tmp_path):
    btc, btc_health, eth, eth_health = valid_pair()
    malformed = btc.model_copy(
        update={"payload_json": json.dumps({**btc.payload(), "InventedMetric": "1"})}
    )
    path = await database(tmp_path / "events.duckdb", [malformed, btc_health, eth, eth_health])

    with pytest.raises(IntegrityError, match="COINMETRICS_INVALID_EVIDENCE"):
        await load(path)


@pytest.mark.parametrize("limit", [3, 4])
async def test_row_and_byte_budgets_fail_closed(tmp_path, limit):
    rows = valid_pair()
    path = await database(tmp_path / "events.duckdb", rows)
    policy = {"max_rows": 3} if limit == 3 else {"max_bytes": 1}

    with pytest.raises(IntegrityError, match="COINMETRICS_CHECKPOINT_OVER_BUDGET"):
        await load(path, **policy)


@pytest.mark.parametrize(
    "policy",
    [
        {"as_of_ms": True},
        {"max_poll_age_ms": 0},
        {"max_data_age_ms": 2**63},
        {"max_rows": True},
        {"max_rows": 100_001},
        {"max_bytes": 0},
        {"max_bytes": 64_000_001},
    ],
)
async def test_invalid_policy_refuses_before_open(tmp_path, monkeypatch, policy):
    monkeypatch.setattr(
        "selective_options.coinmetrics_checkpoint.duckdb.connect",
        lambda *args, **kwargs: pytest.fail("invalid policy must be rejected before opening"),
    )
    options = {"max_poll_age_ms": DAY_MS, "max_data_age_ms": 3 * DAY_MS}
    as_of = policy.pop("as_of_ms", FINISH)
    options.update(policy)

    with pytest.raises(ValueError, match="COINMETRICS_CHECKPOINT_INVALID_POLICY"):
        await load_coinmetrics_resume(tmp_path / "absent.duckdb", as_of, **options)


async def test_missing_database_does_not_create_file(tmp_path):
    absent = tmp_path / "absent.duckdb"

    with pytest.raises(FileNotFoundError, match="COINMETRICS_CHECKPOINT_DATABASE_MISSING"):
        await load(absent)
    assert not absent.exists()


async def test_requires_current_healthy_bound_history_for_both_assets(tmp_path):
    btc, btc_health, _, _ = valid_pair()
    path = await database(tmp_path / "events.duckdb", [btc, btc_health])

    with pytest.raises(IntegrityError, match="COINMETRICS_NO_BOUND_POLL"):
        await load(path)
