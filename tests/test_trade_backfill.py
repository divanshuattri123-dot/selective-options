import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import duckdb
import pytest

from selective_options.config import AppConfig
from selective_options.errors import IntegrityError
from selective_options.spot_trades import KrakenTrade, trade_event
from selective_options.store import DuckStore
from selective_options.trade_backfill import (
    MAX_BYTES,
    backfill_trades,
    collect_range,
    fetch_page,
    parse_page,
)


def anchor():
    return trade_event(
        KrakenTrade(
            symbol="BTC/USD",
            side="buy",
            price=100,
            qty=0.1,
            trade_id=3,
            timestamp=datetime.fromtimestamp(303, UTC),
        ),
        "BTC",
        500000,
    )


def body(ids, cursor="310000000000", **changes):
    return json.dumps(
        {
            "error": [],
            "result": {
                "BTC/USD": [["100.00000", "0.10000000", 300 + i, "b", "l", "", i] for i in ids],
                "last": cursor,
            },
        }
        | changes
    ).encode()


def config(**changes):
    return AppConfig.model_validate(
        {"ingestion": {"kraken_backfill_page_size": 2, "kraken_backfill_max_pages": 4, **changes}}
    )


async def seed(path):
    store = await DuckStore.open(path)
    await store.append([anchor()])
    await store.close()


def test_decimal_timestamp_is_not_rounded_through_binary_float():
    raw = b'{"error":[],"result":{"BTC/USD":[["100.00000","0.10000000",303.123456789,"b","l","",4]],"last":"303123456789"}}'
    rows, cursor = parse_page(raw, "BTC/USD", 1000)
    assert rows[0].timestamp == Decimal("303.123456789")
    event = rows[0].event(anchor(), 600000)
    assert event.source == "kraken_rest" and event.event_time_ms == 303123
    assert event.payload()["source_timestamp_seconds"] == "303.123456789"
    assert event.payload()["price_decimal"] == "100.00000"
    assert cursor == "303123456789"


@pytest.mark.parametrize(
    "raw,error",
    [
        (body([], error=["rate limit"]), "PROVIDER_ERROR"),
        (body([3]), "PAIR_MISMATCH"),
        (body([4, 3]), "UNORDERED"),
        (body([3], cursor="bad"), "INVALID_PAGE"),
        (body([3, 4, 5]), "INVALID_PAGE"),
        (b" " * (MAX_BYTES + 1), "TOO_LARGE"),
    ],
    ids=["provider", "pair", "order", "cursor", "count", "size"],
)
def test_invalid_response_fails(raw, error):
    with pytest.raises(IntegrityError, match=error):
        parse_page(raw, "ETH/USD" if error == "PAIR_MISMATCH" else "BTC/USD", 2)


async def test_paginated_overlap_range_persists_correct_pit(tmp_path, monkeypatch):
    database = tmp_path / "before.duckdb"
    await seed(database)
    fetch = AsyncMock(
        side_effect=[
            (body([2, 3], "304000000000"), 600000),
            (body([3, 4], "305000000000"), 601000),
            (body([5, 6], "307000000000"), 602000),
        ]
    )
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    monkeypatch.setattr("selective_options.trade_backfill.asyncio.sleep", AsyncMock())
    output = tmp_path / "range"
    report = await backfill_trades(config(), database, output, "BTC/USD", 6)
    assert report["rows"] == 3 and report["pages"] == 3 and report["status"] == "RANGE_VERIFIED"
    assert report["stream_repaired"] is report["bars_rebuilt"] is report["trading_enabled"] is False
    assert [call.args[2] for call in fetch.await_args_list] == [
        "302000000000",
        "304000000000",
        "305000000000",
    ]
    store = await DuckStore.open(output / "events.duckdb")
    assert not (await store.snapshot("BTC", 600999, "public_reference")).rows()
    rows = (await store.snapshot("BTC", 601000, "public_reference")).rows()
    assert len(rows) == 1 and rows[0].payload()["trade_id"] == 4
    assert await store.pending_delivery_count() == 3
    await store.close()
    with duckdb.connect(str(output / "events.duckdb"), read_only=True) as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 3
        assert (
            db.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(output / "events.parquet")]
            ).fetchone()[0]
            == 3
        )
    assert (output / "events.parquet.manifest.json").exists()


@pytest.mark.parametrize(
    "pages,error",
    [
        ([(body([4, 5]), 600000)], "ANCHOR_MISSING"),
        ([(body([3, 5]), 600000)], "SEQUENCE_GAP"),
        ([(body([3], "302000000000"), 600000)], "CURSOR_STALLED"),
        ([(body([]), 600000)], "CURSOR_STALLED"),
        ([(body([3, 4]), 499999)], "OBSERVATION_REGRESSION"),
        ([(body([3], "310000000000"), 600000), (body([3], "311000000000"), 600000)], "PAGE_LIMIT"),
    ],
)
async def test_incomplete_range_leaves_diagnostics_without_database(
    tmp_path, monkeypatch, pages, error
):
    database = tmp_path / "prior.duckdb"
    await seed(database)
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", AsyncMock(side_effect=pages))
    monkeypatch.setattr("selective_options.trade_backfill.asyncio.sleep", AsyncMock())
    output = tmp_path / "failed"
    with pytest.raises(IntegrityError, match=error):
        await backfill_trades(config(kraken_backfill_max_pages=2), database, output, "BTC/USD", 5)
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "FAILED" and error in report["failure_reason"]
    assert not (output / "events.duckdb").exists()


@pytest.mark.parametrize(
    "mode,error",
    [("anchor", "ANCHOR_CONFLICT"), ("duplicate", "CONFLICTING_ID"), ("future", "CLOCK_SKEW")],
)
async def test_conflicting_values_and_future_source_time(tmp_path, monkeypatch, mode, error):
    raw = json.loads(body([3, 4]))
    rows = raw["result"]["BTC/USD"]
    if mode == "anchor":
        rows[0][0] = "999"
    elif mode == "future":
        rows[1][2] = 999999
    else:
        rows.insert(1, rows[0].copy())
        rows[1][0] = "999"
    monkeypatch.setattr(
        "selective_options.trade_backfill.fetch_page",
        AsyncMock(return_value=(json.dumps(raw).encode(), 600000)),
    )
    with pytest.raises(IntegrityError, match=error):
        await collect_range(config(kraken_backfill_page_size=3), anchor(), 4, tmp_path)


async def test_deadline_failure_is_recorded(tmp_path, monkeypatch):
    database = tmp_path / "prior.duckdb"
    await seed(database)

    async def timeout(*args):
        raise TimeoutError("external detail")

    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", timeout)
    output = tmp_path / "failed"
    with pytest.raises(TimeoutError):
        await backfill_trades(config(), database, output, "BTC/USD", 4)
    report = json.loads((output / "report.json").read_text())
    assert (
        report["failure_type"] == "TimeoutError"
        and "external detail" not in report["failure_reason"]
    )


async def test_budgets_symbol_and_overwrite_refused_before_fetch(tmp_path, monkeypatch):
    database = tmp_path / "prior.duckdb"
    await seed(database)
    fetch = AsyncMock()
    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", fetch)
    for target in (3, 2, 10000):
        with pytest.raises(ValueError):
            await backfill_trades(config(), database, tmp_path / "new", "BTC/USD", target)
    with pytest.raises(IntegrityError, match="NO_SYMBOL"):
        await backfill_trades(config(), database, tmp_path / "new", "ETH/USD", 4)
    with pytest.raises(FileExistsError):
        await backfill_trades(config(), database, tmp_path, "BTC/USD", 4)
    fetch.assert_not_awaited()


@pytest.mark.parametrize(
    "status,size,error",
    [(429, 1, "HTTP_FAILURE"), (200, MAX_BYTES + 1, "TOO_LARGE"), (200, 5, None)],
)
async def test_http_limits_and_fixed_public_endpoint(status, size, error):
    class Response:
        def __init__(self):
            self.status = status
            self.content = self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def iter_chunked(self, size_arg):
            yield b"x" * size

    class Session:
        def get(self, url, **kwargs):
            assert url == "https://api.kraken.com/0/public/Trades"
            assert kwargs["allow_redirects"] is False and kwargs["params"]["assetVersion"] == "1"
            return Response()

    if error:
        with pytest.raises(IntegrityError, match=error):
            await fetch_page(Session(), "BTC/USD", "1", 2)
    else:
        raw, observed = await fetch_page(Session(), "BTC/USD", "1", 2)
        assert raw == b"xxxxx" and observed > 0


def test_backfill_cli_forwards_explicit_bound(monkeypatch, tmp_path):
    from selective_options.cli import main

    fn = AsyncMock(return_value={"status": "RANGE_VERIFIED"})
    monkeypatch.setattr("selective_options.cli.backfill_trades", fn)
    monkeypatch.setattr(
        "sys.argv",
        [
            "selective-options",
            "backfill-trades",
            "--database",
            str(tmp_path / "old"),
            "--output",
            str(tmp_path / "new"),
            "--symbol",
            "BTC/USD",
            "--through-trade-id",
            "120",
        ],
    )
    assert main() == 0
    assert fn.await_args.args[1:] == (tmp_path / "old", tmp_path / "new", "BTC/USD", 120)


async def test_total_deadline_cancels_blocked_fetch(tmp_path, monkeypatch):
    cancelled = asyncio.Event()

    async def blocked(*args):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr("selective_options.trade_backfill.fetch_page", blocked)
    with pytest.raises(TimeoutError):
        await collect_range(config(kraken_backfill_timeout_s=1), anchor(), 4, tmp_path)
    assert cancelled.is_set()


def test_numeric_overflow_is_not_persisted():
    from selective_options.trade_backfill import RestTrade

    row = RestTrade(price=Decimal("1e1000"), qty=1, timestamp=304, side="buy", trade_id=4)
    with pytest.raises(IntegrityError, match="NUMERIC_OVERFLOW"):
        row.event(anchor(), 600000)
