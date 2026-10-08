"""Async DuckDB access, append-only deduplication, and immutable Parquet exports."""

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import ParamSpec, Self, TypeVar, cast

import duckdb

from .errors import IntegrityError
from .eth_gas import PUBLIC_PROVIDERS, Provider
from .eth_gas_quality import eth_gas_quality as assess_eth_gas
from .events import Environment, MarketEvent, PointInTimeSnapshot
from .news import NewsSource
from .news_quality import policy_news_quality as assess_policy_news
from .option_coverage import coverage_at
from .solana_quality import network_quality

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
 event_id VARCHAR PRIMARY KEY, source VARCHAR NOT NULL, environment VARCHAR NOT NULL,
 asset VARCHAR NOT NULL, symbol VARCHAR NOT NULL, kind VARCHAR NOT NULL,
 event_time_ms BIGINT NOT NULL, observed_at_ms BIGINT NOT NULL,
 timestamp_kind VARCHAR NOT NULL, schema_version INTEGER NOT NULL, payload_json VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS pit_lookup ON events(asset, environment, event_time_ms, observed_at_ms);
CREATE TABLE IF NOT EXISTS delivery_outbox (
 event_id VARCHAR PRIMARY KEY, delivered BOOLEAN NOT NULL DEFAULT FALSE
);
INSERT INTO delivery_outbox SELECT event_id, FALSE FROM events ON CONFLICT DO NOTHING;
"""

P = ParamSpec("P")
T = TypeVar("T")


async def complete_thread(function: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Drain the worker before propagating cancellation, including repeated cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError as exc:
            cancelled = cancelled or exc
            if task.done():
                # A worker can itself raise CancelledError; do not spin on a done task.
                try:
                    result = task.result()
                except BaseException as failure:
                    raise cancelled from failure
                break
        except BaseException as failure:
            if cancelled is not None:
                raise cancelled from failure
            raise
    if cancelled is not None:
        raise cancelled
    return result


async def acquire_thread_resource(
    function: Callable[P, T], release: Callable[[T], object], *args: P.args, **kwargs: P.kwargs
) -> T:
    """Close a successfully acquired resource if cancellation prevents its handoff."""
    acquired: list[T] = []

    def acquire() -> T:
        result = function(*args, **kwargs)
        acquired.append(result)
        return result

    try:
        return await complete_thread(acquire)
    except BaseException:
        if acquired:
            await complete_thread(release, acquired[0])
        raise


class DuckStore:
    def __init__(self, connection: duckdb.DuckDBPyConnection, max_skew_ms: int) -> None:
        self._connection = connection
        self._lock = asyncio.Lock()
        self._max_skew_ms = max_skew_ms
        self._closed = False

    @classmethod
    async def open(cls, path: Path, max_skew_ms: int = 5000) -> Self:
        connection = await acquire_thread_resource(
            cls._open_connection, lambda value: value.close(), path
        )
        return cls(connection, max_skew_ms)

    @staticmethod
    def _open_connection(path: Path) -> duckdb.DuckDBPyConnection:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(path))
        try:
            connection.execute(SCHEMA)
            return connection
        except BaseException:
            connection.close()
            raise

    async def append(self, events: list[MarketEvent], *, exact: bool = False) -> None:
        if not events:
            return
        for event in events:
            if event.event_time_ms > event.observed_at_ms + self._max_skew_ms:
                raise IntegrityError("CLOCK_SKEW: event is too far ahead of observation time")
            event.payload()  # Validate persisted payload even for externally constructed events.
        rows = [tuple(e.model_dump().values()) for e in events]
        async with self._lock:
            await complete_thread(self._insert, rows, exact)

    def _insert(self, rows: list[tuple[object, ...]], exact: bool = False) -> None:
        self._connection.execute("BEGIN TRANSACTION")
        try:
            if exact:
                expected: dict[str, tuple[object, ...]] = {}
                for row in rows:
                    key = str(row[0])
                    if key in expected and expected[key] != row:
                        raise IntegrityError("EXACT_EVENT_CONFLICT")
                    expected[key] = row
                existing = self._connection.execute(
                    "SELECT e.* FROM events e SEMI JOIN json_each(?) ids ON e.event_id=(ids.value ->> '$')",
                    [json.dumps(list(expected))],
                ).fetchall()
                if any(expected[str(row[0])] != tuple(row) for row in existing):
                    raise IntegrityError("EXACT_EVENT_CONFLICT")
            # One vectorized insert avoids a Python/SQL call per event.
            columns = ", ".join(f"(value ->> {index})" for index in range(11))
            self._connection.execute(
                f"INSERT INTO events SELECT {columns} FROM json_each(?) ON CONFLICT DO NOTHING",
                [json.dumps(rows, allow_nan=False)],
            )
            self._connection.execute(
                "INSERT INTO delivery_outbox SELECT (value ->> 0), FALSE FROM json_each(?) ON CONFLICT DO NOTHING",
                [json.dumps(rows, allow_nan=False)],
            )
            self._connection.execute("COMMIT")
        except Exception:
            self._connection.execute("ROLLBACK")
            raise

    async def snapshot(
        self, asset: str, as_of_ms: int, environment: Environment = "testnet"
    ) -> PointInTimeSnapshot:
        async with self._lock:
            values = await complete_thread(
                self._select,
                "SELECT * FROM events WHERE asset=? AND environment=? AND event_time_ms<=? AND observed_at_ms<=? ORDER BY event_time_ms, observed_at_ms, event_id",
                [asset, environment, as_of_ms, as_of_ms],
            )
        events = tuple(MarketEvent.model_validate(row) for row in values)
        return PointInTimeSnapshot(as_of_ms, asset, environment, events)

    def _select(self, sql: str, params: list[object]) -> list[dict[str, object]]:
        cursor = self._connection.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    async def option_coverage(
        self,
        asset: str,
        as_of_ms: int,
        max_leg_age_ms: int = 300000,
        max_universe_age_ms: int = 3600000,
        max_rows: int = 100000,
    ) -> dict[str, object]:
        """Bound Python reads to controls, bound manifest metadata and latest tied legs.

        The full append-only history is retained. SQL may scan that history; this is
        not a database size, execution-time or engine memory bound.
        """
        coverage_at([], asset, as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows)
        async with self._lock:
            return await complete_thread(
                self._option_coverage,
                asset,
                as_of_ms,
                max_leg_age_ms,
                max_universe_age_ms,
                max_rows,
            )

    def _option_coverage(
        self,
        asset: str,
        as_of_ms: int,
        max_leg_age_ms: int,
        max_universe_age_ms: int,
        max_rows: int,
    ) -> dict[str, object]:
        # Audit every eligible scoped control, including corrupt status roots, before
        # choosing any metadata generation. Unknown valid statuses are irrelevant to
        # coverage_at and need not be decoded. Control history has the shared budget;
        # SQL scans and engine memory are not bounded by this application row limit.
        base = self._select(
            """SELECT * FROM events
            WHERE source='deribit' AND environment='testnet'
            AND event_time_ms<=? AND observed_at_ms<=?
            AND asset='SYSTEM' AND kind='status' AND (
                    NOT coalesce(TRY(json_type(payload_json))='OBJECT', FALSE)
                    OR TRY(json_extract_string(payload_json, '$.reason')) IN
                        ('STREAM_RESYNC','STREAM_SUBSCRIBED','STREAM_DISCONNECTED','STREAM_REFRESH_DUE')
                    OR (TRY(json_extract_string(payload_json, '$.reason'))='UNIVERSE_DISCOVERED'
                        AND TRY(json_extract_string(payload_json, '$.asset'))=?))
            ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?""",
            [as_of_ms, as_of_ms, asset, max_rows + 1],
        )
        events = [MarketEvent.model_validate(row) for row in base]
        initial = coverage_at(
            events, asset, as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows
        )
        # With no metadata supplied, this precise canonical failure means control,
        # session, acknowledged manifest scope/counts and age checks have passed.
        # Any other failure must block without reading metadata or quote history.
        if initial["blocker"] != "OPTION_METADATA_MISSING_OR_CONFLICTING":
            return initial
        by_id = {row.event_id: row for row in events}
        ack = by_id[cast(str, initial["stream_control_event_id"])].payload()
        # The canonical context check above established exactly one scoped manifest;
        # do not rank or arbitrarily choose among ambiguous acknowledgements/controls.
        manifest = next(
            row.payload()
            for row in events
            if row.event_id in cast(list[str], ack["universe_event_ids"])
            and row.payload().get("reason") == "UNIVERSE_DISCOVERED"
        )
        metadata = self._select(
            """SELECT * FROM events WHERE source='deribit' AND environment='testnet'
            AND asset=? AND kind='instrument' AND event_id IN (SELECT value ->> '$' FROM json_each(?))
            AND event_time_ms<=? AND observed_at_ms<=?
            ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?""",
            [
                asset,
                json.dumps(manifest["metadata_event_ids"]),
                as_of_ms,
                as_of_ms,
                max_rows - len(events) + 1,
            ],
        )
        events.extend(MarketEvent.model_validate(row) for row in metadata)
        initial = coverage_at(
            events, asset, as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows
        )
        if initial["state"] == "BLOCKED":
            return initial
        # Exact metadata identities and all native lifecycle/scope checks have now
        # passed. Earlier generations remain stored but cannot satisfy this manifest.
        start = next(
            row.observed_at_ms
            for row in events
            if row.kind == "status"
            and row.payload().get("reason") == "STREAM_RESYNC"
            and type(row.payload().get("connection")) is int
            and row.payload().get("connection") == ack["connection"]
            and row.observed_at_ms
            <= by_id[cast(str, initial["stream_control_event_id"])].observed_at_ms
        )
        quotes = self._select(
            """SELECT * FROM events WHERE source='deribit' AND environment='testnet'
            AND asset=? AND kind IN ('ticker','book')
            AND symbol IN (SELECT value ->> '$' FROM json_each(?))
            AND event_time_ms<=? AND observed_at_ms BETWEEN ? AND ?
            QUALIFY dense_rank() OVER (
                PARTITION BY symbol,kind ORDER BY event_time_ms DESC,observed_at_ms DESC)=1
            ORDER BY symbol,kind,event_id LIMIT ?""",
            [
                asset,
                json.dumps(manifest["selected_instruments"]),
                as_of_ms,
                start,
                as_of_ms,
                max_rows - len(events) + 1,
            ],
        )
        events.extend(MarketEvent.model_validate(row) for row in quotes)
        return coverage_at(events, asset, as_of_ms, max_leg_age_ms, max_universe_age_ms, max_rows)

    async def counts(self) -> list[dict[str, object]]:
        async with self._lock:
            return await complete_thread(
                self._select,
                "SELECT source, environment, asset, kind, COUNT(*) AS count FROM events GROUP BY ALL ORDER BY ALL",
                [],
            )

    async def policy_news_quality(
        self,
        source: NewsSource,
        as_of_ms: int,
        max_poll_age_ms: int = 600_000,
        max_publication_age_ms: int = 2_592_000_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Assess all eligible scoped RSS evidence with a bounded Python fetch.

        No history is deleted or pruned into a pass. Database scans/time/memory
        remain unbounded; overflow is passed to the canonical quality validator.
        """
        assess_policy_news(
            [],
            source=source,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_publication_age_ms=max_publication_age_ms,
            max_rows=max_rows,
        )
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT * FROM events WHERE source=? AND environment='public_reference' "
                "AND asset='GLOBAL' AND symbol='POLICY_NEWS' AND kind IN ('news','feed_health') "
                "AND event_time_ms<=? AND observed_at_ms<=? "
                "ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?",
                [source, as_of_ms, as_of_ms, max_rows + 1],
            )
        return assess_policy_news(
            (MarketEvent.model_validate(row) for row in rows),
            source=source,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_publication_age_ms=max_publication_age_ms,
            max_rows=max_rows,
        )

    async def eth_gas_quality(
        self,
        provider: Provider,
        as_of_ms: int,
        max_poll_age_ms: int,
        max_block_age_ms: int,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Bound Python fetches while auditing all eligible provider-scoped gas evidence.

        Both clocks constrain rows before LIMIT. History is retained; SQL scans,
        execution time and engine memory are not bounded by the application limit.
        """
        assess_eth_gas(
            [],
            provider=provider,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_block_age_ms=max_block_age_ms,
            max_rows=max_rows,
        )
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT * FROM events WHERE source=? AND environment='public_reference' "
                "AND asset='ETH' AND symbol='ETH_MAINNET_GAS' AND kind IN ('on_chain','feed_health') "
                "AND event_time_ms<=? AND observed_at_ms<=? "
                "ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?",
                [PUBLIC_PROVIDERS[provider][1], as_of_ms, as_of_ms, max_rows + 1],
            )
        return assess_eth_gas(
            (MarketEvent.model_validate(row) for row in rows),
            provider=provider,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_block_age_ms=max_block_age_ms,
            max_rows=max_rows,
        )

    async def _scoped_quality_rows(
        self,
        source: str,
        asset: str,
        symbols: tuple[str, ...],
        kinds: tuple[str, ...],
        as_of_ms: int,
        max_rows: int,
    ) -> tuple[MarketEvent, ...]:
        """Fetch one overflow sentinel; caller must preflight its canonical policy."""
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT * FROM events WHERE source=? AND environment='public_reference' "
                "AND asset=? AND symbol IN (SELECT value ->> '$' FROM json_each(?)) "
                "AND kind IN (SELECT value ->> '$' FROM json_each(?)) "
                "AND event_time_ms<=? AND observed_at_ms<=? "
                "ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?",
                [
                    source,
                    asset,
                    json.dumps(symbols),
                    json.dumps(kinds),
                    as_of_ms,
                    as_of_ms,
                    max_rows + 1,
                ],
            )
        return tuple(MarketEvent.model_validate(row) for row in rows)

    async def coinmetrics_quality(
        self,
        asset: str,
        as_of_ms: int,
        max_poll_age_ms: int = 7_200_000,
        max_data_age_ms: int = 259_200_000,
        max_rows: int = 10000,
    ) -> dict[str, object]:
        """Exact asset scope and both clocks before one overflow sentinel; no truncation."""
        from .coinmetrics import SOURCE, SYMBOL
        from .coinmetrics_quality import coinmetrics_quality

        coinmetrics_quality(
            [],
            asset=asset,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_data_age_ms=max_data_age_ms,
            max_rows=max_rows,
        )
        rows = await self._scoped_quality_rows(
            SOURCE, asset, (SYMBOL,), ("on_chain", "feed_health"), as_of_ms, max_rows
        )
        return coinmetrics_quality(
            rows,
            asset=asset,
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_data_age_ms=max_data_age_ms,
            max_rows=max_rows,
        )

    async def crypto_news_quality(
        self,
        as_of_ms: int,
        max_poll_age_ms: int = 600_000,
        max_publication_age_ms: int = 2_592_000_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Bound exact metadata/health materialization while retaining all history."""
        from .crypto_news import crypto_news_quality

        policy = dict(
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_publication_age_ms=max_publication_age_ms,
            max_rows=max_rows,
        )
        crypto_news_quality([], **policy)
        rows = await self._scoped_quality_rows(
            "coindesk_rss",
            "GLOBAL",
            ("CRYPTO_NEWS",),
            ("news", "feed_health"),
            as_of_ms,
            max_rows,
        )
        return crypto_news_quality(rows, **policy)

    async def stablecoin_supply_quality(
        self,
        as_of_ms: int,
        max_poll_age_ms: int = 1_200_000,
        max_block_age_ms: int = 1_800_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Assess retained paired supply revisions; no engine/time/memory bound."""
        from .stablecoin_supply import stablecoin_supply_quality

        policy = dict(
            as_of_ms=as_of_ms,
            max_poll_age_ms=max_poll_age_ms,
            max_block_age_ms=max_block_age_ms,
            max_rows=max_rows,
        )
        stablecoin_supply_quality([], **policy)
        rows = await self._scoped_quality_rows(
            "ethereum_drpc",
            "GLOBAL",
            ("ETH_MAINNET_STABLECOIN_SUPPLY",),
            ("on_chain", "feed_health"),
            as_of_ms,
            max_rows,
        )
        return stablecoin_supply_quality(rows, **policy)

    async def tips_yield_quality(
        self,
        as_of_ms: int,
        max_poll_age_ms: int = 7_200_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Assess all eligible full-window real-yield revisions with bounded fetches."""
        from .tips_yield import tips_yield_quality

        policy = dict(as_of_ms=as_of_ms, max_poll_age_ms=max_poll_age_ms, max_rows=max_rows)
        tips_yield_quality([], **policy)
        rows = await self._scoped_quality_rows(
            "fred_tips",
            "GLOBAL",
            ("DFII10",),
            ("index", "feed_health"),
            as_of_ms,
            max_rows,
        )
        return tips_yield_quality(rows, **policy)

    async def liquidations_quality(
        self,
        asset: str,
        as_of_ms: int,
        max_health_age_ms: int = 300_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Assess a lossy snapshot stream; no completeness or zero-event inference."""
        from .liquidations import liquidations_quality

        liquidations_quality(
            [],
            asset=asset,
            as_of_ms=as_of_ms,
            max_health_age_ms=max_health_age_ms,
            max_rows=max_rows,
        )
        rows = await self._scoped_quality_rows(
            "binance_liquidations",
            asset,
            (asset + "USDT",),
            ("liquidation", "feed_health"),
            as_of_ms,
            max_rows,
        )
        return liquidations_quality(
            rows,
            asset=asset,
            as_of_ms=as_of_ms,
            max_health_age_ms=max_health_age_ms,
            max_rows=max_rows,
        )

    async def solana_network_quality(
        self,
        as_of_ms: int,
        max_receipt_age_ms: int = 600_000,
        max_sample_age_ms: int = 780_000,
        max_rows: int = 10_000,
    ) -> dict[str, object]:
        """Keep the exact SOL metric pairs and both clocks before bounded fetching."""
        network_quality([], as_of_ms, max_receipt_age_ms, max_sample_age_ms, max_rows)
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT * FROM events WHERE source='solana_public' "
                "AND environment='public_reference' AND asset='SOL' "
                "AND ((kind='on_chain' AND symbol IN ('SOL_PERFORMANCE','SOL_VOTE_STAKE')) "
                "OR (kind='feed_health' AND symbol='SOL_NETWORK')) "
                "AND event_time_ms<=? AND observed_at_ms<=? "
                "ORDER BY event_time_ms,observed_at_ms,event_id LIMIT ?",
                [as_of_ms, as_of_ms, max_rows + 1],
            )
        return network_quality(
            (MarketEvent.model_validate(row) for row in rows),
            as_of_ms,
            max_receipt_age_ms,
            max_sample_age_ms,
            max_rows,
        )

    async def read_events(self, event_ids: list[str]) -> tuple[MarketEvent, ...]:
        """Read bounded committed evidence through the writer's serialized connection."""
        if len(event_ids) > 20001:
            raise ValueError("event lookup exceeds recovery evidence budget")
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT e.* FROM events e SEMI JOIN json_each(?) ids ON e.event_id=(ids.value ->> '$')",
                [json.dumps(event_ids)],
            )
        return tuple(MarketEvent.model_validate(row) for row in rows)

    async def pending_delivery(self, limit: int = 500) -> list[MarketEvent]:
        if limit < 1:
            raise ValueError("delivery batch limit must be positive")
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT e.* FROM events e JOIN delivery_outbox o USING(event_id) WHERE NOT o.delivered ORDER BY e.observed_at_ms, e.event_id LIMIT ?",
                [limit],
            )
        return [MarketEvent.model_validate(row) for row in rows]

    async def acknowledge_delivery(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        async with self._lock:
            await complete_thread(
                self._connection.execute,
                "UPDATE delivery_outbox SET delivered=TRUE WHERE event_id IN (SELECT unnest(?))",
                [event_ids],
            )

    async def pending_delivery_count(self) -> int:
        async with self._lock:
            rows = await complete_thread(
                self._select,
                "SELECT COUNT(*) AS count FROM delivery_outbox WHERE NOT delivered",
                [],
            )
        return cast(int, rows[0]["count"])

    async def export_parquet(self, path: Path) -> int:
        if await asyncio.to_thread(path.exists):
            raise FileExistsError("refusing to overwrite an immutable Parquet export")
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        async with self._lock:
            return await complete_thread(self._export, path)

    def _export(self, path: Path) -> int:
        row = self._connection.execute("SELECT COUNT(*) FROM events").fetchone()
        assert row is not None
        count = cast(int, row[0])
        # Bind the filename, never interpolate an operator-controlled path into SQL.
        self._connection.execute(
            "COPY (SELECT * FROM events ORDER BY event_time_ms, observed_at_ms, event_id) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(path)],
        )
        return count

    async def close(self) -> None:
        async with self._lock:
            if not self._closed:
                await complete_thread(self._connection.close)
                self._closed = True
