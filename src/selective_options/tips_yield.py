"""Official FRED DFII10 daily real-yield CSV snapshots, available at receipt only."""

import asyncio
import csv
import hashlib
import io
import re
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from urllib.parse import urlencode

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from .collectors import Sink, now_ms
from .config import AppConfig
from .errors import IntegrityError
from .events import MarketEvent, make_event

SOURCE = "fred_tips"
SYMBOL = "DFII10"
CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
MAX_BYTES = 65_536
MAX_DAYS = 366
REFERENCES = (
    "https://fred.stlouisfed.org/series/DFII10",
    "https://fredhelp.stlouisfed.org/fred/data/downloading/using-the-download-data-link/",
    "https://www.federalreserve.gov/releases/h15/",
)
DECIMAL_TEXT = re.compile(r"-?[0-9]+(?:\.[0-9]+)?\Z")
POLL_SCOPE = "DFII10 CSV snapshot; sink accepted, no atomic commit claim"


class NativeRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    observation_date: StrictStr = Field(min_length=10, max_length=10)
    yield_percent_text: StrictStr = Field(max_length=32)
    missing: Literal["dot", "empty", "not_missing"]


class SnapshotPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    series: Literal["DFII10"]
    units: Literal["percent"]
    frequency: Literal["daily"]
    seasonal_adjustment: Literal["not seasonally adjusted"]
    window_start_date: StrictStr
    window_end_date: StrictStr
    request_url: StrictStr
    raw_csv_hex: StrictStr = Field(max_length=MAX_BYTES * 2)
    raw_csv_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    rows: tuple[NativeRow, ...] = Field(max_length=MAX_DAYS)
    date_basis: Literal["logical observation date; publication instant and timezone unavailable"]
    publication_time_available: Literal[False]
    historical_vintage_available: Literal[False]
    coverage: Literal["returned bounded CSV window; no complete or continuous history claim"]


class PollHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    ok: StrictBool
    snapshot_event_ids: tuple[StrictStr, ...] = Field(max_length=1)
    response_received_at_ms: StrictInt | None
    http_status: StrictInt | None
    error_type: StrictStr | None = Field(max_length=256)
    scope: Literal["DFII10 CSV snapshot; sink accepted, no atomic commit claim"]


def _date(text: str) -> date:
    result = date.fromisoformat(text)
    if result.isoformat() != text:
        raise IntegrityError("TIPS_DATE_FORMAT")
    return result


def request_url(start_date: date, end_date: date) -> str:
    if (
        type(start_date) is not date
        or type(end_date) is not date
        or not 1 <= (end_date - start_date).days + 1 <= MAX_DAYS
    ):
        raise ValueError("ordered inclusive daily window of 1..366 dates required")
    return (
        CSV_URL
        + "?"
        + urlencode({"id": SYMBOL, "cosd": start_date.isoformat(), "coed": end_date.isoformat()})
    )


def tips_yield_snapshot(
    raw_csv: bytes, start_date: date, end_date: date, received_ms: int
) -> MarketEvent:
    url = request_url(start_date, end_date)
    if type(received_ms) is not int or received_ms <= 0:
        raise ValueError("positive integer receipt required")
    if end_date > datetime.fromtimestamp(received_ms / 1000, UTC).date():
        raise IntegrityError("TIPS_FUTURE_WINDOW")
    if type(raw_csv) is not bytes or len(raw_csv) > MAX_BYTES:
        raise IntegrityError("TIPS_CSV_BYTE_BUDGET")
    reader = csv.reader(io.StringIO(raw_csv.decode("utf-8-sig"), newline=""), strict=True)
    if next(reader, None) != ["observation_date", SYMBOL]:
        raise IntegrityError("TIPS_CSV_HEADER")
    rows: list[NativeRow] = []
    previous: date | None = None
    for fields in reader:
        if len(rows) >= MAX_DAYS or len(fields) != 2:
            raise IntegrityError("TIPS_CSV_ROW_BUDGET_OR_SHAPE")
        dated, value = fields
        logical_date = _date(dated)
        if not start_date <= logical_date <= end_date or (
            previous is not None and logical_date <= previous
        ):
            raise IntegrityError("TIPS_CSV_DATE_WINDOW_ORDER_OR_DUPLICATE")
        if value not in (".", "") and (
            len(value) > 32
            or DECIMAL_TEXT.fullmatch(value) is None
            or not Decimal(value).is_finite()
        ):
            raise IntegrityError("TIPS_CSV_NATIVE_PERCENT")
        rows.append(
            NativeRow(
                observation_date=dated,
                yield_percent_text=value,
                missing="dot" if value == "." else "empty" if value == "" else "not_missing",
            )
        )
        previous = logical_date
    body = SnapshotPayload(
        series=SYMBOL,
        units="percent",
        frequency="daily",
        seasonal_adjustment="not seasonally adjusted",
        window_start_date=start_date.isoformat(),
        window_end_date=end_date.isoformat(),
        request_url=url,
        raw_csv_hex=raw_csv.hex(),
        raw_csv_sha256=hashlib.sha256(raw_csv).hexdigest(),
        rows=tuple(rows),
        date_basis="logical observation date; publication instant and timezone unavailable",
        publication_time_available=False,
        historical_vintage_available=False,
        coverage="returned bounded CSV window; no complete or continuous history claim",
    )
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="index",
        timestamp_kind="receipt",
        event_time_ms=received_ms,
        observed_at_ms=received_ms,
        payload=body.model_dump(mode="json"),
    )


def tips_yield_health(
    received_ms: int,
    *,
    snapshot_event_id: str | None = None,
    response_received_at_ms: int | None = None,
    http_status: int | None = None,
    error_type: str | None = None,
) -> MarketEvent:
    body = PollHealth(
        ok=error_type is None,
        snapshot_event_ids=(snapshot_event_id,) if snapshot_event_id is not None else (),
        response_received_at_ms=response_received_at_ms,
        http_status=http_status,
        error_type=error_type,
        scope=POLL_SCOPE,
    )
    if (
        type(received_ms) is not int
        or received_ms <= 0
        or (response_received_at_ms is not None and not 0 < response_received_at_ms <= received_ms)
        or (http_status is not None and not 100 <= http_status <= 599)
        or (
            body.ok
            and (snapshot_event_id is None or response_received_at_ms is None or http_status != 200)
        )
        or (not body.ok and (snapshot_event_id is not None or not error_type))
        or (
            snapshot_event_id is not None
            and re.fullmatch(r"[0-9a-f]{64}", snapshot_event_id) is None
        )
        or ((http_status is None) != (response_received_at_ms is None))
    ):
        raise IntegrityError("TIPS_HEALTH_FIELDS_OR_CLOCK")
    return make_event(
        source=SOURCE,
        environment="public_reference",
        asset="GLOBAL",
        symbol=SYMBOL,
        kind="feed_health",
        timestamp_kind="receipt",
        event_time_ms=received_ms,
        observed_at_ms=received_ms,
        payload=body.model_dump(mode="json"),
    )


class TipsYieldCollector:
    def __init__(self, config: AppConfig, sink: Sink) -> None:
        self.config, self.sink = config, sink
        self.last_response_received_at_ms: int | None = None
        self.last_http_status: int | None = None

    async def response_received(
        self, body: bytes, status: int, received: int, start_date: date, end_date: date
    ) -> None:
        """Optional evidence hook, called with exact decoded HTTP entity bytes."""

    async def request(
        self, session: aiohttp.ClientSession, start_date: date, end_date: date
    ) -> tuple[bytes, int]:
        timeout = aiohttp.ClientTimeout(total=min(self.config.venue.request_timeout_s, 30.0))
        async with asyncio.timeout(timeout.total):
            async with session.get(
                request_url(start_date, end_date), allow_redirects=False, timeout=timeout
            ) as response:
                body = bytearray()
                async for chunk in response.content.iter_chunked(8192):
                    body.extend(chunk)
                    if len(body) > MAX_BYTES:
                        raise IntegrityError("TIPS_CSV_BYTE_BUDGET")
                received = now_ms()
                self.last_response_received_at_ms, self.last_http_status = received, response.status
                await self.response_received(
                    bytes(body), response.status, received, start_date, end_date
                )
                if response.status != 200:
                    raise IntegrityError("TIPS_HTTP_STATUS")
        return bytes(body), received

    async def poll(
        self,
        session: aiohttp.ClientSession,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> None:
        self.last_response_received_at_ms = self.last_http_status = None
        if (start_date is None) != (end_date is None):
            raise ValueError("both explicit window dates required")
        requested_date = datetime.fromtimestamp(now_ms() / 1000, UTC).date()
        if start_date is None or end_date is None:
            end_date = requested_date
            start_date = end_date - timedelta(
                days=self.config.ingestion.tips_yield_history_days - 1
            )
        request_url(start_date, end_date)
        if end_date > requested_date:
            raise ValueError("future request windows refused before HTTP")
        try:
            body, received = await self.request(session, start_date, end_date)
            snapshot = tips_yield_snapshot(body, start_date, end_date, received)
        except (aiohttp.ClientError, TimeoutError, IntegrityError, ValueError, csv.Error) as exc:
            await self.sink(
                tips_yield_health(
                    now_ms(),
                    response_received_at_ms=self.last_response_received_at_ms,
                    http_status=self.last_http_status,
                    error_type=type(exc).__name__,
                )
            )
            return
        await self.sink(snapshot)
        await self.sink(
            tips_yield_health(
                now_ms(),
                snapshot_event_id=snapshot.event_id,
                response_received_at_ms=received,
                http_status=200,
            )
        )

    async def run(self) -> None:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=min(self.config.venue.request_timeout_s, 30.0)),
            headers={"User-Agent": "SelectiveOptionsResearch/0.3 (public DFII10 CSV)"},
        ) as session:
            while True:
                await self.poll(session)
                await asyncio.sleep(self.config.ingestion.tips_yield_poll_interval_s)


def tips_yield_quality(
    events: Iterable[MarketEvent],
    *,
    as_of_ms: int,
    max_poll_age_ms: int = 7_200_000,
    max_rows: int = 10_000,
) -> dict[str, object]:
    """Latest bound receipt poll; daily value age is descriptive, never publication age."""
    if (
        any(type(value) is not int or value <= 0 for value in (as_of_ms, max_poll_age_ms, max_rows))
        or max_rows > 100_000
    ):
        raise ValueError("positive clocks/ages and bounded row policy required")
    result: dict[str, object] = {
        "source": SOURCE,
        "asset": "GLOBAL",
        "symbol": SYMBOL,
        "as_of_ms": as_of_ms,
        "state": "BLOCKED",
        "blocker": None,
        "health_event_id": None,
        "snapshot_event_id": None,
        "health_received_at_ms": None,
        "response_received_at_ms": None,
        "poll_ok": None,
        "error_type": None,
        "http_status": None,
        "max_poll_age_ms": max_poll_age_ms,
        "returned_rows": None,
        "missing_dot_rows": None,
        "missing_empty_rows": None,
        "latest_returned_date": None,
        "latest_value_date": None,
        "latest_value_percent_text": None,
        "latest_value_date_age_days": None,
        "dated_value_state": "UNVERIFIED",
        "dated_age_basis": "calendar date difference against UTC evaluation date; not publication latency",
        "scope": "One bounded official FRED DFII10 daily real-yield CSV snapshot; native percent units. Daily dates are logical observations, not publication instants. Sink acceptance is not atomic commit proof.",
        "publication_time_available": False,
        "historical_vintage_available": False,
        "continuous_history_verified": False,
        "complete_feed_available": False,
        "trading_approval": False,
    }
    try:
        eligible: dict[str, MarketEvent] = {}
        count = 0
        for row in events:
            if (
                (row.source, row.environment, row.asset, row.symbol)
                != (SOURCE, "public_reference", "GLOBAL", SYMBOL)
                or row.kind not in ("index", "feed_health")
                or max(row.event_time_ms, row.observed_at_ms) > as_of_ms
            ):
                continue
            count += 1
            if count > max_rows:
                raise IntegrityError("TIPS_QUALITY_OVER_BUDGET")
            if row.event_id in eligible and eligible[row.event_id] != row:
                raise IntegrityError("TIPS_EVENT_ID_CONFLICT")
            eligible[row.event_id] = row
        snapshots: dict[str, tuple[MarketEvent, SnapshotPayload]] = {}
        health: list[tuple[MarketEvent, PollHealth]] = []
        for row in eligible.values():
            if row.kind == "index":
                payload = SnapshotPayload.model_validate_json(row.payload_json)
                canonical = tips_yield_snapshot(
                    bytes.fromhex(payload.raw_csv_hex),
                    _date(payload.window_start_date),
                    _date(payload.window_end_date),
                    row.observed_at_ms,
                )
                if canonical != row:
                    raise IntegrityError("TIPS_SNAPSHOT_CANONICAL")
                snapshots[row.event_id] = row, payload
            else:
                status = PollHealth.model_validate_json(row.payload_json)
                canonical = tips_yield_health(
                    row.observed_at_ms,
                    snapshot_event_id=status.snapshot_event_ids[0]
                    if status.snapshot_event_ids
                    else None,
                    response_received_at_ms=status.response_received_at_ms,
                    http_status=status.http_status,
                    error_type=status.error_type,
                )
                if canonical != row:
                    raise IntegrityError("TIPS_HEALTH_CANONICAL")
                health.append((row, status))
        if not health:
            raise IntegrityError("TIPS_HEALTH_MISSING")
        latest = max(row.observed_at_ms for row, _ in health)
        choices = [(row, body) for row, body in health if row.observed_at_ms == latest]
        if len(choices) != 1:
            raise IntegrityError("TIPS_HEALTH_AMBIGUOUS")
        selected, status = choices[0]
        result.update(
            health_event_id=selected.event_id,
            health_received_at_ms=latest,
            response_received_at_ms=status.response_received_at_ms,
            poll_ok=status.ok,
            error_type=status.error_type,
            http_status=status.http_status,
        )
        if not status.ok:
            raise IntegrityError("TIPS_FEED_UNAVAILABLE")
        assert status.response_received_at_ms is not None
        if as_of_ms - status.response_received_at_ms > max_poll_age_ms:
            raise IntegrityError("TIPS_POLL_STALE")
        identity = status.snapshot_event_ids[0]
        if identity not in snapshots:
            raise IntegrityError("TIPS_REFERENCED_SNAPSHOT_MISSING")
        snapshot, payload = snapshots[identity]
        if snapshot.observed_at_ms != status.response_received_at_ms:
            raise IntegrityError("TIPS_SNAPSHOT_RESPONSE_RECEIPT_MISMATCH")
        values = [row for row in payload.rows if row.missing == "not_missing"]
        newest = values[-1] if values else None
        result.update(
            state="POLL_SNAPSHOT_VERIFIED",
            snapshot_event_id=identity,
            returned_rows=len(payload.rows),
            missing_dot_rows=sum(row.missing == "dot" for row in payload.rows),
            missing_empty_rows=sum(row.missing == "empty" for row in payload.rows),
            latest_returned_date=payload.rows[-1].observation_date if payload.rows else None,
            latest_value_date=newest.observation_date if newest else None,
            latest_value_percent_text=newest.yield_percent_text if newest else None,
            latest_value_date_age_days=(
                datetime.fromtimestamp(as_of_ms / 1000, UTC).date() - _date(newest.observation_date)
            ).days
            if newest
            else None,
            dated_value_state="REPORTED_DATED_VALUE" if newest else "NO_REPORTED_VALUE",
            window_start_date=payload.window_start_date,
            window_end_date=payload.window_end_date,
            raw_csv_sha256=payload.raw_csv_sha256,
        )
    except (IntegrityError, ValueError, TypeError, KeyError, OverflowError, csv.Error) as exc:
        result["blocker"] = str(exc) if isinstance(exc, IntegrityError) else "TIPS_INVALID_EVIDENCE"
    return result
