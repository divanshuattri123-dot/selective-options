"""Atomic recovery evidence publication; validated mixed-source plans survive restart."""

import hashlib
import json
from pathlib import Path
from typing import Annotated, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from .collectors import now_ms
from .errors import IntegrityError
from .events import MarketEvent, make_event
from .store import DuckStore, complete_thread
from .trade_reconciliation import RecoveryBuffer, RecoveryPlan

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class CommitPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    reason: Literal["RECOVERY_PLAN_VERIFIED"] = "RECOVERY_PLAN_VERIFIED"
    version: Literal[1] = 1
    anchor_event_id: Digest
    anchor_id: int = Field(ge=0)
    through_id: int = Field(ge=0)
    verified_at_ms: int = Field(gt=0)
    raw_event_ids: tuple[Digest, ...] = Field(min_length=1, max_length=20000)
    selected_event_ids: tuple[Digest, ...] = Field(min_length=1, max_length=20000)
    evidence_sha256: Digest


def _digest(anchor: MarketEvent, raw: tuple[MarketEvent, ...]) -> str:
    encoded = json.dumps(
        [row.model_dump() for row in (anchor, *raw)], sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def validate_plan(anchor: MarketEvent, plan: RecoveryPlan) -> RecoveryPlan:
    buffer = RecoveryBuffer(anchor, plan.through_id)
    for event in plan.raw:
        buffer.add(event)
    checked = buffer.finish(plan.verified_at_ms)
    if checked.anchor_id != plan.anchor_id or checked.selected != plan.selected:
        raise IntegrityError("RECOVERY_PLAN_SELECTION_MISMATCH")
    checked.ready_rows(plan.verified_at_ms)
    # Event storage retains a single first observation per stable event identity.
    unique: dict[str, MarketEvent] = {}
    for row in checked.raw:
        if row.event_id == anchor.event_id:
            row = anchor
        previous = unique.get(row.event_id)
        if previous is None or row.observed_at_ms < previous.observed_at_ms:
            unique[row.event_id] = row
    raw = tuple(unique[key] for key in sorted(unique))
    return RecoveryPlan(
        checked.anchor_id, checked.through_id, checked.verified_at_ms, checked.selected, raw
    )


def marker_for(anchor: MarketEvent, plan: RecoveryPlan) -> MarketEvent:
    payload = CommitPayload(
        anchor_event_id=anchor.event_id,
        anchor_id=plan.anchor_id,
        through_id=plan.through_id,
        verified_at_ms=plan.verified_at_ms,
        raw_event_ids=tuple(row.event_id for row in plan.raw),
        selected_event_ids=tuple(row.event_id for row in plan.selected),
        evidence_sha256=_digest(anchor, plan.raw),
    )
    return make_event(
        source="kraken_recovery",
        environment="public_reference",
        asset=anchor.asset,
        symbol=anchor.symbol,
        kind="status",
        event_time_ms=plan.verified_at_ms,
        observed_at_ms=plan.verified_at_ms,
        timestamp_kind="receipt",
        payload=json.loads(payload.model_dump_json()),
    )


async def publish_recovery(
    store: DuckStore, anchor: MarketEvent, plan: RecoveryPlan
) -> MarketEvent:
    checked = await complete_thread(validate_plan, anchor, plan)
    if checked.verified_at_ms > now_ms():
        raise IntegrityError("RECOVERY_VERIFICATION_IN_FUTURE")
    marker = marker_for(anchor, checked)
    # One transaction commits evidence, selection/cursor marker and pending delivery.
    # Exact matching prevents dedup from silently substituting different receipt times.
    await store.append([anchor, *checked.raw, marker], exact=True)
    return marker


def _marker_payload(marker: MarketEvent, as_of_ms: int) -> CommitPayload:
    if marker.source != "kraken_recovery" or marker.kind != "status":
        raise IntegrityError("RECOVERY_INVALID_MARKER")
    payload = CommitPayload.model_validate(marker.payload())
    if marker.observed_at_ms > as_of_ms or marker.event_time_ms > as_of_ms:
        raise IntegrityError("RECOVERY_NOT_YET_PUBLISHED")
    raw_ids, selected_ids = payload.raw_event_ids, payload.selected_event_ids
    if (
        len(set(raw_ids)) != len(raw_ids)
        or len(set(selected_ids)) != len(selected_ids)
        or not set(selected_ids).issubset(raw_ids)
    ):
        raise IntegrityError("RECOVERY_INVALID_REFERENCES")
    return payload


def _restore_plan(
    marker: MarketEvent, payload: CommitPayload, records: dict[str, MarketEvent], as_of_ms: int
) -> tuple[MarketEvent, RecoveryPlan]:
    if set(records) != {payload.anchor_event_id, *payload.raw_event_ids}:
        raise IntegrityError("RECOVERY_EVIDENCE_MISSING")
    anchor = records[payload.anchor_event_id]
    plan = RecoveryPlan(
        payload.anchor_id,
        payload.through_id,
        payload.verified_at_ms,
        tuple(records[key] for key in payload.selected_event_ids),
        tuple(records[key] for key in payload.raw_event_ids),
    )
    checked = validate_plan(anchor, plan)
    if marker_for(anchor, checked) != marker:
        raise IntegrityError("RECOVERY_MARKER_OR_EVIDENCE_CHANGED")
    checked.ready_rows(as_of_ms)
    return anchor, checked


def _load_evidence(
    database: Path, marker_id: str, as_of_ms: int
) -> tuple[MarketEvent, RecoveryPlan]:
    if not database.is_file():
        raise FileNotFoundError("recovery database does not exist")
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute("SELECT * FROM events WHERE event_id=?", [marker_id])
        columns = [item[0] for item in cursor.description]
        found = cursor.fetchone()
        if found is None:
            raise IntegrityError("RECOVERY_MARKER_MISSING")
        marker = MarketEvent.model_validate(dict(zip(columns, found, strict=True)))
        payload = _marker_payload(marker, as_of_ms)
        requested = {payload.anchor_event_id, *payload.raw_event_ids}
        cursor = connection.execute(
            "SELECT * FROM events WHERE event_id IN (SELECT unnest(?))", [list(requested)]
        )
        records = {
            str(row[0]): MarketEvent.model_validate(dict(zip(columns, row, strict=True)))
            for row in cursor.fetchall()
        }
    return _restore_plan(marker, payload, records, as_of_ms)


async def load_committed_recovery(
    store: DuckStore, marker_id: str, as_of_ms: int
) -> tuple[MarketEvent, RecoveryPlan]:
    """Revalidate immutable committed evidence while the capture writer stays open."""
    markers = await store.read_events([marker_id])
    if not markers:
        raise IntegrityError("RECOVERY_MARKER_MISSING")
    marker = markers[0]
    payload = _marker_payload(marker, as_of_ms)
    records = await store.read_events(list({payload.anchor_event_id, *payload.raw_event_ids}))
    return await complete_thread(
        _restore_plan, marker, payload, {row.event_id: row for row in records}, as_of_ms
    )


async def load_recovery(database: Path, marker_id: str, as_of_ms: int) -> RecoveryPlan:
    """Read-only, by explicit durable marker; no implicit activation of runtime capture."""
    return (await complete_thread(_load_evidence, database, marker_id, as_of_ms))[1]


async def load_recovery_evidence(
    database: Path, marker_id: str, as_of_ms: int
) -> tuple[MarketEvent, RecoveryPlan]:
    """Validated anchor and plan for exact publication into a new capture database."""
    return await complete_thread(_load_evidence, database, marker_id, as_of_ms)
