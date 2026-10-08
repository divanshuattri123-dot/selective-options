"""Offline audit of a retained public FOMC page using the current typed parser."""

import argparse
import hashlib
import json
from pathlib import Path

import duckdb

from selective_options import __version__
from selective_options.events import MarketEvent
from selective_options.fomc_calendar import (
    MAX_AGE_MS,
    SOURCE,
    SYMBOL,
    CalendarPayload,
    calendar_at,
    calendar_quality,
    parse_calendar,
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_events(database: Path, archive: Path) -> tuple[list[MarketEvent], list[MarketEvent]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        db_cursor = connection.execute("SELECT * FROM events ORDER BY observed_at_ms,event_id")
        db_columns = [column[0] for column in db_cursor.description]
        database_rows = [
            MarketEvent.model_validate(dict(zip(db_columns, row, strict=True)))
            for row in db_cursor.fetchall()
        ]
        pq_cursor = connection.execute(
            "SELECT * FROM read_parquet(?) ORDER BY observed_at_ms,event_id", [str(archive)]
        )
        pq_columns = [column[0] for column in pq_cursor.description]
        parquet_rows = [
            MarketEvent.model_validate(dict(zip(pq_columns, row, strict=True)))
            for row in pq_cursor.fetchall()
        ]
    return database_rows, parquet_rows


def audit(source: Path, output: Path) -> dict[str, object]:
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    raw = (source / "response.html").read_bytes()
    body_hash = digest(source / "response.html")
    if len(raw) != report["response_bytes"] or body_hash != report["response_sha256"]:
        raise ValueError("saved response does not match the public probe report")
    receipt = report["response_received_at_ms"]
    event = parse_calendar(raw, receipt)
    payload = CalendarPayload.model_validate(event.payload())
    entries = payload.entries
    meetings = [entry for entry in entries if entry.event_type == "meeting"]
    notation_votes = [entry for entry in entries if entry.event_type == "notation_vote"]
    if len(entries) != 57 or len(meetings) != 56 or len(notation_votes) != 1:
        raise ValueError("unexpected saved schedule-entry classification")
    if calendar_at([event], receipt - 1) is not None or calendar_at([event], receipt) != event:
        raise ValueError("saved response receipt boundary failed")
    database_rows, parquet_rows = read_events(source / "events.duckdb", source / "events.parquet")
    manifest = json.loads((source / "events.parquet.manifest.json").read_text(encoding="utf-8"))
    parquet_path = source / "events.parquet"
    if (
        database_rows != parquet_rows
        or len(database_rows) != 2
        or manifest["rows"] != 2
        or manifest["bytes"] != parquet_path.stat().st_size
        or manifest["sha256"] != digest(parquet_path)
    ):
        raise ValueError("retained database/archive consistency check failed")
    old_snapshot = next(
        row
        for row in database_rows
        if row.kind == "calendar" and row.source == SOURCE and row.symbol == SYMBOL
    )
    health = next(
        row for row in database_rows if row.kind == "feed_health" and row.source == SOURCE
    )
    if (
        old_snapshot.event_id != report["snapshot_event_id"]
        or old_snapshot.observed_at_ms != receipt
        or old_snapshot.payload().get("body_sha256") != body_hash
    ):
        raise ValueError("stored event does not bind the original public page")
    as_of_ms = report["quality"]["as_of_ms"]
    quality = calendar_quality([event, health], as_of_ms, MAX_AGE_MS)
    if quality["state"] != "OBSERVED_SCOPE_AVAILABLE":
        raise ValueError("saved response plus its actual health did not pass current quality")
    return {
        "status": "SAVED_PUBLIC_FOMC_RESPONSE_RECLASSIFIED_AND_AUDITED",
        "version": __version__,
        "network_requests": 0,
        "raw_response_sha256": body_hash,
        "raw_response_bytes": len(raw),
        "original_public_probe_event_id": old_snapshot.event_id,
        "current_parser_event_id": event.event_id,
        "current_parser_sha256": digest(
            Path(__file__).parents[1] / "src/selective_options/fomc_calendar.py"
        ),
        "audit_script_sha256": digest(Path(__file__)),
        "regular_meetings": len(meetings),
        "notation_vote_entries": len(notation_votes),
        "total_schedule_entries": len(entries),
        "listed_years": sorted({entry.year for entry in entries}),
        "receipt_exclusion_and_visibility": True,
        "current_quality_against_actual_saved_health": quality,
        "original_duckdb_rows": len(database_rows),
        "original_parquet_rows": len(parquet_rows),
        "original_duckdb_parquet_exact_match": True,
        "original_parquet_manifest_verified": True,
        "milestone_2_complete": False,
        "trading_enabled": False,
        "orders_submitted": 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.source, args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(json.dumps(result, indent=2))
