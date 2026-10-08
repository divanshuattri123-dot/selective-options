"""Offline saved-public exact rotation, restore, availability and failure proof.

No network transport is imported or called. Native source captures stay immutable;
all added events are explicitly labelled verification fixtures.
"""

import argparse
import asyncio
import io
import json
import shutil
from collections import Counter
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import cast

import duckdb
from defusedxml.ElementTree import parse
from pydantic import JsonValue

from selective_options.archive import _verify_archive, file_digest
from selective_options.archive_rotation import rotate_archive, validate_rotation_catalog
from selective_options.crypto_news import crypto_news_quality, news_poll_health
from selective_options.errors import IntegrityError
from selective_options.events import MarketEvent, make_event
from selective_options.rotation_restore import restore_rotation
from selective_options.stablecoin_supply import stablecoin_supply_quality
from selective_options.store import DuckStore, complete_thread

ROOT = Path(__file__).resolve().parents[1]
MAX_BYTES = 32 * 1024 * 1024
MAX_FILES = 24
MAX_ROWS = 256
PARTITION_MS = 86400000
VERIFIER = Path(__file__).resolve()


@dataclass(frozen=True)
class SavedSources:
    rows: tuple[MarketEvent, ...]
    hashes: dict[str, dict[str, str]]
    paths: tuple[Path, Path]
    native_facts: dict[str, object]


def source_hashes(path: Path) -> dict[str, str]:
    if path.is_symlink() or not path.is_dir():
        raise ValueError("rotation proof source must be a real directory")
    files = sorted(path.iterdir())
    if len(files) > MAX_FILES:
        raise IntegrityError("ROTATION_PROOF_SOURCE_FILE_BUDGET")
    if any(item.is_symlink() or not item.is_file() for item in files):
        raise ValueError("rotation proof source must contain only regular files")
    if sum(item.stat().st_size for item in files) > MAX_BYTES:
        raise IntegrityError("ROTATION_PROOF_SOURCE_BYTE_BUDGET")
    return {item.name: file_digest(item) for item in files}


def read_rows(database: Path, archive: Path | None = None) -> tuple[MarketEvent, ...]:
    with duckdb.connect(str(database), read_only=True) as connection:
        cursor = connection.execute(
            "SELECT * FROM events ORDER BY event_id LIMIT ?", [MAX_ROWS + 1]
        )
        columns = [entry[0] for entry in cursor.description]
        values = cursor.fetchall()
        if len(values) > MAX_ROWS:
            raise IntegrityError("ROTATION_PROOF_SOURCE_ROW_BUDGET")
        if archive is not None:
            manifest = _verify_archive(archive)
            archived = connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY event_id LIMIT ?",
                [str(archive), MAX_ROWS + 1],
            ).fetchall()
            if values != archived or manifest.rows != len(values):
                raise IntegrityError("ROTATION_PROOF_DATABASE_ARCHIVE_MISMATCH")
        return tuple(
            MarketEvent.model_validate(dict(zip(columns, value, strict=True))) for value in values
        )


def independent_news(source: Path, rows: tuple[MarketEvent, ...]) -> dict[str, object]:
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if (
        report.get("status") != "PUBLIC_CRYPTO_NEWS_METADATA_VERIFIED"
        or report.get("real_public_requests_attempted") != 1
    ):
        raise IntegrityError("ROTATION_PROOF_NEWS_PUBLIC_PROVENANCE")
    entities = report.get("entities")
    if not isinstance(entities, list) or len(entities) != 1:
        raise IntegrityError("ROTATION_PROOF_NEWS_ENTITY_COUNT")
    body = (source / "response.xml").read_bytes()
    entity = entities[0]
    if (
        len(body) > 2097152
        or entity.get("sha256") != file_digest(source / "response.xml")
        or entity.get("bytes") != len(body)
        or entity.get("http_status") != 200
    ):
        raise IntegrityError("ROTATION_PROOF_NEWS_NATIVE_ENTITY")
    tree = parse(io.BytesIO(body), forbid_dtd=True, forbid_entities=True, forbid_external=True)
    items = tree.findall("./channel/item")
    news = [event for event in rows if event.kind == "news"]
    health = [event for event in rows if event.kind == "feed_health"]
    if len(items) != 25 or len(news) != 25 or len(health) != 1 or len(rows) != 26:
        raise IntegrityError("ROTATION_PROOF_NEWS_NATIVE_COUNT")
    expected = {}
    for item in items:
        fields = {}
        for native, xml in (
            ("native_title", "title"),
            ("native_link", "link"),
            ("native_guid", "guid"),
            ("native_pub_date", "pubDate"),
        ):
            children = item.findall(xml)
            if len(children) != 1:
                raise IntegrityError("ROTATION_PROOF_NEWS_NATIVE_SHAPE")
            fields[native] = "".join(children[0].itertext())
        expected[fields["native_guid"]] = fields
    for row in news:
        payload = row.payload()
        native_fields = expected.get(str(payload["native_guid"]))
        if (
            native_fields is None
            or any(payload.get(key) != value for key, value in native_fields.items())
            or row.observed_at_ms != entity["received_at_ms"]
            or row.event_time_ms
            != int(parsedate_to_datetime(native_fields["native_pub_date"]).timestamp() * 1000)
        ):
            raise IntegrityError("ROTATION_PROOF_NEWS_NATIVE_MISMATCH")
    quality = crypto_news_quality(rows, as_of_ms=health[0].observed_at_ms)
    if quality["state"] != "POLL_OBSERVED_VERIFIED":
        raise IntegrityError("ROTATION_PROOF_NEWS_SOURCE_QUALITY")
    return {
        "records": 26,
        "metadata_items": 25,
        "entity_sha256": entity["sha256"],
        "quality": quality,
        "classification": "NOT_CLASSIFIED",
        "first_publication_verified": False,
    }


def independent_supply(source: Path, rows: tuple[MarketEvent, ...]) -> dict[str, object]:
    report = json.loads((source / "report.json").read_text(encoding="utf-8"))
    if (
        report.get("status") != "PUBLIC_ETHEREUM_SUPPLY_PAIR_VERIFIED"
        or report.get("real_public_requests_attempted") != 7
    ):
        raise IntegrityError("ROTATION_PROOF_SUPPLY_PUBLIC_PROVENANCE")
    entries = report.get("entities")
    if not isinstance(entries, list) or len(entries) != 7:
        raise IntegrityError("ROTATION_PROOF_SUPPLY_ENTITY_COUNT")
    snapshots = [event for event in rows if event.kind == "on_chain"]
    health = [event for event in rows if event.kind == "feed_health"]
    if len(rows) != 2 or len(snapshots) != 1 or len(health) != 1:
        raise IntegrityError("ROTATION_PROOF_SUPPLY_NATIVE_COUNT")
    payload = snapshots[0].payload()
    results = []
    for index, entry in enumerate(entries):
        path = source / f"response-{index + 1:02}.json"
        body = path.read_bytes()
        raw = json.loads(body)
        if (
            entry.get("sha256") != file_digest(path)
            or entry.get("bytes") != len(body)
            or entry.get("body_hex") != body.hex()
            or entry.get("http_status") != 200
            or raw.get("jsonrpc") != "2.0"
            or raw.get("id") != index + 1
            or "error" in raw
        ):
            raise IntegrityError("ROTATION_PROOF_SUPPLY_NATIVE_ENTITY")
        results.append(raw["result"])
    header = results[1]
    if (
        results[0] != "0x1"
        or results[6] != header
        or payload.get("block_number") != int(header["number"], 16)
        or payload.get("block_hash") != header["hash"]
        or snapshots[0].event_time_ms != int(header["timestamp"], 16) * 1000
        or snapshots[0].observed_at_ms != entries[-1]["received_at_ms"]
    ):
        raise IntegrityError("ROTATION_PROOF_SUPPLY_NATIVE_HEADER")
    for key, supply_index, decimals_index in (("usdc", 2, 3), ("usdt", 4, 5)):
        token = cast(dict[str, JsonValue], payload[key])
        if (
            token.get("total_supply_abi_word") != results[supply_index]
            or token.get("total_supply_base_units") != str(int(results[supply_index], 16))
            or token.get("decimals_abi_word") != results[decimals_index]
            or token.get("decimals") != int(results[decimals_index], 16)
        ):
            raise IntegrityError("ROTATION_PROOF_SUPPLY_NATIVE_ABI")
    quality = stablecoin_supply_quality(rows, as_of_ms=health[0].observed_at_ms)
    if quality["state"] != "POLL_OBSERVED_VERIFIED":
        raise IntegrityError("ROTATION_PROOF_SUPPLY_SOURCE_QUALITY")
    return {
        "records": 2,
        "native_response_entities": 7,
        "quality": quality,
        "finality_basis": "RPC provider assertion; not independently verified",
    }


def preflight(news: Path, supply: Path, output: Path) -> SavedSources:
    scratch = (ROOT.parents[1] / "work/rotation-0342").resolve()
    output = output.resolve()
    if output != (
        ROOT / "evidence/rotation-saved-public-01"
    ).resolve() and not output.is_relative_to(scratch):
        raise ValueError("output must be rotation-saved-public-01 or within workspace scratch")
    if output.exists() or any(
        output == path.resolve() or path.resolve().is_relative_to(output) for path in (news, supply)
    ):
        raise FileExistsError("refusing to overwrite rotation or original evidence")
    before = {"news": source_hashes(news), "supply": source_hashes(supply)}
    news_rows = read_rows(news / "events.duckdb", news / "events.parquet")
    supply_rows = read_rows(supply / "events.duckdb", supply / "events.parquet")
    facts: dict[str, object] = {
        "news": independent_news(news, news_rows),
        "supply": independent_supply(supply, supply_rows),
    }
    return SavedSources(news_rows + supply_rows, before, (news, supply), facts)


def fixture_events(native: tuple[MarketEvent, ...]) -> tuple[list[MarketEvent], int, int]:
    latest = max(max(event.event_time_ms, event.observed_at_ms) for event in native)
    cutoff = latest + 3000
    base = cutoff // PARTITION_MS * PARTITION_MS
    synthetic = []
    clocks = (
        (base - 1, base - 1),
        (base, base),
        (base + 1, base + 1),
        (base - 1, base + 2),
        (base + 2, base - 1),
        (cutoff, cutoff),
        (cutoff + PARTITION_MS, base),
        (base, cutoff + PARTITION_MS),
        (cutoff + 2 * PARTITION_MS, base),
        (base, cutoff + 2 * PARTITION_MS),
    )
    for index, (source_clock, receipt) in enumerate(clocks):
        synthetic.append(
            make_event(
                source="rotation_verification_fixture",
                environment="testnet",
                asset="FIXTURE",
                symbol=f"BOUNDARY_{index}",
                kind="status",
                event_time_ms=source_clock,
                observed_at_ms=receipt,
                payload={
                    "provenance": "synthetic_offline_verification_fixture",
                    "purpose": "both clocks before partition budget and availability boundary",
                    "fixture_index": index,
                },
            )
        )
    # Canonical failed health is synthetic, never an actual provider outage claim.
    synthetic.append(news_poll_health(latest + 1000, error_type="SYNTHETIC_OFFLINE_FAILURE"))
    return synthetic, cutoff, base


def outbox_rows(database: Path) -> list[tuple[str, bool]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(
            "SELECT event_id, delivered FROM delivery_outbox ORDER BY event_id"
        ).fetchall()


async def quality_views(
    database: Path, cutoffs: list[int], expected: tuple[MarketEvent, ...]
) -> list[dict[str, object]]:
    store = DuckStore(duckdb.connect(str(database), read_only=True), 5000)
    checks = []
    try:
        for cutoff in cutoffs:
            actual = crypto_news_quality(expected, as_of_ms=cutoff)
            bounded = await store.crypto_news_quality(cutoff)
            supply = stablecoin_supply_quality(expected, as_of_ms=cutoff)
            bounded_supply = await store.stablecoin_supply_quality(cutoff)
            if actual != bounded or supply != bounded_supply:
                raise IntegrityError("ROTATION_PROOF_SHARED_QUALITY_MISMATCH")
            checks.append({"as_of_ms": cutoff, "crypto_news": actual, "stablecoin_supply": supply})
        return checks
    finally:
        await store.close()


async def verify(news: Path, supply: Path, output: Path) -> dict[str, object]:
    saved = await complete_thread(preflight, news, supply, output)
    before_code = {
        name: file_digest(ROOT / "src/selective_options" / name)
        for name in ("archive_rotation.py", "rotation_restore.py")
    }
    before_verifier = file_digest(VERIFIER)
    output = await complete_thread(output.resolve)
    output.mkdir(parents=True, exist_ok=False)
    fixture, cutoff, base = fixture_events(saved.rows)
    all_rows = tuple(sorted((*saved.rows, *fixture), key=lambda event: event.event_id))
    eligible = tuple(
        event
        for event in all_rows
        if event.event_time_ms <= cutoff and event.observed_at_ms <= cutoff
    )
    database = output / "source.duckdb"
    # Large skew is for deliberately inconsistent future-clock fixtures only.
    store = await DuckStore.open(database, max_skew_ms=2**63 - 1)
    try:
        await store.append(list(all_rows), exact=True)
        await store.acknowledge_delivery([saved.rows[0].event_id])
    finally:
        await store.close()
    before_hash = file_digest(database)
    before_outbox = outbox_rows(database)
    buckets = Counter(
        max(row.event_time_ms, row.observed_at_ms) // PARTITION_MS for row in eligible
    )
    directory = output / "rotation"
    rotation = await rotate_archive(database, directory, cutoff, PARTITION_MS, 3, len(buckets))
    catalog = await complete_thread(validate_rotation_catalog, directory)
    if catalog.total_rows != len(eligible) or len(catalog.parts) != len(buckets):
        raise IntegrityError("ROTATION_PROOF_CATALOG_COUNTS")
    archived: list[MarketEvent] = []
    partitions = []
    with duckdb.connect() as connection:
        for part in catalog.parts:
            cursor = connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) ORDER BY event_id",
                [str(directory / part.file)],
            )
            columns = [entry[0] for entry in cursor.description]
            rows = tuple(
                MarketEvent.model_validate(dict(zip(columns, values, strict=True)))
                for values in cursor.fetchall()
            )
            if len(rows) != buckets[part.start_ms // PARTITION_MS] or any(
                not part.start_ms <= max(row.event_time_ms, row.observed_at_ms) < part.end_ms
                for row in rows
            ):
                raise IntegrityError("ROTATION_PROOF_AVAILABILITY_PARTITION")
            archived.extend(rows)
            partitions.append(
                {
                    "file": part.file,
                    "start_ms": part.start_ms,
                    "end_ms": part.end_ms,
                    "rows": len(rows),
                    "sha256": part.sha256,
                    "manifest_sha256": part.manifest_sha256,
                }
            )
    if tuple(sorted(archived, key=lambda event: event.event_id)) != eligible:
        raise IntegrityError("ROTATION_PROOF_ARCHIVE_EXACT_ROWS")
    over_budget = output / "over-budget"
    try:
        await rotate_archive(database, over_budget, cutoff, PARTITION_MS, 3, len(buckets) - 1)
    except IntegrityError as exc:
        if "ARCHIVE_ROTATION_PARTITION_BUDGET" not in str(exc) or over_budget.exists():
            raise
    else:
        raise IntegrityError("ROTATION_PROOF_PARTITION_BUDGET_NOT_REFUSED")
    restored = output / "restored"
    restore = await restore_rotation(directory / "catalog.json", restored, batch_size=3)
    restored_rows = read_rows(restored / "events.duckdb")
    if (
        restored_rows != eligible
        or any(delivered for _, delivered in outbox_rows(restored / "events.duckdb"))
        or len(outbox_rows(restored / "events.duckdb")) != len(eligible)
    ):
        raise IntegrityError("ROTATION_PROOF_RESTORE_EXACT_ROWS_OR_OUTBOX")
    native_restored = tuple(
        row for row in restored_rows if row.event_id in {event.event_id for event in saved.rows}
    )
    if native_restored != tuple(sorted(saved.rows, key=lambda event: event.event_id)):
        raise IntegrityError("ROTATION_PROOF_NATIVE_RECEIPTS_CHANGED")
    cutoffs = sorted(
        {
            value
            for event in eligible
            for value in (
                event.event_time_ms - 1,
                event.event_time_ms,
                event.observed_at_ms - 1,
                event.observed_at_ms,
            )
            if value > 0
        }
        | {base - 1, base, base + 1, cutoff}
    )
    pit = []
    with duckdb.connect(str(restored / "events.duckdb"), read_only=True) as connection:
        for asof in cutoffs:
            values = connection.execute(
                "SELECT event_id FROM events WHERE event_time_ms<=? AND observed_at_ms<=? ORDER BY event_id",
                [asof, asof],
            ).fetchall()
            expected_ids = sorted(
                event.event_id
                for event in eligible
                if event.event_time_ms <= asof and event.observed_at_ms <= asof
            )
            if values != [(identity,) for identity in expected_ids]:
                raise IntegrityError("ROTATION_PROOF_BOTH_CLOCKS_PIT")
            pit.append({"as_of_ms": asof, "available_rows": len(values), "event_ids": expected_ids})
    health_cutoffs = sorted(
        {row.observed_at_ms for row in saved.rows if row.kind == "feed_health"}
        | {max(row.observed_at_ms for row in saved.rows) + 1000, cutoff}
    )
    quality = await quality_views(restored / "events.duckdb", health_cutoffs, eligible)
    if quality[-1]["crypto_news"]["state"] != "BLOCKED":  # type: ignore[index]
        raise IntegrityError("ROTATION_PROOF_LATEST_SYNTHETIC_FAILURE_LOST")
    corrupt = output / "corrupt-rotation"
    shutil.copytree(directory, corrupt)
    with (corrupt / catalog.parts[0].file).open("ab") as stream:
        stream.write(b"synthetic corruption")
    failed = output / "corrupt-restore"
    try:
        await restore_rotation(corrupt / "catalog.json", failed, batch_size=3)
    except (IntegrityError, ValueError) as exc:
        corruption_error = str(exc)
        if "ARCHIVE_ROTATION_CHECKSUM_MISMATCH" not in corruption_error:
            raise
        if (failed / "events.duckdb").exists():
            raise IntegrityError("ROTATION_PROOF_CORRUPT_RESTORE_PUBLISHED") from exc
    else:
        raise IntegrityError("ROTATION_PROOF_CORRUPTION_NOT_REFUSED")
    if (
        source_hashes(news) != saved.hashes["news"]
        or source_hashes(supply) != saved.hashes["supply"]
        or before_hash != file_digest(database)
        or before_outbox != outbox_rows(database)
    ):
        raise IntegrityError("ROTATION_PROOF_SOURCE_MUTATED")
    code = {
        name: file_digest(ROOT / "src/selective_options" / name)
        for name in ("archive_rotation.py", "rotation_restore.py")
    }
    if code != before_code or file_digest(VERIFIER) != before_verifier:
        raise IntegrityError("ROTATION_PROOF_CODE_CHANGED_DURING_VERIFICATION")
    report: dict[str, object] = {
        "status": "OFFLINE_SAVED_PUBLIC_ROTATION_RESTORE_VERIFIED",
        "real_public_requests_attempted": 0,
        "saved_native_records": len(saved.rows),
        "synthetic_records": len(fixture),
        "source_records": len(all_rows),
        "eligible_records": len(eligible),
        "excluded_future_clock_fixtures": len(all_rows) - len(eligible),
        "as_of_ms": cutoff,
        "partition_ms": PARTITION_MS,
        "partitions": partitions,
        "rotation": rotation,
        "restore": restore,
        "pit_checks": pit,
        "shared_quality_checks": quality,
        "native_facts": saved.native_facts,
        "source_hashes_before": saved.hashes,
        "source_hashes_after": {"news": source_hashes(news), "supply": source_hashes(supply)},
        "working_source_sha256_before": before_hash,
        "working_source_sha256_after": file_digest(database),
        "source_outbox_before": before_outbox,
        "source_outbox_after": outbox_rows(database),
        "synthetic_events": [row.model_dump() for row in fixture],
        "corrupt_restore": {"error": corruption_error, "final_database_published": False},
        "partition_budget_refused_before_output_creation": True,
        "code_sha256": code,
        "verifier_sha256": file_digest(VERIFIER),
        "ready_for_signals": False,
        "trading_enabled": False,
        "limitation": "Offline event rotation/restore only. Synthetic failed health is not an actual provider outage. No purge, Redis reconciliation, running capture, complete history or seven-day validation is proved.",
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    shutil.copyfile(VERIFIER, output / "verification-script.py")
    (output / "README.md").write_text(
        "# Saved-public rotated archive proof\n\nZero new public requests. Original CoinDesk metadata 26 rows and Ethereum USDC/USDT pair 2 rows are unchanged. Ten labelled clock/boundary fixtures plus one explicitly synthetic failed news health are added. Native source clocks and receipts are never rewritten. Availability is the greater of the two clocks; both clocks are filtered before the partition budget. Source database, original files and delivery acknowledgements remain unchanged. Restored delivery is pending for every eligible event, including one previously acknowledged source event. Corrupt partition refusal is a deliberate offline scenario. Provider finality and publisher publication claims retain their original limitations. This is not purge, complete history, Redis reconciliation, running recovery or seven-day validation.\n",
        encoding="utf-8",
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news", type=Path, default=ROOT / "evidence/crypto-news-public-02")
    parser.add_argument(
        "--supply", type=Path, default=ROOT / "evidence/stablecoin-supply-public-01"
    )
    parser.add_argument("--output", type=Path, default=ROOT / "evidence/rotation-saved-public-01")
    args = parser.parse_args()
    result = asyncio.run(verify(args.news, args.supply, args.output))
    print(
        json.dumps(
            {
                "status": result["status"],
                "native_records": result["saved_native_records"],
                "restored_records": result["eligible_records"],
                "public_requests": 0,
            }
        )
    )


if __name__ == "__main__":
    main()
