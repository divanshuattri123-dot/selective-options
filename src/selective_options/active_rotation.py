"""Serialized committed-cohort snapshots without closing or hashing the active store.

The producer lock protects only creation and exact verification of one closed
events-only snapshot. Partition validation then uses the unchanged closed-source
rotator while producers may append. This is a finite committed cohort, not proof
of complete history, current feed health, or a copied delivery acknowledgement.
"""

import asyncio
import json
import os
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, cast

from .archive import file_digest
from .archive_rotation import (
    _policy,
    _safe_path,
    _schema,
    rotate_archive,
    validate_rotation_catalog,
)
from .errors import IntegrityError
from .store import DuckStore, complete_thread

_ALIAS = "active_rotation_snapshot"
_COHORT = "event_time_ms<=$cutoff AND observed_at_ms<=$cutoff"


def _snapshot(
    store: DuckStore,
    output: Path,
    as_of_ms: int,
    partition_ms: int,
    max_partitions: int,
    created: list[bool],
) -> int:
    """The caller owns the store lock until this entire worker has drained."""
    output = _safe_path(output)
    if output.exists():
        raise FileExistsError("active rotation requires a new immutable output directory")
    if not output.parent.is_dir():
        raise FileNotFoundError("active rotation output parent must already exist")
    if store._closed:
        raise IntegrityError("ACTIVE_ROTATION_STORE_CLOSED")
    connection = store._connection
    transaction = False
    attached = False
    # Two separate autocommit SELECTs have different IDs; an explicit transaction
    # retains its ID. Probe before BEGIN, which otherwise aborts an existing DuckDB
    # transaction on error. The store lock excludes managed concurrent operations.
    first_transaction = connection.execute("SELECT current_transaction_id()").fetchone()
    second_transaction = connection.execute("SELECT current_transaction_id()").fetchone()
    if first_transaction == second_transaction:
        raise IntegrityError("ACTIVE_ROTATION_SOURCE_TRANSACTION_ALREADY_OPEN")
    try:
        # Reject an unmanaged pre-existing transaction rather than rolling it back.
        connection.execute("BEGIN TRANSACTION")
        transaction = True
        _schema(connection, "SELECT * FROM main.events LIMIT 0", [])
        ranges = connection.execute(
            "SELECT greatest(event_time_ms,observed_at_ms)//$partition AS bucket,count(*) "
            f"FROM main.events WHERE {_COHORT} GROUP BY bucket ORDER BY bucket LIMIT $budget",
            {"partition": partition_ms, "cutoff": as_of_ms, "budget": max_partitions + 1},
        ).fetchall()
        if len(ranges) > max_partitions:
            raise IntegrityError("ACTIVE_ROTATION_PARTITION_BUDGET")
        total = sum(count for _, count in ranges)
        counts = connection.execute(
            f"SELECT count(*),count(DISTINCT event_id) FROM main.events WHERE {_COHORT}",
            {"cutoff": as_of_ms},
        ).fetchone()
        if counts != (total, total):
            raise IntegrityError("ACTIVE_ROTATION_GLOBAL_ID_MISMATCH")
        aliases = connection.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        if (_ALIAS,) in aliases:
            raise IntegrityError("ACTIVE_ROTATION_ALIAS_ALREADY_ATTACHED")
        output.mkdir(exist_ok=False)
        created.append(True)
        snapshot = _safe_path(output / "snapshot.duckdb")
        # Installed DuckDB rejects parameters in ATTACH grammar. Restrict the path,
        # escape every SQL apostrophe, and use only a fixed internal alias. All
        # selection values remain bound parameters; no SQL identifiers are supplied.
        quoted = str(snapshot).replace("'", "''")
        connection.execute(f"ATTACH '{quoted}' AS {_ALIAS}")
        attached = True
        connection.execute(
            f"CREATE TABLE {_ALIAS}.events AS SELECT * FROM main.events WHERE {_COHORT}",
            {"cutoff": as_of_ms},
        )
        _schema(connection, f"SELECT * FROM {_ALIAS}.events LIMIT 0", [])
        saved = connection.execute(
            f"SELECT count(*),count(DISTINCT event_id) FROM {_ALIAS}.events"
        ).fetchone()
        difference = connection.execute(
            "SELECT count(*) FROM (("
            f"SELECT * FROM main.events WHERE {_COHORT} EXCEPT ALL SELECT * FROM {_ALIAS}.events"
            ") UNION ALL ("
            f"SELECT * FROM {_ALIAS}.events EXCEPT ALL SELECT * FROM main.events WHERE {_COHORT}"
            ")) AS differences",
            {"cutoff": as_of_ms},
        ).fetchone()
        if saved != (total, total) or difference != (0,):
            raise IntegrityError("ACTIVE_ROTATION_SNAPSHOT_TYPED_RECORD_MISMATCH")
        connection.execute("COMMIT")
        transaction = False
        connection.execute(f"DETACH {_ALIAS}")
        attached = False
        # DETACH closes/checkpoints the new file. No read or hash of the active
        # source file is attempted; its writer remains open throughout.
        if not snapshot.is_file() or snapshot.with_suffix(".duckdb.wal").exists():
            raise IntegrityError("ACTIVE_ROTATION_SNAPSHOT_NOT_CLOSED")
        return cast(int, total)
    finally:
        try:
            if transaction:
                connection.execute("ROLLBACK")
        finally:
            if attached:
                # DuckDB rolls an in-transaction ATTACH back along with CTAS. Do
                # not mask the original failure with a detach of an absent alias.
                aliases = connection.execute(
                    "SELECT database_name FROM duckdb_databases()"
                ).fetchall()
                if (_ALIAS,) in aliases:
                    connection.execute(f"DETACH {_ALIAS}")


def _publish(output: Path, report: dict[str, object]) -> None:
    pending = output / ".active-rotation.pending.json"
    with pending.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, output / "active-rotation.json")
    pending.unlink()


def _failed(output: Path, failure: str) -> None:
    output = _safe_path(output)
    success = output / "active-rotation.json"
    if success.exists():
        # Cancellation can arrive while the publication worker drains. Revoke the
        # active success claim while preserving all snapshot/partition bytes.
        success.rename(output / "active-rotation-incomplete.json")
        incomplete = json.loads(
            (output / "active-rotation-incomplete.json").read_text(encoding="utf-8")
        )
        incomplete["status"] = "PUBLICATION_REVOKED"
        (output / "active-rotation-incomplete.json").write_text(
            json.dumps(incomplete, indent=2) + "\n", encoding="utf-8"
        )
    with (output / "active-rotation-failed.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps({"status": "FAILED", "failure_type": failure}) + "\n")


async def _cleanup(operation: Coroutine[Any, Any, None]) -> None:
    task = asyncio.create_task(operation)
    while True:
        try:
            await asyncio.shield(task)
            return
        except asyncio.CancelledError:
            if task.done():
                task.result()
                return


async def rotate_active_store(
    store: DuckStore,
    output: Path,
    as_of_ms: int,
    partition_ms: int = 86_400_000,
    batch_size: int = 500,
    max_partitions: int = 366,
) -> dict[str, object]:
    """Save the exact cohort committed when the source transaction begins.

    Later commits, including records with clocks before the same cutoff, are absent
    from this snapshot. The caller can first flush its ingestion queue if it needs a
    producer barrier. Budgets bound Python batches and partition count, not SQL cost
    or the duration of the snapshot lock. A failed or cancelled operation retains
    inspectable artifacts without an active success report and drains its workers.
    """
    _policy(as_of_ms, partition_ms, batch_size, max_partitions)
    output = _safe_path(output)
    created: list[bool] = []
    try:
        async with store._lock:
            total = await complete_thread(
                _snapshot, store, output, as_of_ms, partition_ms, max_partitions, created
            )
        snapshot = output / "snapshot.duckdb"
        # The source lock is released before any full typed partition validation.
        catalog = await rotate_archive(
            snapshot, output / "rotation", as_of_ms, partition_ms, batch_size, max_partitions
        )
        if catalog["total_rows"] != total:
            raise IntegrityError("ACTIVE_ROTATION_CATALOG_COHORT_MISMATCH")
        saved_catalog = await complete_thread(validate_rotation_catalog, output / "rotation")
        if saved_catalog.model_dump(mode="json") != catalog:
            raise IntegrityError("ACTIVE_ROTATION_CATALOG_CHANGED")
        snapshot_sha = await complete_thread(file_digest, snapshot)
        if catalog["source_sha256"] != snapshot_sha:
            raise IntegrityError("ACTIVE_ROTATION_SNAPSHOT_CHANGED")
        report: dict[str, object] = {
            "status": "ACTIVE_ROTATION_VERIFIED",
            "rows": total,
            "parts": len(cast(list[object], catalog["parts"])),
            "as_of_ms": as_of_ms,
            "partition_ms": partition_ms,
            "snapshot_file": "snapshot.duckdb",
            "snapshot_sha256": snapshot_sha,
            "catalog": "rotation/catalog.json",
            "catalog_sha256": await complete_thread(file_digest, output / "rotation/catalog.json"),
            "source_scope": "committed events in the open store transaction; both clocks at or before cutoff",
            "source_database_hash_claimed": False,
            "delivery_acknowledgements_copied": False,
            "source_history_modified": False,
            "history_complete": False,
            "current_feed_health_verified": False,
            "ready_for_signals": False,
            "trading_enabled": False,
            "source_timestamps_preserved": True,
            "observation_timestamps_preserved": True,
            "identities_preserved": True,
            "producer_lock_released_before_partition_validation": True,
            "later_commits_included": False,
            "limitation": "Snapshot contains only the cohort committed at source transaction start. Later commits are excluded even when both clocks precede the cutoff; delivery acknowledgements are not copied. Application budgets do not bound SQL scans, engine memory or lock duration.",
        }
        final_catalog = await complete_thread(validate_rotation_catalog, output / "rotation")
        if (
            final_catalog.model_dump(mode="json") != catalog
            or await complete_thread(file_digest, output / "rotation/catalog.json")
            != report["catalog_sha256"]
            or await complete_thread(file_digest, snapshot) != snapshot_sha
        ):
            raise IntegrityError("ACTIVE_ROTATION_ARTIFACT_CHANGED_BEFORE_PUBLICATION")
        await complete_thread(_publish, output, report)
        return report
    except BaseException as exc:
        if created:
            await _cleanup(complete_thread(_failed, output, type(exc).__name__))
        raise
