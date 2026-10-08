"""Opt-in bounded checkpoints during supervised ingestion; no implicit retention purge."""

import asyncio
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from .active_rotation import rotate_active_store
from .bus import EventBus
from .collectors import now_ms
from .config import AppConfig
from .errors import IntegrityError
from .store import DuckStore, complete_thread

SCOPE = (
    "Cumulative checkpoints of rows committed when the store checkpoint transaction starts; "
    "both clocks must be at or before a cutoff selected after the FIFO commit barrier. "
    "Later commits can contain earlier clocks; no complete source frontier, feed coverage, "
    "market history, retention purge, or trading approval is inferred."
)


async def drain_shutdown(operation: Coroutine[Any, Any, None]) -> asyncio.CancelledError | None:
    """Finish supervised cleanup while retaining additional caller cancellation."""
    task = asyncio.create_task(operation)
    cancelled: asyncio.CancelledError | None = None
    while True:
        try:
            await asyncio.shield(task)
            return cancelled
        except asyncio.CancelledError as exc:
            cancelled = cancelled or exc
            if task.done():
                task.result()
                return cancelled


class RotationRuntime:
    """One managed capture task with bounded snapshot records and explicit stop state."""

    def __init__(
        self,
        config: AppConfig,
        bus: EventBus,
        store: DuckStore,
        output: Path,
        stopped: asyncio.Event,
    ) -> None:
        self.config = config
        self.bus = bus
        self.store = store
        self.output = output
        self.stopped = stopped
        self.snapshots: list[dict[str, object]] = []
        self.attempts = 0
        self.last_attempt: dict[str, object] | None = None
        self.failure_type: str | None = None
        self.status = (
            "DISABLED" if config.ingestion.archive_rotation_interval_s is None else "NOT_STARTED"
        )

    async def _wait_due(self) -> bool:
        interval = self.config.ingestion.archive_rotation_interval_s
        assert interval is not None
        try:
            async with asyncio.timeout(interval):
                await self.stopped.wait()
            return False
        except TimeoutError:
            return not self.stopped.is_set()

    async def run(self) -> None:
        if self.config.ingestion.archive_rotation_interval_s is None:
            return
        try:
            self.status = "WAITING"
            while await self._wait_due():
                if len(self.snapshots) >= self.config.ingestion.archive_rotation_max_snapshots:
                    raise IntegrityError("ARCHIVE_ROTATION_SNAPSHOT_BUDGET_EXHAUSTED")
                self.status = "COMMIT_BARRIER"
                await self.bus.flush()
                if self.stopped.is_set():
                    break
                cutoff = now_ms()
                self.attempts += 1
                relative = f"archive-rotations/snapshot-{len(self.snapshots):06d}"
                directory = self.output / relative
                self.last_attempt = {
                    "directory": relative,
                    "as_of_ms": cutoff,
                    "status": "IN_PROGRESS",
                }
                await complete_thread(directory.parent.mkdir, exist_ok=True)
                self.status = "ROTATING"
                report = await rotate_active_store(
                    self.store,
                    directory,
                    cutoff,
                    partition_ms=self.config.ingestion.archive_rotation_partition_ms,
                    batch_size=self.config.ingestion.storage_batch_size,
                    max_partitions=self.config.ingestion.archive_rotation_max_partitions,
                )
                if (
                    report.get("status") != "ACTIVE_ROTATION_VERIFIED"
                    or report.get("as_of_ms") != cutoff
                    or report.get("history_complete") is not False
                    or report.get("ready_for_signals") is not False
                    or report.get("trading_enabled") is not False
                ):
                    raise IntegrityError("ARCHIVE_ROTATION_RUNTIME_INVALID_COMPLETION")
                completed = {"directory": relative, **report}
                self.snapshots.append(completed)
                self.last_attempt = completed
                self.status = "WAITING"
            self.status = "STOPPED"
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError) and self.stopped.is_set():
                self.status = "STOPPED"
            else:
                self.status = "FAILED"
                self.failure_type = type(exc).__name__
            if self.last_attempt is not None and self.last_attempt.get("status") == "IN_PROGRESS":
                self.last_attempt = {
                    **self.last_attempt,
                    "status": "CANCELLED" if isinstance(exc, asyncio.CancelledError) else "FAILED",
                    "failure_type": type(exc).__name__,
                }
            raise

    def summary(self) -> dict[str, object]:
        return {
            "enabled": self.config.ingestion.archive_rotation_interval_s is not None,
            "status": self.status,
            "interval_s": self.config.ingestion.archive_rotation_interval_s,
            "partition_ms": self.config.ingestion.archive_rotation_partition_ms,
            "max_partitions": self.config.ingestion.archive_rotation_max_partitions,
            "max_snapshots": self.config.ingestion.archive_rotation_max_snapshots,
            "attempts": self.attempts,
            "completed_snapshots": len(self.snapshots),
            "snapshots": [dict(report) for report in self.snapshots],
            "last_attempt": dict(self.last_attempt) if self.last_attempt is not None else None,
            "failure_type": self.failure_type,
            "scope": SCOPE,
            "delivery_acknowledgements_copied": False,
            "source_history_modified": False,
            "history_complete": False,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
