"""Finite, opt-in OPEN-bar generations from a mutation-gated live capture.

Each wrapper is immutable and published last. An explicitly selected wrapper is
verified against its supplied marker digest and its complete closed checkpoint.
This is not whole-source, service, feed-health or periodic crash-recovery proof.
No latest-generation fallback, source purge, or delivery acknowledgement is inferred.
"""

import asyncio
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

from .bar_checkpoint import (
    DEFAULT_MAX_BOOKS,
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_EVENTS,
    BarCheckpointResume,
    CheckpointManifest,
    _canonical,
    _cleanup,
    _pairs,
    _path,
    _read,
    load_bar_checkpoint,
)
from .bar_runtime import save_active_collector_bar_checkpoint
from .bar_state import MAX_CLOCK
from .bus import EventBus
from .capture_lifecycle import cancelled_worker_failure
from .config import AppConfig
from .errors import IntegrityError
from .spot_trades import SpotTradeCollector
from .store import DuckStore, complete_thread

MAX_MARKER_BYTES = 1024 * 1024
SCOPE = (
    "Exact consumed native WS inputs and book tails frozen at a complete collector batch, "
    "then committed through the FIFO writer barrier at a cutoff selected afterward. "
    "Each immutable generation is explicitly selected by its marker digest and fully verified. "
    "The collector gate remains held through INNER checkpoint codec validation/publication; "
    "outer immutable re-verification and generation sealing happen outside that gate. There is no "
    "throughput or lock-duration bound, source purge, automatic fallback, complete market/source "
    "frontier, delivery acknowledgement, sustained service or periodic crash-recovery claim."
)


class BarGeneration(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    format_version: StrictInt = Field(default=1, ge=1, le=1)
    status: Literal["COMMITTED_BAR_GENERATION"] = "COMMITTED_BAR_GENERATION"
    index: StrictInt = Field(ge=0, le=255)
    as_of_ms: StrictInt = Field(gt=0, le=MAX_CLOCK)
    checkpoint_file: Literal["checkpoint/checkpoint.json"] = "checkpoint/checkpoint.json"
    checkpoint_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_bytes: StrictInt = Field(gt=0, le=MAX_MARKER_BYTES)
    history_complete: bool = False
    source_frontier_complete: bool = False
    current_feed_health_verified: bool = False
    delivery_acknowledgements_copied: bool = False
    source_database_hash_claimed: bool = False
    periodic_crash_recovery: bool = False
    ready_for_signals: bool = False
    trading_enabled: bool = False

    @model_validator(mode="after")
    def controls(self) -> "BarGeneration":
        if any(
            getattr(self, name) is not False
            for name in (
                "history_complete",
                "source_frontier_complete",
                "current_feed_health_verified",
                "delivery_acknowledgements_copied",
                "source_database_hash_claimed",
                "periodic_crash_recovery",
                "ready_for_signals",
                "trading_enabled",
            )
        ):
            raise ValueError("generation cannot promote history, health, ACK or live claims")
        return self


def _new_directory(output: Path, created: list[bool]) -> None:
    output.mkdir(exist_ok=False)
    created.append(True)


def _seal(output: Path, index: int, expected: dict[str, object]) -> tuple[BarGeneration, str]:
    raw = _read(_path(output / "checkpoint" / "checkpoint.json"), MAX_MARKER_BYTES)
    checkpoint = CheckpointManifest.model_validate_json(raw)
    if checkpoint.model_dump(mode="json") != expected:
        raise IntegrityError("BAR_GENERATION_CHECKPOINT_CHANGED")
    marker = BarGeneration(
        index=index,
        as_of_ms=checkpoint.as_of_ms,
        checkpoint_sha256=hashlib.sha256(raw).hexdigest(),
        checkpoint_bytes=len(raw),
    )
    data = _canonical(marker.model_dump(mode="json")) + b"\n"
    pending = output / ".generation.pending.json"
    with pending.open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, output / "generation.json")
    pending.unlink()
    return marker, hashlib.sha256(data).hexdigest()


def _failed(output: Path, failure: str) -> None:
    output = _path(output)
    for success, incomplete in (
        (output / "generation.json", output / "generation-incomplete.json"),
        (
            output / "checkpoint" / "checkpoint.json",
            output / "checkpoint" / "checkpoint-incomplete.json",
        ),
    ):
        if success.exists():
            success.rename(incomplete)
    with (output / "failure.json").open("x", encoding="utf-8") as stream:
        stream.write(json.dumps({"status": "FAILED", "failure_type": failure}) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


async def create_bar_generation(
    collector: SpotTradeCollector,
    bus: EventBus,
    store: DuckStore,
    output: Path,
    index: int,
) -> dict[str, object]:
    """Create one fresh immutable generation; a failed attempt is never selected."""
    if type(index) is not int or not 0 <= index <= 255:
        raise ValueError("generation index must be an explicit integer in 0..255")
    output = _path(output)
    if output.name != f"generation-{index:06d}":
        raise ValueError("generation basename must match its explicit index")
    if output.exists():
        raise FileExistsError("bar generation requires a new output directory")
    if not output.parent.is_dir():
        raise FileNotFoundError("bar generation parent must already exist")
    created: list[bool] = []
    try:
        await complete_thread(_new_directory, output, created)
        checkpoint = await save_active_collector_bar_checkpoint(
            collector, bus, store, output / "checkpoint"
        )
        # The unchanged codec has already validated the closed artifact. Verify
        # again before the outer success marker, with no source/collector reads.
        parsed = CheckpointManifest.model_validate_json(_canonical(checkpoint))
        await load_bar_checkpoint(
            output / "checkpoint",
            parsed.as_of_ms,
            collector.config.system.assets,
            collector.config.ingestion.bar_timeframes_s,
            int(collector.config.ingestion.bar_lateness_s * 1000),
            max_events=collector.bar_input_limit,
            expected_manifest_sha256=hashlib.sha256(_canonical(checkpoint) + b"\n").hexdigest(),
        )
        marker, digest = await complete_thread(_seal, output, index, checkpoint)
        return {**marker.model_dump(mode="json"), "generation_sha256": digest}
    except BaseException as exc:
        if created:
            # Drain any staging/publish worker before revoking its late marker.
            # Filesystem failure may still prevent durable failure evidence.
            worker_failure = (
                cancelled_worker_failure(exc) if isinstance(exc, asyncio.CancelledError) else None
            )
            failure_type = type(worker_failure if worker_failure is not None else exc).__name__
            await _cleanup(complete_thread(_failed, output, failure_type))
        raise


def _selected(output: Path, digest: str) -> BarGeneration:
    if {path.name for path in output.iterdir()} != {"checkpoint", "generation.json"}:
        raise IntegrityError("BAR_GENERATION_UNEXPLAINED_OR_INCOMPLETE_FILES")
    raw = _read(_path(output / "generation.json"), MAX_MARKER_BYTES)
    if hashlib.sha256(raw).hexdigest() != digest:
        raise IntegrityError("BAR_GENERATION_SELECTION_DIGEST_MISMATCH")
    marker = BarGeneration.model_validate(json.loads(raw, object_pairs_hook=_pairs))
    if output.name != f"generation-{marker.index:06d}":
        raise IntegrityError("BAR_GENERATION_DIRECTORY_INDEX_MISMATCH")
    checkpoint = _read(_path(output / marker.checkpoint_file), MAX_MARKER_BYTES)
    if (
        len(checkpoint) != marker.checkpoint_bytes
        or hashlib.sha256(checkpoint).hexdigest() != marker.checkpoint_sha256
        or CheckpointManifest.model_validate_json(checkpoint).as_of_ms != marker.as_of_ms
    ):
        raise IntegrityError("BAR_GENERATION_CHECKPOINT_CHANGED")
    return marker


async def load_bar_generation(
    output: Path,
    now_ms: int,
    assets: tuple[str, ...],
    timeframes_s: tuple[int, ...],
    lateness_ms: int,
    *,
    expected_generation_sha256: str,
    max_events: int = DEFAULT_MAX_EVENTS,
    max_books: int = DEFAULT_MAX_BOOKS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> BarCheckpointResume:
    """Fully verify one explicitly pinned generation, without searching/fallback."""
    if type(expected_generation_sha256) is not str or not re.fullmatch(
        r"[0-9a-f]{64}", expected_generation_sha256
    ):
        raise ValueError("selection requires an explicit lowercase SHA256 marker digest")
    output = _path(output)
    marker = await complete_thread(_selected, output, expected_generation_sha256)
    resume = await load_bar_checkpoint(
        output / "checkpoint",
        now_ms,
        assets,
        timeframes_s,
        lateness_ms,
        max_events=max_events,
        max_books=max_books,
        max_bytes=max_bytes,
        expected_manifest_sha256=marker.checkpoint_sha256,
    )
    if resume.as_of_ms != marker.as_of_ms:
        raise IntegrityError("BAR_GENERATION_CHECKPOINT_CUTOFF_MISMATCH")
    if await complete_thread(_selected, output, expected_generation_sha256) != marker:
        raise IntegrityError("BAR_GENERATION_CHANGED_DURING_SELECTION")
    return resume


class PeriodicBarRuntime:
    """One supervised task; unsafe state, exhausted budget or failure halts capture."""

    def __init__(
        self,
        config: AppConfig,
        bus: EventBus,
        store: DuckStore,
        collector: SpotTradeCollector | None,
        output: Path,
        stopped: asyncio.Event,
    ) -> None:
        self.config, self.bus, self.store = config, bus, store
        self.collector, self.output, self.stopped = collector, output, stopped
        self.generations: list[dict[str, object]] = []
        self.attempts = 0
        self.last_attempt: dict[str, object] | None = None
        self.failure_type: str | None = None
        self.enabled = (
            config.ingestion.bar_checkpoint_interval_s is not None and collector is not None
        )
        self.status = "NOT_STARTED" if self.enabled else "DISABLED"

    async def _wait_due(self) -> bool:
        interval = self.config.ingestion.bar_checkpoint_interval_s
        assert interval is not None
        try:
            async with asyncio.timeout(interval):
                await self.stopped.wait()
            return False
        except TimeoutError:
            return not self.stopped.is_set()

    async def run(self) -> None:
        if not self.enabled:
            return
        assert self.collector is not None
        try:
            self.status = "WAITING"
            while await self._wait_due():
                if self.stopped.is_set():
                    break
                if len(self.generations) >= self.config.ingestion.bar_checkpoint_max_generations:
                    raise IntegrityError("BAR_GENERATION_BUDGET_EXHAUSTED")
                index = len(self.generations)
                relative = f"bar-generations/generation-{index:06d}"
                self.attempts += 1
                self.last_attempt = {"directory": relative, "index": index, "status": "IN_PROGRESS"}
                await complete_thread((self.output / "bar-generations").mkdir, exist_ok=True)
                self.status = "CHECKPOINTING"
                report = await create_bar_generation(
                    self.collector, self.bus, self.store, self.output / relative, index
                )
                try:
                    marker = BarGeneration.model_validate(
                        {key: value for key, value in report.items() if key != "generation_sha256"}
                    )
                except ValueError as exc:
                    raise IntegrityError("BAR_GENERATION_RUNTIME_INVALID_COMPLETION") from exc
                digest = report.get("generation_sha256")
                if (
                    marker.index != index
                    or type(digest) is not str
                    or not re.fullmatch(r"[0-9a-f]{64}", digest)
                ):
                    raise IntegrityError("BAR_GENERATION_RUNTIME_INVALID_COMPLETION")
                completed = {"directory": relative, **report}
                self.generations.append(completed)
                self.last_attempt = completed
                self.status = "WAITING"
            self.status = "STOPPED"
        except BaseException as exc:
            worker_failure = (
                cancelled_worker_failure(exc) if isinstance(exc, asyncio.CancelledError) else None
            )
            pure_cancellation = isinstance(exc, asyncio.CancelledError) and worker_failure is None
            failure_type = type(worker_failure if worker_failure is not None else exc).__name__
            if pure_cancellation and self.stopped.is_set():
                self.status = "STOPPED"
            else:
                self.status = "FAILED"
                self.failure_type = failure_type
            if self.last_attempt is not None and self.last_attempt.get("status") == "IN_PROGRESS":
                self.last_attempt = {
                    **self.last_attempt,
                    "status": "CANCELLED" if pure_cancellation else "FAILED",
                    "failure_type": failure_type,
                }
            if worker_failure is not None:
                # complete_thread must retain caller cancellation while draining,
                # but its explicit worker failure cause still wins over a requested
                # stop. Expose a non-cancellation exception to capture supervision.
                raise IntegrityError(
                    "BAR_GENERATION_CANCELLATION_DRAIN_FAILURE"
                ) from worker_failure
            raise

    def summary(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "status": self.status,
            "interval_s": self.config.ingestion.bar_checkpoint_interval_s,
            "max_generations": self.config.ingestion.bar_checkpoint_max_generations,
            "attempts": self.attempts,
            "completed_generations": len(self.generations),
            "generations": [dict(report) for report in self.generations],
            "last_attempt": dict(self.last_attempt) if self.last_attempt is not None else None,
            "failure_type": self.failure_type,
            "scope": SCOPE,
            "history_complete": False,
            "source_frontier_complete": False,
            "current_feed_health_verified": False,
            "delivery_acknowledgements_copied": False,
            "source_database_hash_claimed": False,
            "source_history_modified": False,
            "periodic_crash_recovery": False,
            "ready_for_signals": False,
            "trading_enabled": False,
        }
