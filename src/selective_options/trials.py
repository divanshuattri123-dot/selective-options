"""Bounded append-only research trial journal; unresolved starts remain counted.

Hashes detect accidental corruption; they are not signatures or tamper-proof audit
storage. An exclusive sidecar lock serializes cooperating writers. A stale lock
after process death requires explicit operator inspection, never automatic removal.
"""

import hashlib
import json
import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictInt, StrictStr

from .errors import IntegrityError

MAX_BYTES = 32 * 1024 * 1024
MAX_RECORDS = 100_000
MAX_RECORD_BYTES = 256 * 1024
ZERO_HASH = "0" * 64
T = TypeVar("T")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class TrialRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    schema_version: StrictInt = Field(default=1, ge=1, le=1)
    sequence: StrictInt = Field(gt=0)
    trial_id: StrictStr = Field(pattern=r"^[0-9a-f]{32}$")
    phase: Literal["STARTED", "COMPLETED", "FAILED"]
    recorded_at_ms: StrictInt = Field(gt=0)
    parameters_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    previous_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    record_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    parameters: dict[str, JsonValue] | None = None
    provenance: dict[str, JsonValue] | None = None
    result: dict[str, JsonValue] | None = None
    failure_type: StrictStr | None = Field(default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,99}$")


class TrialHandle(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    trial_id: StrictStr = Field(pattern=r"^[0-9a-f]{32}$")
    parameters_sha256: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")


def _safe_path(path: Path) -> Path:
    # Check ancestors before resolve, which would hide an existing symlink.
    absolute = path.absolute()
    for part in (absolute, *absolute.parents):
        if part.is_symlink() or (
            part.exists() and getattr(part.lstat(), "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("trial journal symlink/reparse/junction paths are refused")
    if absolute.exists() and not absolute.is_file():
        raise ValueError("trial journal must be a regular file")
    return absolute.resolve()


def _validate(records: tuple[TrialRecord, ...]) -> None:
    previous = ZERO_HASH
    started: dict[str, TrialRecord] = {}
    ended: set[str] = set()
    clock = 0
    for index, record in enumerate(records, 1):
        data = record.model_dump(mode="json", exclude={"record_sha256"})
        if (
            record.sequence != index
            or record.previous_sha256 != previous
            or hashlib.sha256(_canonical(data)).hexdigest() != record.record_sha256
            or record.recorded_at_ms < clock
        ):
            raise IntegrityError("TRIAL_JOURNAL_CHAIN")
        if record.phase == "STARTED":
            if (
                record.trial_id in started
                or record.parameters is None
                or record.provenance is None
                or record.result is not None
                or record.failure_type is not None
                or hashlib.sha256(_canonical(record.parameters)).hexdigest()
                != record.parameters_sha256
            ):
                raise IntegrityError("TRIAL_JOURNAL_START")
            started[record.trial_id] = record
        else:
            start = started.get(record.trial_id)
            if (
                start is None
                or record.trial_id in ended
                or start.parameters_sha256 != record.parameters_sha256
                or record.parameters is not None
                or record.provenance is not None
                or (record.phase == "COMPLETED" and (record.result is None or record.failure_type))
                or (
                    record.phase == "FAILED"
                    and (record.result is not None or not record.failure_type)
                )
            ):
                raise IntegrityError("TRIAL_JOURNAL_FINISH")
            ended.add(record.trial_id)
        previous = record.record_sha256
        clock = record.recorded_at_ms


def read_trials(path: Path) -> tuple[TrialRecord, ...]:
    """Refuse corruption, incomplete trailing records and finite read overflow."""
    path = _safe_path(path)
    if not path.exists():
        return ()
    if path.stat().st_size > MAX_BYTES:
        raise IntegrityError("TRIAL_JOURNAL_BYTE_BUDGET")
    with path.open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise IntegrityError("TRIAL_JOURNAL_BYTE_BUDGET")
    if data and not data.endswith(b"\n"):
        raise IntegrityError("TRIAL_JOURNAL_INCOMPLETE_RECORD")
    lines = data.splitlines()
    if len(lines) > MAX_RECORDS:
        raise IntegrityError("TRIAL_JOURNAL_RECORD_BUDGET")
    if any(not line or len(line) > MAX_RECORD_BYTES for line in lines):
        raise IntegrityError("TRIAL_JOURNAL_RECORD_BUDGET")
    records = tuple(TrialRecord.model_validate_json(line) for line in lines)
    if any(
        _canonical(record.model_dump(mode="json")) != line
        for record, line in zip(records, lines, strict=True)
    ):
        raise IntegrityError("TRIAL_JOURNAL_NONCANONICAL_RECORD")
    _validate(records)
    return records


@contextmanager
def _exclusive(path: Path) -> Iterator[None]:
    lock = _safe_path(path.with_name(path.name + ".lock"))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise IntegrityError("TRIAL_JOURNAL_LOCKED") from exc
    try:
        os.close(descriptor)
        yield
    finally:
        lock.unlink()


def _append(path: Path, fields: dict[str, JsonValue]) -> TrialRecord:
    path = _safe_path(path)
    with _exclusive(path):
        records = read_trials(path)
        if len(records) >= MAX_RECORDS:
            raise IntegrityError("TRIAL_JOURNAL_RECORD_BUDGET")
        data = {
            "schema_version": 1,
            "sequence": len(records) + 1,
            "recorded_at_ms": int(datetime.now(UTC).timestamp() * 1000),
            "previous_sha256": records[-1].record_sha256 if records else ZERO_HASH,
            "parameters": None,
            "provenance": None,
            "result": None,
            "failure_type": None,
            **fields,
        }
        record = TrialRecord.model_validate_json(
            _canonical(data | {"record_sha256": hashlib.sha256(_canonical(data)).hexdigest()})
        )
        _validate((*records, record))
        encoded = _canonical(record.model_dump(mode="json")) + b"\n"
        if len(encoded) > MAX_RECORD_BYTES:
            raise IntegrityError("TRIAL_JOURNAL_RECORD_BUDGET")
        if (path.stat().st_size if path.exists() else 0) + len(encoded) > MAX_BYTES:
            raise IntegrityError("TRIAL_JOURNAL_BYTE_BUDGET")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            # A crash or short write can leave a partial line. Readers refuse it;
            # no retry/truncate can erase evidence of an uncertain append.
            if os.write(descriptor, encoded) != len(encoded):
                raise IntegrityError("TRIAL_JOURNAL_SHORT_WRITE")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return record


def start_trial(
    path: Path, parameters: dict[str, JsonValue], provenance: dict[str, JsonValue]
) -> TrialHandle:
    record = _append(
        path,
        {
            "trial_id": uuid.uuid4().hex,
            "phase": "STARTED",
            "parameters_sha256": hashlib.sha256(_canonical(parameters)).hexdigest(),
            "parameters": parameters,
            "provenance": provenance,
        },
    )
    return TrialHandle(trial_id=record.trial_id, parameters_sha256=record.parameters_sha256)


def finish_trial(path: Path, handle: TrialHandle, result: dict[str, JsonValue]) -> TrialRecord:
    return _append(
        path,
        {
            "trial_id": handle.trial_id,
            "parameters_sha256": handle.parameters_sha256,
            "phase": "COMPLETED",
            "result": result,
        },
    )


def fail_trial(path: Path, handle: TrialHandle, failure_type: str) -> TrialRecord:
    """Record a fixed exception type, never an arbitrary exception/secret payload."""
    return _append(
        path,
        {
            "trial_id": handle.trial_id,
            "parameters_sha256": handle.parameters_sha256,
            "phase": "FAILED",
            "failure_type": failure_type,
        },
    )


def run_registered(
    path: Path,
    parameters: dict[str, JsonValue],
    provenance: dict[str, JsonValue],
    runner: Callable[[], T],
    summarize: Callable[[T], dict[str, JsonValue]],
) -> tuple[TrialHandle, T]:
    """Persist STARTED before computation; failures and interrupted runs count."""
    handle = start_trial(path, parameters, provenance)
    try:
        result = runner()
        summary = summarize(result)
    except BaseException as exc:
        try:
            fail_trial(path, handle, type(exc).__name__)
        except BaseException as journal_failure:
            raise IntegrityError("TRIAL_FAILURE_RECORD_UNCERTAIN") from journal_failure
        raise
    finish_trial(path, handle, summary)
    return handle, result


def trial_counts(path: Path) -> dict[str, int]:
    records = read_trials(path)
    starts = sum(row.phase == "STARTED" for row in records)
    completed = sum(row.phase == "COMPLETED" for row in records)
    failed = sum(row.phase == "FAILED" for row in records)
    return {
        "started": starts,
        "completed": completed,
        "failed": failed,
        "unresolved": starts - completed - failed,
        "trial_count_N": starts,
    }
