"""Explicit finite-capture stop requests and scoped POSIX SIGTERM handling.

This does not implement an unattended service, forced-termination recovery or a
trading kill switch. SIGINT and task cancellation retain the capture failure path.
"""

import asyncio
import os
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType
from typing import Literal

from .errors import IntegrityError

StopReason = Literal["OPERATOR", "SIGTERM"]


def cancelled_worker_failure(error: asyncio.CancelledError) -> BaseException | None:
    """Retain an explicit drained worker error hidden by caller cancellation.

    Pure cancellation has no worker failure. Thread-drain helpers preserve a
    worker's error as an explicit cause; gather(return_exceptions=True) on a
    cancelled Task can discard that cause, so classify inside its direct await.
    """
    current: BaseException | None = error
    visited: set[int] = set()
    while isinstance(current, asyncio.CancelledError):
        if id(current) in visited:
            return IntegrityError("CAPTURE_CANCELLATION_CAUSE_CYCLE")
        visited.add(id(current))
        current = current.__cause__
    return current


def _supports_sigterm() -> bool:
    return os.name == "posix"


class CaptureStopRequest:
    """One-use request owned by a capture's event loop; the first reason wins."""

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._used = False
        self._reason: StopReason | None = None
        self.signal_handling = "NOT_REQUESTED"

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> StopReason | None:
        return self._reason

    def bind(self) -> None:
        if self._used:
            raise ValueError("capture stop request cannot be reused")
        if self.requested:
            raise ValueError("capture stop request was already requested before startup")
        self._loop = asyncio.get_running_loop()
        self._used = True

    def request(self, reason: StopReason = "OPERATOR") -> None:
        if reason not in ("OPERATOR", "SIGTERM"):
            raise ValueError("unsupported capture stop reason")
        if self._loop is not None and asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("capture stop request belongs to another event loop")
        if not self.requested:
            self._reason = reason
            self._event.set()

    async def wait(self) -> None:
        await self._event.wait()


@contextmanager
def scoped_sigterm_stop(request: CaptureStopRequest) -> Iterator[None]:
    """Install only for the current CLI capture and restore on every exit.

    Windows external termination does not deliver a catchable POSIX SIGTERM.
    Refuse unsupported requested guarantees before capture creates any output.
    """
    if not _supports_sigterm():
        raise ValueError("graceful signal stop requires POSIX SIGTERM; unsupported on Windows")
    if threading.current_thread() is not threading.main_thread():
        raise ValueError("graceful signal stop requires the main thread")
    loop = asyncio.get_running_loop()
    previous = signal.getsignal(signal.SIGTERM)

    def stop_handler(signum: int, frame: FrameType | None) -> None:
        # Python handlers run on the main thread; enqueue rather than mutate
        # asyncio state in the handler. Repeated requests never cancel cleanup.
        loop.call_soon_threadsafe(request.request, "SIGTERM")

    signal.signal(signal.SIGTERM, stop_handler)
    request.signal_handling = "SCOPED_POSIX_SIGTERM"
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)
