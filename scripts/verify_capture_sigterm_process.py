"""No-network finite-capture subprocess fixture for actual POSIX SIGTERM evidence.

The parent test supplies the OS signal after this fixture publishes readiness.
Windows instead proves explicit CLI refusal. This is a transport-mocked process
lifecycle check, not a public collection run or sustained recovery certification.
"""

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import signal
import socket
import sys
from pathlib import Path
from types import FrameType
from typing import Any, cast
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402

from selective_options import cli, ingestion  # noqa: E402
from selective_options.bus import EventBus  # noqa: E402
from selective_options.collectors import DeribitCollector  # noqa: E402
from selective_options.config import AppConfig  # noqa: E402
from selective_options.events import MarketEvent, make_event  # noqa: E402
from selective_options.spot_trades import SpotTradeCollector  # noqa: E402


def write_json(path: Path, value: dict[str, object]) -> None:
    staging = path.with_name(path.name + ".partial")
    with staging.open("x", encoding="utf-8") as destination:
        destination.write(json.dumps(value, indent=2) + "\n")
    staging.rename(path)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture_row(index: int) -> MarketEvent:
    return make_event(
        source="process_lifecycle_fixture",
        environment="public_reference",
        asset="BTC",
        symbol="BTC/USD",
        kind="spot",
        event_time_ms=1000 + index,
        observed_at_ms=2000 + index,
        payload={"price": 100 + index, "fixture_only": True},
    )


def run(evidence: Path) -> int:
    evidence.mkdir(parents=True, exist_ok=False)
    output = evidence / "capture"
    rows = (fixture_row(0), fixture_row(1))
    blocked_attempts = 0
    previous_invocations: list[int] = []
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_resolve = socket.getaddrinfo

    def check_address(host: object) -> None:
        nonlocal blocked_attempts
        try:
            local = ipaddress.ip_address(str(host)).is_loopback
        except ValueError:
            local = False
        # Local loopback is allowed only for Python's event-loop socketpair.
        # No application transport is started; all producer methods are mocked.
        if not local:
            blocked_attempts += 1
            raise RuntimeError("subprocess fixture blocked external network access")

    def connect(sock: socket.socket, address: Any) -> None:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            check_address(address[0])
        original_connect(sock, address)

    def connect_ex(sock: socket.socket, address: Any) -> int:
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            check_address(address[0])
        return original_connect_ex(sock, address)

    def resolve(host: Any, *args: Any, **kwargs: Any) -> Any:
        check_address(host)
        return original_resolve(host, *args, **kwargs)

    def previous_handler(signum: int, frame: FrameType | None) -> None:
        previous_invocations.append(signum)

    async def idle(*args: object) -> None:
        await asyncio.Event().wait()

    async def produce(collector: DeribitCollector) -> None:
        try:
            await collector.sink(rows[0])
            # Readiness proves the actual consumer committed the initial row.
            bus = cast(EventBus, cast(Any, collector.sink).__self__)
            await bus.flush()
            write_json(
                evidence / "ready.json",
                {"pid": os.getpid(), "first_committed_event_id": rows[0].event_id},
            )
            await idle()
        finally:
            # Capture must retain its writer while producer cleanup publishes.
            await collector.sink(rows[1])
            write_json(evidence / "producer-stopped.json", {"tail_event_id": rows[1].event_id})

    cfg = AppConfig(
        system={"assets": ["BTC"]},
        ingestion={
            "reference_sources": [],
            "derivatives_enabled": False,
            "spot_trades_enabled": False,
            "policy_news_sources": [],
            "flush_interval_s": 0.001,
        },
    )
    original_handler = signal.getsignal(signal.SIGTERM)
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, previous_handler)
    summary: dict[str, object] = {
        "platform": sys.platform,
        "scope": "Finite child process; actual capture, mocked producers and no external network",
        "expected_event_ids": [row.event_id for row in rows],
        "orders_submitted": 0,
        "trading_enabled": False,
        "actual_sigterm_expected": os.name == "posix",
    }
    try:
        with (
            patch.object(DeribitCollector, "run", produce),
            patch.object(ingestion, "poll_chains", idle),
            patch.object(SpotTradeCollector, "run", idle),
            patch.object(cli, "load_config", return_value=cfg),
            patch.object(socket.socket, "connect", connect),
            patch.object(socket.socket, "connect_ex", connect_ex),
            patch.object(socket, "getaddrinfo", resolve),
            patch.object(
                sys,
                "argv",
                [
                    "selective-options",
                    "capture",
                    "--output",
                    str(output),
                    "--duration",
                    "60",
                    "--graceful-stop-signals",
                ],
            ),
        ):
            result = cli.main()
        restored = signal.getsignal(signal.SIGTERM) is previous_handler
        summary["capture_exit_code"] = result
        summary["previous_sigterm_handler_restored"] = restored
        summary["sigint_handler_preserved"] = signal.getsignal(signal.SIGINT) is original_sigint
        summary["previous_handler_invocations"] = len(previous_invocations)
        summary["blocked_external_network_attempts"] = blocked_attempts
        assert restored and not previous_invocations and blocked_attempts == 0
        assert summary["sigint_handler_preserved"] is True
        if os.name != "posix":
            assert result == 2 and not output.exists() and not (evidence / "ready.json").exists()
            summary["status"] = "UNSUPPORTED_PLATFORM_REFUSED"
        else:
            assert result == 0
            report = json.loads((output / "report.json").read_text(encoding="utf-8"))
            assert report["status"] == "CAPTURE_FINISHED" and report["rows"] == 2
            assert report["capture_stop"]["reason"] == "STOP_REQUESTED"
            assert report["capture_stop"]["request_reason"] == "SIGTERM"
            assert report["capture_stop"]["signal_handling"] == "SCOPED_POSIX_SIGTERM"
            assert report["redis_delivery_pending"] == 2
            assert report["orders_submitted"] == 0 and report["trading_enabled"] is False
            expected_ids = {row.event_id for row in rows}
            with duckdb.connect(str(output / "events.duckdb"), read_only=True) as database:
                actual_ids = {
                    row[0] for row in database.execute("SELECT event_id FROM events").fetchall()
                }
                assert actual_ids == expected_ids
                assert database.execute(
                    "SELECT count(*) FROM delivery_outbox WHERE delivered"
                ).fetchone() == (0,)
                archived_ids = {
                    row[0]
                    for row in database.execute(
                        "SELECT event_id FROM read_parquet(?)", [str(output / "events.parquet")]
                    ).fetchall()
                }
                assert archived_ids == expected_ids
            manifest = json.loads(
                (output / "events.parquet.manifest.json").read_text(encoding="utf-8")
            )
            assert manifest["rows"] == 2 and manifest["sha256"] == digest(output / "events.parquet")
            assert (evidence / "producer-stopped.json").is_file()
            summary["status"] = "ACTUAL_POSIX_SIGTERM_CAPTURE_VERIFIED"
            summary["report_sha256"] = digest(output / "report.json")
            summary["parquet_sha256"] = manifest["sha256"]
            summary["duckdb_sha256"] = digest(output / "events.duckdb")
            summary["durable_rows"] = 2
            summary["producer_cleanup_tail_drained"] = True
        return 0
    except BaseException as exc:
        summary["status"] = "FAILED"
        summary["failure_type"] = type(exc).__name__
        return 1
    finally:
        signal.signal(signal.SIGTERM, original_handler)
        write_json(evidence / "verification.json", summary)
        print(
            json.dumps({"sigterm_process_status": summary["status"], "platform": sys.platform}),
            flush=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    return run(args.evidence.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
