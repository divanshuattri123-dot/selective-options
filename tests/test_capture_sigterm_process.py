"""Actual POSIX child-process SIGTERM; explicit unsupported branch, never skips."""

import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/verify_capture_sigterm_process.py"


def isolated_environment():
    # Allow only runtime plumbing. Provider, GitHub, Python import/pytest/plugin
    # variables and credential-bearing application variables never reach child.
    allowed = {
        "SYSTEMROOT",
        "WINDIR",
        "SYSTEMDRIVE",
        "PATH",
        "PATHEXT",
        "TEMP",
        "TMP",
        "TMPDIR",
        "COMSPEC",
        "LANG",
        "LC_ALL",
        "TZ",
    }
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def retain_evidence(temporary, summary):
    cloud_output = ROOT / "state/cloud-validation"
    parent = cloud_output if cloud_output.is_dir() else ROOT / "state"
    destination = parent / "capture-sigterm-process" / uuid4().hex
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(temporary, destination)
    files = {
        file.relative_to(destination).as_posix(): {
            "bytes": file.stat().st_size,
            "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
        }
        for file in sorted(destination.rglob("*"))
        if file.is_file()
    }
    (destination / "parent-verification.json").write_text(
        json.dumps({**summary, "files": files}, indent=2) + "\n", encoding="utf-8"
    )
    return destination


def test_actual_sigterm_process_or_explicit_unsupported_refusal(tmp_path):
    temporary = tmp_path / "process-evidence"
    temporary.mkdir()
    child_evidence = temporary / "child"
    summary = {"platform": sys.platform, "actual_signal_sent": False, "status": "IN_PROGRESS"}
    process = None
    try:
        with (
            (temporary / "stdout.log").open("wb") as stdout,
            (temporary / "stderr.log").open("wb") as stderr,
        ):
            process = subprocess.Popen(
                [sys.executable, "-I", str(HELPER), "--evidence", str(child_evidence)],
                cwd=ROOT,
                env=isolated_environment(),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
            )
            if os.name == "posix":
                ready = child_evidence / "ready.json"
                deadline = time.monotonic() + 20
                while (
                    not ready.is_file() and process.poll() is None and time.monotonic() < deadline
                ):
                    time.sleep(0.02)
                assert ready.is_file(), "child did not publish committed readiness within 20s"
                marker = json.loads(ready.read_text(encoding="utf-8"))
                assert marker["pid"] == process.pid
                assert process.poll() is None
                os.kill(process.pid, signal.SIGTERM)
                summary["actual_signal_sent"] = True
            assert process.wait(timeout=20) == 0, "child fixture verification failed"
        verification = json.loads(
            (child_evidence / "verification.json").read_text(encoding="utf-8")
        )
        assert verification["previous_sigterm_handler_restored"] is True
        assert verification["sigint_handler_preserved"] is True
        assert verification["blocked_external_network_attempts"] == 0
        if os.name == "posix":
            assert verification["status"] == "ACTUAL_POSIX_SIGTERM_CAPTURE_VERIFIED"
            assert verification["capture_exit_code"] == 0
            assert verification["durable_rows"] == 2
            assert verification["producer_cleanup_tail_drained"] is True
            report = json.loads(
                (child_evidence / "capture/report.json").read_text(encoding="utf-8")
            )
            assert report["status"] == "CAPTURE_FINISHED"
            assert report["capture_stop"]["request_reason"] == "SIGTERM"
            assert report["orders_submitted"] == 0 and report["trading_enabled"] is False
        else:
            assert verification["status"] == "UNSUPPORTED_PLATFORM_REFUSED"
            assert verification["capture_exit_code"] == 2
            assert not (child_evidence / "capture").exists()
            assert summary["actual_signal_sent"] is False
        summary["status"] = verification["status"]
        summary["child_exit_code"] = process.returncode
    except BaseException as exc:
        summary["status"] = "FAILED"
        summary["failure_type"] = type(exc).__name__
        raise
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        destination = retain_evidence(temporary, summary)
        print(
            json.dumps({"sigterm_process_status": summary["status"], "evidence": str(destination)})
        )
