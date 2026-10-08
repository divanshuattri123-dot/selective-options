"""Portable, fail-closed offline validation with an explicit historical test scope.

No market commands run here. Missing evidence, stale exclusions, unexpected skips,
test failures and build failures all produce a nonzero exit and a retained report.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
SCOPE = ROOT / "ci/cloud-test-scope.json"


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_scope(path: Path, system: str) -> tuple[dict[str, Any], dict[str, str]]:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise ValueError("unsupported cloud scope schema")
    exclusions: dict[str, str] = {}
    seen: set[str] = set()
    for item in document["exclusions"]:
        nodeid, reason = item["nodeid"], item["reason"]
        if (
            not isinstance(nodeid, str)
            or not nodeid.startswith("tests/")
            or "::" not in nodeid
            or any(value in nodeid for value in ("*", "?", "..", "\\"))
            or nodeid in seen
            or not isinstance(reason, str)
            or not reason.strip()
        ):
            raise ValueError("cloud scope needs unique exact test nodeids and reasons")
        seen.add(nodeid)
        if system in item["platforms"]:
            exclusions[nodeid] = reason
    return document, exclusions


def evidence_prerequisites(root: Path) -> dict[str, Any]:
    manifest_path = root / "GITHUB_ARTIFACTS.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    issues: list[dict[str, str]] = []
    records = [(root / item["path"], item) for item in manifest["evidence_files"]]
    records.extend(
        (root.parent / item["name"], item)
        for item in manifest["assets"]
        if item["name"] in manifest["validation_assets"]
    )
    for path, item in records:
        label = item.get("path", item.get("name", ""))
        if not path.is_file():
            issues.append({"path": label, "reason": "MISSING"})
        elif path.stat().st_size != item["bytes"] or digest(path) != item["sha256"]:
            issues.append({"path": label, "reason": "CHECKSUM_OR_SIZE_MISMATCH"})
    return {
        "manifest_sha256": digest(manifest_path),
        "required_files": len(records),
        "verified_files": len(records) - len(issues),
        "issues": issues,
        "restore_command": "python tools/restore_github_evidence.py",
    }


def safe_environment() -> dict[str, str]:
    result = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DERIBIT_", "SELECTIVE_"))
        and key
        not in {
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "PYTEST_ADDOPTS",
            "PYTEST_PLUGINS",
            "PYTHONPATH",
            "PYTHONHOME",
        }
    }
    result["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    return result


def loopback_host(host: Any) -> bool:
    if isinstance(host, bytes):
        host = host.decode("ascii")
    if host is None or host in {"localhost", ""}:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class CloudScopePlugin:
    """Select exact cases and block external socket/DNS use in the test process."""

    def __init__(self, exclusions: dict[str, str], report: Path) -> None:
        self.exclusions = exclusions
        self.report = report
        self.selection: dict[str, Any] = {
            "collected_count": 0,
            "selected_nodeids": [],
            "excluded": [],
            "unapproved_skips": [],
            "blocked_network_attempts": [],
        }
        self.originals: list[tuple[Any, str, Any]] = []

    def pytest_sessionstart(self, session: Any) -> None:
        def check(host: Any) -> None:
            if not loopback_host(host):
                self.selection["blocked_network_attempts"].append(str(host))
                raise RuntimeError("external network is disabled during cloud offline tests")

        connect, connect_ex, getaddrinfo = (
            socket.socket.connect,
            socket.socket.connect_ex,
            socket.getaddrinfo,
        )

        def guarded_connect(sock: Any, address: Any) -> Any:
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                check(address[0])
            return connect(sock, address)

        def guarded_connect_ex(sock: Any, address: Any) -> Any:
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                check(address[0])
            return connect_ex(sock, address)

        def guarded_getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
            check(host)
            return getaddrinfo(host, *args, **kwargs)

        for owner, name, replacement in (
            (socket.socket, "connect", guarded_connect),
            (socket.socket, "connect_ex", guarded_connect_ex),
            (socket, "getaddrinfo", guarded_getaddrinfo),
        ):
            self.originals.append((owner, name, getattr(owner, name)))
            setattr(owner, name, replacement)

    def pytest_collection_modifyitems(self, session: Any, config: Any, items: list[Any]) -> None:
        import pytest

        self.selection["collected_count"] = len(items)
        present = {item.nodeid for item in items}
        unknown = sorted(set(self.exclusions) - present)
        if unknown:
            self.selection["unknown_exclusions"] = unknown
            raise pytest.UsageError(
                "cloud scope contains uncollected exclusions: " + ", ".join(unknown)
            )
        kept, removed = [], []
        for item in items:
            if item.nodeid in self.exclusions:
                removed.append(item)
                self.selection["excluded"].append(
                    {"nodeid": item.nodeid, "reason": self.exclusions[item.nodeid]}
                )
            else:
                kept.append(item)
        items[:] = kept
        self.selection["selected_nodeids"] = [item.nodeid for item in kept]
        config.hook.pytest_deselected(items=removed)

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.skipped:
            self.selection["unapproved_skips"].append(
                {"nodeid": report.nodeid, "reason": str(report.longrepr)}
            )

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        self.selection["pytest_exit_code"] = int(exitstatus)
        self.report.write_text(json.dumps(self.selection, indent=2) + "\n", encoding="utf-8")
        for owner, name, original in reversed(self.originals):
            setattr(owner, name, original)


def junit_counts(path: Path) -> dict[str, int]:
    document = ElementTree.parse(path).getroot()
    suites = [document] if document.tag == "testsuite" else list(document.findall("testsuite"))
    return {
        name: sum(int(suite.attrib.get(name, "0")) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }


def run_step(
    name: str, command: list[str], root: Path, output: Path, env: dict[str, str]
) -> dict[str, Any]:
    print(f"CLOUD_CHECK {name}", flush=True)
    started = time.monotonic()
    log = output / f"{name}.log"
    try:
        with log.open("w", encoding="utf-8") as stream:
            completed = subprocess.run(
                command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT, check=False
            )
        code = completed.returncode
    except OSError as exc:
        log.write_text(f"Unable to execute step: {type(exc).__name__}\n", encoding="utf-8")
        code = 127
    return {
        "name": name,
        "command": command,
        "exit_code": code,
        "elapsed_s": round(time.monotonic() - started, 3),
        "log": log.name,
    }


def generated_documents(root: Path, output: Path, env: dict[str, str]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="cloud-docs-") as temporary:
        scratch = Path(temporary)
        shutil.copyfile(root / "README.md", scratch / "README.md")
        result = run_step(
            "generated-documents",
            [sys.executable, str(root / "scripts/render_config.py")],
            scratch,
            output,
            env,
        )
        if result["exit_code"] == 0:
            changed = [
                name
                for name in ("README.md", "PARAMETERS.md", "config/default.yaml")
                if (root / name).read_text(encoding="utf-8")
                != (scratch / name).read_text(encoding="utf-8")
            ]
            result["changed_documents"] = changed
            result["exit_code"] = 1 if changed else 0
        return result


def wheel_modules(root: Path, wheel: Path) -> dict[str, Any]:
    modules = {
        path.relative_to(root / "src").as_posix(): digest(path)
        for path in sorted((root / "src").rglob("*.py"))
    }
    with zipfile.ZipFile(wheel) as archive:
        packaged = {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in archive.namelist()
            if name.startswith("selective_options/") and name.endswith(".py")
        }
    if not modules or packaged != modules:
        raise ValueError("wheel Python modules differ from the current source")
    version = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "version"
    ]
    return {
        "module_count": len(modules),
        "module_inventory_sha256": hashlib.sha256(
            json.dumps(modules, sort_keys=True).encode()
        ).hexdigest(),
        "source_version": version,
        "modules_match_current_source": True,
    }


def validate(root: Path, output: Path, scope_path: Path) -> int:
    output.mkdir(parents=True, exist_ok=False)
    env = safe_environment()

    def git_result(arguments: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ["git", *arguments], cwd=root, capture_output=True, text=True, check=False
            )
        except OSError:
            return subprocess.CompletedProcess(["git", *arguments], 127, "", "")

    commit = git_result(["rev-parse", "HEAD"])
    dirty = git_result(["status", "--porcelain"])
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "FAILED",
        "started_at_utc": datetime.now(UTC).isoformat(),
        "git_commit": commit.stdout.strip() if commit.returncode == 0 else None,
        "working_tree_dirty": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None,
        "git_provenance": {
            "commit_exit_code": commit.returncode,
            "status_exit_code": dirty.returncode,
        },
        "platform": platform.platform(),
        "sys_platform": sys.platform,
        "python_version": platform.python_version(),
        "scope": "portable offline engineering checks; full historical Windows validation is separate",
        "trading_enabled": False,
        "network_guard_scope": "external TCP connects and DNS in the pytest worker process; child processes and build/install traffic are outside this guard",
        "steps": [],
    }
    exit_code = 1
    try:
        if (
            commit.returncode != 0
            or dirty.returncode != 0
            or re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", commit.stdout.strip()) is None
            or report["working_tree_dirty"]
        ):
            report["status"] = "BLOCKED_UNCOMMITTED_OR_UNAVAILABLE_PROVENANCE"
            exit_code = 2
            return 2
        scope, _ = read_scope(scope_path, sys.platform)
        report["scope_manifest_sha256"] = digest(scope_path)
        report["historical_authority"] = scope["historical_authority"]
        report["dependency_locks"] = {
            name: digest(root / name) for name in ("requirements.lock", "ci/requirements-dev.lock")
        }
        report["prerequisites"] = evidence_prerequisites(root)
        if report["prerequisites"]["issues"]:
            report["status"] = "BLOCKED_MISSING_OR_CHANGED_EVIDENCE"
            exit_code = 2
            return 2
        commands = [
            (
                "lint",
                [
                    sys.executable,
                    "-m",
                    "ruff",
                    "check",
                    "src",
                    "scripts",
                    "tests",
                    "tools",
                    "migration-tests",
                ],
            ),
            (
                "format",
                [
                    sys.executable,
                    "-m",
                    "ruff",
                    "format",
                    "--check",
                    "src",
                    "scripts",
                    "tests",
                    "tools",
                    "migration-tests/test_cloud_validate.py",
                    "migration-tests/test_ci_junit.py",
                ],
            ),
            ("types", [sys.executable, "-m", "mypy"]),
            (
                "offline-tests",
                [
                    sys.executable,
                    "-m",
                    "tools.cloud_validate",
                    "--pytest-worker",
                    "--scope",
                    str(scope_path),
                    "--output",
                    str(output),
                ],
            ),
            (
                "wheel-build",
                [sys.executable, "-m", "build", "--wheel", "--outdir", str(output / "dist")],
            ),
        ]
        report["steps"].append(generated_documents(root, output, env))
        for name, command in commands:
            report["steps"].append(run_step(name, command, root, output, env))
        selection_path = output / "test-selection.json"
        if selection_path.is_file():
            report["test_selection"] = json.loads(selection_path.read_text(encoding="utf-8"))
        junit = output / "tests.xml"
        if junit.is_file():
            report["test_counts"] = junit_counts(junit)
        wheels = sorted((output / "dist").glob("*.whl"))
        if len(wheels) == 1:
            wheel_verification = wheel_modules(root, wheels[0])
            with tempfile.TemporaryDirectory(prefix="cloud-wheel-") as temporary:
                smoke = Path(temporary)
                step = run_step(
                    "wheel-environment",
                    [sys.executable, "-m", "venv", str(smoke)],
                    root,
                    output,
                    env,
                )
                report["steps"].append(step)
                python = smoke / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
                if step["exit_code"] == 0:
                    report["steps"].append(
                        run_step(
                            "wheel-runtime-install",
                            [
                                str(python),
                                "-m",
                                "pip",
                                "install",
                                "-r",
                                str(root / "requirements.lock"),
                            ],
                            smoke,
                            output,
                            env,
                        )
                    )
                    report["steps"].append(
                        run_step(
                            "wheel-install",
                            [
                                str(python),
                                "-m",
                                "pip",
                                "install",
                                "--no-index",
                                "--no-deps",
                                "--force-reinstall",
                                str(wheels[0]),
                            ],
                            root,
                            output,
                            env,
                        )
                    )
                    clean_env = dict(env)
                    clean_env.pop("PYTHONPATH", None)
                    import_check = (
                        "import json,sys; from pathlib import Path; import selective_options; "
                        "module=Path(selective_options.__file__).resolve(); "
                        "assert module.is_relative_to(Path(sys.prefix).resolve()), 'wheel import escaped isolated venv'; "
                        f"assert selective_options.__version__ == {wheel_verification['source_version']!r}; "
                        "print(json.dumps({'module_path': str(module), 'version': selective_options.__version__, "
                        "'inside_wheel_environment': True}))"
                    )
                    report["steps"].append(
                        run_step(
                            "wheel-import-smoke",
                            [str(python), "-c", import_check],
                            smoke,
                            output,
                            clean_env,
                        )
                    )
                    report["steps"].append(
                        run_step(
                            "wheel-config-smoke",
                            [
                                str(python),
                                "-m",
                                "selective_options.cli",
                                "--config-dir",
                                str(root / "config"),
                                "check-config",
                            ],
                            smoke,
                            output,
                            clean_env,
                        )
                    )
                    smoke_log = output / "wheel-config-smoke.log"
                    smoke_result = json.loads(smoke_log.read_text(encoding="utf-8"))
                    if (
                        smoke_result.get("config") != "VALID"
                        or smoke_result.get("trading_enabled") is not False
                    ):
                        raise ValueError(
                            "installed wheel config smoke did not establish safe valid configuration"
                        )
                    report["wheel"] = {
                        "name": wheels[0].name,
                        "sha256": digest(wheels[0]),
                        **wheel_verification,
                        "import_smoke": json.loads(
                            (output / "wheel-import-smoke.log").read_text(encoding="utf-8")
                        ),
                        "config_smoke": smoke_result,
                    }
        selection = report.get("test_selection", {})
        counts = report.get("test_counts", {})
        success = (
            all(step["exit_code"] == 0 for step in report["steps"])
            and bool(report.get("wheel"))
            and counts.get("tests", 0) > 0
            and not any(counts.get(name, 0) for name in ("failures", "errors", "skipped"))
            and not selection.get("unapproved_skips")
            and not selection.get("blocked_network_attempts")
            and not selection.get("unknown_exclusions")
            and counts["tests"] == len(selection.get("selected_nodeids", []))
        )
        report["status"] = "PASSED" if success else "FAILED"
        exit_code = 0 if success else 1
        return exit_code
    except Exception as exc:
        report["error_type"] = type(exc).__name__
        return 1
    finally:
        report["exit_code"] = exit_code
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": report["status"], "report": str(output / "summary.json")}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "state/cloud-validation")
    parser.add_argument("--scope", type=Path, default=SCOPE)
    parser.add_argument("--pytest-worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    output = args.output.resolve()
    scope_path = args.scope.resolve()
    if args.pytest_worker:
        import pytest

        import selective_options

        package_origin = Path(selective_options.__file__).resolve()
        if not package_origin.is_relative_to(ROOT / "src"):
            raise ValueError("pytest worker package does not come from this checkout")

        _, exclusions = read_scope(scope_path, sys.platform)
        return int(
            pytest.main(
                ["tests", "migration-tests", "--junitxml=" + str(output / "tests.xml")],
                plugins=[CloudScopePlugin(exclusions, output / "test-selection.json")],
            )
        )
    return validate(ROOT, output, scope_path)


if __name__ == "__main__":
    raise SystemExit(main())
