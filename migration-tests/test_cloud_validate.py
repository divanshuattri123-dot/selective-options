"""Cloud checks must retain honest scope and fail for missing evidence or failed checks."""

import importlib.util
import json
import socket
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "tools/cloud_validate.py"
SPEC = importlib.util.spec_from_file_location("cloud_validate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
cloud = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cloud)


def scope_file(tmp_path, exclusions=None):
    path = tmp_path / "scope.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "historical_authority": ".github/workflows/ci.yaml",
                "exclusions": exclusions or [],
            }
        ),
        encoding="utf-8",
    )
    return path


def exclusion(nodeid="tests/test_proof.py::test_case"):
    return {"nodeid": nodeid, "platforms": ["linux"], "reason": "Immutable Windows path"}


def test_scope_is_exact_and_platform_specific(tmp_path):
    path = scope_file(tmp_path, [exclusion()])
    _, linux = cloud.read_scope(path, "linux")
    _, windows = cloud.read_scope(path, "win32")
    assert linux == {"tests/test_proof.py::test_case": "Immutable Windows path"}
    assert windows == {}


@pytest.mark.parametrize("nodeid", ["tests/*.py::test_case", "tests/test.py", "../tests/a.py::b"])
def test_scope_refuses_broad_or_unsafe_exclusions(tmp_path, nodeid):
    with pytest.raises(ValueError, match="unique exact"):
        cloud.read_scope(scope_file(tmp_path, [exclusion(nodeid)]), "linux")


def test_scope_refuses_duplicate_exclusions(tmp_path):
    with pytest.raises(ValueError, match="unique exact"):
        cloud.read_scope(scope_file(tmp_path, [exclusion(), exclusion()]), "linux")


def test_collection_reports_every_selected_and_excluded_case(tmp_path):
    removed = []
    plugin = cloud.CloudScopePlugin(
        {"tests/a.py::historical": "Windows path"}, tmp_path / "scope.json"
    )
    items = [
        SimpleNamespace(nodeid=name) for name in ("tests/a.py::historical", "tests/a.py::portable")
    ]
    config = SimpleNamespace(
        hook=SimpleNamespace(pytest_deselected=lambda items: removed.extend(items))
    )
    plugin.pytest_collection_modifyitems(None, config, items)
    assert plugin.selection["collected_count"] == 2
    assert plugin.selection["selected_nodeids"] == ["tests/a.py::portable"]
    assert plugin.selection["excluded"] == [
        {"nodeid": "tests/a.py::historical", "reason": "Windows path"}
    ]
    assert [item.nodeid for item in removed] == ["tests/a.py::historical"]


def test_scope_drift_fails_collection_instead_of_silently_skipping(tmp_path):
    plugin = cloud.CloudScopePlugin(
        {"tests/a.py::missing": "Windows path"}, tmp_path / "scope.json"
    )
    with pytest.raises(pytest.UsageError, match="uncollected exclusions"):
        plugin.pytest_collection_modifyitems(
            None, None, [SimpleNamespace(nodeid="tests/a.py::portable")]
        )
    assert plugin.selection["unknown_exclusions"] == ["tests/a.py::missing"]


def test_evidence_prerequisites_detect_missing_and_changed_bytes(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    native = root / "native.dat"
    native.write_bytes(b"verified")
    manifest = {
        "evidence_files": [{"path": "native.dat", "bytes": 8, "sha256": cloud.digest(native)}],
        "assets": [],
        "validation_assets": [],
    }
    (root / "GITHUB_ARTIFACTS.json").write_text(json.dumps(manifest))
    assert cloud.evidence_prerequisites(root)["issues"] == []
    native.write_bytes(b"tampered")
    assert cloud.evidence_prerequisites(root)["issues"] == [
        {"path": "native.dat", "reason": "CHECKSUM_OR_SIZE_MISMATCH"}
    ]
    native.unlink()
    assert cloud.evidence_prerequisites(root)["issues"] == [
        {"path": "native.dat", "reason": "MISSING"}
    ]


def test_step_retains_failure_exit_code_and_log(tmp_path):
    result = cloud.run_step(
        "intentional-failure",
        [sys.executable, "-c", "print('failure evidence'); raise SystemExit(7)"],
        tmp_path,
        tmp_path,
        cloud.safe_environment(),
    )
    assert result["exit_code"] == 7
    assert "failure evidence" in (tmp_path / result["log"]).read_text()


def fake_root(tmp_path, monkeypatch):
    root = tmp_path / "project"
    (root / "ci").mkdir(parents=True)
    for name in ("requirements.lock", "ci/requirements-dev.lock"):
        (root / name).write_text("locked\n")
    monkeypatch.setattr(
        cloud.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, 0, "a" * 40 + "\n" if "rev-parse" in args else "", ""
        ),
    )
    return root


def test_missing_prerequisite_publishes_blocked_report_without_running_checks(
    tmp_path, monkeypatch
):
    root = fake_root(tmp_path, monkeypatch)
    monkeypatch.setattr(
        cloud,
        "evidence_prerequisites",
        lambda _: {"issues": [{"path": "proof.duckdb", "reason": "MISSING"}]},
    )
    monkeypatch.setattr(cloud, "run_step", lambda *args: pytest.fail("checks ran without proof"))
    output = tmp_path / "report"
    assert cloud.validate(root, output, scope_file(tmp_path)) == 2
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "BLOCKED_MISSING_OR_CHANGED_EVIDENCE"
    assert report["git_commit"] == "a" * 40
    assert report["steps"] == [] and report["trading_enabled"] is False


def test_failed_tests_never_produce_success_even_with_zero_prerequisite_issues(
    tmp_path, monkeypatch
):
    root = fake_root(tmp_path, monkeypatch)
    monkeypatch.setattr(cloud, "evidence_prerequisites", lambda _: {"issues": []})
    monkeypatch.setattr(
        cloud, "generated_documents", lambda *args: {"name": "documents", "exit_code": 0}
    )

    def step(name, command, root, output, env):
        if name == "offline-tests":
            assert command[:3] == [sys.executable, "-m", "tools.cloud_validate"]
            (output / "test-selection.json").write_text(
                json.dumps(
                    {
                        "selected_nodeids": ["tests/test_case.py::failure"],
                        "excluded": [],
                        "unapproved_skips": [],
                    }
                )
            )
            (output / "tests.xml").write_text(
                '<testsuites><testsuite tests="1" failures="1" errors="0" skipped="0" /></testsuites>'
            )
        return {"name": name, "exit_code": 1 if name in {"offline-tests", "wheel-build"} else 0}

    monkeypatch.setattr(cloud, "run_step", step)
    output = tmp_path / "report"
    assert cloud.validate(root, output, scope_file(tmp_path)) == 1
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "FAILED" and report["test_counts"]["failures"] == 1
    assert report["scope_manifest_sha256"] == cloud.digest(tmp_path / "scope.json")
    assert report["test_selection"]["selected_nodeids"] == ["tests/test_case.py::failure"]


@pytest.mark.parametrize("failure", ["commit", "status", "dirty", "invalid_commit"])
def test_unavailable_or_dirty_git_provenance_never_certifies_a_commit(
    tmp_path, monkeypatch, failure
):
    root = fake_root(tmp_path, monkeypatch)

    def git(args, **kwargs):
        if "rev-parse" in args:
            return subprocess.CompletedProcess(
                args,
                1 if failure == "commit" else 0,
                "bad" if failure == "invalid_commit" else "a" * 40,
                "",
            )
        return subprocess.CompletedProcess(
            args, 1 if failure == "status" else 0, " M source.py" if failure == "dirty" else "", ""
        )

    monkeypatch.setattr(cloud.subprocess, "run", git)
    monkeypatch.setattr(
        cloud,
        "evidence_prerequisites",
        lambda _: pytest.fail("evidence ran without clean git provenance"),
    )
    output = tmp_path / "report"
    assert cloud.validate(root, output, scope_file(tmp_path)) == 2
    report = json.loads((output / "summary.json").read_text())
    assert report["status"] == "BLOCKED_UNCOMMITTED_OR_UNAVAILABLE_PROVENANCE"
    assert report["exit_code"] == 2 and report["steps"] == []
    if failure == "status":
        assert report["working_tree_dirty"] is None


def test_wheel_verification_binds_exact_module_set_and_bytes(tmp_path):
    root = tmp_path / "project"
    package = root / "src/selective_options"
    package.mkdir(parents=True)
    (root / "pyproject.toml").write_text('[project]\nversion = "1.2.3"\n')
    (package / "__init__.py").write_text('__version__ = "1.2.3"\n')
    wheel = tmp_path / "package.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.write(package / "__init__.py", "selective_options/__init__.py")
    report = cloud.wheel_modules(root, wheel)
    assert report["module_count"] == 1 and report["source_version"] == "1.2.3"
    assert report["modules_match_current_source"] is True
    (package / "__init__.py").write_text('__version__ = "tampered"\n')
    with pytest.raises(ValueError, match="differ"):
        cloud.wheel_modules(root, wheel)


def test_unexpected_fixture_skip_remains_visible_in_scope_report(tmp_path):
    plugin = cloud.CloudScopePlugin({}, tmp_path / "selection.json")
    plugin.pytest_runtest_logreport(
        SimpleNamespace(
            skipped=True, nodeid="tests/a.py::required_proof", longrepr="private proof missing"
        )
    )
    plugin.pytest_sessionfinish(None, 0)
    report = json.loads((tmp_path / "selection.json").read_text())
    assert report["unapproved_skips"] == [
        {"nodeid": "tests/a.py::required_proof", "reason": "private proof missing"}
    ]


def test_offline_guard_blocks_external_dns_and_restores_socket_functions(tmp_path):
    original = socket.getaddrinfo
    plugin = cloud.CloudScopePlugin({}, tmp_path / "selection.json")
    plugin.pytest_sessionstart(None)
    try:
        assert cloud.loopback_host("127.0.0.1") and cloud.loopback_host("::1")
        with pytest.raises(RuntimeError, match="external network"):
            socket.getaddrinfo("api.github.com", 443)
    finally:
        plugin.pytest_sessionfinish(None, 0)
    assert socket.getaddrinfo is original
    assert json.loads((tmp_path / "selection.json").read_text())["blocked_network_attempts"] == [
        "api.github.com"
    ]


def test_safe_test_environment_removes_market_and_repository_credentials(monkeypatch):
    for key in (
        "DERIBIT_TESTNET_CLIENT_SECRET",
        "SELECTIVE_INGESTION__REDIS_URL",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTHONPATH",
        "PYTHONHOME",
    ):
        monkeypatch.setenv(key, "private-fixture")
    env = cloud.safe_environment()
    assert all(
        key not in env
        for key in (
            "DERIBIT_TESTNET_CLIENT_SECRET",
            "SELECTIVE_INGESTION__REDIS_URL",
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "PYTEST_ADDOPTS",
            "PYTEST_PLUGINS",
            "PYTHONPATH",
            "PYTHONHOME",
        )
    )


def test_junit_counts_sum_suites_and_preserve_skips(tmp_path):
    path = tmp_path / "tests.xml"
    path.write_text(
        '<testsuites><testsuite tests="5" failures="1" errors="0" skipped="1"/><testsuite tests="3" failures="0" errors="1" skipped="0"/></testsuites>'
    )
    assert cloud.junit_counts(path) == {"tests": 8, "failures": 1, "errors": 1, "skipped": 1}
