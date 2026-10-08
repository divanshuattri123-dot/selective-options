"""Migration-only synthetic ZIP/HTTP fixtures. No GitHub or market requests."""

import hashlib
import importlib.util
import io
import os
import subprocess
import zipfile
from email.message import Message
from pathlib import Path, PureWindowsPath
from urllib import request, response

import pytest
import yaml


def helper_path():
    candidates = [
        Path(os.environ.get("RESTORE_HELPER_PATH", "__explicit_unset__")),
        Path(__file__).resolve().parents[1] / "tools/restore_github_evidence.py",
        Path(__file__).resolve().parents[1]
        / "github-migration-files/tools/restore_github_evidence.py",
        Path(__file__).resolve().parents[1] / "restore_github_evidence.py",
        Path.cwd() / "tools/restore_github_evidence.py",
    ]
    return next(path for path in candidates if path.is_file())


@pytest.fixture
def module():
    spec = importlib.util.spec_from_file_location("migration_restore_review", helper_path())
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def sha(body):
    return hashlib.sha256(body).hexdigest()


def zip_fixture(tmp_path, values):
    path = tmp_path / "fixture.zip"
    records = []
    with zipfile.ZipFile(path, "w") as archive:
        for name, body in values:
            archive.writestr("selective-options/" + name, body)
            records.append({"path": name, "bytes": len(body), "sha256": sha(body)})
    return path, records


def local_https(monkeypatch, module, replies):
    """Real urllib redirect processing over an in-memory HTTPS handler only."""
    calls = []
    real_build = request.build_opener

    class LocalHTTPS(request.HTTPSHandler):
        def https_open(self, req):
            calls.append((req.full_url, dict(req.header_items())))
            status, location, body = replies[req.full_url]
            headers = Message()
            headers["Content-Type"] = "application/json"
            if location:
                headers["Location"] = location
            result = response.addinfourl(io.BytesIO(body), headers, req.full_url, status)
            result.msg = "Synthetic fixture"
            return result

    default = real_build(LocalHTTPS())
    monkeypatch.setattr(
        module.urllib.request, "urlopen", lambda req, **kwargs: default.open(req, **kwargs)
    )
    monkeypatch.setattr(
        module.urllib.request, "build_opener", lambda *handlers: real_build(LocalHTTPS(), *handlers)
    )
    return calls


def expected_asset(body):
    return {"id": 123, "size": len(body), "digest": "sha256:" + sha(body)}, {
        "bytes": len(body),
        "sha256": sha(body),
    }


def test_metadata_cross_origin_redirect_never_carries_authorization(module, monkeypatch):
    api = module.API + "/fixture"
    external = "https://external.invalid/redirected"
    calls = local_https(
        monkeypatch, module, {api: (302, external, b""), external: (200, None, b"{}")}
    )
    with pytest.raises((RuntimeError, ValueError)):
        module.api_json("/fixture", "EXPLICIT_SYNTHETIC_TEST_TOKEN")
    assert [url for url, _ in calls] == [api]


def test_metadata_response_bound_refuses_before_json_decode(module, monkeypatch):
    api = module.API + "/fixture"
    local_https(monkeypatch, module, {api: (200, None, b" " * (4 * 1024 * 1024 + 1))})
    with pytest.raises(ValueError, match="byte budget"):
        module.api_json("/fixture", "EXPLICIT_SYNTHETIC_TEST_TOKEN")


def test_storage_redirect_strips_auth_and_checks_pinned_bytes(module, monkeypatch, tmp_path):
    content = b"EXPLICIT_SYNTHETIC_ASSET"
    asset, expected = expected_asset(content)
    api = module.API + f"/repos/{module.REPOSITORY}/releases/assets/{asset['id']}"
    storage = "https://release-assets.githubusercontent.com/fixture?signature=EXPLICIT_FIXTURE"
    calls = local_https(
        monkeypatch, module, {api: (302, storage, b""), storage: (200, None, content)}
    )
    destination = tmp_path / "download.zip"
    module.download_asset(asset, "EXPLICIT_SYNTHETIC_TEST_TOKEN", destination, expected)
    assert destination.read_bytes() == content
    assert calls[0][1]["Authorization"] == "Bearer EXPLICIT_SYNTHETIC_TEST_TOKEN"
    assert all(key.lower() != "authorization" for key in calls[1][1])


def test_storage_second_redirect_cannot_escape_allowlist(module, monkeypatch, tmp_path):
    content = b"EXPLICIT_SYNTHETIC_ASSET"
    asset, expected = expected_asset(content)
    api = module.API + f"/repos/{module.REPOSITORY}/releases/assets/{asset['id']}"
    storage = "https://release-assets.githubusercontent.com/fixture"
    external = "https://external.invalid/payload"
    calls = local_https(
        monkeypatch,
        module,
        {api: (302, storage, b""), storage: (302, external, b""), external: (200, None, content)},
    )
    with pytest.raises((RuntimeError, ValueError)):
        module.download_asset(
            asset, "EXPLICIT_SYNTHETIC_TEST_TOKEN", tmp_path / "asset.zip", expected
        )
    assert [url for url, _ in calls] == [api, storage]


def test_download_overflow_never_writes_unbounded_body(module, monkeypatch, tmp_path):
    asset, expected = expected_asset(b"12345678")
    api = module.API + f"/repos/{module.REPOSITORY}/releases/assets/{asset['id']}"
    local_https(monkeypatch, module, {api: (200, None, b"x" * 100)})
    destination = tmp_path / "asset.zip"
    with pytest.raises(ValueError, match="byte budget"):
        module.download_asset(asset, "EXPLICIT_SYNTHETIC_TEST_TOKEN", destination, expected)
    assert not destination.exists() or destination.stat().st_size <= expected["bytes"]


@pytest.mark.parametrize("field,value", [("size", 999), ("digest", "sha256:" + "0" * 64)])
def test_remote_metadata_disagreement_refuses_before_any_request(
    module, monkeypatch, tmp_path, field, value
):
    asset, expected = expected_asset(b"fixture")
    asset[field] = value

    def unexpected(*_args, **_kwargs):
        raise AssertionError("network opener should not be constructed")

    monkeypatch.setattr(module.urllib.request, "build_opener", unexpected)
    with pytest.raises(ValueError):
        module.download_asset(
            asset, "EXPLICIT_SYNTHETIC_TEST_TOKEN", tmp_path / "asset.zip", expected
        )


def test_zip_restore_is_exact_and_idempotent(module, tmp_path):
    archive, records = zip_fixture(tmp_path, [("evidence/a.bin", b"a"), ("evidence/b.bin", b"b")])
    root = tmp_path / "checkout"
    root.mkdir()
    assert module.restore_entries(archive, records, root) == 2
    assert (root / "evidence/a.bin").read_bytes() == b"a"
    assert module.restore_entries(archive, records, root) == 0


def test_conflicting_existing_file_refuses_before_other_publication(module, tmp_path):
    archive, records = zip_fixture(
        tmp_path, [("evidence/new.bin", b"new"), ("evidence/existing.bin", b"expected")]
    )
    root = tmp_path / "checkout"
    (root / "evidence").mkdir(parents=True)
    conflict = root / "evidence/existing.bin"
    conflict.write_bytes(b"EXPLICIT_EXISTING_CONFLICT")
    with pytest.raises(ValueError, match="refusing overwrite"):
        module.restore_entries(archive, records, root)
    assert (
        conflict.read_bytes() == b"EXPLICIT_EXISTING_CONFLICT"
        and not (root / "evidence/new.bin").exists()
    )


def test_later_bad_checksum_refuses_before_first_file_is_published(module, tmp_path):
    archive, records = zip_fixture(tmp_path, [("evidence/a.bin", b"a"), ("evidence/b.bin", b"b")])
    records[1]["sha256"] = "0" * 64
    root = tmp_path / "checkout"
    root.mkdir()
    with pytest.raises(ValueError, match="SHA-256"):
        module.restore_entries(archive, records, root)
    assert not list(root.rglob("*.bin"))


def test_staged_copy_corruption_cannot_publish_bytes_outside_pin(module, monkeypatch, tmp_path):
    archive, records = zip_fixture(tmp_path, [("evidence/new.bin", b"new")])
    root = tmp_path / "checkout"
    root.mkdir()

    def corrupt_copy(source, destination, **_kwargs):
        source.read()
        destination.write(b"bad")  # Explicit same-length staging fault after prior ZIP validation.

    monkeypatch.setattr(module.shutil, "copyfileobj", corrupt_copy)
    with pytest.raises(ValueError):
        module.restore_entries(archive, records, root)
    assert not (root / "evidence/new.bin").exists()


@pytest.mark.parametrize(
    "path",
    [
        "../outside.bin",
        "/absolute.bin",
        "C:/absolute.bin",
        "evidence/../../outside.bin",
        "evidence\\outside.bin",
        ".git/config",
        ".venv/file",
        "evidence/.env.secret",
    ],
)
def test_inventory_traversal_and_local_resource_paths_refused(module, path):
    with pytest.raises(ValueError):
        module.safe_relative(path)


def symlink(target, link):
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as error:
        if os.name == "nt":
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(link), str(target)],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                return
        pytest.skip(
            f"Directory symlink/junction unavailable on this host: {error.winerror if hasattr(error, 'winerror') else error.errno}"
        )


def test_preexisting_parent_symlink_escape_refused(module, tmp_path):
    archive, records = zip_fixture(tmp_path, [("evidence/new.bin", b"new")])
    root, outside = tmp_path / "checkout", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    symlink(outside, root / "evidence")
    with pytest.raises(ValueError, match="escapes"):
        module.restore_entries(archive, records, root)
    assert not (outside / "new.bin").exists()


def test_parent_swapped_after_preflight_cannot_publish_outside(module, monkeypatch, tmp_path):
    archive, records = zip_fixture(tmp_path, [("evidence/new.bin", b"new")])
    root, outside = tmp_path / "checkout", tmp_path / "outside"
    (root / "evidence").mkdir(parents=True)
    outside.mkdir()
    # Probe symlink permission first so the race test cannot silently degrade.
    probe = tmp_path / "symlink-probe"
    symlink(outside, probe)
    if getattr(os.path, "isjunction", lambda path: False)(probe):
        probe.rmdir()
    else:
        probe.unlink()
    real = module.tempfile.NamedTemporaryFile

    def swap(*args, **kwargs):
        (root / "evidence").rename(root / "original-evidence")
        symlink(outside, root / "evidence")
        return real(*args, **kwargs)

    monkeypatch.setattr(module.tempfile, "NamedTemporaryFile", swap)
    with pytest.raises(ValueError):
        module.restore_entries(archive, records, root)
    assert not (outside / "new.bin").exists()


def test_ci_uses_physical_history_clone_and_correct_venv_layout():
    draft = helper_path().resolve().parents[1]
    workflow = yaml.load((draft / ".github/workflows/ci.yaml").read_text(), Loader=yaml.BaseLoader)
    job = workflow["jobs"]["verify"]
    project, python = PureWindowsPath(job["env"]["PROJECT"]), PureWindowsPath(job["env"]["PYTHON"])
    assert python.parent.parent == project.parent.parent / "work/.venv"
    assert job["runs-on"] == "windows-latest" and job["defaults"]["run"]["shell"] == "pwsh"
    staged = next(
        step["run"]
        for step in job["steps"]
        if step.get("name") == "Preserve physical historical-proof layout"
    )
    assert "clone --local --no-hardlinks" in staged and "core.autocrlf=false" in staged
    assert "python -m venv (Split-Path -Parent (Split-Path -Parent $env:PYTHON))" in staged
    assert workflow["permissions"] == {"contents": "read"}
    suite = next(
        step["run"] for step in job["steps"] if step.get("name") == "Full offline validation suite"
    )
    assert "pytest --cov --cov-report=term-missing" in suite
    assert "-k " not in suite and "--ignore" not in suite
    doc = (draft / "GITHUB_MIGRATION.md").read_text()
    assert "GitHub CI results are separate" in doc and "inspect the Actions run" in doc
