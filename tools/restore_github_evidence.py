"""Restore checksum-bound private release evidence without overwriting working source."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "divanshuattri123-dot/selective-options"
API = "https://api.github.com"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: Any
    ) -> None:
        return None


def file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def authenticate() -> str:
    for name in ("GITHUB_TOKEN", "GH_TOKEN"):
        if value := os.environ.get(name):
            return value
    account = REPOSITORY.split("/", 1)[0]
    completed = subprocess.run(
        ["git", "-c", "credential.interactive=never", "credential", "fill"],
        input=f"protocol=https\nhost=github.com\nusername={account}\n\n",
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        credential = dict(
            line.split("=", 1) for line in completed.stdout.splitlines() if "=" in line
        )
        if value := credential.get("password"):
            return value
    raise RuntimeError(
        "Sign in to GitHub through Git Credential Manager or set GITHUB_TOKEN locally"
    )


def api_json(path: str, token: str) -> Any:
    req = urllib.request.Request(
        API + path,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "selective-options-evidence-restore",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.build_opener(NoRedirect()).open(req, timeout=60) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise ValueError("GitHub metadata response exceeds its byte budget")
            return json.loads(raw)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Private release lookup failed with HTTP {error.code}") from None


def download_asset(
    asset: dict[str, Any], token: str, target: Path, expected: dict[str, Any]
) -> None:
    if asset["size"] != expected["bytes"]:
        raise ValueError("Remote asset size differs from the committed inventory")
    remote_digest = asset.get("digest")
    if remote_digest and remote_digest != "sha256:" + expected["sha256"]:
        raise ValueError("Remote asset digest differs from the committed inventory")
    request = urllib.request.Request(
        API + f"/repos/{REPOSITORY}/releases/assets/{asset['id']}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/octet-stream",
            "User-Agent": "selective-options-evidence-restore",
        },
    )
    opener = urllib.request.build_opener(NoRedirect())
    try:
        response = opener.open(request, timeout=180)
    except urllib.error.HTTPError as error:
        if error.code not in (301, 302, 303, 307, 308):
            raise RuntimeError(f"Private asset download failed with HTTP {error.code}") from None
        url = error.headers.get("Location", "")
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "release-assets.githubusercontent.com":
            raise ValueError("Unexpected private release redirect host") from None
        # The signed storage URL receives no GitHub authorization header.
        try:
            response = opener.open(url, timeout=180)
        except urllib.error.HTTPError as storage_error:
            raise RuntimeError(
                f"Signed release storage request failed with HTTP {storage_error.code}; redirects refused"
            ) from None
    with response, target.open("xb") as destination:
        remaining = expected["bytes"]
        while chunk := response.read(min(1024 * 1024, remaining + 1)):
            if len(chunk) > remaining:
                raise ValueError("Downloaded asset exceeds its declared byte budget")
            destination.write(chunk)
            remaining -= len(chunk)
    if target.stat().st_size != expected["bytes"] or file_sha256(target) != expected["sha256"]:
        raise ValueError("Downloaded asset failed byte-size or SHA-256 verification")


def safe_relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or not path.parts
        or any(part in {"..", "."} or ":" in part or "\\" in part for part in path.parts)
    ):
        raise ValueError("Unsafe archive inventory path")
    if path.parts[0] in {".git", "state", ".venv"} or any(
        part.startswith(".env") for part in path.parts
    ):
        raise ValueError("Local or credential path is not restorable")
    return path


def ensure_target(root: Path, target: Path) -> None:
    if not target.resolve().is_relative_to(root):
        raise ValueError("Evidence target escapes the checkout")
    for candidate in (target, *target.parents):
        if candidate.exists() or candidate.is_symlink():
            metadata = candidate.lstat()
            if candidate.is_symlink() or getattr(metadata, "st_file_attributes", 0) & 0x400:
                raise ValueError("Evidence target has a symlink or reparse ancestor")
        if candidate == root:
            break


def restore_entries(archive_path: Path, entries: list[dict[str, Any]], root: Path) -> int:
    root = root.resolve()
    targets: list[tuple[dict[str, Any], Path]] = []
    with zipfile.ZipFile(archive_path) as archive:
        names = [item.filename for item in archive.infolist()]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate names in release archive")
        for record in entries:
            relative = safe_relative(record["path"])
            target = root.joinpath(*relative.parts)
            ensure_target(root, target)
            member = "selective-options/" + relative.as_posix()
            info = archive.getinfo(member)
            if info.file_size != record["bytes"]:
                raise ValueError("Archived evidence size differs from its inventory")
            if target.exists():
                if (
                    target.stat().st_size != record["bytes"]
                    or file_sha256(target) != record["sha256"]
                ):
                    raise ValueError(f"Existing evidence differs; refusing overwrite: {relative}")
                continue
            targets.append((record, target))
        # Validate every selected member before publishing any evidence.
        for record, _ in targets:
            with archive.open("selective-options/" + record["path"]) as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != record["sha256"]:
                    raise ValueError("Archived evidence failed SHA-256 verification")
        for record, target in targets:
            target.parent.mkdir(parents=True, exist_ok=True)
            ensure_target(root, target)
            with tempfile.NamedTemporaryFile(
                dir=root, delete=False, suffix=".partial"
            ) as temporary:
                staging = Path(temporary.name)
                with archive.open("selective-options/" + record["path"]) as stream:
                    shutil.copyfileobj(stream, temporary, length=1024 * 1024)
            try:
                ensure_target(root, staging)
                ensure_target(root, target)
                if (
                    staging.stat().st_size != record["bytes"]
                    or file_sha256(staging) != record["sha256"]
                ):
                    raise ValueError("Staged evidence changed before publication")
                if target.exists():
                    raise ValueError("Evidence appeared during restore; refusing overwrite")
                # Hard-link publication creates the destination only if it is absent.
                ensure_target(root, target)
                os.link(staging, target)
            finally:
                staging.unlink(missing_ok=True)
    return len(targets)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all-checkpoints",
        action="store_true",
        help="Download all preserved historical release assets too",
    )
    args = parser.parse_args()
    manifest = json.loads((ROOT / "GITHUB_ARTIFACTS.json").read_text(encoding="utf-8"))
    entries = manifest["evidence_files"]
    selected = [
        asset
        for asset in manifest["assets"]
        if args.all_checkpoints or asset["name"] in manifest["validation_assets"]
    ]
    missing_evidence = []
    for entry in entries:
        relative = safe_relative(entry["path"])
        target = ROOT.joinpath(*relative.parts)
        if target.exists():
            if file_sha256(target) != entry["sha256"]:
                raise ValueError(f"Existing evidence differs; refusing overwrite: {relative}")
        else:
            missing_evidence.append(entry)
    token = None
    release_cache: dict[str, list[dict[str, Any]]] = {}
    local_assets: dict[str, Path] = {}
    for expected in selected:
        target = ROOT.parent / expected["name"]
        if target.exists():
            if (
                target.stat().st_size != expected["bytes"]
                or file_sha256(target) != expected["sha256"]
            ):
                raise ValueError(f"Existing checkpoint differs; refusing overwrite: {target.name}")
        else:
            if token is None:
                token = authenticate()
            tag = expected["release_tag"]
            if tag not in release_cache:
                release = api_json(
                    f"/repos/{REPOSITORY}/releases/tags/{urllib.parse.quote(tag, safe='')}",
                    token,
                )
                asset_rows = []
                page = 1
                while True:
                    rows = api_json(
                        f"/repos/{REPOSITORY}/releases/{release['id']}/assets?per_page=100&page={page}",
                        token,
                    )
                    asset_rows.extend(rows)
                    if len(rows) < 100:
                        break
                    page += 1
                release_cache[tag] = asset_rows
            matches = [asset for asset in release_cache[tag] if asset["name"] == expected["name"]]
            if len(matches) != 1:
                raise ValueError(f"Required release asset is unavailable: {expected['name']}")
            with tempfile.TemporaryDirectory(
                dir=ROOT.parent, prefix=".evidence-download-"
            ) as staging:
                temporary = Path(staging) / expected["name"]
                download_asset(matches[0], token, temporary, expected)
                if target.exists():
                    raise ValueError("Checkpoint appeared during download; refusing overwrite")
                os.link(temporary, target)
            print(f"Verified checkpoint: {expected['name']}", flush=True)
        local_assets[expected["name"]] = target
    archive = local_assets["selective-options-0.3.47.zip"]
    restored = restore_entries(archive, missing_evidence, ROOT)
    print(
        f"Verified {len(entries)} evidence files; restored {restored}. No market or trading account requests."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
