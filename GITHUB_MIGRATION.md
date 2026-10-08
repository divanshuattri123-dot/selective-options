# Private GitHub development repository

Repository: https://github.com/divanshuattri123-dot/selective-options

The first Git commit imports the verified 0.3.47 project. It includes the Python source,
tests, configuration, documentation, original specification, durable work log, small
native response files, and validation/provenance summaries. The original file bytes
are preserved: automatic line-ending conversion is disabled because the evidence
binds exact source, script and document hashes.

The full source-and-evidence snapshot and every preserved earlier checkpoint are
private GitHub release assets. `GITHUB_ARTIFACTS.json` records their SHA-256 hashes,
byte sizes, release tags and omitted evidence paths. This keeps multi-hundred-megabyte
databases out of ordinary Git while preserving the complete reviewed deliverables.
The repository's automatically generated source ZIP contains the Git files only;
use the explicitly named checkpoint ZIP for the full original evidence.

- Current complete snapshot: https://github.com/divanshuattri123-dot/selective-options/releases/tag/v0.3.47
- Earlier checkpoint downloads: https://github.com/divanshuattri123-dot/selective-options/releases/tag/historical-checkpoints-2026-10-08

Credentials, `.env` files, API keys, local state, virtual environments, caches and
redundant build directories are local resources. They are excluded from the import.
No earlier version is represented as a fabricated historical Git commit: its
original immutable archive and checksums preserve its actual checkpoint history.

## Clone and install

Authenticate to the private GitHub repository through your normal Git client first.
API secrets are never needed in chat or in a committed file.

```powershell
git clone https://github.com/divanshuattri123-dot/selective-options.git
cd selective-options
python -m venv .venv
& .venv/Scripts/python.exe -m pip install -r requirements.lock
& .venv/Scripts/python.exe -m pip install -r ci/requirements-dev.lock
& .venv/Scripts/python.exe -m pip install -e .
& .venv/Scripts/selective-options.exe check-config
```

Restore all omitted native evidence and the four earlier archives needed by the
full saved-proof tests:

```powershell
python tools/restore_github_evidence.py
```

The restore tool uses an existing Git Credential Manager login, or the local
`GITHUB_TOKEN`/`GH_TOKEN` environment variable when provided by GitHub Actions. It
checks each private release asset against the committed SHA-256 and size, strips
authorization before following the signed GitHub storage redirect, validates the
selected archive members, and refuses to overwrite conflicting existing bytes.
Downloads are placed next to the checkout and evidence is restored under `evidence`.
It makes GitHub requests only; it does not contact market-data or trading accounts.

To download every older checkpoint as well:

```powershell
python tools/restore_github_evidence.py --all-checkpoints
```

## Full historical-proof validation

The original 0.3.47 validation passed 3,192 tests with 96.16% combined coverage on
Windows/Python 3.12. Some immutable proofs bind the original absolute source path
and deliberately reject junctions or symlinks. Another test launches the original
`work/.venv/Scripts/python.exe`. A relocated checkout therefore cannot claim that
it reproduced the complete historical suite simply by omitting those checks.

The GitHub Actions workflow stages a **physical local clone** in the historical
workspace layout on its disposable Windows runner, restores checksum-verified
evidence and the exact 0.3.42/0.3.44/0.3.45/0.3.46 sibling ZIPs, and creates that
layout's virtual environment. It then checks static analysis, generated documents,
configuration, credential patterns and the full suite. Existing source, test,
auditor and proof bytes remain unchanged. No trading credentials are configured.

The initial migration enables manual workflow dispatch while release assets are
uploaded. Normal push and pull-request checks are enabled after restoration and
validation are verified. GitHub CI results are separate from the local 0.3.48
suite; inspect the Actions run before treating a commit as passed. The first clean Windows GitHub run passed at
`34ed883748e31d56d936096bc543f8ffc1d053fd`: 3,192 full-suite tests, zero failures,
errors or skips, plus successful dependency installation, static analysis, generated
document consistency, secret-pattern check, configuration validation, and the
migration-helper step. The exact result is recorded in
`evidence/github-migration/ci-run-37773515709.json`; the run is
https://github.com/divanshuattri123-dot/selective-options/actions/runs/37773515709.
That run validates only the 0.3.47 import commit. Later commits require their own
validation run before they can be described as CI-passed.

The main branch now contains version 0.3.48. Its first workflow attempt passed
restore, installation, static checks and all 3,231 offline tests, but the separate
migration-helper suite caught a documentation-phrase regression. After restoring
the expected wording, the full Windows workflow passed on
`74d98f1a946e666132742f1476e5c43a3dc73e1c`: all 3,231 offline tests, 22 migration
tests, Ruff, strict mypy, generated-document consistency, secret-pattern checks,
evidence restore and configuration validation. Its exact summary is
`evidence/github-migration/ci-run-37782279806.json`; the run is
https://github.com/divanshuattri123-dot/selective-options/actions/runs/37782279806.
The immutable downloadable full-source/evidence snapshot remains the 0.3.47
release asset; 0.3.48 source, tests, small validation evidence and history are on
the private `main` branch.

## Continuing development

The local canonical project is still `outputs/selective-options`; it now has a Git
remote and a `main` branch. Continue implementing there, keep the work log and
verification evidence current, then commit and push reviewed changes. Use new
checkpoint releases for new large immutable data artifacts; retain the historical
archives and their hashes.

The build remains **milestone 2 PARTIAL**. Full ingestion, full feature/backtester
acceptance, sustained validation, signals and production execution remain
incomplete. Migration and CI are engineering checks; trading remains disabled.
