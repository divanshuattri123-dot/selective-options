# Frozen 0.3.47 clean-clone validation portability review

This review is read-only with respect to the canonical project. No source, tests,
scripts, configuration, saved evidence, GitHub settings or secrets were changed.
No market request was made. The original release records 3,192 passing tests and
96.16% coverage; this review does not claim a new clean-clone full-suite run.

## Result

A normal relocated clone cannot reproduce the entire frozen suite unaided.
Windows physical staging at the original project path, the fully packaged 0.3.47
evidence and four unchanged sibling baseline ZIPs satisfy the identified local
dependencies without changing an auditor or historical proof.

Historical project path:

`C:/Users/Divan/Documents/Codex/2026-09-20/files-pasted-by-the-user-build/outputs/selective-options`

Historical workspace path:

`C:/Users/Divan/Documents/Codex/2026-09-20/files-pasted-by-the-user-build`

## Exact external release dependencies

Each file belongs at `<historical-workspace>/outputs/<filename>`, the project
root's immediate parent. Download or copy the original file; rebuilding an archive
from the same content does not reproduce its pinned ZIP digest.

| Filename | Bytes | SHA256 | Consumer |
|---|---:|---|---|
| selective-options-0.3.42.zip | 420591102 | 809cd0a75bf176d678aec21a2ed129f9b63b692a84e3d3ec95625221703b0208 | scripts/verify_active_rotation.py:345; tests/test_active_rotation_verifier.py |
| selective-options-0.3.44.zip | 420981587 | 211521b5c2e2c3b22ab677df3e4a3f9cbc1952552cfaab31c60f4e50d8a26d82 | scripts/audit_parallel_foundations.py:761 and verify_parallel_foundations.py:157; tests/test_parallel_foundations_audit.py |
| selective-options-0.3.45.zip | 421270840 | 5019f646b9b554801a0e48c9ba6e539a78e8b0336f7c77aa976dff2f0288514d | scripts/audit_bar_checkpoint.py:566 and verify_bar_checkpoint.py:287; tests/test_bar_checkpoint_audit.py |
| selective-options-0.3.46.zip | 421622875 | bfcebe34564f69b2816e8b646cffa552bdd91b807b641ef021930768db8bf9f8 | scripts/verify_options_features.py:433; tests/test_options_features_verifier.py:215 |

Evidence hydration source: `selective-options-0.3.47.zip`, 429955213 bytes,
SHA256 `59322565071d7a85446895c6571ac39fca6080afbe8f4ce11449383fc92e6991`.
Its archive root is `selective-options/`; it contains 1,032 files, including 774
files under `selective-options/evidence/`. Hydrate all these evidence files with
their exact bytes. Excluding large DuckDB files or responses from the Git checkout
is compatible with validation only if this hydration occurs before pytest.

## Existing CI mismatches

`.github/workflows/ci.yaml:6` selects Ubuntu and lines 9-10 select Python 3.11/3.12.
`tests/test_coinmetrics_audit.py:160` starts the actual subprocess at
`ROOT.parents[1]/work/.venv/Scripts/python.exe`, rather than the running
interpreter. That Windows executable does not exist in a normal Ubuntu checkout.
All dependencies must be installed into the staged environment, since the child
process uses it too. Use Windows Python 3.12 for the historical suite.

`scripts/audit_parallel_foundations.py:682` requires the published report's
`source_evidence` to resolve to the active project's `evidence/live-gap-02`.
The saved report contains the original absolute Windows path. Even
`require_current_code=False` does not disable that identity check. A relocated
clone fails `ORIGINAL_SOURCE_HASH_SCOPE` before reaching later audit checks.
Symlinks and junctions are unsuitable substitutes: `tree()` and `digest()` in that
same auditor reject reparse ancestors. Materialize regular physical directories
at the original path for the frozen historical tests.

The canonical checkout originally had no `.gitattributes`. Git normalization can
change frozen hashes without changing displayed code. The migration's chosen
`* -text` attributes and `core.autocrlf=false` preserve exact file bytes. Verify
the installed source/scripts against archive bytes before the historical run.

Several saved-native tests use `Path('evidence/...')` relative to the process
working directory: test_binance_funding_view.py:183, test_binance_taker.py:145,
test_flow_quality.py:230 and test_flow_view.py:166. Run pytest with its working
directory set to the staged project root.

`pyproject.toml` supplies broad dependency ranges, not a frozen dependency lock.
For strongest repeatability use the successful environment's package-version
inventory in inventory.json; new allowed dependency releases still require a new
test run. This review has not tested an independent Windows or Linux host.

## Work directories

No existing staged-proof work directory needs to be distributed beyond the fully
packaged evidence. A validation-support ZIP is unnecessary.

The suite creates temporary output directories under the historical workspace's
`work/active-rotation-0343`, `work/crypto-news-0341`, `work/rotation-0342`,
`work/spot-batch-0344`, `work/stablecoin-restart-0340` and `work/tips-yield-0339`.
These are writable output locations, not saved inputs. The relevant fixtures call
`mkdir(parents=True, exist_ok=True)` themselves. Other verifier preflights name
`work/coinmetrics-0347`, `work/options-features-0347`, `work/liquidations-0339`,
`work/stablecoin-supply-0339`, `work/bar-checkpoint-0346` and
`work/parallel-foundations-0345`; their tests either patch ROOT or use tmp_path.

One older news restart verifier intentionally writes beneath the project itself:
`<project>/work/news-restart-0338` (test_news_restart_verifier.py:26 and
verify_news_restart.py:80). Allow this temporary directory. Do not upload it as
saved evidence.

Two audit fixtures have historical scratch fallbacks:

- tests/test_parallel_foundations_audit.py:14-18 chooses
  `evidence/parallel-foundations-01` before
  `work/parallel-foundations-0345/attempt-02`.
- tests/test_bar_checkpoint_audit.py:20-24 chooses
  `evidence/bar-checkpoint-resume-01` before
  `work/bar-checkpoint-0346/attempt-02`.

Both primary evidence directories are present in the 0.3.47 archive. Assert their
presence during hydration so a missing release asset produces an error rather
than silently skipping these historical audit tests.

The only required pre-existing work runtime is
`<historical-workspace>/work/.venv/Scripts/python.exe`. Create it on the runner;
never distribute an old venv, caches, credentials, or machine-local state.

## Windows CI staging outline

1. Check out the private Git repository normally with byte preservation enabled.
   Resolve the actual uploaded private release asset locations, then obtain the
   exact 0.3.47 evidence ZIP plus four baseline ZIPs. Verify all five hashes and
   sizes above before extraction or copying. GitHub release downloads are asset
   hydration, not new market collection. This review performs neither download.
2. On a Windows runner, refuse any pre-existing historical project path. Create
   physical parent directories and locally clone the checkout into the historical
   project path with `git -c core.autocrlf=false clone <checkout> <historical-path>`.
   Do not use a junction. Do not normalize or rewrite saved report paths.
3. Extract only the exact `selective-options/evidence/` members from the verified
   0.3.47 ZIP into the staged project's evidence directory, rejecting traversal,
   symlink entries, duplicates and unlisted paths. If an evidence file already
   exists, require byte equality rather than overwriting it. Verify each hydrated
   file against its archive bytes and require the two published audit roots above.
4. Copy the four exact baseline ZIPs to the staged project's immediate parent.
   Verify their digests there. Do not unpack the baseline ZIPs.
5. Create the historical workspace's `work/.venv` using actions/setup-python's
   Python 3.12. Install the project with its dev dependencies into that venv, using
   a recorded version lock or an offline wheelhouse if exact dependency replay is
   required. Validate `Scripts/python.exe` exists and can import the package.
6. Run the complete pytest coverage command from the staged project root using
   that same interpreter:
   `.../work/.venv/Scripts/python.exe -m pytest --cov --cov-report=term-missing`.
   Save JUnit/coverage outputs outside the immutable evidence directories and
   verify collected/passed counts against 3,192, with skips explicitly reviewed.
   Do not remove saved-native audit tests or lower the coverage requirement.
7. Run Ruff, formatting, mypy, render-config/diff, config validation and tracked
   secret checks on the locally cloned Git checkout as the current workflow does.
   On PowerShell replace Ubuntu `xargs -0` with a correctly quoted argument-list
   invocation of check_secrets.py; do not concatenate tracked file names into
   shell code. Keep all validation offline with respect to market providers.

This preserves frozen historical evidence. General app/runtime/core fixture work
can remain in a normal clone; the complete immutable historical suite has the
staging requirement described here.
