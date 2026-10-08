# Private GitHub migration inventory

Metadata-only snapshot, 8 October 2026, while 0.3.47 publication ran. No project or release was edited; no GitHub/network action occurred; credential/account candidate contents were not opened. inventory.json contains the totals, archive central-directory metadata and literal-based test dependencies. file-metadata.json contains per-file paths/sizes/classification. These are migration diagnostics, not new validation or readiness evidence.

| Scope | Size / count |
| --- | --- |
| Current project including caches/build products | 1,377 files; 1,411.19 MiB |
| Non-cache/build project files | 1,032 files; about 1,371.07 MiB |
| Evidence | 774 files; about 1,367.98 MiB |
| Code/docs/config/examples/tests/scripts/CI, without caches/evidence/old full-tree manifest | about 2.98 MiB |
| Historical ZIP/wheel assets including the now-visible 0.3.47 assets | 53 archives; 6,718,836,540 bytes, about 6.26 GiB |
| Latest 0.3.47 ZIP | 429,955,213 bytes; 410.04 MiB |
| Evidence directories directly named by current tests | 15 directories; 148 files; about 166.65 MiB |

The test dependency inventory is a conservative static path map, not a dynamically proven minimal fixture set. Test imports may introduce additional script dependencies. Preserve the entire reviewed evidence bundle for an initial reproducible migration; optimize later with explicit tests.

Five individual project files exceed 50 MiB; three exceed 100 MiB:

| File | MiB |
| --- | ---: |
| evidence/metadata-projection-replay-01/reused-validation-public-01.duckdb | 214.51 |
| evidence/option-projection-replay-02/reused-validation-public-01.duckdb | 252.26 |
| evidence/reused-validation-public-01/events.duckdb | 112.01 |
| evidence/multiasset-options-public-01/events.duckdb | 81.76 |
| evidence/reused-validation-replay-01/events.duckdb | 59.01 |

Fifteen historical ZIPs exceed 100 MiB, from 0.3.33 through 0.3.47. No historical ZIP/wheel should be added as a normal Git blob. Original archives/checksum files should remain immutable local artifacts and, after privacy review, separate private release assets.

## Minimal migration

1. Commit all meaningful source, scripts, tests, examples, config, documentation, pyproject/lock, Docker/compose and CI files. Keep .env.example only. Preserve all tests, including saved-native assertions and negative/cancellation cases. Do not delete tests to make a smaller repository.
2. Keep small human-readable validation/release summaries and a new fixture asset index in Git. The index should identify the exact private release/tag/asset name, SHA-256, byte count, expected extracted paths and original manifest. Keep existing immutable MANIFEST.sha256 as a labelled 0.3.47 release manifest, not a claim that the slimmer Git tree has every listed file. Large JSON proof outputs can also be assets; the four options API/CLI outputs alone are roughly 15 MiB and duplicate data.
3. Put complete reviewed native DuckDB/Parquet/raw-body/provenance evidence bundles and historical ZIP/wheels in private release asset storage. Reference their exact digests from Git. Avoid duplicating all 6.26 GiB across Git history. Keep local originals even if a separate sanitized derivative must be prepared; sanitized bytes need a new checksum and explicit derivation record.
4. Exclude caches, builds, .coverage, __pycache__, egg-info, state, temporary work, .env/.env.*, private keys and credential configuration from the first Git import. Existing .gitignore handles most of these, but does not exclude evidence binaries, root project/work or release ZIP/wheels; stage through a separate migration checkout/allowlist without altering the verified 0.3.47 tree.
5. Preserve byte identity on the first import, including source/script/proof bytes. Freeze maps, release manifests and wheel/proof comparisons depend on exact bytes. Use explicit Git attributes or a byte-preserving import with autocrlf disabled, then compare the migration checkout to the recorded hashes. Do not silently normalize line endings and describe the result as the original frozen source.

## Privacy candidates

Eight filename-only candidates occur in the current evidence tree: authenticated-check.json, authenticated-diagnostics.json, authenticated-diagnostics-v2.json, authenticated-probe.json and authenticated-chains/{BTC,ETH,SOL,PAXG}-chain.json. They may be harmless authenticated testnet market diagnostics; this inventory does not assert that they contain secrets or private account data. Exclude them from initial Git staging until a separate redacted-content review clears them. Fifty archive central directories contain similar candidates, including the newest ZIP, so moving an original ZIP to release assets does not by itself eliminate the privacy review requirement. No .env/private-key credential filename was found in the project metadata snapshot apart from the intended .env.example template, which was not classified as a secret.

## Reproducible CI

The existing CI workflow is labelled milestone-1, runs Ubuntu/Python 3.11 and 3.12, and assumes all evidence is present. Do not claim it reproduces the Windows 3.12 full release before the new remote run succeeds.

- Start with an explicit Windows/Python 3.12 baseline using the exercised runtime versions in requirements.lock, the dev tools and existing Ruff/mypy/config-idempotence/pytest checks. Keep Linux/3.11 portability as separately observed jobs rather than claiming an unrun matrix passed.
- Restore reviewed fixture assets before the full suite. Fail clearly on absent/hash-invalid inputs; do not convert missing saved-native proofs into passing tests. Require the expected baseline count and zero skips for the complete 0.3.47 reproducibility claim. A fast synthetic-only job is useful if clearly labelled partial.
- Current test paths assume the project layout outputs/selective-options, with sibling release ZIPs under outputs and scratch under work. A checkout using that layout is the smallest adaptation. Extract only the required reviewed evidence into evidence, not archived code over the checkout under test.
- Saved native options tests explicitly require evidence/reused-validation-public-01/events.duckdb, and some proof tests require the exact sibling selective-options-0.3.46.zip (SHA-256 bfcebe34564f69b2816e8b646cffa552bdd91b807b641ef021930768db8bf9f8). Other direct dependencies include bar-checkpoint-resume-01, prior funding/flow/news/stablecoin proofs and live-gap-02. The full map is in inventory.json.
- Default CI makes no exchange, account or paid-service calls. Use explicit labelled fixtures and saved native assets. Keep collectors optional, M2 PARTIAL, ready_for_signals=false and trading_enabled=false. A successful Git import or CI run does not establish continuous ingestion, a strategy edge or live readiness.

No deletions, GitHub mutations, account access, public requests, purchases or orders were performed by this inventory task.
