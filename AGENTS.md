# Working on selective-options

Read SPECIFICATION.md, INGESTION_STATUS.md, COMPLETION_CHECKLIST.md and the newest
WORK_LOG.md entry before changing this project. The current application is 0.3.56;
milestone 2 remains PARTIAL. Independent feature/backtester engineering is authorized
alongside ingestion using saved data and clearly labelled fixtures. Acceptance of
later milestones still depends on their preceding specification gates.

Use this private repository as the development source of truth. Create reviewable
commits or pull requests, keep work and validation evidence durable, and honor a
direct user pause immediately. The former desktop heartbeat was deleted on pause;
resuming a coding task does not automatically recreate a recurring automation.

## Cloud development

See CLOUD_MIGRATION.md for setup, exact test scope and phone controls. Run
`bash tools/cloud_setup.sh` in a Linux cloud checkout, then use the documented
validation runner. Restore native evidence with tools/restore_github_evidence.py
using an existing authorized GitHub login or setup-only GitHub token. Do not print
credentials or put them in a command argument, source, artifact or work log.

The portable Linux suite explicitly inventories historical tests tied to the
original Windows layout. Preserve those proof bytes and their full Windows check.
Never describe a portable test subset as full historical-proof acceptance. Missing
evidence, unexpected skips, failing tests and withheld feeds remain explicit.

Runtime source and native proof files bind exact hashes. Inspect that provenance
before changing them. Use existing saved data when a new network experiment would
add no relevant verification. Public capture must have a finite duration and a
fresh output directory; do not invent continuous data or missing market history.

## Validation and boundaries

Use locked runtime and development dependencies. Run relevant tests and Ruff for
changed Python tools, strict mypy for source/script changes, configuration checks,
and the cloud workflow. Keep historical Windows verification available. Document
what passed and the precise commit/platform/scope; a job being dispatched is not
evidence it passed.

No accounts or paid services may be created or purchased under the current scope.
No production execution or real-money orders are authorized. Preserve the live
arming refusal, testnet defaults and secrets isolation. CI and Codex workspaces are
development resources; a sustained collector needs separately provisioned hosting,
persistent storage, backup/recovery and operational validation.
