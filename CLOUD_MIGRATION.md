# Cloud development and phone controls

The private development repository is
[divanshuattri123-dot/selective-options](https://github.com/divanshuattri123-dot/selective-options).
The phone controls the work; hosted runners perform the builds. A copy of the
repository and original large evidence/checkpoints are already in private GitHub
storage. Trading is disabled and milestone 2 remains PARTIAL.

## Verified hosted development setup

The corrected Linux/container run
[37805078059](https://github.com/divanshuattri123-dot/selective-options/actions/runs/37805078059)
binds commit `dcbc52a4e0d25538aec6c5127b10dac0d607bbfd`. It exercises the actual Bash
setup and cloud virtual environment, passes 3,345 selected cases with zero failures,
errors or skips, and records seven exact Windows-bound historical exclusions. All
191 required evidence/archive hashes pass. The installed 0.3.50 wheel matches all
80 source modules; non-root container configuration, named-volume persistence and
mandatory arming refusal pass. No blocked external network attempts occur in the
pytest worker. Full historical Windows verification remains a separate workflow.

The complete hosted Windows run
[37805077882](https://github.com/divanshuattri123-dot/selective-options/actions/runs/37805077882)
passes on the same commit: 3,287 application tests, 65 migration-helper tests,
95.03% coverage and zero failures/errors/skips. No historical cases are excluded
from that Windows suite. All three original artifact archives and their checksums
are also preserved in the private
[cloud validation release](https://github.com/divanshuattri123-dot/selective-options/releases/tag/cloud-validation-2026-10-08),
so the verified result remains downloadable after temporary Actions artifacts expire.
The following documentation/evidence-only commit records these results; the tested
executable source and workflow bytes bind the exact commit above.

The first Linux attempt is preserved as a failure: its stricter offline guard caught
two Coin Metrics capture tests missing an unrelated spot-transport mock. The repair
adds that mock without changing their assertions, production behavior or exclusions.
The corrected 32-case file also passes locally under the network guard.

## Start or inspect a build from your phone

1. Sign in to GitHub in your phone browser and open
   [Actions](https://github.com/divanshuattri123-dot/selective-options/actions).
2. Select **cloud-build**, then **Run workflow**, branch **main**, and **Run workflow**.
   This checks the portable Linux development suite, packages the wheel and validates
   the container. The run summary and downloadable artifacts report their precise
   scope. You can inspect steps, logs and artifacts or cancel a running job there.
3. Select **verified-checkpoint** to run the complete historical Windows proof suite.
   That runner reconstructs the original physical folder layout because immutable
   proof files bind that path. This preservation does not require your own PC.
4. Both workflows also run for pushes to main and pull requests targeting main.
   A successful job is bound to its displayed commit; dispatching a job alone does
   not certify a build. Temporary build artifacts have an explicit retention period.
   Keep durable native checkpoint releases and their checksums separately.

GitHub documents these controls in
[Manually running a workflow](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).
Use the phone browser if a particular GitHub mobile-app view omits a dispatch control.
CI consumes the account's included or already configured Actions quota; no extra
billing, paid plan, service or scheduled runner has been enabled by this migration.

## Give development instructions from your phone

Use a published **Codex Cloud** environment for this repository. The browser available
in this task currently shows the logged-out Codex landing page, so the environment
has not been created or published. The desktop conversation itself has not moved.

Open [Codex](https://chatgpt.com/codex) on the web and sign in with your account.
Choose **Work in > Cloud**, **Select environment**, **Create environment**, then select
the private **selective-options** repository. If GitHub access needs approval, review
the requested repository access yourself. Keep the environment private to you.

Give environment setup this instruction:

> Prepare this repository for Python 3.12.10 development. Run
> `bash tools/cloud_setup.sh` to install its locked dependencies. Restore the private
> release evidence using `.venv-cloud/bin/python tools/restore_github_evidence.py` with
> authorized GitHub access during setup. Run
> `.venv-cloud/bin/python tools/cloud_validate.py --output state/cloud-validation`.
> Report the exact test scope and excluded historical Windows cases. The complete
> historical proof check remains the separate GitHub verified-checkpoint workflow.
> Do not configure market or trading credentials, start continuous capture, or enable
> execution. Keep milestone 2 PARTIAL and read AGENTS.md before changing code.

Review setup/check results, then **Publish** and wait for **Environment published**.
From your phone, open Codex in the ChatGPT mobile app and select that environment.
You can then give development instructions, review changes and request follow-ups.
The repository's AGENTS.md, work log and scope manifest carry the project context
into each cloud workspace; no local desktop filesystem is assumed by the setup script.

These instructions follow the current official
[Codex Cloud quickstart](https://learn.chatgpt.com/docs/cloud) and
[cloud environment setup](https://learn.chatgpt.com/docs/environments/cloud-environments).
Account availability and authentication are separate from repository preparation.
No OpenAI API key is required by these repository build scripts.

## Portable validation and historical proof

`ci/cloud-test-scope.json` lists exact historical Windows cases excluded from the
portable suite and explains each exclusion. `tools/cloud_validate.py` records
that manifest, source commit, platform, actual collected/executed cases and result.
It rejects unexpected skips and missing required evidence. Preserve those historical
tests and native proof bytes; portable validation does not replace their full Windows
workflow or satisfy the specification's paper/statistical gates.

The cloud build retains JUnit, a machine-readable report, command logs and wheel
artifacts. The container check executes testnet configuration validation and verifies
that live arming refuses. No market account secrets are supplied to either workflow.

## Continuous collector hosting remains a separate deployment

You confirmed that you have no cloud hosting account and selected GitHub mobile build
controls. The current migration therefore prepares remote development and builds.
A continuously running collector/database has not been deployed.

That deployment needs an authorized Linux server/container host, persistent storage
for DuckDB and native archives, a backup destination, and operational checks for
restart, retention, clock health and data reconciliation. Existing `compose.yaml`
is a one-shot public probe; it is not a sustained collector service or a mobile
operations dashboard. A Codex task workspace or GitHub runner is temporary development
compute and should not be treated as the retained production database.

The container job checks UID 10001 and a named-volume write/read across two stopped
containers. The collector still needs one DuckDB writer, finite segment orchestration
and graceful SIGTERM handling before a continuously hosted service can be certified;
its existing stopped-checkpoint and cancellation paths do not establish that behavior.

The application retains its testnet defaults and mandatory live-execution refusal.
Moving build infrastructure does not complete missing feed/history coverage, the
seven-day all-feed run, strategy validation or the required 60 actual paper days
and 25 paper trades.
