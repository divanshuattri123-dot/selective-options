#!/usr/bin/env bash
# Run from any directory. Optional restoration contacts GitHub only.
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$project_dir"
task_python="${CLOUD_PYTHON:-python3}"
task_venv="${CLOUD_VENV:-.venv-cloud}"
restore_evidence=false
if [[ "${1:-}" == "--restore-evidence" ]]; then
  restore_evidence=true
elif [[ $# -ne 0 ]]; then
  printf 'Usage: bash tools/cloud_setup.sh [--restore-evidence]\n' >&2
  exit 2
fi
"$task_python" -c 'import sys; assert sys.version_info[:2] == (3, 12), "Use Python 3.12 to match the checkpoint dependency locks"'
"$task_python" -m venv "$task_venv"
task_python="$task_venv/bin/python"
export PIP_DISABLE_PIP_VERSION_CHECK=1
"$task_python" -m pip install -r requirements.lock -r ci/requirements-dev.lock
"$task_python" -m pip install --no-deps -e .
"$task_python" -m selective_options.cli check-config
if "$restore_evidence"; then
  # Requires normal GitHub credentials or GITHUB_TOKEN/GH_TOKEN for this private repo.
  "$task_python" tools/restore_github_evidence.py
fi
printf '\nReady: %s tools/cloud_validate.py\n' "$task_python"
