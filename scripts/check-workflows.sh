#!/usr/bin/env bash
set -euo pipefail

actionlint_command="${1:-actionlint}"
if (($# > 0)); then shift; fi
if ! shellcheck_path=$(command -v shellcheck) || [[ ! -f "$shellcheck_path" || ! -x "$shellcheck_path" ]]; then
  echo 'shellcheck is required for workflow validation; install it and add it to PATH' >&2
  exit 1
fi
if (($# == 0)); then set -- .github/workflows/*.yml; fi
for workflow in "$@"; do
  [[ "$workflow" != -* ]] || { echo 'expected workflow paths, not actionlint options' >&2; exit 1; }
done
"$actionlint_command" -shellcheck "$shellcheck_path" "$@"
