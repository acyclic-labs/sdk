#!/usr/bin/env bash
# Build binaries/dependencies before invoking this runtime qualification.
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo 'usage: qualify-graphcoder-e2e.sh GRAPHCODER MOCK_MODEL' >&2
  exit 2
fi
node scripts/node-tests.mjs --only scripts/graphcoder-e2e
if [ -n "${E2E_OUTPUT:-}" ]; then
  output=$E2E_OUTPUT
  E2E_OUTPUT="$output/web-lifecycle" node scripts/qualify-web-lifecycle.mjs "$1"
  E2E_OUTPUT="$output/fetch-cancellation" node scripts/qualify-fetch-cancellation.mjs "$1"
  E2E_OUTPUT="$output/matrix" node scripts/graphcoder-e2e/serial.mjs "$1" "$2"
else
  node scripts/qualify-web-lifecycle.mjs "$1"
  node scripts/qualify-fetch-cancellation.mjs "$1"
  node scripts/graphcoder-e2e/serial.mjs "$1" "$2"
fi
