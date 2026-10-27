#!/usr/bin/env bash
# Build binaries/dependencies before invoking this runtime qualification.
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo 'usage: qualify-graphcoder-e2e.sh GRAPHCODER MOCK_MODEL' >&2
  exit 2
fi
node scripts/graphcoder-e2e/helpers.mjs
node scripts/qualify-web-lifecycle.mjs "$1"
node scripts/qualify-fetch-cancellation.mjs "$1"
node scripts/graphcoder-e2e/serial.mjs "$1" "$2"
