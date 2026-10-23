#!/usr/bin/env bash
# Build binaries/dependencies before invoking this runtime qualification.
set -euo pipefail
if [ "$#" -ne 2 ]; then
  echo 'usage: qualify-graphcoder-e2e.sh GRAPHСODER MOCK_MODEL' >&2
  exit 2
fi
node --test scripts/graphcoder-e2e/*.test.mjs
node scripts/graphcoder-e2e/run.mjs "$1" "$2"
