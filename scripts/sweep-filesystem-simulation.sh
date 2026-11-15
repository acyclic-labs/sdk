#!/usr/bin/env bash
set -euo pipefail

# Explicit long-running target; the ordinary test retains its small seed set.
: "${DST_SEEDS:?set the seed range, for example 0..10000}"
export DST_SEEDS
exec nice -n 19 cargo nextest run --locked -p acyclic-fs --features test-support \
  --lib -E 'test(seeded_)' --no-tests=fail
