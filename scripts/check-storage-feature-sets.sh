#!/usr/bin/env bash
set -euo pipefail

# Every published crate alone, then each distinct selection declared by its
# workspace consumers. No package unification or dev dependencies may hide a
# missing feature gate. The serial checks share one Cargo target directory.
# Also check real downstream workspaces, with defaults alone and with no
# features, so no SDK test dependency can enable native-watch on their behalf.
node "$(dirname "$0")/check-feature-matrix.mjs" --consumers "$@"
