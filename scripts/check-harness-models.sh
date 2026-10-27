#!/usr/bin/env bash
set -euo pipefail

# Ordinary all-feature tests do not compile the Loom dependency composition.
# Each qualification compiles that composition and checks complete model counts.
core=1; facade=1
if (($#)); then
  if [ "$#" != 2 ] || [ "$1" != --packages-file ] || [ ! -r "$2" ]; then
    echo 'error: expected --packages-file READABLE_PACKAGE_LIST' >&2
    exit 2
  fi
  core=0; facade=0
  while IFS= read -r package || [ -n "$package" ]; do
    package=${package%$'\r'}
    case "$package" in
      acyclic-harness-core) core=1;;
      acyclic-harness) facade=1;;
    esac
  done < "$2"
fi
echo "GATE_HARNESS_LOOM_SCOPE core=$core facade=$facade"
if [ "$facade" = 1 ]; then
  node scripts/qualify-mcp-loom.mjs --harness-models --all-features
fi
