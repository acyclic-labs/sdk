#!/usr/bin/env bash
set -euo pipefail
# Keep linker selection when qualification supplies its own RUSTFLAGS.
exec clang -fuse-ld=mold "$@"
