#!/usr/bin/env bash
set -euo pipefail
# Keep linker selection when qualification supplies its own RUSTFLAGS.
# rustc may add -fuse-ld=lld; the final driver option selects mold.
exec clang "$@" -fuse-ld=mold
