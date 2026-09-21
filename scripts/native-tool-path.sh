#!/usr/bin/env bash
set -euo pipefail

[[ $# == 1 && -n "$1" ]] || { echo 'usage: native-tool-path.sh PATH' >&2; exit 2; }
if command -v cygpath >/dev/null 2>&1; then
  cygpath -m "$1"
elif command -v wslpath >/dev/null 2>&1; then
  wslpath -m "$1"
else
  printf '%s\n' "$1"
fi
