#!/bin/bash
# Compile affected production and test targets; never execute test binaries.
set -u
exec 2>&1
started=$SECONDS
args=()
if [ "$(tr -d '\r' < .gate-scope-mode)" = full ]; then
  args=(--workspace)
  fmt_args=(--all)
else
  while IFS= read -r p; do [ -z "$p" ] || args+=(-p "$p"); done < <(cat .gate-tests-packages .gate-clippy-packages | tr -d '\r' | sort -u)
  fmt_args=("${args[@]}")
fi
echo "GATE_LITE_START scope=$(tr -d '\r' < .gate-scope-mode) crates=$((${#args[@]} / 2))"
if [ "${args[0]:-}" = --workspace ]; then
  bash scripts/gate-rust-format.sh --stage; rc=$?
else
  cargo fmt "${fmt_args[@]}" -- --check; rc=$?
fi
if [ "$rc" != 0 ]; then
  if [ "${args[0]:-}" = --workspace ]; then
    printf 'GATE_LITE_FORMAT_FAILED rc=%s; recovery: format the reported files and rerun bash scripts/gate-rust-format.sh\n' "$rc"
  else
    printf 'GATE_LITE_FORMAT_FAILED rc=%s; recovery: cargo fmt' "$rc"
    printf ' %q' "${fmt_args[@]}"
    printf '\n'
  fi
  echo "GATE_LITE_WALL seconds=$((SECONDS-started)) rc=$rc"
  echo "GATE_DONE rc=$rc"
  exit "$rc"
fi
# The job-owned target plus the host sccache authority keeps rebuilds warm.
cargo check --locked --all-features --bins --tests --message-format=short "${args[@]}"; rc=$?
if [ "$rc" != 0 ]; then
  printf 'GATE_LITE_COMPILE_FAILED rc=%s; recovery: cargo check --locked --all-features --bins --tests --message-format=short' "$rc"
  printf ' %q' "${args[@]}"
  printf '\n'
fi
echo "GATE_LITE_WALL seconds=$((SECONDS-started)) rc=$rc"
echo "GATE_DONE rc=$rc"
exit "$rc"
