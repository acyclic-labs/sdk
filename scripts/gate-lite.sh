#!/bin/bash
# Compile affected production and test targets; never execute test binaries.
set -u
exec 2>&1
started=$SECONDS
args=(); selected=()
full=0
# Use the existing metadata phase before compilation so full scope remains the
# original SDK set after on-demand fuzz, proof and generator packages join.
meta=$(cargo metadata --locked --no-deps --format-version 1) || {
  rc=$?; echo 'STEP_FAILED gate-metadata; recovery: cargo metadata --locked --no-deps --format-version 1'
  echo "GATE_DONE rc=$rc"; exit "$rc"
}
mode=$(tr -d '\r' < .gate-scope-mode 2>/dev/null) || mode=
case "$mode" in
  full|affected) ;;
  *) echo "STEP_FAILED gate-scope: invalid or missing .gate-scope-mode; recovery: printf 'full\\n' > .gate-scope-mode && bash scripts/gate-lite.sh"
     echo 'GATE_DONE rc=2'; exit 2;;
esac
if [ "$mode" = full ]; then
  full=1
  names=$(printf '%s' "$meta" | python3 scripts/gate-packages.py --defaults) || {
    rc=$?; echo "GATE_DONE rc=$rc"; exit "$rc"
  }
  while IFS= read -r p; do args+=(-p "$p"); selected+=("$p"); done <<< "$names"
  fmt_args=(--all)
else
  if [ ! -f .gate-tests-packages ] || [ ! -f .gate-clippy-packages ]; then
    echo "STEP_FAILED gate-packages: missing affected scope files; recovery: printf 'full\\n' > .gate-scope-mode && bash scripts/gate-lite.sh"
    echo 'GATE_DONE rc=2'; exit 2
  fi
  while IFS= read -r p; do
    [ -z "$p" ] || { args+=(-p "$p"); selected+=("$p"); }
  done < <(cat .gate-tests-packages .gate-clippy-packages | tr -d '\r' | sort -u)
  if [ ${#selected[@]} = 0 ]; then
    echo 'GATE_EMPTY no changed Cargo inputs'
    echo 'GATE_DONE rc=0'; exit 0
  fi
  fmt_args=("${args[@]}")
fi
echo "GATE_LITE_START scope=$mode crates=$((${#args[@]} / 2))"
if [ "$full" = 1 ]; then
  bash scripts/gate-rust-format.sh --stage; rc=$?
else
  cargo fmt "${fmt_args[@]}" -- --check; rc=$?
fi
if [ "$rc" != 0 ]; then
  if [ "$full" = 1 ]; then
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
# A Linux check never compiles cfg(windows) code: cross-check every selected
# crate whose own sources name a Windows cfg, derived from the workspace.
windows=()
if [ "$rc" = 0 ]; then
  # A metadata failure leaves the selector unparseable input: never a silent skip.
  list=$(printf '%s' "$meta" | python3 scripts/gate-windows-crates.py "${selected[@]}") || {
    rc=$?; echo "GATE_LITE_WINDOWS_SELECTION_FAILED rc=$rc"; }
  for p in $list; do windows+=(-p "$p"); done
fi
# --all-features never builds the narrower selections consumers compile: when
# the batch touched a crate's feature gating (.gate-feature-packages, written by
# the outer gate from the diff), check each consumer set the crate declares in
# [package.metadata.acyclic] consumer-features.
if [ "$rc" = 0 ] && [ -s .gate-feature-packages ]; then
  sets=$(printf '%s' "$meta" | python3 scripts/gate-feature-crates.py --sets .gate-feature-packages) || {
    rc=$?; echo "GATE_LITE_FEATURE_SELECTION_FAILED rc=$rc"; }
  while [ "$rc" = 0 ] && IFS=$'\t' read -r p features; do
    [ -n "$p" ] || continue
    echo "GATE_LITE_FEATURES_START crate=$p features=${features:-none}"
    cargo check --locked -p "$p" --no-default-features ${features:+--features "$features"} --lib --message-format=short
    rc=$?
    [ "$rc" = 0 ] || printf 'GATE_LITE_FEATURES_FAILED rc=%s; recovery: cargo check --locked -p %s --no-default-features%s --lib\n' \
      "$rc" "$p" "${features:+ --features $features}"
  done <<< "$sets"
fi
if [ "$rc" = 0 ] && [ ${#windows[@]} -gt 0 ]; then
  echo "GATE_LITE_WINDOWS_START target=x86_64-pc-windows-msvc crates=$((${#windows[@]} / 2)) features=all targets=all"
  python3 scripts/with-xwin-cache.py --workers 1 -- \
    cargo xwin check --locked --offline --target x86_64-pc-windows-msvc \
      "${windows[@]}" --all-features --all-targets --message-format=short
  rc=$?
  echo "GATE_LITE_WINDOWS_DONE rc=$rc"
fi
echo "GATE_LITE_WALL seconds=$((SECONDS-started)) rc=$rc"
echo "GATE_DONE rc=$rc"
exit "$rc"
