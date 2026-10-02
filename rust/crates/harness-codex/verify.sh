#!/usr/bin/env bash
# Phase A gates for acyclic-harness-codex. See DESIGN.md "Verification".
#
#   verify.sh unit     fmt, clippy (CI flags), and every test that should pass now
#   verify.sh status   acceptance board: which pending (#[ignore]d) tests pass yet
#   verify.sh e2e      the A8 tests against the real pinned Codex (no OpenAI calls)
#   verify.sh all      unit + status; exits non-zero if an un-ignored test fails
#
# A task is done when its acceptance tests pass under `status` and their
# #[ignore] is removed, so `unit` runs them from then on.
set -euo pipefail

crate=acyclic-harness-codex
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
cd "$root"

unit() {
  cargo fmt -p "$crate" --check
  cargo clippy -p "$crate" --all-targets --all-features --locked -- -D warnings
  cargo test -p "$crate" --all-features --locked
}

# Runs each pending test alone and prints PASS / pending per task.
status() {
  local file name task result passed=0 pending=0
  cargo test -q -p "$crate" --no-run --locked 2>/dev/null
  printf '%-6s %-9s %s\n' TASK RESULT TEST
  for file in proxy mcp executor; do
    while IFS= read -r line; do
      name=${line%%: test}
      task=$(grep -B1 -A1 "fn $name" "$here/tests/$file.rs" | grep -o 'ignore = "A[0-9]' | head -1 | cut -d'"' -f2)
      [ -n "$task" ] || continue
      if cargo test -q -p "$crate" --test "$file" -- --ignored --exact "$name" >/dev/null 2>&1; then
        result=PASS; passed=$((passed + 1))
      else
        result=pending; pending=$((pending + 1))
      fi
      printf '%-6s %-9s %s::%s\n' "$task" "$result" "$file" "$name"
    done < <(cargo test -q -p "$crate" --test "$file" -- --ignored --list 2>/dev/null | grep ': test$')
  done
  echo "acceptance: $passed passing, $pending pending (PASS rows are ready to un-ignore)"
}

e2e() {
  local bin=${ACYCLIC_CODEX_BIN:-}
  if [ -z "$bin" ]; then
    npm ci --prefix plugin/tests/hosts --ignore-scripts --silent
    local out
    out=$(mktemp)
    GITHUB_OUTPUT=$out node plugin/scripts/resolve-qualification-host.mjs codex >/dev/null
    bin=$(sed -n 's/^binary=//p' "$out")
    rm -f "$out"
  fi
  "$bin" --version | grep -q "0.155.1" || { echo "expected codex 0.155.1 at $bin" >&2; exit 1; }
  # Codex must reach only our 127.0.0.1 proxy; no real key is ever needed.
  env -u OPENAI_API_KEY -u CODEX_API_KEY ACYCLIC_CODEX_BIN="$bin" \
    cargo test -p "$crate" --test e2e --locked -- --ignored --nocapture
}

case "${1:-all}" in
  unit) unit ;;
  status) status ;;
  e2e) e2e ;;
  all) unit && status ;;
  *) sed -n '2,10p' "$0"; exit 2 ;;
esac
