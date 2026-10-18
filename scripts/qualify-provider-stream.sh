#!/usr/bin/env bash
# The parser properties and checked-in corpus run in the providers lib suite; keep the
# cargo-fuzz entry point compiling too. Run from the repository root on the build box.
set -euo pipefail

# Every provider timer is created by deadline::timer, which admits it on the native ledger
# whenever there is one: no other production code may call a transport's timer methods, and
# only the platform transports may touch a platform timer.
src=rust/crates/harness-providers/src
production() {
  grep -rnE "$1" "$src" --include='*.rs' | grep -vE '_tests\.rs:|/tests\.rs:' || true
}
calls=$(production '\.delay(_with_native_work)?\(')
stray=$(grep -v "^$src/deadline.rs:" <<<"$calls" || true)
owned=$(grep -c "^$src/deadline.rs:" <<<"$calls" || true)
platform=$(production 'tokio::time::(sleep|timeout|interval)|futures_timer|gloo_timers|"setTimeout"' |
  grep -vE "^$src/(http\.rs|http/browser_delay\.rs):" || true)
# deadline.rs holds exactly the constructor's two calls and Bounded's two delegations.
if [[ -n $stray || -n $platform || $owned != 4 ]]; then
  printf 'unadmitted provider timer outside deadline::timer:\n%s\n%s\n(deadline.rs calls: %s, expected 4)\n' \
    "$stray" "$platform" "$owned" >&2
  exit 1
fi

cargo check --locked --manifest-path rust/crates/harness-providers/fuzz/Cargo.toml --bin provider_stream
