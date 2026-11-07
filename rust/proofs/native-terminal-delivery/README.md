# Native terminal delivery during consumer unwinding

Readiness wakers and wall-clock alarm callbacks use the same owned-iterator
drain in `native-runtime/src/delivery.rs`. Outcomes leave the original registry
or index before delivery, and callbacks run outside those locks. The iterator
owns the remaining deliveries until each callback has been invoked.

Normal callbacks run directly. If a callback unwinds, the iterator's drop guard
invokes each remaining callback inside `catch_unwind`, preserving the original
unwind. If delivery begins during producer unwinding, it enters that guarded
drain immediately. A later callback unwind cannot escape the cleanup guard and
abort delivery to the remaining original consumers. This covers readiness
settlement and all three alarm paths: due callbacks, native wait refusal, and
producer loss. It assumes callbacks return or unwind; an aborting or blocking
callback cannot be recovered by in-process Rust unwinding.

The existing readiness property now registers two panicking wakers followed by
its healthy original waiter. It checks the first unwind and the retained native
failure on the surviving waiter. The existing alarm property retains its real
closed-native-timer cases and adds a panicking callback before the surviving
callback during producer unwinding. It checks both the original unwind and the
survivor's terminal error. These call the shipped settlement and alarm driver,
including their actual cleanup. They do not establish deployed multi-process
or cross-platform acceptance by themselves.

The two properties run in owned child test processes. A double unwind aborts
that child, so the parent's OS process wait returns a named failure instead of
aborting the test runner or waiting for a callback whose producer is gone.
The shared child helper also replaces the readiness ownership property's
previous private copy. No readiness probes or timeout retries are involved.

## Negative controls

Run from the repository root on Linux. Each mutation changes the actual shared
drain. Require the named runtime failure and exactly one failed selected test;
a compile error or empty selection does not qualify. Restore each mutation and
then run both original properties together. Other platforms retain their own
native qualification obligations.

```bash
set -eu
ulimit -c 0
readiness=process_tree::events::tests::native_wait_failure_settles_live_waits_and_producer_loss_is_terminal
alarm=wall_alarm::tests::alarm_producer_loss_retains_native_failure_and_refuses_late_registration
active_patch=
restore() {
  if [ -n "$active_patch" ]; then
    git apply -R "$active_patch" || {
      printf 'STEP_FAILED restore; recovery: git apply -R "%s"\n' "$active_patch" >&2
      return 1
    }
    active_patch=
  fi
}
trap restore EXIT
negative() {
  patch=rust/proofs/native-terminal-delivery/$1
  property=$2
  git apply "$patch" || {
    printf 'STEP_FAILED mutation; recovery: git apply --check "%s"\n' "$patch" >&2
    exit 1
  }
  active_patch=$patch
  log=$(mktemp) || {
    printf 'STEP_FAILED control log; recovery: mktemp\n' >&2
    exit 1
  }
  if cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=$property)" >"$log" 2>&1; then result=0; else result=$?; fi
  cat "$log"
  restore
  if [ "$result" -eq 0 ] || ! grep -F 'native terminal delivery lost remaining consumers' "$log" || ! grep -E 'Summary.*1 test run: 0 passed, 1 failed' "$log"; then
    printf 'STEP_FAILED intended negative; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s)"\n' "$property" >&2
    exit 1
  fi
  rm "$log"
}
negative escaping-cleanup-unwind.patch "$readiness"
negative unguarded-producer-unwind.patch "$alarm"
cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=$readiness) | test(=$alarm)" || {
  printf 'STEP_FAILED restored properties; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s) | test(=%s)"\n' "$readiness" "$alarm" >&2
  exit 1
}
```

## Required work, progress, and area cleanup

One original iterator item is consumed per callback. The drain introduces no
lookup, scan of unrelated registrations, wait, queue, thread, allocation, payload
copy, syscall, round trip, durability barrier, lock, or additional callback
clone. Normal delivery adds one `thread::panicking()` check per batch; callbacks
remain direct. Exceptional delivery has one unwind boundary per remaining
callback. Both paths have O(1) drain work per consumed item and O(deliveries)
total work. The iterator and guard use stack storage. Compiler unwinding and
user callback costs are separate; no wall-clock or allocation reduction is
claimed for those.

The retained iterator is the producer of each pending delivery; no additional
event wait is added. The original waker or callback still produces its owner's
completion. The two negative controls remove the two unwind boundaries and
must lose the process before the survivor can complete. Neither guard changes
the first terminal outcome stored by readiness or the first native alarm
failure. The private readiness cleanup loop and three unguarded alarm loops
are replaced by this one implementation; duplicated child isolation is removed.
No decoder, wire format, persisted format, or new example-only test is added.
