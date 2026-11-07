# Original wall-clock alarm cuts

Wall alarms use the same `acyclic_collections::FixedIndex` as process readiness.
The key is the unsigned big-endian deadline followed by the original registration
token, eight bytes each. Its lexical order is exactly `(deadline, token)` order.
An inclusive cut at `(now, u64::MAX)` therefore selects every elapsed original
owner, including equal deadlines and both clock endpoints, without arithmetic
overflow. Clock reversal leaves already delivered owners retired.

`extract_through` is retained-owner cleanup in the shared collection. Its first
descent follows the cut to one original leaf and finds their first differing
coordinate. A subtree below that coordinate is entirely on the leaf's side of
the cut. Only earlier branch coordinates can straddle it. The second descent
splits that original path, retaining untouched siblings and reusing the original
branches on each side. Branch coordinates strictly increase, so both descents
end within the key width, independently of either population. Splitting one tree
into two nonempty trees makes exactly one branch redundant. Its original backing
and funding stay in the extracted owner until consumption outside the lock.
No original payload is cloned or reconstructed, and both roots retain the
original search invariants. Empty and zero-width indexes use the same mechanism.

The shared prefix property now generates ordered cuts too. It audits both real
roots against a `BTreeMap` after extraction, including original values, deferred
funding, cancellation of the issuing admission, partial-consumer withdrawal and
allocation-free retirement. Physical allocation accounting covers a mixed cut;
deep cuts include every branch coordinate, and owned traversal covers widths
zero, four and forty. The existing retirement cursor serves both extraction
forms; there is no second traversal implementation.

The native property runs generated histories through the production alarm
index, retains each original callback by strong reference, and compares actual
delivery, cancellation, order and next deadline with an ordered oracle. Every
history includes exact clock endpoints, cancellation and a backward clock cut.
It counts actual shared-index work. Existing native quiet-expiry, clock-change
driver and original-producer-loss properties retain the wait, failure, callback
unwinding and late-registration obligations. The index adds no wait or producer:
the original timerfd, kqueue/clock notification or waitable timer produces expiry;
loss settles pending owners with the original typed native cause.

## Negative controls

Run from the repository root. Every patch modifies the production operation,
not its oracle or a separate model. The population mutation really traverses
the original index; it does not invent counter increments. Require the specified
failure, restore the source even on failure, then run the restored properties.

```bash
set -eu
alarm=wall_alarm::tests::alarm_histories_preserve_exact_owners_at_clock_cuts_without_scanning
prefix=tests::prefix::prefix_extraction_preserves_exact_keys_original_custody_and_bounded_work
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
  patch=rust/proofs/native-wall-alarm-index/$1
  package=$2
  property=$3
  assertion=$4
  git apply "$patch" || {
    printf 'STEP_FAILED mutation; recovery: git apply --check "%s"\n' "$patch" >&2
    exit 1
  }
  active_patch=$patch
  log=$(mktemp) || {
    printf 'STEP_FAILED control log; recovery: mktemp\n' >&2
    exit 1
  }
  if [ "$package" = acyclic-native-runtime ]; then set -- --features test-support; else set --; fi
  if cargo nextest run --locked -p "$package" "$@" --lib -E "test(=$property)" >"$log" 2>&1; then result=0; else result=$?; fi
  cat "$log"
  restore
  if [ "$result" -eq 0 ] || ! grep -F "$assertion" "$log" || ! grep -E 'Summary.*1 test run: 0 passed, 1 failed' "$log"; then
    printf 'STEP_FAILED intended negative; recovery: inspect the original named test "%s" and restore "%s" before changing source or command\n' "$property" "$patch" >&2
    exit 1
  fi
  rm "$log"
}
negative unordered-alarm-time.patch acyclic-native-runtime "$alarm" 'alarm clock cut lost its next producer deadline'
negative retained-cancelled-alarm.patch acyclic-native-runtime "$alarm" 'alarm clock cut stranded or prematurely delivered an owner'
negative stranded-clock-endpoint.patch acyclic-native-runtime "$alarm" 'alarm clock cut stranded or prematurely delivered an owner'
negative live-population-scan.patch acyclic-native-runtime "$alarm" 'alarm clock cut scanned live owners'
negative excluded-original-cut.patch acyclic-collections "$prefix" 'prefix extraction differs from complete original keys'
negative premature-cut-retirement.patch acyclic-collections "$prefix" 'extraction retired original backing'
cargo nextest run --locked -p acyclic-collections --lib -E "test(=$prefix)" || {
  printf 'STEP_FAILED restored collection; recovery: inspect the original named prefix property before changing source or command\n' >&2
  exit 1
}
cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E 'test(wall_alarm::tests::) | test(=process_tree::events::deadlines::tests::deadline_histories_preserve_native_order_and_bounded_retirement)' || {
  printf 'STEP_FAILED restored native properties; recovery: inspect the original alarm/deadline property output before changing source or command\n' >&2
  exit 1
}
```

## Required work and area cleanup

The previous comparison tree had population-dependent admission, cancellation
and split paths. Its due batch also allocated a vector and moved all callbacks
into it under the index lock. The fixed-width index admits at most two nodes
per new alarm, versus amortized comparison-tree node backing before; this is a
backing tradeoff, not an allocation reduction on admission. Cancellation and
lookup allocate zero nodes. They examine at most `17N + 1` and `8N + 1` items;
admission stays within `25N + 3`, with `N = 16`.

An ordered extraction examines at most `17N + 3` nodes/key bytes. Together with
the next-deadline lookup, each alarm cut examines at most `25N + 4`, with zero
allocations, callback copies, reference increments or backing destruction under
the lock. The old batch vector allocation and its callback moves are removed.
Outside the lock the shared cursor retires only the extracted nodes, in linear
total work and constant space, retaining each yielded leaf's original funding.
Thus extraction is O(1) for fixed key width and delivery is O(1) amortized per
due owner, with no work proportional to unrelated future owners.

The fixed time/token key is constructed once at admission and once per clock
cut; cancellation reconstructs its fixed-width original key. The existing
timer rearm, clock read, one registration lock and one driver lock per cut are
unchanged. Added syscalls, durability barriers and round trips are zero. Original
terminal-failure delivery uses the same owned cursor. Process readiness and wall
alarms now share the host index work adapter; release builds retain no test
counters. The private comparison tree, callback batch vector and redundant
private-state example are deleted. No wire decoder or persisted format changes.

These properties qualify their exact executed source and platform. Native
Windows and macOS execution and full platform acceptance remain separate checks.
