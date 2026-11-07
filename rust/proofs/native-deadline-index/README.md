# Native deadline ordering and retirement

Process readiness uses the shared `acyclic_collections::FixedIndex`. Its
strictly increasing branch coordinates bound traversal by key width, rather
than by the number of live waits. There is no second radix implementation.

The production adapter captures one original `Instant` per producer. For a
deadline `t`, its time component is `2^127 + (t - origin)` on or after the
origin, and `2^127 - (origin - t)` before it, with differences measured in
nanoseconds using checked native `Duration` arithmetic. A `Duration` has fewer
than 95 nanosecond bits, so these operations fit in `u128`. Unsigned big-endian
time bytes followed by the original native-width token preserve the order of
`(Instant, token)`, including equal deadlines and times before the origin.
There is no rounding, opaque-clock transmutation, periodic tick, or preset
time horizon. An unrepresentable native difference is a typed refusal before
deadline publication; the original registration rolls back through its owner.

The index stores the original `Instant` for the existing native timeout and
dispatch cut. Each armed entry retains its exact admitted key. Cancellation
removes that key without allocation, fresh admission or clock conversion.
The original producer, its retained wake and its terminal failure continue to
produce progress; these changes add no wait or producer.

`deadline_histories_preserve_native_order_and_bounded_retirement` exercises
the actual registration, cancellation and dispatch code against a standard
ordered-set oracle over real native `Instant`s. Histories include both sides
of the captured origin and all native token bytes. After every dispatch cut,
the original futures must retain the correct pending or deadline outcome.
The existing two-producer native-socket property also counts the actual index
work at one and 100,000 live waits, alongside its original ownership checks.
The generic index's branch, original-custody and ordered-history properties
remain in the collections crate.

For `N = 16 + size_of::<usize>()` key bytes, a first-entry lookup examines at
most `8N + 1` items. Deadline admission, including that lookup, examines at
most `25N + 3`; cancellation examines at most `17N + 1`. The counters are
supplied directly to the shipped index, rather than to a parallel model.
Hash-map operations elsewhere retain their amortized bounds. Dispatch and
producer retirement necessarily visit the entries actually delivered.

## Negative controls

Run from the repository root. Each mutation must fail the exact original
property and its named assertion; compilation errors and zero selected tests
do not qualify. Restore every mutation, then require the property to pass.
The population-scan control deliberately walks the actual shared index; use
the bounded generated histories below, not the 100,000-wait property.

```bash
set -eu
property=process_tree::events::deadlines::tests::deadline_histories_preserve_native_order_and_bounded_retirement
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
  patch=rust/proofs/native-deadline-index/$1
  assertion=$2
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
  if [ "$result" -eq 0 ] || ! grep -F "$assertion" "$log" || ! grep -E 'Summary.*1 test run: 0 passed, 1 failed' "$log"; then
    printf 'STEP_FAILED intended negative; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s)"\n' "$property" >&2
    exit 1
  fi
  rm "$log"
}
negative reversed-past-clock.patch 'deadline order or original cancellation diverged'
negative retained-cancelled-deadline.patch 'cancelled deadline retained native wait ownership'
negative population-scan.patch 'deadline admission scanned the live population'
cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=$property)" || {
  printf 'STEP_FAILED restored property; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s)"\n' "$property" >&2
  exit 1
}
```

## Required work and costs

The previous comparison tree had population-dependent paths and amortized
node allocation. The fixed-width index admits at most two node allocations
per new deadline (one for its first entry), and none on cancellation or lookup.
This deliberately states the additional backing cost rather than claiming an
allocation reduction. One source-only wait adds no index allocation. A wait's
original `Signal` and registration-vector allocations are unchanged.

Each admission constructs one fixed-width time/token key and copies it into
the shared index and original armed entry. The armed entry replaces its old
optional `Instant` with that key. The index retains the original time/token
value. There are no payload copies or extra per-operation native calls, clock
reads, locks, round trips or durability barriers. Capturing the clock origin
costs one clock read per producer at startup. Keyed cancellation performs no
clock read or conversion. Earlier-deadline native notification is unchanged.

The private comparison tree and its stale-entry recovery branch are removed.
Existing native deadline and ownership properties are extended; no decoder,
persisted format, timer thread or independent scheduler is added. Native
Windows and macOS execution remains a separate platform obligation.
