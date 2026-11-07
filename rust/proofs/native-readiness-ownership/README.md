# Original native readiness ownership

The production readiness pool opens one native queue and starts one producer
per available CPU before spawning a child. Unix descriptor/PID identity selects
its registry directly. A Windows pipe retains the producer whose completion
port originally associated its handle. Exit callback tokens advance by the
immutable pool width, so the callback can recover that same producer after its
registration is unregistered, without borrowing or allocating callback state.
Tokens have the native pointer width and refuse exhaustion before zero or the
reserved wake value can be emitted.

Each native registration owns its original producer, token, source claim and
native release. A partial admission rolls back after unlocking that producer.
Closing or transferring a Unix source removes only its original keyed claim;
dropping an older observation cannot remove a successor. Multiple sources share
one `Signal`, so readiness, deadline, cancellation and producer loss retain one
terminal winner. Producer failure settles its own live entries and refuses new
spawns; unrelated producers continue servicing their existing sources.

Startup has one condition-variable event. The startup guard publishes either
running or aborted. On a partial thread-start refusal, every already-created
thread observes aborted before entering its native wait, then releases its
original pool reference. The original spawn error is returned and retained.
No replacement producer or readiness probe is started.

Group-member exit registration also requires an original producer. Darwin can
return `ESRCH` before a removed process becomes a zombie. A non-child cannot
be settled with `waitid`, so this path returns that native refusal, keeps tree
custody and never returns an empty queue for an unbounded wait. The direct-child
path retains its existing terminal wait under exclusive unreaped PID custody.

## Production controls

Run from the repository root:

```sh
cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib \
  -E 'test(process_tree::events::) | test(=process_tree::exit_wait::tests::a_missing_exit_registration_settles_the_owned_child_before_returning)'
```

The ownership property opens actual native queues, registers both ends of a
real socket pair, dispatches their original native packets, forces original
producer retirement, checks the first terminal cause and services the unrelated
producer. Its isolated process prevents unrelated descriptor allocation from
changing the chosen two-producer topology. The same `Pool::open`, `Pool::arm`,
registration, dispatch, retirement and `Signal` implementations are shipped.
Existing public descriptor and real-child controls exercise the automatically
sized service, close/transfer/reuse, partial admission and native refusal paths.

The property counts actual registry acquisitions at one and 100,000 live
deadlines: admitting an independent source takes one original registry lock,
withdrawing it takes one more, and neither visits the other producer. Startup
abort is checked separately on every platform with actual producer threads and
the production condition-variable event. Its rescue releases those threads if
the omission assertion fails, so the negative control ends without a hang.

| Mutation | Exact property suffix | Required failure |
| --- | --- | --- |
| `central-registry.patch` | `original_producers_retire_independently_of_other_sources_and_history` | `distinct native sources share one registry` |
| `unrelated-callback-owner.patch` | `original_producers_retire_independently_of_other_sources_and_history` | `native packet lost its original producer` |
| `retained-cancelled-entry.patch` | `original_producers_retire_independently_of_other_sources_and_history` | `cancelled source retained a registry entry` |
| `startup-without-abort.patch` | `startup_abort_releases_original_native_producers` | `startup refusal retained a producer without a start event` |
| `missing-member-producer.patch` (macOS) | `a_missing_exit_registration_settles_the_owned_child_before_returning` | `missing group-member registration created an empty exit watch` |

The member-registration control extends the existing real-child property. It
retains an actual exited, unreaped child to prevent PID reuse, calls the shipped
member registration against that exact Darwin process, and requires `ESRCH`,
two original read attempts, one prepaid queue close and retained tree custody.
It does not claim that Linux executes the Darwin branch. The former Boolean
helper matrix is deleted: actual registration, native refusal and child terminal
properties cover those paths.

For each mutation, apply the patch, run its exact production property, require
a nonzero test result containing the named assertion and exactly one failed
test, reverse the patch, then require that same property to pass. A compiler
failure, signal or empty test selection is not an intended negative. Restore
the patch on every exit before proceeding to another control.

For example, the startup omission is checked on every supported platform:

```sh
(
  set -eu
  patch=rust/proofs/native-readiness-ownership/startup-without-abort.patch
  test=process_tree::events::pool::tests::startup_abort_releases_original_native_producers
  log=$(mktemp)
  applied=false
  restore() {
    if [ "$applied" = true ]; then
      git apply -R "$patch" || {
        printf 'STEP_FAILED restore startup control; recovery: git apply -R "%s"\n' "$patch" >&2
        return 1
      }
      applied=false
    fi
  }
  trap 'restore && rm -f "$log"' EXIT
  git apply "$patch" || {
    printf 'STEP_FAILED apply startup control; recovery: git apply --check "%s"\n' "$patch" >&2
    exit 1
  }
  applied=true
  if cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib \
    -E "test(=$test)" >"$log" 2>&1; then result=0; else result=$?; fi
  cat "$log"
  restore
  if [ "$result" -eq 0 ] ||
    ! grep -F 'startup refusal retained a producer without a start event' "$log" ||
    ! grep -E 'Summary.*1 test run: 0 passed, 1 failed' "$log"; then
    printf 'STEP_FAILED intended startup negative; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s)"\n' "$test" >&2
    exit 1
  fi
  cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib \
    -E "test(=$test)" || {
    printf 'STEP_FAILED restored startup property; recovery: cargo nextest run --locked -p acyclic-native-runtime --features test-support --lib -E "test(=%s)"\n' "$test" >&2
    exit 1
  }
)
```

## Cost and limits of the claim

Source selection is one indexed lookup; admission and withdrawal touch only
that original producer. One-source waits use one `Signal` allocation instead
of a `Signal` plus a registration vector. Deadline-only waits retain one
allocation. Multiple-source waits use one `Signal` plus one pre-sized vector
for the remaining registrations, and one live registry entry per source rather
than one entry for the whole wait. Each entry requires one admission and one
withdrawal lock; no operation visits all producers. A deadline-only selection
adds one relaxed atomic increment. No source descriptor or payload is copied,
and no new round trip or durability barrier is introduced. Native registration
and earlier-deadline notification syscalls are unchanged per admitted source.
The missing-member refusal adds no syscall, allocation, copy, wait or scan: it
checks the already-returned registration result and releases the same queue.
It removes the possible unbounded wait after a refused native registration.

The shared startup latch and pool allocation are paid once. Native queues and
threads grow from one to the number of available CPUs, independently of agents
or wait history. Batch dispatch and terminal delivery remain proportional to
the entries delivered. Hash indexes retain their amortized cost, and the
existing `BTreeSet` deadline ordering remains logarithmic. These controls prove
constant original-owner lookup and a fixed number of registry acquisitions;
they do not claim worst-case constant allocation, hashing or deadline work.
Native Windows and macOS execution remain separate platform obligations.
