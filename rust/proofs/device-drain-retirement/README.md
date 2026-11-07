# Original device drain retirement

The existing
`device_flush::tests::failed_and_unwound_drains_release_original_lock_without_acknowledging`
property exercises the production coalescer and native file lock. A real native
error or an unwinding drain must release both original owners without advancing
durability completion. A subsequent request must perform its own covering drain.
The same property also admits a concurrent waiter before its producer unwinds.

Run from the repository root:

```sh
cargo test --locked -p acyclic-native-runtime --lib device_flush::tests::failed_and_unwound_drains_release_original_lock_without_acknowledging -- --exact --nocapture
```

`mutation.patch`, generated from the production `Draining::drop`, omits the
release of the original in-flight state. Apply it and run the same property:

```sh
git apply rust/proofs/device-drain-retirement/mutation.patch
```

The negative must fail at `original drain ended without releasing its in-flight
state`. The property inspects that state before another acquisition, so this
mutation cannot strand the test on the abandoned producer. A compiler error,
another assertion, a timeout or zero selected tests does not qualify. Restore
the exact source and require the same property to pass:

```sh
git apply -R rust/proofs/device-drain-retirement/mutation.patch
```

`acknowledge-failure.patch` independently removes the successful-result condition
from the original destructor's completion publication. Run the same property
with that patch alone. It must fail at `unsuccessful original drain acknowledged
durability`, then pass after exact restoration:

```sh
git apply rust/proofs/device-drain-retirement/acknowledge-failure.patch
git apply -R rust/proofs/device-drain-retirement/acknowledge-failure.patch
```

No new test or synchronization primitive is added. The
original drain owns its completion condition: its scoped destructor clears the
in-flight state under the original mutex and notifies one successor on return or
unwind. Each waiter can then issue its own drain; an unsuccessful predecessor
cannot certify its writes. The existing property checks that native lock custody
also ends before reacquisition and that the next request drains once.

The retained negatives qualify the mapped release and completion obligations.
They do not independently mutate notification, establish every concurrency interleaving,
measure population cost or qualify a physical Apple device flush. Production
bookkeeping remains fixed per drain; notification work depends on actual waiters.
Syscalls, barriers, round trips, allocations and copies are unchanged. The
property and this negative use the same production coalescer, rather than a
parallel state machine.


The same property now starts the production coalescer at ordinary and terminal
counter generations. Native lock exclusion, real I/O failure, unwind, release
and successful subsequent requests take the same code path in both cases.
The mapped-counter property
`device_flush::apple::tests::processes_share_a_drain_that_started_after_their_ticket`
also seeds the two real shared mappings immediately before exhaustion. The last
representable ticket can still share its covering drain; every newly requested
operation after that point must execute its own drain.

Bug class: unchecked generation arithmetic can panic or wrap and acknowledge
stale completion. Census:
`rg -n 'started.*\+|ticket\(\)|settle\(' rust/crates/native-runtime/src/device_flush.rs`.
All four increments now use checked next tickets and saturating drain numbers.
An absent next ticket prevents coalescing; requests remain unlimited. No counter
reset, file-format change, alternate owner or new error is introduced.

The retained `unchecked-counters.patch` reinstates all four original unchecked
increments with the optional ticket representation preserved so it compiles.
The local property must fail at `exhausted drain counters must keep successful
requests executing`. It qualifies the original overflow before the repair.
The independent `exhausted-ticket.patch` instead falsely treats the saturated
last number as a new covering ticket. The local property must fail at
`exhausted drain counters reused a stale completion`. Apply each alone and
restore it before running the same property green. On native macOS the mapped
property must also reject the latter at `exhausted shared counters reused a
stale completion`. A compilation failure or an unrelated panic is not evidence.

These controls execute the production coalescer or its actual mapped-counter
adapter, with the original native file lock; their drain callbacks do not
qualify physical device I/O. Native macOS execution is required for the mapped
branch. Integer safety does not establish every scheduling interleaving or
cross-process failure topology. The existing drain guard and OS lock remain the
only producers for their waits; error and unwind still release them without
publishing success. Two unchecked scalar increments become one checked ticket
and one saturating increment per domain. Locks, atomic loads/stores, native
stages, barriers, allocations and copies are unchanged. After exhaustion each
request retains its own one drain instead of accepting a stale completion.
Area cleanup replaces overflow-prone bookkeeping in both original domains and
extends their existing controls without adding a parallel state machine.


The existing dedicated `device_flush_loom` target includes this same production
coalescer with Loom locks. Its two existing properties now start two drain generations
before exhaustion, so the same four concurrent
requests reach both a representable covering ticket and exhaustion. This
replaces the initial zero generation rather than multiplying models. Both
states have the same ordinary transition until exhaustion. Each actual
request stages a distinct write; the supplied drain records exactly the writes
already staged, and every successful return must have its own write covered.
The failure property retains the original drain's error branch and completion
joins. Exploration retains the existing bound of three preemptions; it does not
prove arbitrary schedules, native Apple I/O or cross-process locking.

Build the dedicated integration target from the repository root with
`cargo rustc --locked -p acyclic-native-runtime --test device_flush_loom --features model-checking -- --cfg loom`,
then run the emitted test binary's two `device_flush::model` properties.
With `exhausted-ticket.patch` applied alone, the successful-request property
must fail at `acknowledged before a covering drain`, then both properties must
pass after exact restoration. This is a mutation of the code included by the
model, not a hand-written protocol with a separate refinement story. No product
operation or test function is added.


The original condition variable now hands completion to one waiter. A caller
whose ticket is covered drops the same progress lock and forwards that original
completion to one successor before returning. A caller that must drain retains
the original guard, which forwards on success, error or unwind. An uncovered
waiter either owns the next drain or waits for that active guard. No terminal
path abandons the handoff, and no second queue or readiness probe is introduced.
Each completed operation notifies at most one waiter instead of a drain waking
all registered waiters; this also prevents repeated broadcast wakes when every
exhausted request needs its own drain. Notifications and scalar bookkeeping are
bounded per return, while OS scheduling fairness remains an OS obligation.
No claim is made about kernel-internal worst-case queue implementation costs.

`unforwarded-wake.patch` removes forwarding from the actual covered-return
branch. The dedicated Loom safety property must report `deadlock; threads =`
with that patch alone; the model diagnoses stranded original waiters without a
wall-clock timeout. Restore the exact source and require both original model
properties and the existing concurrent native-coalescer control to pass. The
model starts at MAX-2 so its four requests can leave two waiters
sharing the last drain, as well as reaching an unrepresentable next ticket.
Starting at MAX-1 reaches exhaustion but cannot explore the covered-waiter
return with the earlier three-request topology. No model functions, polling, native
I/O, allocations or copies are added. The earlier release and failed-publication
mutations retain their actual production targets with updated notification
context. Native Apple mapped locking and physical device work remain separate
qualification obligations.


The lost-forwarding mutation survived the initial three-request model. With
one initial drain and one later drain owner, only one covered waiter remains,
so nothing depends on its forwarded notification. Both existing concurrent
controls therefore use four requests: one original drain, one later drain owner
and two covered waiters. Four is the smallest topology that can distinguish
this missing handoff. The number of properties and the existing preemption
bound stay unchanged. This adds one test participant to expose an actual lost
notification; it is not a capacity or timeout increase. The surviving negative
supplies no handoff qualification.


Each model request now returns its verdict through its original join instead
of retaining a third shared atomic counter just for violations. It checks the
same successful return against the same durable-write bit at the same point;
the final assertion requires all four returned verdicts. Staged writes,
production coalescer operations, drain publication, four participants and the
preemption bound are unchanged. Removing one oracle allocation and its shared
reference-count transitions reduces observation work without pruning a product
branch, adding a model or weakening either retained negative.

The model's coalescer and its staged/durable observations also share one
reference-counted fixture, retained until the original joins. This replaces
three allocations and three clones per caller with one of each. The actual
mutex, condition variable, staged/durable atomic operations and successful
return observation remain unchanged, as do all four callers and the scheduling
bound. Both retained counterexamples must still fail before exact restoration
passes the same two properties.
