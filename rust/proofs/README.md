# Production proof map

This index records component obligations, their actual production entry points,
and the mutations or adversarial schedules that challenge them. A source mapping
is not a runtime result. Passing one row does not qualify a whole primitive,
another backend, a different platform, or a deployment topology.

For cost, distinguish fixed overhead per bounded operation from its requested
payload, page, subscription fanout or durability work. A fixed number of charged
operations does not establish a fixed number of physical allocations, locks or
visited nodes. A complete cost proof needs both original-owner observations and
an oracle that rejects a population-dependent implementation.

| Primitive | Safety and production correspondence | Progress and negative control | Cost coverage and remaining obligation |
| --- | --- | --- | --- |
| Streams | [Local follow models](../crates/stream/tests/local_follow_loom.rs) call [the production memory/local paths](../crates/stream/src/memory/recovery_model.rs). Pending publication preserves accepted reads and writer ownership; their two negative controls retain visible state or release ordering early. | Executor shutdown, cancelled snapshot consumers and parked follows must return original reservations and wake. Kept negatives discard ownership, leak a reservation, omit cancellation/publication wakes, or retain a permit across a wait. | [Session model mapping](../crates/stream/models/README.md) records actual source queues, bounded lookahead and cancellation. These bounded schedules do not prove population-independent append, fork, read, retention or replicated transport cost. That complete operation/counter join remains open. |
| Objects | [Native outcome controls](../crates/objects/src/local/outcome_tests.rs) call the actual `LocalMedium` and `LocalObjects`, reopen their real logs and inject failures at original native durable-write boundaries. Failed, torn and lost-acknowledgement writes must preserve uncertainty rather than report a definite refusal. | `owned_close_reports_physical_completion_and_failure` checks both original flush barriers, waiter cancellation, actual task retirement and reopening after success/error. The injected before-effect/after-effect faults challenge the success-only path; the release barrier must outlive a cancelled waiter. | `empty_ranges_preserve_native_presence_without_dispatch` counts actual native submissions: zero for valid empty ranges, one for requested bytes and public HEAD. Its nonempty read checks counter sensitivity; the [production mutation](objects-empty-dispatch/README.md) challenges the empty-range fast path. This establishes dispatch counts for those paths, not a complete population-dependent index, listing, compaction or backend cost proof. |
| Actors | [Guest-state Loom target](../crates/actors/tests/guest_loom.rs) includes [the shipped state module](../crates/actors/src/guest/state.rs), replacing synchronization with Loom. Two handlers cannot tear state or lose updates. The split read/modify/write negative loses an update. | The same model completes both handlers and refuses nested mutations before another lock. Its erased held-lock marker negative violates that refusal. [Ready-set models](../crates/actors/src/local/ready/tests.rs) additionally race wake, park and registration cancellation on the real queue. | The ready-set dormant-population check observes no queued work and removes registrations directly; [qualification](../../scripts/qualify-actors-ready.py) retains production mutations and actual activation/observation controls. Wake/take still use B-tree registration lookups: a fixed number of lookups does not prove constant comparison work. This is not proof of every actor delivery, alarm, durable transition or user handler's cost/progress. |
| Auth | [Signed lossy-follow simulation](../crates/auth/src/trust/follower_tests.rs) applies actual issuer records to `Follower` and invokes the production verifier. Serving before catch-up is its negative; it must admit stale trust and fail the oracle. [Authorization checks](../crates/auth/src/verify/authorization_tests.rs) use real current admission and dependency changes. | The generated waiter schedule calls actual `changed`, fence, connect, resume, serve and close. Delaying registration until first poll loses an event and fails its negative. Close must resolve every wait with its cause and remain final. | `swap_precedes_wake_and_lost_follow_fences` holds the actual account-map write lock while fencing: the former population walk cannot progress under that control. Dependency tests reject unrelated wakes. This proves that withdrawal avoids that map, not that arbitrary credential verification or revocation fanout is constant work. |
| Filesystem kernel | [Full-width accounting](../crates/filesystem/src/kernel/allocation/accounting_tests.rs) checks the real accounting contexts against checked counter arithmetic. [Retained-credit conservation](retained-credit/README.md) uses the original ledger and guards; its production mutation allows scratch to retire retained backing. | [Native retirement Loom model](../crates/filesystem/src/materializer/allocation_model.rs) races actual release and cancellation across nested resource owners. Its negative reacquires the original leaf for zero retirement. [Blob writer model](../crates/filesystem/fuzz/accounting.md) runs the production writer and original effect-credit primitive; refusing all writes must fail its completed-work oracle. | Retained split/absorb/drop preserve charged work, and zero retirement must avoid reentering the leaf. [Kernel charge controls](../crates/filesystem/src/kernel/allocation/accounting/charge_tests.rs) measure the real allocator. These checks do not prove every filesystem operation's full native, storage, page, fork-tree or cleanup cost. |

## Current closure work

The [original-budget control](borrowed-budget/README.md) replaces a hand-written
former completion body with a mutation of the actual kernel accounting method.
The existing address, verification-count, allocation and receipt observations
remain on that production entry point; no machine-level copy count is inferred.

The [work arithmetic control](work-arithmetic/README.md) compares actual receipt
addition with independent wider arithmetic across every declared field, including
peak/max semantics and unchanged receipts on refused addition or charge. Its
production mutation removes the overflow fence; the original property must
reject a wrapped sum. Reservation ownership and native concurrency remain
separate obligations.

The [Auth producer-loss control](auth-producer-loss/README.md) retains a
production mutation that captures the global watch sender in its own account
dependency future. The original retained-account oracle must reject the resulting
pending wait when the follower is dropped, without waiting for a timeout.

The [Objects empty-range dispatch control](objects-empty-dispatch/README.md)
retains a production mutation that disables the index-only presence path. The
existing real-log oracle must reject the resulting extra native submission;
its nonempty read separately checks counter sensitivity. This establishes
dispatch counts, without inferring unobserved physical work or index complexity.

The [admission rebind control](admission-rebind/README.md) measures actual
pending-membership home rebinding and retains a mutation of the production
method. Its zero-allocation oracle replaces a hand-written former-mechanism
comparison. Allocation-free traversal and complete membership-operation cost
remain separate obligations.

The retained-credit property addresses ownership conservation behind double
retirement: scratch and retained guards must never refund the same live bytes.
Its scope includes the standalone guard path and full-width arithmetic; the
separate native Loom model covers actual leaf/resource locks. Neither is a
substitute for the provider-level reservation regression or for their original
platform qualification.

The [settlement terminal control](settlement-terminal/README.md) joins the
actual serving runtime's completion event to raw actor-checkpoint observation.
It drops the model dispatch producer and requires the original error without
settling recoverable input; removing the join must fail the unchanged oracle.
The native spawn-brief end-to-end test still requires an original runtime
diagnosis for its macOS timeout. This event-consumption control does not qualify
its native projection or identify that timeout's cause. Increasing a timeout
or using a successful model response would not prove it.

The open whole-operation and topology obligations in the table must remain
explicit until their checked refinement, original counters and independent
negative controls exist. In particular, a single-member memory fixture cannot
certify non-owner members, partial writes, upgrades or larger fork trees.
