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
| Actors | [Guest-state Loom target](../crates/actors/tests/guest_loom.rs) includes [the shipped state module](../crates/actors/src/guest/state.rs), replacing synchronization with Loom. Two handlers cannot tear state or lose updates. The split read/modify/write negative loses an update. | The same model completes both handlers and refuses nested mutations before another lock. Its [production refusal mutation](guest-refusal/README.md) must fail the original typed-error oracle. [Ready-set models](../crates/actors/src/local/ready/tests.rs) additionally race wake, park and registration cancellation on the real queue. | The ready-set dormant-population check observes no queued work and removes registrations directly; [qualification](../../scripts/qualify-actors-ready.py) retains production mutations and actual activation/observation controls. Wake/take still use B-tree registration lookups: a fixed number of lookups does not prove constant comparison work. This is not proof of every actor delivery, alarm, durable transition or user handler's cost/progress. |
| Auth | [Signed lossy-follow simulation](../crates/auth/src/trust/follower_tests.rs) applies actual issuer records to `Follower` and invokes the production verifier. Serving before catch-up is its negative; it must admit stale trust and fail the oracle. [Authorization checks](../crates/auth/src/verify/authorization_tests.rs) use real current admission and dependency changes. | The generated waiter schedule calls actual `changed`, fence, connect, resume, serve and close. Delaying registration until first poll loses an event and fails its negative. Close must resolve every wait with its cause and remain final. | `swap_precedes_wake_and_lost_follow_fences` holds the actual account-map write lock while fencing: the former population walk cannot progress under that control. Dependency tests reject unrelated wakes. This proves that withdrawal avoids that map, not that arbitrary credential verification or revocation fanout is constant work. |
| Filesystem kernel | [Full-width accounting](../crates/filesystem/src/kernel/allocation/accounting_tests.rs) checks the real accounting contexts against checked counter arithmetic. [Retained-credit conservation](retained-credit/README.md) uses the original ledger and guards; its production mutation allows scratch to retire retained backing. | [Native retirement Loom model](../crates/filesystem/src/materializer/allocation_model.rs) races actual release and cancellation across nested resource owners. Its negative reacquires the original leaf for zero retirement. [Blob writer model](../crates/filesystem/fuzz/accounting.md) runs the production writer and original effect-credit primitive; refusing all writes must fail its completed-work oracle. | Retained split/absorb/drop preserve charged work, and zero retirement must avoid reentering the leaf. [Kernel charge controls](../crates/filesystem/src/kernel/allocation/accounting/charge_tests.rs) measure the real allocator. These checks do not prove every filesystem operation's full native, storage, page, fork-tree or cleanup cost. |

## Current closure work

The [scalar-charge cost control](scalar-charge-cost/README.md) joins exact typed
errors, complete receipts and actual arithmetic/allocation counts in the existing
generated kernel property. A production mutation substitutes general charging
and must fail the scalar arithmetic bound. Native ledger costs remain separate.

The [child-selection control](children-selection/README.md) binds actual provider
pages to the original parent, cursor, hierarchy fence and projection, including
authorization-hidden children. Its production mutation restores tail-only
validation; it does not establish flat supervisor/root ancestry.

The [ready-source allocation control](ready-source-allocation/README.md) retains
actual committed records and physical heap measurements while replacing a former
scheduler comparison with a production mutation. It qualifies ready-record
allocation only. The [bounded provider-turn control](ready-source-turn/README.md)
challenges actual wake draining, parent registration and record delivery on the
same scheduler. The [source-index control](ready-source-index/README.md) separately
bounds physical lookup work in the shared fixed-width index. Wake-hint queue
allocation and complete operation cost remain open.

The [exact budget-refusal control](budget-errors/README.md) extends the existing
generated verifier oracle with an isolated refusal for every declared counter.
It retains full-width mixed inputs and first-error priority; a production
mutation mislabels the final field and must fail the unchanged oracle. This
subsumes two fixed counter examples without relying on random early errors to
exercise later fields.

The [original-budget control](borrowed-budget/README.md) replaces a hand-written
former completion body with a mutation of the actual kernel accounting method.
The existing address, verification-count, allocation and receipt observations
remain on that production entry point; no machine-level copy count is inferred.

The [native-charge control](native-charge/README.md) inserts a physical allocation
in the actual charge method. The existing completed-prefix property must reject
it through the real allocator. It replaces a test-only hook, thread-local
injection flag and redundant fixed example; the separate mutex oracle remains.

The [work arithmetic control](work-arithmetic/README.md) compares actual receipt
addition with independent wider arithmetic across every declared field, including
peak/max semantics and unchanged receipts on refused addition or charge. Its
production mutation removes the overflow fence; the original property must
reject a wrapped sum. A second production mutation corrupts a successful mixed
sum; every generated invocation includes a positive mixture that cannot overflow.
Reservation ownership and native concurrency remain
separate obligations.

The [Auth producer-loss control](auth-producer-loss/README.md) retains a
production mutation that captures the global watch sender in its own account
dependency future. The original retained-account oracle must reject the resulting
pending wait when the follower is dropped, without waiting for a timeout.

The [trust catch-up control](trust-catchup/README.md) mutates the production
Stream driver's reconnect serving decision. An original signed revocation is
held before the observed tail; trust must remain fenced until it applies, and
losing its producer must return the original typed error. The existing follow
loss test covers this boundary without introducing a second trust driver.

The [Auth generation control](auth-generation/README.md) mutates the shared
withdrawal increment to reuse generation zero after exhaustion. The existing
signed follower test must reject a live original change wait, require the exact
terminal cause and refuse reopening. The same test holds account storage during
withdrawal; this establishes absence of that traversal, while notification work
still depends on actual waiters.

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

The [follow digest control](follow-digest-cost/README.md) measures the actual
digest's scratch allocation as admitted clauses repeat at fixed size. Its
retained production mutation discards that scratch buffer; the same signed
provider-hop control must reject it. An independent v1 framing oracle retains
format stability without using a copied authorization builder as a cost oracle.

The [change marker control](change-marker-cost/README.md) measures original
attribute probes against the fixture's marker position. Its retained mutation
repeats the production lookup, replacing a copied classifier and simulated old
calls while keeping the retained-version identity and refusal property.

The [source index control](ready-source-index/README.md) binds the scheduler's
original channel identities to the shared fixed-width collection. Actual
record delivery traverses all 64 branch positions; a repeated production
lookup must fail the original physical node/key examination bound.

### Selectable large-follow workload

The Session swarm and opening-allocation properties run quick growth cases by
default. Set `ACYCLIC_STREAM_FOLLOW_SCALE=large` to select 10,000 live follows
in each property, using the same drivers and assertions. The swarm retains its
MemoryStream and eight-node gRPC paths, paired cross-stream transactions,
per-node connection and byte counters, and event-driven retirement. The
allocation property retains both public and reviewed opening paths with every
interest alive. Neither path adds a latency deadline or changes its negative
controls.

From the repository root, select either existing property:

```sh
ACYCLIC_STREAM_FOLLOW_SCALE=large cargo test --locked -p acyclic-stream --lib session::client::tests::tenk::agent_transactions_share_one_session_per_node_as_the_swarm_grows -- --exact --nocapture
ACYCLIC_STREAM_FOLLOW_SCALE=large cargo test --locked -p acyclic-stream --lib session::client::tests::allocations::follow_opening_allocations_grow_only_by_the_public_per_interest_cost -- --exact --nocapture
```

In PowerShell, set `$env:ACYCLIC_STREAM_FOLLOW_SCALE = 'large'` before the
chosen `cargo test` command. Remove the variable afterward to restore quick
cases. Other values refuse with the selection and recovery instructions.
Selection preserves the workload; qualification requires running that selected
workload and recording its result. It does not establish restart, upgrade or
cross-platform coverage.

The [retained commit index control](retained-commit-index/README.md) binds
Streams' original retained slots and the shared boxed index to one radix
selection loop. It challenges original comparison admission and retains
prepared completion after cancellation; variable-width replay and whole-request
cost remain open.
Run `node rust/proofs/qualify.mjs conversation-observation` to challenge the
native parent/child volume property with the original duplicate-settlement
observation path, then restore the exact production source and rerun the same
property. Both phases retain the original oracle bytes, source hashes and exact
test-selection checks under `target/conversation-observation`.
