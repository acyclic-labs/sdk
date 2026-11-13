# Atomic subscriber activation — design only

Implementation waits for #3555 to land and will be based on its landed head.
No actor implementation changes belong in this draft.

## Root cause

Subscription cursors live in actor state; dormant actors have no live follow.
Producers therefore append input and manually join an activation effect. A
producer omitting that effect can leave committed input stranded indefinitely.

## Fix

Register subscriptions in Stream authority state, indexed by exact source path.
Each source entry names an immutable paged set of registration ids. A registration
binds source, subscriber identity, delivery generation and activation destination;
it is authorized at registration, not supplied by a later producer. Actor state
still owns its consumption cursor. Register/unregister and actor state changes
join one conditional Stream commit. No in-memory reverse index is authoritative.

Use the existing namespace lookup for the source entry. Its persistent membership
root is directly available when appending: no actor scan, subscription scan,
per-actor task or fanout enumeration on the producer path. If a separate lookup
is unavoidable, use an exact-key index bounded by fixed-width identity depth;
do not claim a variable-height tree is asymptotically O(1). Hashing an arbitrarily
long path remains proportional to the input bytes and should not be repeated.

For each distinct source receiving records in an accepted commit, the Stream
core adds one immutable activation-batch descriptor to a partitioned durable
ready feed. It binds the original commit id, committed source range and the
subscriber-set root at that commit's final conditional cut. Input, membership
changes, batch descriptor and terminal retry receipt share ONE atomic decision.
No activation is visible for refused/skipped effects. Retries reuse the original
decision and batch identity. Append, coordinated commit, prepared publication,
fork-supplied records and remote writes all use this same core expansion.
Generated ready-feed records do not recursively generate activation batches.

The descriptor IS the durable activation for every member of that pinned set;
it is not an intent to later make activation durable. Dispatchers follow assigned
source-hash ready partitions, page the set and deliver existing actor activation
semantics. Restart resumes a durable (batch, member) cursor. It advances past a
member only after durable handoff, with stable (commit, registration, generation)
identity, so crash/replay can duplicate a wake but cannot lose it. Placement and
actor epoch checks remain in #3555's one actor core. No second actor scheduler.
Do not checkpoint a batch while any member lacks durable handoff.

This representation is essential for the work bound: individually materializing
k activation records requires at least k writes. A single set-reference batch
costs O(1) additional append work and space per distinct written source,
independent of k and of 10^5 dormant actors. Dispatch necessarily costs O(k),
paid lazily in bounded pages with backpressure. Partition count is a deployment
chunking parameter, not a subscriber or queue cap. No timers or polling.

Registration is fenced with the same source cut: start-at-tail excludes earlier
input; start-at-cursor atomically creates one catch-up activation when that cursor
is behind the accepted tail. This closes the subscribe-after-last-append hole.
Unregister changes future membership; an already committed batch remains pinned.
Generation/terminal-state checks can suppress obsolete delivery but cannot erase
required input for a still-live registration. Forks do not copy registrations
implicitly: source identity remains exact; child registrations and initial input
must be installed together explicitly. Multiple mutations of one source in one
commit produce one batch over the complete committed range, in original order.
Membership within that commit uses its final accepted membership, independent
of mutation ordering; same-commit start-at-tail uses the final tail, while an
explicit cursor includes its remaining range. Conflicting duplicate registration
operations are rejected rather than interpreted by list ordering.

The shared Stream core supplies these semantics on every backend. Adapters only
persist the shared authority state and bounded ready-feed records. An unavailable
dispatcher delays activation delivery without losing its durable obligation.
Registration authorization cannot grant producers write authority over subscriber
state. Batch membership is validated before delivery; arbitrary user records
cannot forge internal ready descriptors. Retention pins batch membership and
input until consumption/durable handoff allows event-driven collection.

## Tests

Drive production registration and commit transitions in deterministic simulation
or exhaustive schedules: subscribe vs append, unregister vs append, conditional
conflict/soft skip, identical retry, owner loss, dormant actors, dispatcher crash
at each member handoff/checkpoint, restart and fork initial records. Assert:
every committed eligible input has its activation in the same decision; no
activation names refused input; no checkpoint passes an unhanded member; cursors
preserve record order. Revert the core batch insertion as a negative control:
the dormant-subscriber orphan assertion must fail. Shared conformance covers
every provider; a counted append with 1 versus 100,000 registered subscribers
must perform the same additional authority lookups/writes and bounded allocation.

## Area cleanup / removals

Delete hand-paired producer activations in actors local/invoke, disposition,
sockets (both), state_binding, reservation and subscription setup; harness-core
agent/handler, agent/host, devices/actor, devices/directory and tools/agents.
Audit their current locations after #3555 lands. Keep explicit activation for
placement, alarm, resume or other control work without appended subscribed input.
Remove duplicate wake records, producer subscriber lookups and any redundant
reverse cache; keep cursor, placement, deadline and custody guarantees.

Draft decision to confirm during implementation: reuse #3555's durable ready
feed/dispatcher by extending its event representation, rather than add another
feed consumer. Exact wire/storage names follow the landed code; version formats
without migration. No unbounded-commit lane helpers are prerequisites for this
design, and this draft does not claim implementation, proof or landing.
