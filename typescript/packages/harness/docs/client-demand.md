# Demand and request composition

`RequestScheduler`, `DemandLoader` and `ScheduledTransport` are effect-free on
construction. Supply finite concurrency, total unsettled requests, bytes and
demand bounds. There is no built-in retry or prefetch policy. Share one scheduler
when reads and transport sends must compete for the same resource budget.

Admission occurs before provider code. Both queued and running requests reserve
bytes. Cancelling a queued request removes it; cancelling running work rejects
the caller and asks the provider to abort, but the capacity remains charged until
the provider settles. A provider that ignores cancellation can exhaust capacity,
but cannot make it grow. An operation cancelled after send starts can have an
indeterminate result; recover using the original operation ID and authority.
`ScheduledTransport` forwards delivery iteration directly and never deduplicates
writes, buffers history or substitutes admission errors. Its underlying transport
owns inbound-frame and established-connection budgets, as it did before wrapping.
Connection ownership remains with the wrapper until handoff completes; abandoned
late connections are closed. If their cleanup fails after the cancelled caller
has already returned, `onCleanupError` reports it to the host (the default raises
an asynchronous error). A delivered connection returns its close failure directly,
including on repeated close calls.

`DemandLoader` deduplicates equal immutable keys and uses the existing
`HydrationCache` for byte/entry-bounded retention. Keys must include exact pinned
authority/provider/generation/content identity, including the security namespace.
Object keys use identity: reuse normalized immutable references rather than
constructing a fresh object on each read. One source serves a loader, so unrelated
providers cannot share a key accidentally. A caller can cancel its own interest;
the last caller cancels shared work. Invalidating a key cancels current interest
and fences late completion. Errors and absence are not cached. `peek` performs no
IO. Each shared request owns one completion callback and a removable set of active
readers: joining/cancelling a slow request cannot accumulate stale callbacks.
Invalidation/disposal settle owned readers directly, including when the provider
has completed but its reader completion callback is still queued.
`peek` returns the same retained value/receipt on warm reads. No cache-miss status
confers authority: keep not-loaded/absent/stale/available and prediction provenance
in existing `ClientViews` and the domain adapter.

The host is trusted to enforce the reserved bound **before** reading/allocating,
authenticate correspondence, return transitively immutable values and account
deep/backing storage in its byte receipt. A post-load receipt check rejects an
oversized result but cannot undo provider allocations. Scheduler reservations and
cache retention are separate budgets; account both, plus caller-held results,
keys, reference-window metadata and established connections. Temporary native or
JS decoder storage belongs in the source's reservation. Cancellation is advisory
for physical IO and does not reverse effects.

The [reference-first conversation example](../examples/client-demand.ts) composes
the existing `PageWindow` with selective text hydration. Page providers enforce
their existing page count and deep-byte bounds; bodies are read only when a
consumer calls `bodies.read(message.content)`. Existing verified FileRef readers
such as `ObjectContentStore.read` retain scope/digest correspondence. No complete
conversation/history hydration or full-state serialization is needed. Dispose
body demand when its owner ends, and dispose the shared scheduler only after all
its owners have ended. Window eviction does not undo an operation or a hypothesis.

## Durable composition

Start one `ProjectionStore` per durable authority with a `ProjectionRecovery`
adapter. Its identity includes the security namespace, reducer/schema version and
domain adapter. The pure encoder produces bounded immutable data, using FileRefs
for content. The decoder validates the data against the exact authority,
generation and revision. Use the same `IndexedDbClientStore` for outbox and
cursors; custom stores must implement its atomic `commit` and `loadRecovery`
contracts and enforce bounds before allocating provider results.

For each contiguous event, the client prepares the reduced state, commits its
checkpoint and cursor with the original operation acknowledgement in one existing
outbox/cursors transaction, then publishes. Capacity, storage and compare-and-swap
failures preserve the previous published cut and retry identity. The expected
cursor fences stale tabs. Cursor-only writes cannot advance an existing checkpoint.
Reopening validates all saved cuts before publishing state or connecting.
Unmatched checkpoints or cursors fail closed. Explicit authoritative `rebase`
can replace an incompatible cut, still comparing the stored cursor and fencing
the old connection. Durable replay errors require authoritative recovery.

Client mutations use the existing finite scheduler instead of a promise tail:
defaults are 128 unsettled mutations, 16 MiB retained input and 1 MiB per command
or delivery. Inputs are bounded and copied before queuing. Built-in outboxes
default to 1024 commands and 16 MiB; IndexedDB separately accounts encoded and
resident bytes, 64 cursors and 256 KiB per checkpoint. The sequence index supplies
pages of at most 64 records over one fixed enqueue cut. Admission visits bounded
records without retaining the full outbox; duplicate IDs keep their enqueue order.
`load()` remains a bounded compatibility collector. Configure application limits
and enforce provider/decoder bounds before transport or IndexedDB allocations;
post-allocation validation cannot undo those allocations.

Database construction starts no IO. The existing version 3 and legacy rejection
policy are unchanged; newly created outboxes include the sequence index.
Existing databases lacking that index fail closed and close the rejected handle
before network resume. There is no implicit migration, scan fallback, synthesized
checkpoint or deletion. They require an explicit application-owned migration or
authoritative recovery policy.

`ClientViews.checkpoint(branch)` exports one selected hypothesis and `sequence()`
exports its allocation watermark. Construct the new kernel with that watermark,
restore verified canonical facts and logical time, then import parents before
descendants with `restore`. The trusted domain's optional `restore` validator
authenticates pins, adapter, dependencies and prediction/operation outcomes;
omission is `Unsupported`. The kernel preserves original branch and operation
IDs, recomputes byte/work admission and installs each hypothesis atomically.
Checkpoint data grants no authority, admits no command and executes no effect.
Fresh trusted domain evidence must establish completion and external outcomes.

`client.failure` exposes failures even during retry. Stop replay before detaching
its durable projection. Client disposal ends owned interest; started storage/send
work retains its reservation until settlement and may have an indeterminate
authoritative outcome. Recover through the original operation ID.

## Simplification and evidence

One request ledger owns admission/cancellation for both consumers; one existing
LRU owns retention. Existing page windows, transports, reducers, cursor/outbox
transactions, Rust speculation and CL2 snapshots retain their own semantics.
There is no parallel retry engine, reducer, journal, page cache, lifecycle timer
or framework store. Separate request and caller-demand bounds are necessary:
deduplicated IO alone would allow unlimited promise/listener retention.

Focused controls exercise distinct capacity/cancellation, selective hydration and
transport failure classes with controlled pending providers. They establish
finite modeled transitions, not a proof about arbitrary providers. Byte receipts
and pre-work enforcement are host obligations; post-load checks alone are not a
security boundary. The installed Chromium control combines verified FileRef
hydration, atomic projection/cursor/outbox restart and native-backed hypothesis
recovery before network resume. Native and TypeScript controls exercise adapter
drift, untrusted outcomes, dependency ordering, stale-tab CAS, capacity failure
and original-ID preservation. They do not authenticate arbitrary host decoders.
The same controls detect the reviewed original source's two cancellation bugs:
1,000 join/cancel operations retained 1,000 callbacks, and an abort during the
connection handoff lost cleanup. Repaired code attaches no per-reader completion
callbacks and closes the unreceived connection. These are focused reproductions,
not an additional validation matrix.
