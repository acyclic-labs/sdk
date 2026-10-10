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

## Durability producer gaps

Current `AtomicClientStateStore.commit(authority, cursor, operationId?)` atomically
updates cursor/outbox. It does not bind a recoverable projection checkpoint. A
fresh `ProjectionStore` is empty even when `HarnessClient` loads saved cursors;
resuming there cannot establish cursor/view correspondence. These modules make
no claim of durable projection recovery.

The owning producer must expose a bounded immutable checkpoint pinned to the
authority, generation, revision, reducer/schema and adapter identity, with atomic
publication of its binding alongside existing cursor/outbox acknowledgement.
Restoration must validate and install that exact checkpoint before transport
resume. A failed/quota-exhausted publication must keep original operation recovery
possible. No new journal or whole-history replay is an acceptable substitute.
Existing `client.ts` IndexedDB schema/outbox hunks remain with their format owner.

Rust `Client` permits by-reference hypothesis inspection/export and exposes its
allocated sequence. Current `ClientViews`/WASM has no qualified bounded durable
import/export path preserving domain pin, branch dependencies, budget accounting
and hypothesis provenance. Durable/shared hypothesis restoration is unsupported
until that producer contract qualifies. Do not serialize JS guesses as trusted
canonical facts. Data persistence alone grants no authority.

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
security boundary. Missing durability evidence remains a dependency, not PASS.
The same controls detect the reviewed original source's two cancellation bugs:
1,000 join/cancel operations retained 1,000 callbacks, and an abort during the
connection handoff lost cleanup. Repaired code attaches no per-reader completion
callbacks and closes the unreceived connection. These are focused reproductions,
not an additional validation matrix.
