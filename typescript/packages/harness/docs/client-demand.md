# Demand and request composition

`RequestScheduler`, `DemandLoader` and `ScheduledTransport` are effect-free on
construction. Supply finite concurrency, total unsettled requests, bytes and
demand bounds. There is no built-in retry or prefetch policy. Share one scheduler
when reads and transport sends must compete for the same resource budget.

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

## Durable composition

Start one `ProjectionStore` per durable authority with a `ProjectionRecovery`
adapter. Its identity includes the security namespace, reducer/schema version and
domain adapter. The pure encoder produces bounded immutable data, using FileRefs
for content. The decoder validates the data against the exact authority,
generation and revision. Use the same `IndexedDbClientStore` for outbox and
cursors; custom stores must implement its atomic `commit` and `loadRecovery`
contracts and enforce bounds before allocating provider results.

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
