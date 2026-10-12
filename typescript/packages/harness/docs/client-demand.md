# Demand and request composition

`RequestScheduler`, `DemandLoader` and `ScheduledTransport` are effect-free on
construction. Supply finite concurrency, total unsettled requests, bytes and
demand bounds. There is no built-in retry or prefetch policy. Share one scheduler
when reads and transport sends must compete for the same resource budget.

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
