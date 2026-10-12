# Harness client views

`@acyclic-labs/harness/client` binds the existing Rust `acyclic_harness::client`
kernel. It holds bounded demand and hypotheses; it performs no IO, admission,
domain reduction, rollback, cancellation or transport. Construction is synchronous
and effect-free after the explicit shared `await ClientViews.initialize()` step.

Supply a trusted, pure `ClientDomain<Value, Assumption, Evidence>`. Its identity
pins the adapter implementation and revision. Its basis must pin the original
authority, generation, revision and content identity. `observe` must consume the
host's existing validated reducer/journal output, preserve original operation
correlation and reject stale/conflicting observations. Raw predictions and remote
`verified: true` flags are not evidence. These are host obligations, rather than
authentication furnished by the generic kernel.

Values and assumptions must be transitively immutable. Pass file/content
references rather than bodies. Declare conservative retained deep bytes, including
strings, basis, assumptions, values and repeated key/operation allocations. The
kernel adds its index/record metadata; caller-held snapshots and host adapter state
remain external. Callbacks must finish within the supplied work allowance. The
binding bounds dependency/overlay lengths before allocating, reads only fixed
metadata fields and bounds copied strings. It cannot meter arbitrary JavaScript
callback CPU or determine an opaque object's true retained heap size.

`select` caches one immutable snapshot for React, Svelte and other consumers.
Reads allocate no new snapshot objects and start no effects. Narrow Rust changes
refresh only relevant selections; unchanged evidence emits no notifications.
All affected caches refresh before listeners run, so related selections agree
inside callbacks. Reentrant updates suppress superseded notifications.
Selection caches have a finite records-plus-branches cap. Dispose each selection
before releasing its key. Explicit `advance(now)` maintains monotonic logical
retention; there are no hidden clocks or timers. Retaining an old snapshot keeps
that caller-owned reference alive intentionally.

`ProjectionStore` now shares the same cached subscription primitive. Its
constructor is effect-free. Call `store.start()` explicitly **before** delivering
authoritative client events; repeated starts are idempotent. It remains attached
across UI subscriber churn so no projection events disappear when a UI unmounts.
`dispose()` detaches the client and clears owned subscriptions. Event reducers
must return immutable state and return the same reference for unchanged results.
For durable use, supply the optional recovery adapter described in
[durable composition](client-demand.md#durable-composition). `HarnessClient`
commits the prepared projection checkpoint with its cursor and acknowledgement
before publication and restores validated cuts before transport resume.
`start()` cannot replay events delivered earlier.

`checkpoint(branch)` exports bounded hypothesis metadata with shared immutable
values. Save `sequence()` and logical time with the host checkpoint. Restore
authenticated canonical facts first, advance logical time, and import parents
before children into a new kernel with that recovered allocation watermark.
The optional trusted domain `restore` validator checks exact pins, adapter and
recorded outcomes; without it import is `Unsupported`. The kernel preserves
identity/dependencies, rejects stale/colliding imports and recomputes admission.
Data alone never turns a prediction or an indeterminate operation into a fact.
