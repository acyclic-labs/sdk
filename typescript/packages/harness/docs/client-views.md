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

```ts
import { ClientViews } from "@acyclic-labs/harness/client";

await ClientViews.initialize();
const views = new ClientViews(host.domain, host.replicaNamespace, 0n, {
  records: 16, branches: 16, edges: 32, bytes: 1_000_000,
  work: 256, retention: 100n, visible: 4,
});
views.observe(messageKey, host.readValidatedMessage(messageKey));
const branch = views.begin({
  key: messageKey, basis: exactBasis, operation: originalOperation,
  predicted: immutableMessageReference, assumption: messageAbsent,
  dependencies: [], expires: 50n,
});
const selected = views.select(messageKey, [branch]);
// This view has hypothesis provenance, not permission to send/run an effect.
const provisional = selected.getSnapshot();
views.observe(messageKey, host.readValidatedOperationOutcome(originalOperation));
// Admitted/Indeterminate remain separate from confirmed correspondence.
// Examine selected.getSnapshot().hypotheses even after canonical replacement.
selected.dispose();
views.discard(branch); // Removes a local guess; does not cancel a durable effect.
views.release(messageKey);
views.dispose();
```

The reducer-backed public message and Scheduler dequeue/claim example lives in
[`rust/crates/harness/examples/client_views.rs`](../../../../rust/crates/harness/examples/client_views.rs).
It runs the actual existing reducer and Scheduler on native and WASM, including a
competing authoritative claim replacing the guess. JavaScript presently lacks a
public Scheduler ready/lease snapshot bridge; the generic example's `host` must
supply a real validated host projection. Task status alone cannot establish queue
ownership. This binding introduces no Scheduler implementation.

[`examples/client-message.ts`](../examples/client-message.ts) also runs a public
TypeScript message prediction against the actual Rust-backed Harness reducer.
Its private in-process observation envelopes pin audience, generation, revision
and canonical reference content; no raw remote trust flag or hypothetical journal
is used. The original command's admitted result confirms correspondence, and
retrying that exact command leaves one canonical message.

`select` caches one immutable snapshot for React, Svelte and other consumers.
Reads allocate no new snapshot objects and start no effects. Narrow Rust changes
refresh only relevant selections; unchanged evidence emits no notifications.
Selection caches have a finite records-plus-branches cap. Dispose each selection
before releasing its key. Explicit `advance(now)` maintains monotonic logical
retention; there are no hidden clocks or timers. Retaining an old snapshot keeps
that caller-owned reference alive intentionally.

```ts
import { useClientSnapshot } from "@acyclic-labs/harness/react";
import { clientReadable } from "@acyclic-labs/harness/svelte";

// Pin this exact immutable value when rendering on the server. Reuse it for
// hydration; each client store/SSR request is independently owned.
const serverSnapshot = selected.getSnapshot();
function Component() {
  const snapshot = useClientSnapshot(selected, serverSnapshot);
  // Render snapshot.value, provenance and honest hypothesis operation outcomes.
}
const readable = clientReadable(selected);
const unsubscribe = readable.subscribe(snapshot => render(snapshot));
// Svelte receives the current value synchronously, including upstream attachment
// changes, and unsubscribes without disposing a shared client owned elsewhere.
unsubscribe();
```

React is an optional peer, imported only by `./react`. Core, `./client`, `./svelte`
and `./worker` install/import without framework packages. Svelte's adapter uses
the structural readable contract and requires no Svelte runtime dependency.

`ProjectionStore` now shares the same cached subscription primitive. Its
constructor is effect-free. Call `store.start()` explicitly **before** delivering
authoritative client events; repeated starts are idempotent. It remains attached
across UI subscriber churn so no projection events disappear when a UI unmounts.
`dispose()` detaches the client and clears owned subscriptions. Event reducers
must return immutable state and return the same reference for unchanged results.
Transport replay/checkpoint correctness remains with `HarnessClient` and the
separate durable-state work; `start()` cannot replay events delivered earlier.

An optional `./worker` bridge carries selected snapshots over explicitly owned
MessagePorts. It creates no worker and transports no commands or evidence. Supply
a codec producing an owned byte buffer and decoding immutable data. The publisher
retains at most one in-flight snapshot plus the latest source reference, enforces
a byte cap, and coalesces intermediate UI views under backpressure. This is safe
for snapshots, not a license to drop journal events. The receiver rejects sequence
gaps/reordering and oversized frames before decoding. Dispose both endpoints;
there is no liveness timer or automatic durable-effect recovery.

## Simplification and limits

The original eager ProjectionStore subscription and unconditional notifications
were replaced, rather than retained behind a compatibility mode. One SnapshotStore
implements reference equality, independent registrations, detachment, idempotent
disposal and subscriber error isolation for both adapters and the worker receiver.
Errors are reported on a microtask after all listeners observe publication, so a
failed callback cannot leave other cached selections stale.

The Rust adapter removed whole-object metadata deserialization and redundant
metadata structs. Only small status/change metadata crosses serde; projection
values remain shared JavaScript handles over the single Rust kernel. The binding
copies immutable Limits solely for pre-allocation checks because the kernel keeps
its configuration private. The view-cache set holds bounded active selections,
not a second state, history, rollback or persistence engine. It scans bounded
active selections instead of maintaining several additional routing indexes.

Qualification covers bounded outcome exploration and real installed framework
lifecycle/SSR/hydration controls. It is not an unrestricted proof of arbitrary
host callbacks, network delivery, durable recovery or all JavaScript heap/RSS
allocations. No persistence, journal transport, inference or authority path was
added by this layer.

The temporary source-mutation runner was removed after its six focused fault
controls detected equality, disposal, capacity, outcome routing, provenance and
worker-byte failures. The retained tests cover those contracts directly; the
existing hosted Chromium driver runs the real React/Svelte consumer without a
new matrix or browser automation framework.

Local qualification used the existing native client/public-consumer tests and
strict Harness Clippy on Windows, Linux and macOS, plus the production WASM
build/strict check. The affected Harness suite passed under repository-pinned
Bun 1.4.2 on Linux. A fresh installed archive imported the optional core exports
without React, passed strict declaration checking, and then passed actual React
19.3.0/Svelte 5.57.2 Chromium SSR/hydration and lifecycle controls. Final WASM
size was 5,494,230 bytes versus 5,394,559 bytes at main d86b660, an increment of
99,671 bytes. Cached reads retain the same snapshot object; this does not claim
zero engine allocations or bounded CPU for arbitrary host callbacks. The
protected repository qualification remains the merge gate.
