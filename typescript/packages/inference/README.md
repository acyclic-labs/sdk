# @acyclic-labs/inference

Immutable inference contexts, recoverable generation runs, streamed events, and explicit warm commitments.

```sh
npm install @acyclic-labs/inference
```

```ts
import { Inference } from "@acyclic-labs/inference";

// Set ACYCLIC_INFERENCE_ENDPOINT (HTTPS) and ACYCLIC_API_KEY.
const inference = await Inference.fromEnv();
const { models } = await inference.models();
console.log(models);
```

`Inference` is the identity-preserving high-level API. A `Context` points to an immutable revision: edits, forks, and transfers return new handles. Items read from a revision use the generated protobuf `Item` type. `Context.generate(...)` returns a `Run`; save `run.id()` so you can call `inference.recoverRun(id)` after an interrupted request, inspect progress, stream `run.events()`, or obtain `run.result()`. Warm commitments have separate retain, renew, inspect, and release operations.

For custom authentication, use `new Inference(new InferenceClient(new HttpInferenceTransport(endpoint, () => ({ authorization: `Bearer ${token}` }))))`. The transport requires an absolute HTTPS URL and enforces bounded responses. The client validates protobuf responses through the bundled Rust WebAssembly contract. Import generated protobuf types and schemas from `@acyclic-labs/inference/proto`.

## Idle KV pins (source contract)

Discover an opaque policy in `models[].idleKvProfiles`, then call
`context.retain({ idleKv: { profile, idleTimeoutMs }, identity })`. Rust uses
`context.retain(Retention::idle_kv(profile, idle_timeout_ms))`. This paid KV pin
does not guarantee capacity, throughput, or latency. Keep the request identity
for retries; Rust admission and renewal builders preserve it when cloned.

`warm.renewIdle(timeout, { identity })` (Rust `renew_idle(timeout)`) changes the
timeout measured from the last verified actual Run reuse, or from the initial
verified pin time before first use. It does not reset the idle window. Only
actual Run reuse of the pinned revision or descendant prefix advances last-use;
admission, fork, edit, inspect and recovery do not. The pin stays on its original
revision. Inspect returns trusted service Unix millisecond timestamps and the
Run identity responsible for the latest verified reuse; before first use, both
last-use fields are absent. The deadline is checked baseline plus timeout.

Release is explicit. Expired or released pins require a new retain identity;
renewal and replay cannot resurrect them. Mutation retries return committed
receipts; inspect reports the current lifecycle. Legacy absolute-expiry fields
remain wire compatible and cannot be combined with idle policy fields.

This additive contract is available in this branch's generated source. It is
not yet published or evidence of Cloud service support.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/inference/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference)
