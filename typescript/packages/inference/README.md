# @acyclic-labs/inference

Immutable inference contexts, recoverable generation runs, streamed events, and explicit warm commitments.

```sh
npm install @acyclic-labs/inference
```

```ts
import { Inference } from "@acyclic-labs/inference";

// Set ACYCLIC_INFERENCE_ENDPOINT (HTTPS) and ACYCLIC_API_KEY.
const inference = await Inference.fromEnv();
const { models } = await inference.client.listModels();
console.log(models);
```

`Inference` retains the client-instance identity; prepare generated requests with `inference.identity()` before dispatch and reuse them for retries. `inference.client` operates on immutable revision, Run, and warm-commitment IDs. Save the Run request ID before dispatch; use `inspectRun`, `watchRun`, and `resultRun` to recover its state.

For custom authentication, use `new Inference(new InferenceClient(new HttpInferenceTransport(endpoint, () => ({ authorization: `Bearer ${token}` }))))`. The transport requires an absolute HTTPS URL and enforces bounded responses. The client validates protobuf responses through the bundled Rust WebAssembly contract. Import generated protobuf types and schemas from `@acyclic-labs/inference/proto`.

## Idle KV pins (source contract)

Discover an opaque policy in `models[].idleKvProfiles`, then call
`client.retainWarm(request)` with `request.idleKv = { profile, idleTimeoutMs }`. Rust uses
`Inference::retention_request(revision, Retention::idle_kv(profile, idle_timeout_ms))`. This paid KV pin
does not guarantee capacity, throughput, or latency. Keep the request identity
for retries; prepared requests retain it when cloned.

`client.renewWarm(request)` with `request.idleTimeoutMs` (Rust `idle_timeout_ms`) changes the
timeout measured from the last verified actual Run reuse, or from the initial
verified pin time before first use. It does not reset the idle window. Only
actual Run reuse of the pinned revision or descendant prefix advances last-use;
admission, fork, edit, inspect and recovery do not. The pin stays on its original
revision. Inspect returns trusted service Unix millisecond timestamps and the
Run identity responsible for the latest verified reuse; before first use, both
last-use fields are absent. The deadline is checked baseline plus timeout.

Release is explicit. Expired or released pins require a new retain identity;
renewal and replay cannot resurrect them. Mutation retries return committed
receipts; inspect reports the current lifecycle. Latency retention uses an
absolute expiry and cannot be combined with idle policy fields. Both modes use
one v1 typed admission/renewal contract, bound to the requested identity and
policy. Unsupported wire-kind aliases are rejected.

This v1 contract is available in the generated source. It is
not yet published or evidence of Cloud service support.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/inference/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference)
