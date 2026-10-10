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

### Static WebAssembly deployments

In Workerd or another deployment that supplies a compiled module, initialize the
same Rust descriptor before creating a native inference client or validating a
message. The normal Node/Bun file loader remains available.

```ts
import { initializeInferenceWasm } from "@acyclic-labs/inference/wasm";
import compiledModule from "@acyclic-labs/inference/module.wasm";

// Configure the deployment bundler to import .wasm as WebAssembly.Module.
await initializeInferenceWasm(compiledModule);
```

## Provider-compatible gateway

`GatewayInferenceClient` is separate from the native context/run API. Use the
gateway HTTPS origin and your customer's gateway authorization bearer from the
trusted application auth flow. Use official provider request types; this client
does not introduce a second provider schema. Never supply service credentials or
upstream provider keys as customer authorization.

```ts
import { GatewayInferenceClient } from "@acyclic-labs/inference";

// ACYCLIC_GATEWAY_ENDPOINT is an HTTPS origin, without /v1.
// ACYCLIC_GATEWAY_TOKEN is the customer's gateway authorization bearer.
const gateway = new GatewayInferenceClient(
  process.env.ACYCLIC_GATEWAY_ENDPOINT!,
  () => ({ authorization: `Bearer ${process.env.ACYCLIC_GATEWAY_TOKEN!}` }),
);
const models = await gateway.models(); // GET /v1/models: OpenAI model list
if (!models.ok) throw new Error(await models.text());
console.log(await models.json());

const controller = new AbortController();
const response = await gateway.chatCompletions(
  JSON.stringify({ model: "your-model-id", messages: [{ role: "user", content: "Hello" }], stream: true }),
  { signal: controller.signal },
);
if (!response.ok) throw new Error(await response.text());
// Consume response.body as the provider's SSE stream; abort to stop the request.
// controller.abort();
```

`chatCompletions`, `responses`, and `messages` POST the supplied JSON string
unchanged to `/v1/chat/completions`, `/v1/responses`, and `/v1/messages`.
Provider `tools`, `tool_calls`, tool results, and streaming fields are passed
through, not translated into native inference items. Per-request `headers`
can supply provider headers such as `anthropic-version`; authentication callback
headers take precedence. All methods return the original `Response`, including
HTTP errors and their status, headers, and body. Network, abort, and body-read
errors propagate unchanged. Stream bodies are never buffered or parsed by this
client; cancel their reader or abort the supplied signal when stopping early.
This does not cancel a native durable `Run`, whose explicit cancellation API is
unchanged. Rust's existing gRPC client and HTTP protobuf codec likewise retain
their native semantics; use an HTTP provider client for gateway compatibility.



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
receipts; inspect reports the current lifecycle. Latency retention uses an
absolute expiry and cannot be combined with idle policy fields. Both modes use
one v1 typed admission/renewal contract, bound to the requested identity and
policy. Unsupported wire-kind aliases are rejected.

This v1 contract is available in the generated source. It is
not yet published or evidence of Cloud service support.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/inference/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference)
