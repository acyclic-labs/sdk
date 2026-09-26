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

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/inference/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference)
