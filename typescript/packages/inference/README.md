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

`Inference` retains a client-instance identity and prepares explicit mutation identities with `inference.identity()`. Its `client` is an `InferenceClient` over the configured transport. Use generated protobuf requests with `createContext`, `mutateContext`, and `generate`; Context revisions are immutable byte identities. Save the admitted Run ID to resume observation with `inspectRun`, `watchRun`, or `resultRun`. Items use the generated protobuf `Item` type. Warm commitments have separate `retainWarm`, `renewWarm`, `inspectWarm`, and `releaseWarm` operations on the client.

For custom authentication, pass shared Auth credential options to `HttpInferenceTransport`, for example `{ credential: cache.provider }` for a `CredentialCache`. The transport retains that exact request lease through dispatch, body reads and each streamed event. Registering the source watch before resolution lets a source change cancel pending headers and the original body reader. Absolute expiry still uses the custodian's original checks and deadline; no timer is invented. HTTP 401 rejects only its original lease at headers and retires its diagnostic body, returning `credential rejected`; a late rejection cannot erase a replacement. HTTP 403, other service errors and caller cancellation leave the lease intact. No request is automatically retried. Raw header callbacks remain per-attempt callbacks without source validity or rejection custody.

The transport requires an absolute HTTPS URL and enforces bounded responses. The client validates protobuf responses through the bundled Rust WebAssembly contract. Import generated protobuf types and schemas from `@acyclic-labs/inference/proto`.

## Idle KV pins (source contract)

Discover an opaque policy in `models[].idleKvProfiles`, then pass a generated
`RetainWarmRequest` with `identity`, `context` and `idleKv` to
`inference.client.retainWarm(request)`. Use `inference.identity()` to prepare
the request identity. Rust prepares the same policy with
`inference.retention_request(revision, Retention::idle_kv(profile, idle_timeout_ms))`
and submits it with `inference.retain_warm(&request).await`. This paid KV pin
does not guarantee capacity, throughput, or latency. Keep the prepared request
and identity for retries.

Pass a generated `RenewWarmRequest` with the pin's `commitment`, a new `identity`,
`idleTimeoutMs` and zero `expiresAtMs` to `inference.client.renewWarm(request)`.
Rust uses `inference.renew_warm(&request).await` with the corresponding
`wire::RenewWarmRequest`. Renewal changes the timeout measured from the last verified actual Run reuse, or from the initial
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
