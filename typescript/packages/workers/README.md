# @acyclic-labs/workers

Workers v1 generated public contract and authenticated HTTP client. Publish
exact JavaScript module bytes, select a deployment alias by expected revision,
and submit durable jobs. Accepted jobs pin the resolved code digest even if the
alias later changes. Invocation has ordinary HTTP ambiguity and does not
acknowledge a durable job. The live Cloud executor and public route are not yet
qualified against this contract.

An ES module may implement `default.fetch(Request)` for HTTP, `default.run(bytes,
context)` for durable jobs, or both. The package exports Rust-generated
`WorkerModule` and `WorkerJobContext` types. Retried jobs receive the same job ID
and input with a higher attempt number; external effects need application
idempotency.

Object inputs name an S3 bucket and key. The service privately retains the
accepted bytes for retries. Job results contain exact bytes bounded by the
accepted output budget (at most 1 MiB), without public Object version references.

The Rust crate `acyclic-workers` and `proto/workers/v1/workers.proto` own the
contract. `WorkersClient` uses the canonical Rust binding; import generated
request schemas from the package or `@acyclic-labs/workers/proto`. Configure
`endpoint` and `token`; optional `ca` adds a private PEM CA and
`maxMessageBytes` bounds requests and responses.

Workerd and Cloudflare Pages must import the compiled WASM asset and initialize
the binding before creating clients:

```ts
import compiledModule from "@acyclic-labs/workers/wasm/module.wasm";
import { initializeWorkersWasm, WorkersClient } from "@acyclic-labs/workers";

await initializeWorkersWasm(compiledModule);
const client = new WorkersClient({ endpoint, token, transport: "wasm" });
```

Configure the deployment bundler to load `*.wasm` as compiled WebAssembly modules.
This path does not read Node files or compile WASM bytes at runtime. Native-only
loaders remain lazy and are not initialized on the WASM path.

Omit `expectedRevision` only to create an absent alias; replacing an existing
selection requires its positive current revision.
