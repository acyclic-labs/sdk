# @acyclic-labs/harness

Framework-neutral Acyclic Agent Runtime client and the canonical Rust reducer hosted through WebAssembly. Includes typed aggregate handles, reconnect/cursors, bounded hydration, safe offline outbox, embedded/JSONL/WebSocket/HTTP-SSE/gRPC transport adapters, and native OpenAI-compatible streaming.

```sh
npm install @acyclic-labs/harness
```

Use `HarnessClient` for aggregate handles and replay-aware commands, `ProjectionStore` for UI projections, and a `WireTransport` matching your deployment.

## Connecting

`connectHarness` discovers the transports a server advertises in its handshake and builds the matching `WireTransport`, so gRPC, WebSocket, and HTTP/SSE are transparent to callers:

```ts
import { connectHarness } from "@acyclic-labs/harness";

const transport = connectHarness("https://harness.example", { negotiation });
const connection = await transport.connect(resume);
```

`auto` (the default) picks the first eligible transport in the order gRPC > gRPC-Web > WebSocket > HTTP/SSE. gRPC kinds require the `grpc` bridge option; WebSocket requires `webSocketFactory` or a global `WebSocket`. Servers that advertise nothing fall back to HTTP/SSE. `ws:`/`wss:` endpoints connect directly and `grpc:`/`grpcs:` endpoints use the bridge, both skipping discovery. Pin a transport with `options.transport` or the `ACYCLIC_TRANSPORT` environment variable; the explicit option always wins. Selection is re-evaluated on every `connect()`, never inside a live connection. The in-memory outbox/cursor stores are process-local; use a persistent store where recovery across restarts matters. Durable behavior depends on the host protocol and provider, not merely on using an async function.

[Client API](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/harness/src) · [Harness protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness) · [Repository guide](https://github.com/acyclic-labs/sdk#readme)
