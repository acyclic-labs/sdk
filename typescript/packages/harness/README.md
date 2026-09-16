# @acyclic-labs/harness

Framework-neutral Acyclic Agent Runtime client and the canonical Rust reducer hosted through WebAssembly. Includes typed aggregate handles, reconnect/cursors, bounded hydration, safe offline outbox, embedded/JSONL/WebSocket/HTTP-SSE/gRPC transport adapters, and native OpenAI-compatible streaming.

```sh
npm install @acyclic-labs/harness
```

Use `HarnessClient` for aggregate handles and replay-aware commands, `ProjectionStore` for UI projections, and a `WireTransport` matching your deployment. The in-memory outbox/cursor stores are process-local; use a persistent store where recovery across restarts matters. Durable behavior depends on the host protocol and provider, not merely on using an async function.

[Client API](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/harness/src) · [Harness protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness) · [Repository guide](https://github.com/acyclic-labs/sdk#readme)
