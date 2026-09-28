# @acyclic-labs/stream

Typed, hierarchical append-only streams with explicit cursors, conditional appends, forks, and coordinated commits. Committed records remain readable from the beginning of a stream; paths and committed history are permanent.

`childrenPage({ parent, limit })` returns direct children with an immutable 32-byte hierarchy version and an optional `nextAfter` cursor. Pass both `after` and `hierarchyVersion` for the next page; a path creation between pages fails with `hierarchy_changed` instead of silently skipping or duplicating agents. `childrenAll(parent, limit)` and its `children(parent, limit)` convenience alias use that same continuation loop, including hierarchies larger than 1,024 children. Ancestor paths are materialized when a nested stream is created.

```sh
npm install @acyclic-labs/stream
```

```ts
import { MemoryStreamProvider, StreamClient } from "@acyclic-labs/stream";

const event = (value: import("@acyclic-labs/stream").JsonValue): { readonly type: string } => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || typeof value.type !== "string") {
    throw new TypeError("expected an event");
  }
  return { type: value.type };
};
const client = new StreamClient(new MemoryStreamProvider());
const events = client.json("runs/example", value => {
  if (value === null || typeof value !== "object" || Array.isArray(value) ||
      !("type" in value) || typeof value.type !== "string") throw new TypeError("expected event");
  return { type: value.type };
});
await events.append({ type: "started" });
for await (const record of events.read({ from: 0n, limit: 100 })) {
  console.log(record.sequence, record.value.type);
}
```

Calling `client.json(path)` gives a stream of `JsonValue`. Pass a parser when the stream has a narrower type; the parser validates each decoded record before it reaches the application.

For a service, use `new StreamClient(new HttpStreamProvider({ endpoint, token }))` or `Stream.fromEnv()` with `ACYCLIC_STREAM_ENDPOINT` and `ACYCLIC_API_KEY`. Service endpoints must be HTTPS. Sequence numbers are `bigint`; persist the last consumed cursor and resume reads or `follow` from the appropriate position. Use `ifTail` for optimistic append concurrency and an idempotency key when retrying mutations.

`MemoryStreamProvider` runs the Rust memory provider through package-local WebAssembly. It is deterministic and process-local; use a durable service for reads across restarts. See the [full quickstart](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/stream/examples/quickstart.ts), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/stream/src), and [protocol](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/stream/proto/stream).
