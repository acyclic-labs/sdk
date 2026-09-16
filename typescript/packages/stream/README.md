# @acyclic-labs/stream

Typed, hierarchical append-only streams with explicit cursors, conditional appends, forks, and coordinated commits.

```sh
npm install @acyclic-labs/stream
```

```ts
import { MemoryStreamProvider, StreamClient } from "@acyclic-labs/stream";

const client = new StreamClient(new MemoryStreamProvider());
const events = client.json<{ type: string }>("runs/example");
await events.append({ type: "started" });
for await (const record of events.read({ from: 0n, limit: 100 })) {
  console.log(record.sequence, record.value.type);
}
```

For a service, use `new StreamClient(new HttpStreamProvider({ endpoint, token }))` or `Stream.fromEnv()` with `ACYCLIC_STREAM_ENDPOINT` and `ACYCLIC_API_KEY`. Service endpoints must be HTTPS. Sequence numbers are `bigint`; persist the last consumed cursor and resume reads or `follow` from the appropriate position. Use `ifTail` for optimistic append concurrency and an idempotency key when retrying mutations.

`MemoryStreamProvider` is deterministic and process-local, not a durable service. See the [full quickstart](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/stream/examples/quickstart.ts), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/stream/src), and [protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/stream).
