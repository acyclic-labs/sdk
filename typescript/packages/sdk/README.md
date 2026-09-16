# @acyclic-labs/sdk

One import for the Acyclic TypeScript SDK families. Each namespace is also available as a standalone package when you only need one service.

```sh
npm install @acyclic-labs/sdk
```

```ts
import { objects, stream } from "@acyclic-labs/sdk";

const bucket = await new objects.Objects(new objects.MemoryObjectsProvider())
  .createBucket("example");
const events = new stream.StreamClient(new stream.MemoryStreamProvider())
  .json<{ type: string }>("example/events");
await events.append({ type: "created" });
console.log(bucket.reference.name);
```

Namespaces: `harness` (agent runtime), `filesystem` (versioned workspaces), `stream` (append-only records), `objects` (immutable object versions), `machines` (machine lifecycle), and `inference` (contexts and runs). Each service has its own authentication and deployment requirements; the in-memory providers shown above are for local use, not durable hosting.

[Package guides](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages) · [Repository overview](https://github.com/acyclic-labs/sdk#readme)
