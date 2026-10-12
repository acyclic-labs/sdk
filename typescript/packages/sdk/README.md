# @acyclic-labs/sdk

The umbrella SDK exports Objects, Streams, Actors, Workers and filesystem namespaces, alongside Harness, Machines and Inference. The examples below use standalone public package imports; Swarm is installed separately.

```sh
npm install @acyclic-labs/objects @acyclic-labs/stream @acyclic-labs/actors @acyclic-labs/workers @acyclic-labs/fs @acyclic-labs/swarm @bufbuild/protobuf @connectrpc/connect @connectrpc/connect-node
npm install --save-dev typescript @types/node
```

Each example is a complete ES module. Save one as `example.mts`, compile with `npx tsc example.mts --module nodenext --target es2022 --types node`, then run `node example.mjs`. Use Node 22 or newer. Set `ACYCLIC_API_KEY`, the example's service-specific `*_ENDPOINT` to its HTTPS service base URL, and its resource variable (`OBJECTS_BUCKET`, `STREAM_PATH`, `ACTOR_ID`, `JOB_ID` or `FS_WORKSPACE`) to an existing resource accessible to that key. These reads require a configured deployment; compilation does not call it. Actors/Workers and Streams require their published Rust WASM/native runtime artifacts.

In this checkout, `bun run check:docs` compiles these exact snippets against source and verifies the copies in [llms.txt](../../../llms.txt). Build the generated WASM declarations with the normal package build first when absent. [Rust examples](../../../rust/README.md) are checked separately by doctests.

## Objects

Logical buckets and objects, with conditional writes and multipart uploads. This example reads an existing bucket over Node HTTP/2 gRPC using the exported canonical service descriptor.

```ts
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { BucketsService } from "@acyclic-labs/objects/proto";

const endpoint = process.env.OBJECTS_ENDPOINT!;
const session = new Http2SessionManager(endpoint);
const buckets = createClient(BucketsService, createGrpcTransport({
  baseUrl: endpoint, sessionManager: session, defaultTimeoutMs: 30_000,
}));
try {
  console.log(await buckets.headBucket({ bucket: { name: process.env.OBJECTS_BUCKET! } }, {
    headers: { authorization: `Bearer ${process.env.ACYCLIC_API_KEY!}` },
  }));
} finally {
  session.abort();
}
```

## Streams

Append-only record streams with forks and atomic commits. This example reads the tail position of an existing stream through the hosted HTTP provider.

```ts
import { HttpStreamProvider } from "@acyclic-labs/stream";

const streams = new HttpStreamProvider({
  endpoint: process.env.STREAM_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
console.log(await streams.tail(process.env.STREAM_PATH!));
```

## Actors

Durable actor identities, invocation and subscriptions. This example inspects an existing actor using the Rust-backed client.

```ts
import { ActorsClient, ActorId } from "@acyclic-labs/actors";

const actors = new ActorsClient({
  endpoint: process.env.ACTORS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
console.log(await actors.inspectActor({
  actorId: await ActorId(process.env.ACTOR_ID!),
}));
```

## Workers

Cloud Workers are stateless Actors. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), creates a deployment, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

```ts
import { readFile } from "node:fs/promises";
import { CodeSha256, PositiveU64 } from "@acyclic-labs/actors";
import { HttpObjectsV1 } from "@acyclic-labs/objects";
import { WorkersDeploymentClient } from "@acyclic-labs/workers";

const options = { endpoint: process.env.EDGE_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! };
const workers = new WorkersDeploymentClient(options, new HttpObjectsV1(options));
const digest = await workers.publish(await readFile("worker.mjs"), "publish-worker");
const { actor } = await workers.create({
  codeSha256: await CodeSha256(digest), homeRegion: "la", bindings: [], subscriptions: [],
  limits: { handlerTimeoutMillis: await PositiveU64(30000n), memoryBytes: await PositiveU64(268435456n), checkpointBytes: await PositiveU64(1048576n) },
  idempotencyKey: "create-worker",
});
if (!actor) throw new Error("create returned no deployment");
console.log(await workers.invoke({ actorId: actor.actorId, method: "GET", url: "/", headers: [], body: new Uint8Array() }));
await workers.delete({ actorId: actor.actorId, idempotencyKey: "delete-worker" });
```

## fs

Versioned filesystem workspaces and generations. This example opens an existing workspace through Node HTTP/2 gRPC.

```ts
import { createFilesystemGrpcClient } from "@acyclic-labs/fs/grpc";

const client = createFilesystemGrpcClient({
  endpoint: process.env.FS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
try {
  console.log(await client.filesystem.openWorkspace({
    selector: { case: "name", value: process.env.FS_WORKSPACE! },
  }));
} finally {
  client.close();
}
```

## Swarm

Agent orchestration, spend and progress over the public Swarm HTTP/SSE API. This example lists swarms visible to the caller. Swarm is a standalone package; the umbrella SDK does not export it.

```ts
import { SwarmClient } from "@acyclic-labs/swarm";

const swarms = new SwarmClient({
  endpoint: process.env.SWARM_ENDPOINT!, token: process.env.ACYCLIC_API_KEY!,
});
console.log(await swarms.list());
```
