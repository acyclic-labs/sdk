# @acyclic-labs/sdk

The umbrella SDK exports Objects, Streams, Actors, Workers and filesystem namespaces, alongside Harness, Machines and Inference. The examples below use standalone public package imports; Swarm is installed separately.

```sh
npm install @acyclic-labs/objects @acyclic-labs/stream @acyclic-labs/actors @acyclic-labs/workers @acyclic-labs/fs @bufbuild/protobuf @connectrpc/connect @connectrpc/connect-node
npm install --save-dev typescript @types/node
```

Each example is a complete ES module, generated from the runnable sources in [examples/sdk-services/examples](../../../examples/sdk-services/examples) by `node scripts/generate-sdk-readmes.mjs write`. Save one as `example.mts`, compile with `npx tsc example.mts --module nodenext --target es2022 --types node`, then run `node example.mjs`. Use Node 22 or newer. Set `ACYCLIC_API_KEY`, the example's service-specific `*_ENDPOINT` to its HTTPS service base URL, and `STREAM_PATH`, `ACTOR_ID` or `FS_WORKSPACE` to an existing resource accessible to that key. The Workers example also reads `deployment.json`, `worker.mjs` and its declared `worker_bg.wasm` asset. These operations require a configured deployment; compilation does not call it. Actors/Workers and Streams require their published Rust WASM/native runtime artifacts.

In this checkout, `bun run check:docs` compiles these exact snippets against source and verifies the copies in [llms.txt](../../../llms.txt). Build the generated WASM declarations with the normal package build first when absent. [Rust examples](../../../rust/README.md) are checked separately by doctests.

## Native commands in Node.js

The Node-only `@acyclic-labs/sdk/process` export provides `commandProcess`,
`commandCapture` and `withProcessSignals`. Install the matching
`acyclic-process-host` executable from `acyclic-native-runtime` and supply its
absolute path as `host`. For example,
`await commandCapture(process.execPath, ['--version'], {host, signal})` returns
UTF-8 `stdout` and `stderr`. Omit `maxBuffer` for unbounded capture or pass an
explicit per-stream byte budget. Use `commandProcess` to stream output without
retaining it. Capture retains bytes already written when cancellation occurs;
only exceeding the explicit capture budget stops accumulation.
Pass `stdio` to forward any standard stream directly to a caller-owned file
descriptor or inherited stream. Only piped outputs reach `onOutput` or capture;
the caller keeps its forwarded descriptors open. Extra descriptors and IPC
channels are unsupported.

The native owner admits the selected command, waits on OS exit events and
retires its descendants on completion or cancellation. The cancellation channel
is separate from command stdin, including when `input` is a `Readable`.
The caller owns `signal` and any deadline. Cancellation requests native tree
retirement immediately; it has no implicit grace timer or configurable signal.
The command's executable and arguments pass directly to the native launcher,
with no shell expansion. Unsupported physical custody is an explicit failure.

`onOutput(stream, bytes, active)` may return a promise for an accepted write;
both streams and those writes settle before return. A failed sink requests
cancellation and retains its original cause. `onTerminal` is called once,
immediately on a local failure or after the original terminal is available.
Nonzero exits are reported before joining accepted output, so an output sink
can release its pending write from that terminal. The terminal callback's own
accepted work is also joined before return.
`onClose` sees the selected root's exit code or signal. `commandProcess` returns
the original bound completion plus the first original failure per phase and
stream, with bounded failure storage; `commandCapture`
throws the original cause or an `AggregateError` with all retained causes.
An input source that outlives the command is destroyed when that command exits.

Validate the published entry and real native executable together with
`cargo test --locked -p acyclic-native-runtime --test node_process` from `rust/`.
Node.js must be installed. The control uses real input/output, a root with a
descendant, caller cancellation, accepted output writes and native receipts.
Run `node --test test/process-admission.test.mjs` from this package to check
host-admission failures through the public entry without an installed host.
It uses a real failed OS spawn, open command input and accepted terminal work.

## Objects

Account-scoped, content-addressed blobs: equal bytes name one blob, manifests join parts, and retained roots keep blobs alive across collection. This example publishes bytes and reads a range back over HTTP.

```ts
import { HttpObjectsV1, verified } from "@acyclic-labs/objects";

const objects = new HttpObjectsV1({ endpoint: process.env.OBJECTS_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! });
const put = verified(await objects.put(new TextEncoder().encode("Hello")));
if (!put) throw new Error("outcome unknown; repeating the put is safe");
console.log((await objects.get({ digest: put.digest, range: { start: 1n }, maximumBytes: 4n })).body);
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
  accountId: new Uint8Array(),
}));
```

## Workers

Cloud Workers are stateless Actors. This example selects `production/example-worker` from `deployment.json`, publishes its exact module closure, deploys it, invokes it and deletes it before releasing the retained root.

```ts
import { readFile } from "node:fs/promises";
import { HttpObjectsV1 } from "@acyclic-labs/objects";
import { WorkersDeploymentClient } from "@acyclic-labs/workers";
import { initializeManifest } from "@acyclic-labs/workers/manifest";

await initializeManifest({ module_or_path: await readFile(new URL(import.meta.resolve("@acyclic-labs/workers/manifest.wasm"))) });
const options = { endpoint: process.env.EDGE_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! };
const workers = new WorkersDeploymentClient(options, new HttpObjectsV1(options));
const intent = new TextEncoder().encode("deployment:deploy-example-worker");
const published = await workers.entries.publishDeployment(intent, {
  manifest: await readFile("deployment.json", "utf8"), environment: "production", entry: "example-worker",
  artifact: await readFile("worker.mjs"),
  files: [{ path: "worker_bg.wasm", bytes: await readFile("worker_bg.wasm") }],
}, "deploy-example-worker");
const { deployment } = await workers.deploy(published);
if (!deployment) throw new Error("deploy returned no deployment");
console.log(await workers.invoke({ workerName: deployment.name, accountId: deployment.accountId, method: "GET", url: "/", headers: [], body: new Uint8Array(), deploymentEnvelope: new Uint8Array() }));
await workers.delete({ workerName: deployment.name, accountId: deployment.accountId, idempotencyKey: "delete-worker" });
await workers.entries.release(intent);
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
