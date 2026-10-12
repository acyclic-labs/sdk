# @acyclic-labs/workers

Cloud Workers are stateless Actors. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), creates a deployment, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

Uses the canonical Objects and Actors contracts. Publication stores exact module bytes at `sha256/<digest>`. No mutation is automatically retried.

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

Golden fixture provenance is recorded in `rust/crates/workers/tests/fixtures/cloud-workers/golden.json`. Local tests verify serialization and client composition; live deployment qualification belongs to the coordinator.
