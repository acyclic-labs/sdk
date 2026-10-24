# @acyclic-labs/workers

Workers are named script deployments, alongside actor namespace deployments. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), deploys a named script, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

Uses the canonical Objects and Actors contracts. Publication stores exact module bytes at `sha256/<digest>`. No mutation is automatically retried.

```ts
import { readFile } from "node:fs/promises";
import { ActorNamespace, CodeSha256 } from "@acyclic-labs/actors";
import { HttpObjectsV1 } from "@acyclic-labs/objects";
import { WorkersDeploymentClient } from "@acyclic-labs/workers";

const options = { endpoint: process.env.EDGE_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! };
const workers = new WorkersDeploymentClient(options, new HttpObjectsV1(options));
const digest = await workers.publish(await readFile("worker.mjs"), "publish-worker");
const { deployment } = await workers.deploy({
  name: await ActorNamespace("example-worker"), accountId: new Uint8Array(), jurisdiction: "", expectedRevision: 0n,
  codeSha256: await CodeSha256(digest), bindings: [], subscriptions: [],
  workerLimits: { cpuMillis: 100, memoryBytes: 268435456, timeoutMillis: 30000 },
  idempotencyKey: "deploy-worker",
});
if (!deployment) throw new Error("deploy returned no deployment");
console.log(await workers.invoke({ workerName: deployment.name, accountId: new Uint8Array(), method: "GET", url: "/", headers: [], body: new Uint8Array() }));
await workers.delete({ workerName: deployment.name, accountId: new Uint8Array(), idempotencyKey: "delete-worker" });
```

Golden fixture provenance is recorded in `rust/crates/workers/tests/fixtures/cloud-workers/golden.json`. Local tests verify serialization and client composition; live deployment qualification belongs to the coordinator.
