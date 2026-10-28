import { readFile } from "node:fs/promises";
import { ActorNamespace, CodeSha256 } from "@acyclic-labs/actors";
import { HttpObjectsV1 } from "@acyclic-labs/objects";
import { WorkersDeploymentClient } from "@acyclic-labs/workers";

const options = { endpoint: process.env.EDGE_ENDPOINT!, token: process.env.ACYCLIC_API_KEY! };
const workers = new WorkersDeploymentClient(options, new HttpObjectsV1(options));
const digest = await workers.publish(await readFile("worker.mjs"));
const { deployment } = await workers.deploy({
  name: await ActorNamespace("example-worker"), accountId: new Uint8Array(), jurisdiction: "", expectedRevision: 0n,
  codeSha256: await CodeSha256(digest), bindings: [], subscriptions: [], secrets: [],
  workerLimits: { cpuMillis: 100, memoryBytes: 268435456, timeoutMillis: 30000 },
  idempotencyKey: "deploy-worker",
});
if (!deployment) throw new Error("deploy returned no deployment");
console.log(await workers.invoke({ workerName: deployment.name, accountId: new Uint8Array(), method: "GET", url: "/", headers: [], body: new Uint8Array() }));
await workers.delete({ workerName: deployment.name, accountId: new Uint8Array(), idempotencyKey: "delete-worker" });
