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
console.log(await workers.invoke({ workerName: deployment.name, accountId: new Uint8Array(), method: "GET", url: "/", headers: [], body: new Uint8Array() }));
await workers.delete({ workerName: deployment.name, accountId: new Uint8Array(), idempotencyKey: "delete-worker" });
await workers.entries.release(intent);