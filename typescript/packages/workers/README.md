# @acyclic-labs/workers

Workers are named script deployments, alongside actor namespace deployments. Publish a selected manifest entry, its exact JS and all declared sibling modules through the original account's Objects provider, then submit the returned canonical Actors request. The shared Rust `EntryStore` owns admission, publication and retention on native and browser callers.

This example selects `production/example-worker` from `deployment.json`, whose artifact is `worker.mjs` and whose declared module assets include `worker_bg.wasm`. Initialize the package's existing manifest module before constructing the client. No mutation is automatically retried.

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
console.log(await workers.invoke({ workerName: deployment.name, accountId: new Uint8Array(), method: "GET", url: "/", headers: [], body: new Uint8Array() }));
await workers.delete({ workerName: deployment.name, accountId: new Uint8Array(), idempotencyKey: "delete-worker" });
await workers.entries.release(intent);
```

Keep the same intent root and canonical published request when reconciling a lost deployment acknowledgement. Release the root only after confirmed deletion. Another deployment retaining the same bytes uses its own intent root.

Publication validates the selected configuration and declared bytes before any account request. Only the selected closure is retained; missing or substituted modules fail before publication. Original Objects publication and retention revisions remain in the returned acknowledgements. Cancellation closes the original request through the shared pull owner.

The public conformance case uses the real shipping Rust WASM account and selected compiled modules. It checks exact request/source identity, changed sibling closure identity, separate deployment retention, account isolation and refusal before effects. Deployment and execution require the original Actors service and host; this case qualifies the publisher.
