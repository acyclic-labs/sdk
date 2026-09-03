import { mkdir } from "node:fs/promises";
import {
  DEFAULT_OBJECT_CACHE_OPTIONS,
  openNativeFs,
} from "@acyclic/fs/native";

const root = process.env.ACYCLIC_FS_EXAMPLE_ROOT ?? ".acyclic-fs";
await mkdir(root, { recursive: true });

const fs = await openNativeFs({ root, objectCache: DEFAULT_OBJECT_CACHE_OPTIONS });
const workspace = await fs.createWorkspace("main");
const payload = new TextEncoder().encode("# workspace\n");
const committed = await workspace.write("/README.md", payload);
if (committed.status !== "committed" && committed.status !== "already-committed") {
  throw new Error(`workspace publication failed: ${committed.status}`);
}
const fork = await workspace.fork("scratch");
fs.close();

const reopened = await openNativeFs({ root, objectCache: DEFAULT_OBJECT_CACHE_OPTIONS });
const durable = await reopened.openWorkspace("main");
const observed = await durable.read("/README.md", BigInt(payload.byteLength));
if (new TextDecoder().decode(observed) !== "# workspace\n") {
  throw new Error("workspace did not survive a clean engine reopen");
}
console.log({ workspace: durable.name, fork: fork.name, bytes: observed.byteLength });
reopened.close();
