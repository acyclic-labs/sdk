import {
  DEFAULT_OBJECT_CACHE_OPTIONS,
  openBrowserFs,
} from "@acyclic/fs/browser";

const fs = await openBrowserFs({
  databaseName: "acyclic-fs-example",
  maximumObjectBytes: 64 * 1024 * 1024,
  objectAcceleration: "opfs-if-available",
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});
const workspace = await fs.createWorkspace("main");
await workspace.write("/README.md", new TextEncoder().encode("# workspace\n"));
const scratch = await workspace.fork("scratch");
console.log({
  workspace: workspace.name,
  scratch: scratch.name,
  generation: [...await workspace.head()],
});
