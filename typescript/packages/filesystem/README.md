# @acyclic-labs/fs

Versioned, forkable workspaces backed by browser, memory, hosted, or native providers. The import path selects the environment and its guarantees.

```sh
npm install @acyclic-labs/fs
```

```ts
import { DEFAULT_OBJECT_CACHE_OPTIONS, openBrowserFs } from "@acyclic-labs/fs/browser";

const fs = await openBrowserFs({
  databaseName: "my-app",
  maximumObjectBytes: 64 * 1024 * 1024,
  objectAcceleration: "opfs-if-available",
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});
const workspace = await fs.createWorkspace("main");
await workspace.write("/hello.txt", new TextEncoder().encode("hello"));
const fork = await workspace.fork("experiment");
console.log(fork.name);
```

`/browser` (also the default export) uses the browser's storage capabilities and bundled WebAssembly; `/memory` is process-local; `/hosted` connects to a service; `/native` uses the native companion. Choose the entry point explicitly when moving between environments. Workspace generations are immutable identities; transactions and forks make changes without rewriting old generations. Durability, isolation, and mount behavior depend on the chosen provider.

See the [browser example](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/filesystem/examples/browser.mjs), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/filesystem/src), and [Filesystem protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem).
