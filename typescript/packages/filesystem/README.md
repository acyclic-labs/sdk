# @acyclic-labs/fs

Versioned, forkable workspaces backed by browser, memory, hosted, or native providers. The package root selects the provider from the Rust-owned option shape; explicit subpaths remain available when an application needs a fixed environment.

Directory pagination is available on an immutable generation, not the moving workspace head. Call `const generation = await workspace.sync()` once, then page through `generation.listDirectory(path, after, maximumEntries)`; retain or pin that generation for a longer-lived walk. This prevents concurrent writes from changing the set between pages.

```sh
npm install @acyclic-labs/fs
```

```ts
import { DEFAULT_OBJECT_CACHE_OPTIONS, openFs } from "@acyclic-labs/fs";

const fs = await openFs({
  databaseName: "my-app",
  maximumObjectBytes: 64 * 1024 * 1024,
  objectAcceleration: "opfs",
  objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
});
const workspace = await fs.createWorkspace("main");
await workspace.write("/hello.txt", new TextEncoder().encode("hello"));
const fork = await workspace.fork("experiment");
console.log(fork.name);
```

The package root's `openFs` uses the hosted provider when given `endpoint` and `bearerToken`, the native companion when given `root`, and the browser's bundled WebAssembly provider for browser options. `/browser`, `/memory`, `/hosted`, and `/native` remain explicit environment entry points. Workspace generations are immutable identities; transactions and forks make changes without rewriting old generations. Durability, isolation, and mount behavior depend on the chosen provider.

See the [browser example](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/filesystem/examples/browser.mjs), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/filesystem/src), and [Filesystem protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem).
