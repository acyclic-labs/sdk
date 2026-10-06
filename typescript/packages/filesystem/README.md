# @acyclic-labs/fs

Versioned, forkable workspaces backed by browser, memory, hosted, or native providers. The import path selects the environment and its guarantees.

Directory pagination is available on an immutable generation, not the moving workspace head. Call `const generation = await workspace.sync()` once, then page through `generation.listDirectory(path, after, maximumEntries)`; retain or pin that generation for a longer-lived walk. This prevents concurrent writes from changing the set between pages.

```sh
npm install @acyclic-labs/fs
```

```ts
import { DEFAULT_OBJECT_CACHE_OPTIONS, openBrowserFs } from "@acyclic-labs/fs/browser";

const fs = await openBrowserFs({
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

`/browser` (also the default export) uses the browser's storage capabilities and bundled WebAssembly; `/memory` is process-local; `/hosted` connects to a service; `/native` uses the native companion. Choose the entry point explicitly when moving between environments. Workspace generations are immutable identities; transactions and forks make changes without rewriting old generations. Durability, isolation, and mount behavior depend on the chosen provider.

Node hosts that need platform-owned child processes may import
`openNativeProcessOwner` from `/native` and pass the returned owner to their
process bridge. Loading is lazy and capability-checked: a companion without
the versioned native process-owner capability fails explicitly, so a host never
mistakes ordinary Node process groups for Windows Job ownership. The owner
returns typed termination outcomes and keeps uncertain cleanup visible to the
caller.

Standalone Node hosts that do not have a native companion can import the
bounded fallback owner from `/native-process-node`. It starts an owned process
group and performs bounded direct-child cleanup, but reports descendant cleanup
as uncertain until a native owner can prove ownership; it is not a sandbox.

See the [browser example](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/filesystem/examples/browser.mjs), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/filesystem/src), and [Filesystem protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem).
