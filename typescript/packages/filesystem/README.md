# @acyclic-labs/fs

Versioned, forkable workspaces backed by browser, memory, hosted, or native providers. The import path selects the environment and its guarantees.

Directory pagination is available on an immutable generation, not the moving workspace head. Call `const generation = await workspace.sync()` once, then page through `generation.listDirectory(path, after, maximumEntries)`; retain or pin that generation for a longer-lived walk. This prevents concurrent writes from changing the set between pages.

For durable transaction retries, persist the original workspace identity, `generation.id`, 16-byte operation key and exact mutation transcript before dispatch. Reload that workspace, obtain its owned immutable handle with `await workspace.generation(originalGenerationId)`, and call `await workspace.beginTransactionAt(generation, originalOperationKey)`. Replay the same mutations and commit; do not call `beginTransaction` against a fresh head or mint a new key after a lost acknowledgement. The original-base retry retains the canonical replay, conflict and fencing semantics. It rejects generations owned by another workspace or client. Pin retained generations when retry retention outlives your provider's ordinary retention window.

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

`await openBrowserOperationWindowCoordinator(filesystem)` binds durable leases to the authority of an opened browser filesystem. Pass the returned lease to `transaction.commit(lease)` or `checkout.commit(operationId, lease)` to fence publication by closed, superseded or expired owners. The lease check and authority publication share one strict IndexedDB transaction. Construction acquires no lease; begin, close and reconciliation use the existing Rust coordinator. OPFS accelerates immutable objects in tabs and workers while IndexedDB retains authority. Control records have a 64 KiB storage ceiling. Opening older database schemas fails; use a fresh database name for this format. Atomicity covers that database, without a cross-volume or mixed-provider guarantee.

See the [browser example](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/filesystem/examples/browser.mjs), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/filesystem/src), and [Filesystem protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem).
