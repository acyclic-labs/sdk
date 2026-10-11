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

The `/native` directory transport uses the existing Rust capture and immutable
archive implementation: `captureNativeDirectoryArchive` returns an opaque
original capture and bounded archive chunks. `nativeDirectoryArchiveFromChunks`
authenticates received bytes; `importNativeDirectoryArchive` uses the same safe
native parser, not ambient extraction. `replaceCapturedNativeDirectoryArchive`
reconciles into the original volume, retaining unchanged identities and
metadata. Capture roots must resolve through real directory components, without
intermediate symlinks or reparse points.

Retain the original operation ID and private `stateRoot`. After restart,
`restoreNativeDirectoryCapture` and `exportNativeDirectoryArchive` reopen the
original receipt and immutable logical generation, never recapture later
physical writers. The capture's `preview`, `apply` and `recover` use existing
generation and materialization-journal contracts; changed paths must still match
their original physical preimages, while unrelated later writers are untouched.
Local capture/archive/apply proof does not qualify a hosted upload or live turn.

The 0.2.0 native release matrix requires Node 24 proof for GNU Linux x64/ARM64, Darwin x64/ARM64, and MSVC Windows x64/ARM64. It also requires Bun 1.4.2 native runtime proof on those targets except Windows ARM64: that Bun release ships only an x64 Windows runtime, usable for installation tooling but not for loading an ARM64 addon. Windows ARM64 native consumers use ARM64 Node. Every companion is admitted against the exact release source and retained compiler/runtime receipts; one local Linux pass does not qualify the complete release.

For repository qualification, first build a clean-source canonical bundle and retain its original compiler receipt. For example, on GNU Linux x64:

```sh
bun install --frozen-lockfile
cargo fetch --locked
bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
native_output="$(mktemp -d)"
native_target="$(mktemp -d)"
node scripts/build-filesystem-native.mjs build \
  --target x86_64-unknown-linux-gnu --output "$native_output/bundle" \
  --target-dir "$native_target"
bun run check:native-adapter --bundle "$native_output/bundle" \
  --producer-receipt "$native_target/filesystem-native-build-inputs.receipt.json"
```

Run these commands from the repository root with the pinned Node, Bun, Rust and wasm-bindgen tools installed. The root adapter command builds the public filesystem distribution and runs the Node-attested checker, including its real supported Bun consumer. Both paths must be absolute; the checker accepts no default `.so`/DLL, debug-target or receipt fallback.

Qualification hashes the installed companion resolved from the generated loader, rejects bytes differing from the source-bound producer, and verifies that the public loader selects that same addon. Node and supported Bun consumers repeat the installed-byte check before running the maintained filesystem model. The retained runtime receipt includes this installed artifact identity; bundle metadata or matching package versions alone are not installation proof. Install caches are private to each archive qualification.

The maintained installed consumer also requires the source-qualified companion and digest explicitly:

```sh
node typescript/packages/filesystem/test/native-public-installed.mjs \
  /absolute/installation/node_modules/@acyclic-labs/fs \
  @acyclic-labs/fs-linux-x64-gnu sha256:EXPECTED_NATIVE_DIGEST
```

`await openBrowserOperationWindowCoordinator(filesystem)` binds durable leases to the authority of an opened browser filesystem. Pass the returned lease to `transaction.commit(lease)` or `checkout.commit(operationId, lease)` to fence publication by closed, superseded or expired owners. The lease check and authority publication share one strict IndexedDB transaction. Construction acquires no lease; begin, close and reconciliation use the existing Rust coordinator. OPFS accelerates immutable objects in tabs and workers while IndexedDB retains authority. Control records have a 64 KiB storage ceiling. Opening older database schemas fails; use a fresh database name for this format. Atomicity covers that database, without a cross-volume or mixed-provider guarantee.

See the [browser example](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/filesystem/examples/browser.mjs), [API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/filesystem/src), and [Filesystem protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/filesystem).
