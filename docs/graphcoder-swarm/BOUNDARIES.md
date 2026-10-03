# GraphCoder ownership boundaries

GraphCoder is the public composition and presentation edge of the local runtime. Its
TypeScript package owns the transport-neutral API, the injected transport adapter,
the wire dispatcher, and terminal or process host adapters.

| Area | Owner | GraphCoder responsibility |
| --- | --- | --- |
| Public application API and UI projection | GraphCoder | `GraphCoderTransport`, `GraphCoderUi`, typed IDs, validation, and detached snapshots |
| JSON-lines and native host adapters | GraphCoder | request correlation, bounded framing, explicit executable and environment inputs |
| Recursive orchestration | Harness | durable admission, child identity, recursive dispatch, cancellation, takeover, and recovery |
| Journals and policy | Harness | execution journals, approvals, owner fences, effect outcomes, model/provider policy, and replay |
| Workspace and source checkout integration | Filesystem | generations, files, diffs, Git/writeback, and workspace authorization |
| Model execution | Host plus Harness | GraphCoder carries an explicit operation and optional fixture name; it does not select or implement a production provider |
| Web, cloud, and sandbox integration | Host or separate product boundary | GraphCoder has no such imports or runtime dependencies |

The root `@acyclic-labs/graphcoder` export is intentionally Node free. It reaches
only the API, injected bridge contract, transport adapter, mock fixture, and wire
dispatcher. Node process, readline, and stream capabilities are available only
through explicit host subpaths:

- `@acyclic-labs/graphcoder/terminal` for terminal I/O
- `@acyclic-labs/graphcoder/node` for an explicitly configured child-process bridge
- `@acyclic-labs/graphcoder/node-dispatcher` for a Node stream dispatcher
- `@acyclic-labs/graphcoder/native-cli` for the explicit native runtime launcher

The package export map is part of this boundary. Host subpaths must remain lazy:
loading the root API cannot load a process, terminal, filesystem, network, cloud, model,
or sandbox implementation. The package has no runtime dependencies; the host supplies
the bridge and owns the executable, arguments, working directory, environment, and
lifecycle.

Public GraphCoder wire methods and their parameter/result maps have one canonical
source in `src/bridge.ts`. `src/dispatcher.ts` consumes those types and dispatches
through the injected `GraphCoderTransport`; it must not create another durable
session store, orchestration reducer, journal, policy engine, workspace adapter, or
model provider. Generated Harness and Filesystem contracts remain shared Rust-owned
artifacts; GraphCoder does not add a private generated schema or duplicate their
domain reducers. The deterministic mock transport is a test and demonstration fixture,
not a production provider.

The source qualification is
[`scripts/graphcoder-boundaries.test.mjs`](../../scripts/graphcoder-boundaries.test.mjs).
It checks the package export map, root dependency closure, lazy host subpaths,
absence of forbidden runtime imports, and single-source public contract definitions.
Run it with:

```text
node --test scripts/graphcoder-boundaries.test.mjs
```

A passing source check establishes the composition boundary only. It does not replace
Harness, Filesystem, native process, WASM, installed package, or PTY qualification.


