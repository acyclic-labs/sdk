# @acyclic-labs/svelte

Thin Svelte bindings for `@acyclic-labs/harness`. Runtime semantics and state remain in the framework-neutral package.

```sh
npm install @acyclic-labs/svelte @acyclic-labs/harness svelte
```

`harnessReadable(store)` exposes the complete `ProjectionStore` as a Svelte-readable store. `selectedHarnessReadable(store, select, equal?)` emits only when the selected value changes. The adapter subscribes and unsubscribes with the Svelte store lifecycle; persistence and transport remain the Harness client's responsibility.

[Store API](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/svelte/src/index.ts) · [Harness guide](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/harness/README.md)
