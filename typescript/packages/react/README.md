# @acyclic-labs/react

Thin React bindings for `@acyclic-labs/harness`. Runtime semantics and state remain in the framework-neutral package.

```sh
npm install @acyclic-labs/react @acyclic-labs/harness react
```

`useHarnessProjection(store)` subscribes to the complete `ProjectionStore` state. `useHarnessSelector(store, select, equal?)` subscribes to a selected value and suppresses equivalent updates. Both use React's external-store subscription contract; create and own the store outside the render path.

[Hook API](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/react/src/index.ts) · [Harness guide](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/harness/README.md)
