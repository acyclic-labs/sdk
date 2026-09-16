# @acyclic-labs/opencode

Independently versioned OpenCode-compatible model bridge for `@acyclic-labs/harness`.

```sh
npm install @acyclic-labs/opencode @acyclic-labs/harness
```

`openCodeProvider({ request, stream, reconcile, part? })` converts OpenCode-style text, reasoning, tool, completion, and error parts into Harness `ModelEvent`s. `request` maps the Harness request, `stream` yields upstream parts, and `reconcile` resolves an interrupted attempt. Supply `part` when the upstream event shape differs from `OpenCodePart`.

This is a contract adapter, not an OpenCode installation or credential manager. [Bridge API](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/opencode/src/index.ts) · [Harness guide](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/harness/README.md)
