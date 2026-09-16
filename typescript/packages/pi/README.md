# @acyclic-labs/pi

Independently versioned Pi-compatible model bridge for `@acyclic-labs/harness`.

```sh
npm install @acyclic-labs/pi @acyclic-labs/harness
```

`piProvider({ request, run, reconcile, event? })` converts Pi-style text, thinking, tool, completion, and error events into Harness `ModelEvent`s. `request` maps the Harness request, `run` yields upstream events, and `reconcile` resolves an interrupted attempt. Supply `event` when the upstream event shape differs from `PiEvent`.

This is a contract adapter, not a Pi installation or credential manager. [Bridge API](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/pi/src/index.ts) · [Harness guide](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/harness/README.md)
