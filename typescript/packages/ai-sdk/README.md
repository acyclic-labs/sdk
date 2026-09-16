# @acyclic-labs/ai-sdk

Official AI SDK bridge for the provider-neutral model contract in `@acyclic-labs/harness`.

```sh
npm install @acyclic-labs/ai-sdk @acyclic-labs/harness ai
```

`aiSdkProvider({ model, streamText, reconcile })` adapts a full-stream model call to the Harness `ModelProvider` contract. `model(request)` selects the upstream model; `streamText` supplies its async `fullStream`; `reconcile` must explain what happened after an interrupted attempt. The default projector handles text, reasoning, tool calls, finish metadata, and errors. Supply `project` for another stream-part shape.

This bridge does not install or configure an upstream AI SDK or credentials. [Adapter API](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/ai-sdk/src/index.ts) · [Harness guide](https://github.com/acyclic-labs/sdk/blob/main/typescript/packages/harness/README.md)
