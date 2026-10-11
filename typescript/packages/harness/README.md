# @acyclic-labs/harness

TypeScript facade for the Rust-first Harness v2 runtime. It includes typed aggregate handles, reconnect/cursors, bounded hydration, an offline outbox, embedded/JSONL/WebSocket/HTTP-SSE/gRPC transports, explicit conversation selection, and native OpenAI-compatible model projection.

```sh
npm install @acyclic-labs/harness
```

The facade keeps composition small while Rust owns admission. A live local
runtime can be assembled once and receive text or ordered file parts through
the same typed entry point:

```ts
const contracts = await NativeContracts.create();
const runtime = await Harness.builder(contracts)
  .limits({ model_steps: 8, attachments: 1024 })
  .grant("model:generate")
  .model(modelIdentity, modelProvider)
  .build();

await runtime.run({
  prompt: "Summarize the supplied file",
  content: [{ kind: "file", file, policy: "bounded_full" }],
});
```

`file` is an already staged, owner-authorized `FileRef`; its bytes never enter
the conversation event or the model request until the selected provider
projection resolves it. Use `MemoryConversation.runConversation` when the
turn must also publish canonical user/assistant history and attachment refs.

The `@acyclic-labs/harness/browser` entry exports `BrowserTaskAuthority`,
`BrowserTaskRegistry`, `BrowserTaskRuntime`, and generated contracts over the
ordinary Rust task runtime. Call `initializeBrowserRuntime()`, register versioned
machines or `registerStockTurn()`, and open with explicit IndexedDB providers,
finite limits, and a signed scope. Initialize volumes separately from admission.
Model composition supplies the selected model's synchronous context-capacity
and canonical request-token callbacks; missing accounting is explicitly unsupported.
Model, tool, and mail recovery retains original operation identities and fences.
Competing tabs and reopened workers use the same coordinator, journals and stores;
no native filesystem or process fallback is selected.

[Client views](docs/client-views.md) · [Demand and recovery](docs/client-demand.md) · [Harness protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness)
