# Harness topic guide

This guide is the Rust source counterpart for the existing Harness topic
routes. The package is `acyclic-harness` at the workspace version (`0.2.0`).

| Existing topic data module | Rust-owned section | Source anchor |
| --- | --- | --- |
| `overview`, `composition` | Composition and overview | [`bundle.rs`](../src/bundle.rs) |
| `quickstart` | Harness quickstart | [`examples/custom_executor.rs`](../examples/custom_executor.rs) |
| `context` | Context | [`context.rs`](../src/context.rs) |
| `tools` | Tools | [`tool.rs`](../src/tool.rs) |
| `recursive-execution` | Recursive execution | [`runtime.rs`](../src/runtime.rs), [`live.rs`](../src/live.rs) |
| `state-recovery` | State recovery | [`executor.rs`](../src/executor.rs), [`durable_tool.rs`](../src/durable_tool.rs) |
| `extensions`, `design-references` | Extensions and design references | [`extension.rs`](../src/extension.rs), [`conversation.rs`](../src/conversation.rs) |
| `infrastructure` | Infrastructure and providers | [`runtime.rs`](../src/runtime.rs) |
| `reference` | Wire and reference | [`wire_api.rs`](../src/wire_api.rs), [`grpc.rs`](../src/grpc.rs) |

## Composition and overview ([bundle.rs](../src/bundle.rs), [lib.rs](../src/lib.rs))

`HarnessBuilder` is the composition root. `bindings`, `model`, `context`,
`tools`, `journal`, `content`, `artifacts`, `execution`, `fork_preparer`, and
`workspaces` each bind one explicit dependency. `build` validates the scope,
provider combinations, and required execution path. The
[quickstart](quickstart.md) links the executable custom executor example.

## Context ([context.rs](../src/context.rs))

`ContextPipeline` holds ordered `ContextStage` implementations. `SourceStage`
and `CompactionStage` are explicit stages; `stage_names` and `contracts`
expose their declared projections. `DurableContextProvider` records bounded
context revisions and compaction references. Context selection is separate from
conversation storage and does not authorize content reads by itself.

## Tools ([tool.rs](../src/tool.rs))

`ToolDefinition` describes the stable name, revision, schemas, and limits;
`Tool` combines that definition with a `ToolExecutor`; and `ToolRegistry`
registers and selects exact revisions. `ToolInvocation::for_model_call` and
`ToolResult` are validated before dispatch. Tool grants and the configured
`ToolPolicy` are checked independently, so registering a tool does not grant a
model permission to call it.

## Recursive execution ([live.rs](../src/live.rs), [runtime.rs](../src/runtime.rs))

`TaskGroup` provides bounded local `spawn` and `spawn_many` operations; the
`join_all`, `race`, `first_success`, `quorum`, and `ordered_reduce` helpers
observe those handles. The durable runtime's task group additionally exposes
typed `map`, `join`, and `as_completed` operations. Child groups must receive
their own concurrency bound. Durable descendants use the typed task and
spawner bindings; a live Rust future is not silently converted into a
resumable durable operation.

## State recovery ([executor.rs](../src/executor.rs), [durable_tool.rs](../src/durable_tool.rs), [scheduler.rs](../src/scheduler.rs))

`ExecutionJournal`, `DurableTaskHost`, `TaskStateProvider`, and `TaskSpawner`
are separate bindings. Admission retains the operation identity and pinned
implementation; observation and cancellation use that identity for
reconciliation. `ResumableToolRegistry` and `ResumableToolSession` persist
validated checkpoints and ref-only command outboxes. Unknown effects remain
reconcilable rather than being re-executed blindly.

## Extensions and design references ([extension.rs](../src/extension.rs), [conversation.rs](../src/conversation.rs))

`ExtensionIdentity` pins a name, version, and digest. `ExtensionRegistry` can
install, disable, pin, and inspect implementations; `ExtensionRuntime` links
the selected tasks and tools into a binding set. `NativeExtensionBundle` and
`HarnessBuilder::extensions` keep extension selection explicit and durable.
Design references are represented by the typed conversation/content contracts;
`FileRef` identity is digest-, owner-, and provider-bound, and possessing one
does not grant access.

## Infrastructure and providers ([runtime.rs](../src/runtime.rs), [filesystem](../src/filesystem/mod.rs), [objects](../src/objects.rs))

The core runtime remains provider-neutral. The native default profile includes `filesystem-local` for durable local filesystem and stream persistence; `filesystem`, `objects`, `machines`, and `grpc` add their respective adapters. Target selection keeps native storage dependencies out of the browser build while preserving the portable runtime surface. The [availability table](service-availability.md)
is the source for feature and target claims. An adapter supplies integration
types only: applications still provide owner authentication, scopes, and
provider implementations.

## Wire and reference ([wire_api.rs](../src/wire_api.rs), [grpc.rs](../src/grpc.rs), [wire_codec.rs](../src/wire_codec.rs))

`HarnessWireApi` is the transport-neutral contract for handshake, submit,
replay, observe, cancellation, and operation control. The `grpc` feature wraps
it in `HarnessGrpcService` and generated `transport` bindings. The wire identity
is `acyclic.harness.v2` with the shared `acyclic.protocol.v1` handshake. The
packaged descriptor and rustdoc output are the reference artifacts; route or
service claims must be qualified against the current source revision and
feature profile.
