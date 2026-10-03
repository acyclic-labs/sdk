# Harness quickstart

`acyclic-harness` is a composition root for a durable agent runtime. The
runtime is transport-neutral: an application supplies bindings for its model,
context, tools, journal, content provider, and optional execution providers.
`HarnessBuilder` keeps those choices explicit and validates the composition at
build time.



The smallest complete replacement loop is the executable
[`examples/custom_executor.rs`](../examples/custom_executor.rs). Its executor
receives typed `TurnInput`, stages a ref-only model observation in the supplied
`ExecutionJournal`, and returns a typed `TurnOutput` with attachment refs. The
example intentionally does not copy file bytes into the durable event.

The builder starts explicitly:

```rust
use acyclic_harness::{HarnessBuilder, Result};

fn configure() -> Result<()> {
    let _builder = HarnessBuilder::new().name("my-runtime");
    // A complete build also binds either an Executor/AgentLoop or the stock
    // model, context, tools, journal, and runtime bindings.
    Ok(())
}
```

This is only the builder start; `HarnessBuilder::build` deliberately rejects
missing execution bindings. Use the custom executor example when an application
owns the complete loop. For the stock loop, bind `model`, `context`, `tools`,
and an owner-controlled `journal`, then provide the authenticated `Bindings`
required by the selected providers.

The native and wasm profiles select target-compatible providers automatically, so this example and `cargo build --target wasm32-unknown-unknown` run without consumer feature flags.

Run the complete custom-loop example with:

```sh
cargo run --example custom_executor
```

For the bounded cancellation and recovery path, the source-owned executable
scenario is [`harness_scenarios.rs`](../../sdk-examples/src/harness_scenarios.rs).
It admits and completes one task, rejects admission after cancellation, then
admits work in a fresh task group. The scenario also checks that a custom
executor cannot be built without the application-owned journal binding.

Conversation bodies and attachments are represented by owner-authenticated,
digest-pinned refs. A ref identifies content but does not grant read or write
authority; the bound provider and scope must authorize resolution.
