# acyclic-harness

Provider-neutral durable substrate and composable agent runtime. The deterministic `core` semantics are separate from `live` helpers: a Rust future is not automatically a durable operation.

```sh
cargo add acyclic-harness
```

Use the core contracts to define tasks, resource references, and replayable operations; add a host adapter only for the transport you deploy. The [custom executor example](https://github.com/acyclic-labs/sdk/blob/main/rust/crates/harness/examples/custom_executor.rs) shows composition. See the [Rust API](https://docs.rs/acyclic-harness/latest/acyclic_harness/) for feature-gated modules and the [Harness protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness) for wire semantics.
