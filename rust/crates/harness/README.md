# acyclic-harness

Provider-neutral Rust agent runtime with typed conversations, durable tasks,
workflow journals, effects and forks. `HarnessBuilder` composes the runtime with
explicit model, content, history and execution providers. Durability depends on
the selected providers and retained journal artifacts.

Limits and signed scopes bound admission and content access. Files are pinned
references; replay verifies retained inputs and preserves operation identities.
Uncertain effects require reconciliation before their reservations are released.

Optional adapters: `filesystem`, `filesystem-local`, `native-execution`,
`objects`, `machines`, `grpc`, `mcp-http`, `mcp-oauth`, `terminal` and `browser`.
Native process execution requires explicit approval and authority.

See the [API](https://docs.rs/acyclic-harness), [filesystem adapter](docs/filesystem.md),
[terminal](docs/terminal.md) and [client views](docs/client/README.md).
`examples/custom_executor.rs` demonstrates a custom turn loop; the contract
examples generate shared Rust/TypeScript fixtures.
