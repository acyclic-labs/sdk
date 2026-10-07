# Harness Rust replacement audit

Scope: current integrated Q source, compared with the Harness Rust crate and
its generated WASM exports. This is a review artifact; it does not modify
production files.

The highest-value semantic functions already route through Rust:

- `src/native-contracts.ts` delegates contract admission, identity parsing,
  model event admission, context projection, task and batch admission, and
  attachment manifest handling to generated `acyclic_harness_wasm` exports.
- `src/memory-conversation.ts` delegates conversation turn preparation and
  message validation to `WasmReducer`/Rust WASM. Its remaining code owns the
  JavaScript host adapter, mounted-volume capabilities, and bounded local
  state.
- `src/index.ts` delegates reducer creation, restore, protocol identity, and
  command application to the Rust `WasmReducer`.

The large remaining files are not safe deletion candidates without a second
Rust contract:

- `runtime.ts` contains the public task/tool registration API, user-supplied
  JavaScript handlers, schema parser callbacks, and host orchestration. Rust
  has admission projections, but no equivalent implementation for arbitrary
  JS task providers or callbacks.
- `wire-transport.ts` is a platform transport adapter for the existing Rust
  protocol. Its handshake, cancellation, and response checks are boundary
  logic rather than a second semantic model.
- `native-contracts.ts` contains public TypeScript shape declarations and
  normalization/freezing around Rust ABI values. Removing it would expose raw
  WASM ABI shapes and lose the package's immutable public view.
- `projection.ts` resolves host-provided attachment bytes and turns Rust's
  projection request into the package's provider-facing model context. Rust
  owns selection and bounds, while the host must supply bytes.

The dependency-complete reduction candidate is therefore the already-achieved
Rust delegation in `native-contracts.ts`, `memory-conversation.ts`, and the
reducer-facing part of `index.ts`. A further bulk deletion of the 8,355-line
Harness TypeScript family would require replacing public callback/orchestration
APIs and host capability adapters, which would be a new contract rather than
an implementation cleanup.
