# Generated protobuf compatibility views

The Rust contract model is the source of truth for the wire contract and every
derived SDK or documentation input. It lives in
`rust/crates/sdk-contract-wire/src` and owns field numbers, presence, oneofs,
enum values, RPC identities, streaming directions, routes, validation options,
operation policies, and Rust-owned documentation.

The `.proto` files in this directory are generated source views and compatibility
outputs. They are not authoring inputs and must not be edited by hand. The
generator also emits descriptor sets, the Rust authority manifest, and family
goldens; those artifacts are the authoritative outputs consumed by downstream
generators. A fresh output directory can be generated and checked with:

```text
cargo run --locked --offline \
  --manifest-path rust/crates/sdk-contract-wire/Cargo.toml \
  --bin sdk-contract-wire -- generate --out target/sdk-contract
cargo run --locked --offline \
  --manifest-path rust/crates/sdk-contract-wire/Cargo.toml \
  --bin sdk-contract-wire -- check --out target/sdk-contract
```

The generator must never read this directory, generated Rust or TypeScript
bindings, or any other derived artifact to discover the contract. The reverse
dependency is intentional:

```text
Rust contract model
  -> protobuf source and descriptors
  -> validation/options and operation policy metadata
  -> OpenAPI, SDKs, snippets, and website documentation
```

The versioned packages remain explicit: Actors, Inference, Machines, Objects,
and Workers use v1; Filesystem, Harness, and Stream use v2. `protocol/v1`
contains the shared version handshake and `validation/v1` contains the custom
option declarations. Those files are rendered from Rust as well.

`objects/v1` and the descriptor fixtures under
`rust/crates/sdk-contract-wire/tests/fixtures` are archived compatibility
baselines. Their bytes, field identities, and handshake digests are immutable;
they are retained for regression and migration checks and are never regenerated
from a newer model. A protocol transition must add a new versioned Rust model
and an explicit compatibility gate.

When a Rust model or its documentation changes, regenerate the derived output
and run `check`; do not patch a generated `.proto` to make drift disappear.
Downstream SDK generators and the website consume the Rust-bound descriptors,
metadata, and docs inputs, with this directory serving the human-readable
protobuf compatibility view.
