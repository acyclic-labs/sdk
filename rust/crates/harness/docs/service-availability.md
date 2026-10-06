# Harness services and availability

The `acyclic-harness` crate keeps its runtime provider-neutral. A consumer uses
the default dependency declaration; native and wasm targets select their
compatible bindings and storage integrations automatically. The table maps
the public surfaces to their implementation profiles; feature composition is
maintainer configuration.

| Capability | Profile | Rust surface |
| --- | --- | --- |
| Core runtime, conversation, tools, effects, and scheduling | default | `acyclic_harness::{HarnessBuilder, HarnessBundle, ...}` |
| Filesystem provider adapter | native integration profile | `acyclic_harness::filesystem` |
| Local filesystem-backed durable host | native default | Local filesystem/stream storage |
| Objects provider adapter | objects integration profile | `acyclic_harness::objects` |
| Machines adapter | machines integration profile | `acyclic_harness::machines` |
| Tonic gRPC adapter | native transport profile | `acyclic_harness::grpc::{HarnessGrpcService, transport}` |
| Wasm bindings | target-selected | `wasm32` bindings and JS projections |

The transport contract is `acyclic.harness.v2`. The
[`HarnessWireApi`](../src/wire_api.rs) trait owns handshake, submit, replay,
observe, cancellation, and operation-control semantics without choosing HTTP,
tonic, or another framing. The native transport profile provides the thin
`HarnessGrpcService` adapter; it validates wire envelopes and delegates policy
and persistence to the application implementation.

Native examples use the default profile directly, for example `cargo run --example custom_executor`. A wasm consumer uses the same dependency declaration and target selection; native local storage dependencies stay out of the browser build automatically.

An adapter integration supplies types but still requires an owner-authenticated
content binding and scope. The gRPC wrapper delegates to the application's
`HarnessWireApi` implementation.

The generated wire module and packaged descriptor are built from the v2 schema.
Clients should negotiate the shared protocol before submitting commands and
preserve operation IDs when observing or cancelling a durable operation. The
[gRPC guide](grpc.md) documents the transport wrapper; the custom executor
example demonstrates the transport-independent runtime path.
