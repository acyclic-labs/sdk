# Harness services and availability

The default `acyclic-harness` crate contains the provider-neutral runtime. Its
provider adapters are opt-in Cargo features:

| Capability | Feature | Rust surface |
| --- | --- | --- |
| Core runtime, conversation, tools, effects, and scheduling | default | `acyclic_harness::{HarnessBuilder, HarnessBundle, ...}` |
| Filesystem provider adapter | `filesystem` | `acyclic_harness::filesystem` |
| Local filesystem-backed durable host | `filesystem-local` | Enables `filesystem` plus local filesystem/stream storage |
| Objects provider adapter | `objects` | `acyclic_harness::objects` |
| Machines adapter | `machines` | `acyclic_harness::machines` |
| Tonic gRPC adapter | `grpc` | `acyclic_harness::grpc::{HarnessGrpcService, transport}` |
| Wasm bindings | `wasm` | `wasm32` bindings and JS projections |

The transport contract is `acyclic.harness.v2`. The
[`HarnessWireApi`](../src/wire_api.rs) trait owns handshake, submit, replay,
observe, cancellation, and operation-control semantics without choosing HTTP,
tonic, or another framing. The `grpc` feature provides the thin
`HarnessGrpcService` adapter; it validates wire envelopes and delegates policy
and persistence to the application implementation.

An adapter feature does not create a provider or grant authority. Enabling
`filesystem` supplies integration types but still requires an owner-authenticated
content binding and scope. Likewise, `grpc` exposes a server wrapper but does
not supply a `HarnessWireApi` implementation.

The generated wire module and packaged descriptor are built from the v2 schema.
Clients should negotiate the shared protocol before submitting commands and
preserve operation IDs when observing or cancelling a durable operation. The
[gRPC guide](grpc.md) documents the transport wrapper; the custom executor
example demonstrates the transport-independent runtime path.
