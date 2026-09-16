# acyclic-harness-grpc

Tonic gRPC server adapter for the transport-neutral `acyclic-harness` wire API. It handles transport framing and validation; the application still supplies a `HarnessWireApi` implementation and its persistence and authorization policy.

```sh
cargo add acyclic-harness-grpc
```

See the [adapter API](https://docs.rs/acyclic-harness-grpc/latest/acyclic_harness_grpc/), [Harness API](https://docs.rs/acyclic-harness/latest/acyclic_harness/), and [wire protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness).
