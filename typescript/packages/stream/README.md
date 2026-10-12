# @acyclic-labs/stream

Typed append-only streams with explicit cursors, forks and atomic commits.

`npm install @acyclic-labs/stream`

Use `StreamClient.memory()` locally, `HttpStreamProvider` in browsers, or
`GrpcStreamProvider` from `@acyclic-labs/stream/grpc` with `{ endpoint, token }`.
Delivery acknowledgements are best-effort; disable with `autoAcknowledge: false`.

[API source](src/) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/stream/proto/stream)
