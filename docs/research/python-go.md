# Python and Go SDK qualification research

## Scope

The target architecture makes Rust the source of truth for wire contracts, SDK metadata, behavior policy, and documentation inputs. The `sdk-contract-wire` Rust CLI now emits Actors and Stream protobufs, descriptors, and a `rust-authority.json` manifest. The generated Python modules below are transport-only: they expose wire messages and gRPC stubs, while the idiomatic facade and policy layer remain a later generated layer derived from Rust metadata.

## Python

The pinned prototype uses `grpcio-tools==1.83.0` and `protobuf==7.36.0`. The standalone Rust generator requires the explicit `sdk-contract-wire` authority export, validates its `rust-authority.json` manifest and descriptor files, and then invokes the thin `grpc_tools.protoc` helper. It stages `_pb2.py` and `_pb2_grpc.py` under `src/acyclic_sdk/generated` and writes package initializers, so generated imports remain installable from a wheel.

The runtime tests cover Protobuf bytes, uint64 values above JavaScript's safe integer range, proto3 optional presence, oneof selection, Actors unary calls, Stream server streaming, bearer metadata, and cancellation. The test server uses the generated service definitions and a real local gRPC channel. The installed wheel is also used by the Rust-owned Actors and Stream snippets; each receipt binds the wheel, rendered source path, source revision, source digest, snippet digest, and captured runtime output.

Python's `grpc.aio` API is the intended async facade substrate. A stream call is cancellable via `call.cancel()` or task cancellation. The generated transport package does not retry or reinterpret errors.

## Go

The qualification uses a contained portable Go `1.27.1` toolchain with pinned `protoc-gen-go v1.36.10`, `protoc-gen-go-grpc v1.5.1`, `google.golang.org/protobuf v1.36.10`, and `google.golang.org/grpc v1.76.0`. The generated tree is consumed through a temporary Go module with an explicit `replace`, so the check compiles the exact rendered snippet against the installed generated package. The Actors and Stream snippets execute against the Rust fixture server; the Go receipt records the generated-tree digest, source binding, snippet digest, and stdout/stderr digests.

```text
protoc -I proto -I rust/crates/stream/proto \
  --go_out=paths=source_relative:go/gen \
  --go-grpc_out=paths=source_relative:go/gen \
  proto/actors/v1/actors.proto \
  rust/crates/stream/proto/stream/v2/stream.proto
```

The existing `go_package` options identify the module paths. Qualification must include unary calls, server streaming, client streaming from Objects, context deadlines/cancellation, serial per-stream reads/writes, bytes, uint64, optional scalars, oneofs, gRPC status details, and bearer metadata. The completed installed-package checks cover the Actors limits `(handler_timeout_millis=1000, memory_bytes=1048576, checkpoint_bytes=4096)`, the fixture actor response, Stream sequences `0, 1`, duplicate idempotency replay, tail conflict, and context cancellation (`Canceled`). A Go facade should be generated from Rust-owned operation metadata; generated transport bindings alone are not the final SDK.

## Generator choice

Cloudflare Forge is an OpenAPI and TypeScript surface generator. It may consume a derived OpenAPI document for website presentation, but it cannot preserve the gRPC streaming, oneof, and retry semantics in these Protobuf contracts. It is not the canonical Python/Go generator for this repository.

For embedded Rust behavior, Python should use PyO3/maturin and Go should use a narrow C ABI generated with cbindgen plus cgo. Those bindings must call the same Rust provider/conformance implementation rather than reimplementing it in the target language.
