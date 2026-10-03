# Go SDK qualification

Go remains a viable target. This directory contains generated packages for all
nine Rust authority families plus the validation options dependency, and a
pinned module for their runtime dependencies.
The module is distributed under the repository's Apache-2.0 `LICENSE` and
`NOTICE` files.

A portable Go `1.27.1` Windows amd64 archive was downloaded from the official
Go distribution and verified with SHA-256
`a3911b5e0e1b1053f25ed0675f4c1c6aad1e2bfcf253df2b9be4caabd2edd95d`. The
archive is kept only under the ignored `rust/crates/sdk-python/target/tools`
qualification directory. The pinned plugins are available there as
`protoc-gen-go v1.36.10` and `protoc-gen-go-grpc 1.5.1`; their binaries are
placed in the sibling `gobin` directory so the toolchain and plugin paths are
portable and explicit.

Generate from the explicit Rust authority export with the same schema root
required by the Python generator:

```text
python -m grpc_tools.protoc -I <rust-authority-export> \
  --plugin=protoc-gen-go=<absolute-path-to-protoc-gen-go> \
  --plugin=protoc-gen-go-grpc=<absolute-path-to-protoc-gen-go-grpc> \
  --go_out=paths=source_relative:go/gen \
  --go-grpc_out=paths=source_relative:go/gen \
  actors/v1/actors.proto stream/v2/stream.proto objects/v2/objects.proto \
  workers/v1/workers.proto filesystem/v2/filesystem.proto harness/v2/harness.proto \
  machines/v1/machines.proto inference/v1/inference.proto protocol/v1/protocol.proto
```

The qualification package must pin the Go module, `google.golang.org/protobuf`,
and `google.golang.org/grpc`. Its tests must cover unary Actors calls, server
streaming and cancellation in Stream, client streaming from Objects, context
deadlines, serial stream reads/writes, bytes, uint64, optional scalars, oneofs,
status details, and bearer metadata. The eventual facade must be emitted from
Rust-owned operation metadata; these generated files only qualify transport
coverage.

Executed qualification evidence:

See [qualification-matrix.md](qualification-matrix.md) for the per-family
receipt matrix and pending rows.

* `go test -mod=readonly ./...` passes with the portable toolchain (including
  vet). The checked-in transport tests cover generated Actors unary metadata,
  Stream server streaming, and context cancellation.
* A portable Go client marshals real generated Actors and Stream messages and
  sends them as protobuf octet streams to `rust/crates/sdk-examples`'s Rust
  fixture server. Actors create, Stream append, and Stream read all pass,
  including bytes, uint64, optional scalar, oneof, and metadata-bearing fields.
