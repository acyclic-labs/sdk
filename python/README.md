# Acyclic Python transport prototype

This package is a bootstrap transport target for Rust-authority schema inputs.
It is intentionally transport-only: generated messages and gRPC stubs are
present, while retries, endpoint policy, canonical semantic errors, and the
idiomatic facade will be generated from Rust-owned metadata in the next layer.

The Rust generator requires exactly `grpcio-tools==1.83.0` and
`protobuf==7.36.0` in the Python environment. First emit the authority export
from the Rust contract model, then generate Python from that directory:

```text
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --locked -- \
  generate --out target/rust-authority
cargo run --manifest-path rust/crates/sdk-python/Cargo.toml --locked -- \
  generate --schema-root target/rust-authority \
  --output python/src/acyclic_sdk/generated
python -m pytest -q python
set SOURCE_DATE_EPOCH=1735689600
python -m pip wheel ./python --no-deps --no-build-isolation --wheel-dir python/dist
```

The Python helper only invokes `grpc_tools.protoc`; it enumerates every family
and in-root imported dependency listed by `rust-authority.json`. The Rust CLI
validates every family source and descriptor content hash, pins tool versions,
adds package initializers, normalizes plugin imports, and owns generate/check
drift behavior. The generated files are disposable outputs; edit the
Rust-owned schema or generator only.
