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
python -m build --sdist --wheel --no-isolation --outdir target/python-artifacts
```

For a source-bound installable package, use the Rust packaging entrypoint after
the Rust authority and Python generated subtree have been emitted. It stages a
fresh source tree, excludes the checkout's disposable `python/dist` output,
builds both wheel and sdist artifacts, and verifies that each archive contains
the Rust-owned `remote.py`, `py.typed`, and generation metadata:

```text
cargo run --manifest-path rust/crates/sdk-python/Cargo.toml --locked -- \
  package --source-root python \
  --generated-root target/python-generated \
  --output target/python-package \
  --python python
```

The package command requires `build==1.3.0` in the selected Python
environment. It clears only its explicit external output directory and never
reads or writes `python/dist`.

The Python helper only invokes `grpc_tools.protoc`; it enumerates every family
and in-root imported dependency listed by `rust-authority.json`. The Rust CLI
validates every family source and descriptor content hash, pins tool versions,
adds package initializers, normalizes plugin imports, and owns generate/check
drift behavior. The generated files are disposable outputs; edit the
Rust-owned schema or generator only.
