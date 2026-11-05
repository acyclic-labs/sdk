# Generated client corpus

`grpc_codegen/actors-native.rs` comes from maintained Tonic writer output read
by `actors::codegen::generate` immediately before its call to the public
`acyclic_grpc_observability::codegen::observe_clients` entrance. After capture,
the generated output is normalized with `rustfmt --edition 2024`; no source is
edited by hand.

- SDK source: `e80c6f4b92a309347b75d2e41a65bfc60bee3f5e`.
- Source tree: `c6809f12e4902886f6eb7a0b5af30c10c46b6746`.
- Capture: `cargo test -p acyclic-actors local::sockets::tests::socket_conformance_passes_on_local_runtime`.
- Target: `x86_64-unknown-linux-gnu`; architecture: `x86_64`.
- Original producer source SHA-256: `f6b36aa788b0dcb34e7f9f6fb72ecb02dbfef32ca205b69ec7f2490194ee3318`.
- Protocol SHA-256: `7c34419f478f01c5cbb9e63ffdde636d3c2dde7d3cb763073d24b2a5296e7c4d`.
- Generator configuration SHA-256: `965262633800f86147f367168705c95394debad253b5229253ba5feb611dde83`.
- Original input SHA-256: `a4cf99440d683be0397b17422b0e889e2a48a5ca4baa5118565d817f127283a0`.
- Formatter: `rustfmt 1.9.0-stable (48a229ceae 2026-09-01)`.
- Formatted input SHA-256: `3792c996064608c1dbdf5dfd711102c7588e02e14c86ff5f595858e59a461a5f`.

When refreshing this corpus, capture the generator's output at that same
boundary, then run
`rustfmt --edition 2024 rust/fuzz/corpus/grpc_codegen/actors-native.rs`
before committing. Record both the original and formatted hashes.

The shared replay/fuzz oracle calls the shipped observer. Every accepted output
must retain all original syntax after removing exactly one observation wrapper
per original async client method and the one appended span factory. RPC names
must come from the same original metadata. The recorded seed checks all thirteen
RPCs, server and synchronous methods. Derived streaming, missing-metadata and
truncated inputs must be refused. These mutations are derived from the recorded
input, not authored substitutes for an external producer.

Replay with `cargo test --locked -p acyclic-sdk-fuzz --no-default-features --features grpc-codegen --test grpc_codegen`.
From `rust/fuzz`, run `cargo +nightly fuzz run --fuzz-dir . --no-default-features --features grpc-codegen grpc_codegen`.
Parser fuzz feedback does not establish generated-program type correctness or
runtime transport behavior; the original producer and transport controls remain
separate qualifications.
