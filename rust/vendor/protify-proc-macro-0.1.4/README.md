# Vendored `protify-proc-macro` 0.1.4

This directory vendors the published `protify-proc-macro` 0.1.4 crate from
<https://github.com/Rick-Phoenix/protify> (upstream commit
`8b74dcd0247c53d0663a3741454d83d7cb19d4b8`). The crate is licensed under
MPL-2.0; the upstream package metadata is retained in `Cargo.toml` and
`.cargo_vcs_info.json`.

The workspace uses this source through the root `[patch.crates-io]` entry. The
focused local patch changes only fallible proxied ingress: when a semantic
declaration is annotated with `fallible = E`, generated `TryFrom<Proto>`
implementations preserve validation errors instead of emitting an infallible
`From<Proto>` that can panic on invalid branded values. This keeps the Rust
semantic declaration authoritative while retaining the generated protobuf
shadow and wire descriptor. The same focused patch provides the narrow
`post_from_proto = path` ingress hook: generated field conversion completes
first, then the callback reuses a semantic type's existing constructor
predicate without a second field registry.

To reproduce the patched behavior, run the Actors crate tests and inspect the
fallible ingress cases in `rust/crates/actors/src/domain.rs`; invalid IDs,
hashes, numeric bounds, missing required messages, and invalid oneof payloads
must return typed errors without panicking.
