# `acyclic-protify-proc-macro`

This directory vendors the published `protify-proc-macro` 0.1.4 crate from
<https://github.com/Rick-Phoenix/protify> (upstream commit
`8b74dcd0247c53d0663a3741454d83d7cb19d4b8`). The crate is licensed under
MPL-2.0; the exact upstream license and `.cargo_vcs_info.json` are retained.
The package has a distinct Acyclic identity and follows the SDK release version.

Actors depends directly on this versioned package for its library and build
script; consumers do not need a workspace patch. Cargo publication order places
this package before Actors. The tooling has an independent workspace so product
feature selection does not enable unrelated generator features. The
focused local patch supports fallible proxied ingress: when a semantic
declaration is annotated with `fallible = E`, generated `TryFrom<Proto>`
implementations preserve validation errors instead of emitting an infallible
`From<Proto>` that can panic on invalid branded values. This keeps the Rust
semantic declaration authoritative while retaining the generated protobuf
shadow and wire descriptor. The same focused patch provides the narrow
`post_from_proto = path` ingress hook: generated field conversion completes
first, then the callback reuses a semantic type's existing constructor
predicate without a second field registry.

The patch also preserves required message presence, returns typed errors for
unknown enum values, exposes the generated wire shadow types publicly, and
correctly infers bytes fields. These gaps are covered by the Actors contract
and ingress tests; protobuf algorithms remain upstream-owned.

To reproduce the patched behavior, run the Actors crate tests and inspect the
fallible ingress cases in `rust/crates/actors/src/domain.rs`; invalid IDs,
hashes, numeric bounds, missing required messages, and invalid oneof payloads
must return typed errors without panicking.
