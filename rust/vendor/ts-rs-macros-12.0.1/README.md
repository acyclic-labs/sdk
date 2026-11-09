# ts-rs-macros 12.0.1

Source: the crates.io `ts-rs-macros` 12.0.1 release, archive SHA-256
`38d90eea51bc7988ef9e674bf80a85ba6804739e535e9cab48e4bb34a8b652aa`,
upstream commit `7182ad8289596097235406b715fa04506443b4ad`, directory `macros`.
The MIT license is retained in LICENSE.

The shared attribute parser recognizes Serde's `try_from` type path and `other`
unit marker. These control deserialization; the generated TypeScript continues
to describe serialization. The original Serde derives, validation, naming and
catch-all behavior remain authoritative. Unknown attributes still report the
upstream diagnostic; `no-serde-warnings` is not enabled.

The parser control compares the generated serialization against the same
declaration without ingress metadata and rejects malformed metadata. Actors'
existing metadata properties, additive-event properties and generated authoring
contract check cover the actual public declarations.

The same parser also recognizes the unit `deny_unknown_fields` marker on structs
and enums, and selects the serialized field of `transparent` containers. Named
and tuple wrappers share the existing newtype generator; serialization skips and
`PhantomData` match Serde's serializer derivation. Explicit TypeScript overrides
still take precedence. Invalid marker assignments and transparent containers
without exactly one serialized field are rejected.

`cargo test --locked --manifest-path rust/vendor/ts-rs-macros-12.0.1/Cargo.toml --features serde-compat container_serialization_preserves_transparent_fields`
checks the actual generated representation. The retained production mutations in
`tests/mutations` remove container recognition, enum ingress recognition, or the
serialized-field selection. Apply a mutation with `git apply`, run its control,
then restore it with `git apply -R`; each mutation must fail its control.

The `fuzz` package includes the production attribute parser directly. Its
`serde_attributes` target exercises all four metadata parsers and compares
transparent field selection with Serde's own checked serialization AST. The
`wire-item.rs` corpus entry is captured from `rust/crates/model-render/src/item.rs`
(`WireItem`, including its skipped cache); `request.rs` is the original private
reservation `Request` enum in `rust/crates/actors/src/reservation.rs`, including
its tagged representation and ingress restriction. The remaining entries cover the named,
marker-only and invalid container shapes used by the representation control.
Run `cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/vendor/ts-rs-macros-12.0.1/fuzz --features fuzzing serde_attributes --target x86_64-unknown-linux-gnu --dev -- -runs=0`
to replay that corpus with coverage instrumentation. The selection mutation must
fail on the original `WireItem` against the real Serde AST.
