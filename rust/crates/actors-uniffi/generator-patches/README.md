# Maintained Kotlin custom-type policy

UniFFI 0.31 models Rust `custom_type!` declarations as Kotlin typealiases by
 default. The generated surface therefore loses the nominal distinction among
 `ActorId`, `CodeSha256`, and `PositiveU64`, even though Rust lifting validates
 them. The maintained generator patch in this directory adds a config-driven
 nominal wrapper mode. It generates private-constructor Kotlin value classes,
 Rust-backed `from(...)` factories, and converters that use the wrapper value.

The policy is task-local and applies to the exact pinned `uniffi_bindgen`
0.31.0 source. It must be applied to a fresh source archive before building the
bindgen executable; the global Cargo registry is never modified.

`uniffi.nominal.toml` is the producer config for the patched executable. The
Rust domain remains the semantic authority: each generated factory calls an
exported Rust validator, and that validator invokes the canonical Rust
constructor before a request is lowered. The Rust custom-type lift still
rejects malformed native values. No semantic predicate is authored in the
Kotlin template or in the config.

`CurrentHead(false)` remains rejected by the Rust domain projection. This
policy does not rewrite the published oneof into a handwritten foreign mirror;
the producer must either expose a unit-like semantic variant or add a Rust
custom type for the validated `true` payload before a no-argument foreign
constructor can be claimed.

