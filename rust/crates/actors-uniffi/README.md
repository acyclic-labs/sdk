# Actors UniFFI facade

This standalone crate is the Rust-owned UniFFI facade for the current Actors
source. It is deliberately outside the SDK workspace until a generated
language package is qualified.

The crate pins the complete UniFFI cohort to `0.31.0` and uses proc-macro
metadata only; there is no UDL or second contract. `ActorId`, `CodeSha256`, and
`PositiveU64` are opaque foreign objects whose constructors call the canonical
`acyclic_actors::domain` constructors. `connect_actors`,
`connect_actors_with_ca`, and all eight `ActorsClient` operations call the
canonical Rust client and use its `run_with_cancellation` helper through an
explicit `CancellationHandle`. CA bytes are passed to the existing Rust-owned
`connect_with_ca_certificate` path.

The opt-in `bindgen` feature exposes a one-function wrapper around the same
UniFFI `0.31.0` CLI. It is used only to generate external qualification files;
those files are not a supported Kotlin, Swift, or Python package.

Kotlin release inputs are source-owned by
`src/kotlin_generation_metadata.rs`. The qualification generator applies the
maintained-template patch under `generator-patches/` before building the pinned
bindgen binary; it makes generated opaque-object raw-handle and `NoHandle`
constructors `internal` while preserving same-module converter/lifting calls.
The generated Kotlin file is never hand-edited, and Maven version data is
derived from this crate's Cargo package version.

The result records are adapter metadata over Rust-owned domain accessors. They
do not contain wire messages, transport code, retry logic, or validators.
Optional Actor observation presence is preserved. The current domain rejects
unknown enum values as typed semantic errors; an `Unknown { raw }` projection
remains a later compatibility decision if the Actors domain changes to retain
unknown values.

## Qualification boundary

The source authority is foundation revision `371bb4170e16aca973176b6756a261ee5add7297`.
The authoritative Actors domain hash used for this slice is
`79092881EC6B9434A3AC3AE6B810DCA46BB85A8E4C8C1E98191982AC5D82B4B2`.

The facade currently includes all eight request/operation paths: create,
update, inspect, add subscription, remove subscription, resume subscription,
checkpoint, and invoke. No supported native artifact or language package is
claimed yet; generated outputs remain qualification artifacts until an
installable package has been built and exercised from this source authority.
