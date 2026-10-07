# Rust-owned language package model prototype

This prototype derives the Rust package name, library crate name, and version
from `cargo_metadata`. A generated-language artifact record is then emitted as
typed Rust data and serialized to JSON only as qualification evidence.

The model keeps four identities together:

- the Cargo package and library target that own the contract;
- the exact Rust source revision and deterministic inventory used to produce the
  artifact;
- the generator identity plus artifact hash and byte length; and
- the receipt hash that proves the claimed qualification scope.

`LanguagePackageArtifact::verify_source` detects dirty tracked or untracked
Rust-owned inputs even when `HEAD` is unchanged. `verify_artifact` and
`verify_qualification_receipt` reject substituted outputs or qualification
evidence. A passed record also requires a marker in the receipt, so a caller
cannot create a passed status from an enum alone.

Example for the maintained UniFFI Python cohort:

    cargo +1.98.1 run --manifest-path Cargo.toml --example emit \
      --source-root <sdk> \
      --manifest-path <sdk>/rust/crates/actors-uniffi/Cargo.toml \
      --package acyclic-actors-uniffi --language python \
      --generator 0.31.0 --artifact <wheel> \
      --receipt <all8-qualification-receipt.json> --receipt-marker PASS

The prototype deliberately has no hand-authored package-name or version JSON.
The source inventory includes tracked and non-ignored untracked Rust inputs
under `rust/` plus Cargo and toolchain manifests. The generator family, version,
and upstream source are recorded with the output, while qualification scope is
tied to the hashed receipt supplied by the qualification runner.
