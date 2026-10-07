# Rust-owned language package model prototype

This prototype derives the Rust package name, library crate name, and version
from `cargo_metadata`. A generated-language artifact record is emitted as
typed Rust data and serialized to JSON only as qualification evidence.

The model keeps four identities together:

- the Cargo package and library target that own the contract;
- the exact Rust source revision and deterministic package-closure inventory;
- the pinned generator family and version; and
- the artifact and qualification receipt hashes.

The receipt is a typed JSON document. A passed record requires its source
revision, `source_inventory_sha256`, generator version, generator source digest, language-specific
artifact entry, artifact path, artifact hash, and byte length to match the
current Rust checkout and produced artifact. A marker alone is not sufficient.
The source inventory covers the selected Cargo package and local path
dependencies, workspace Cargo/toolchain manifests, and protocol/generated Rust
inputs. This keeps local verification bounded while detecting dirty source.

`LanguagePackageArtifact::verify_source` detects source changes even when
`HEAD` is unchanged. `verify_artifact` and
`verify_qualification_receipt` reject substituted outputs or evidence.

Example for a canonical typed receipt produced by the release qualification
runner:

    cargo +1.98.1 run --manifest-path Cargo.toml --example emit \
      --source-root <sdk> \
      --manifest-path <sdk>/rust/crates/actors-uniffi/Cargo.toml \
      --package acyclic-actors-uniffi --language python \
      --generator 0.31.0 --generator-source-sha256 <patch-or-source-digest> \
      --artifact <wheel> \
      --receipt <typed-qualification-receipt.json> --receipt-marker PASS

The current historical all8 receipt is intentionally rejected because it does
not contain the exact source inventory digest required by this model. The
release gate must emit that digest in the typed receipt before a `passed`
record can be claimed.

The existing Rust integration point is `rust/crates/sdk-generation/src/main.rs`,
where the generator already owns the `source: Vec<FileHash>` and
`source_sha256` fields in `acyclic.sdk.generation.v1`. The prototype's
`source_inventory_from_generation_manifest` consumes that vector and verifies
its declared digest; it does not emit a competing source manifest. The smallest
production port is therefore to move that adapter and receipt binding into the
existing generator module, while keeping Cargo metadata as the sole package and
target identity source. No separate TypeScript or JSON authority is needed.

