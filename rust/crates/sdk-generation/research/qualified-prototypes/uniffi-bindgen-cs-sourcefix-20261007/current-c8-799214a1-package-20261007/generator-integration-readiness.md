# C# generator integration readiness

This package is bound to the maintained `uniffi-bindgen-cs` source revision
`e10ce410eb3a10cc19c7928b93ea8d84e038c034`, generator binary SHA-256
`59A191F74BEC339C37BE15AB70B0BF638578665B33A057C2D6BA5171298D7F55`, Rust
toolchain `1.98.1`, and UniFFI `0.31.0` workspace pins. The template patch is
generic: it adds `CancellationToken` parameters and Rust future cancellation,
and makes raw object-handle construction internal with a marker argument. A
search of the patch contains no Actors type, operation, or semantic predicate
name.

The maintained project documents the pinned generator/UniFFI pairing as
`v0.11.0+v0.31.0`. A reproducible generation step for the frozen producer is:

```text
cargo build --release -p uniffi-bindgen-cs
uniffi-bindgen-cs --library --crate <producer-crate> -c uniffi.toml -o <component-output>
```

The current cohort has two UniFFI components, so the generated component files
are merged mechanically into one C# source file; no contract types or methods
are authored in C#. The producer's Rust metadata remains the source of public
roots, wire types, and semantic validation.

## Package manifest qualifiers

The local package is `Acyclic.Actors` version `0.2.0-c8.799214a1`, targeting
`net8.0`, with Apache-2.0 producer licensing and generator MPL-2.0 attribution.
Its native assets are keyed by `win-x64`, `linux-x64`, and `osx-arm64`; the
managed assembly simple name is `Acyclic.Actors`. The package receipt records
the exact Rust revision, domain/Cargo.lock hashes, generated-source hash,
generator hash, native hashes, and fresh external-consumer logs.

Before an integration PR is prepared, freeze the signed producer revision and
rerun the one-command generation/build/package recipe with those exact hashes.
Run external PackageReference consumers on all three RIDs, full `ulong`,
in-flight cancellation/server abort cleanup, and nominal/readonly negative
compilation. Retain prior receipts as historical evidence instead of retagging
them onto a new producer.
