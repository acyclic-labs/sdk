# Kotlin generation data design notes

These are source-only design notes and derived evidence. They are deliberately
kept under `research/qualified-prototypes`; they do not publish a Maven
artifact or make the qualification package a release. The release authority is
the Rust module
`rust/crates/actors-uniffi/src/kotlin_generation_metadata.rs`; the machine-
readable file `KOTLIN-GENERATION-EVIDENCE.json` records observed artifacts and
receipts only.

## Authority and versioning

The authority chain starts at the standalone Rust package
`rust/crates/actors-uniffi/Cargo.toml`, whose `cargo metadata` result is
`acyclic-actors-uniffi` version `0.2.0`. The candidate Maven coordinate is
`dev.acyclic:acyclic-actors-uniffi-kotlin:0.2.0`; its version is generated from
`cargo.metadata.packages[name=acyclic-actors-uniffi].version`. A release step
must reject any generated POM or module metadata whose version differs from
Cargo metadata. The current installed JAR still has the qualification-only
placeholder `0.0.0-qualification`, so it is not evidence of a release
coordinate.

The source closure is anchored by Rust-owned metadata and the Actors domain hash
`79092881EC6B9434A3AC3AE6B810DCA46BB85A8E4C8C1E98191982AC5D82B4B2`, the
standalone Cargo manifest hash
`3208F2D3F9F52F9D06C8BBED59225D3B58F6BAF1132EB803C070946F9DAB7377`, its
lockfile hash
`9ECA86804D52B3AF30463F88B9F582D3B65D1ADCB92D74CD7C4BD60CFE04F890`, and
the immutable-record UniFFI configuration hash
`950AB29CBC05E20643831B08A16FA5CABB99258FB4E0EACEA1FC78D54718B599`.
UniFFI and bindgen are both pinned to `0.31.0`. The generated Kotlin hash is
`6E73BC8CA0D0C47DA51C362E98D06AFBC2B29E6F0A7F12385401B59521623A3B`.

The pinned JVM inputs are Kotlin `1.9.21`, kotlinx-coroutines `1.8.0`, JNA
`5.15.0`, and JDK target `17`. Native resources are selected by standard JNA
prefixes and are recorded with hashes in the derived evidence JSON: Linux x86_64
`AA6427DD...`, Windows x86_64 `A09452F2...`, and macOS arm64
`28D88556...`. No custom loader, `java.library.path`, or consumer native-path
setup is part of the candidate. The maintained generated binding retains
UniFFI's optional `uniffi.component.<name>.libraryOverride` property, but no
package or probe sets it.

## Typing gate

The generated records and enum payloads have the desired source-level typing:
immutable records expose `val` properties, `SubscriptionStart.Cursor` accepts
`ULong`, and `SubscriptionStart.CurrentHead` accepts `Boolean`. Kotlin rejects
wrong payload types at the call site. Rust constructors continue to own the
semantic checks for `ActorId`, `CodeSha256`, and `PositiveU64`; a Kotlin
`ULong` or byte array is not itself proof that the value is valid.

The stronger nominal-handle requirement is currently **blocked** by the
maintained generated surface. `ActorId`, `ActorsClient`, `CancellationHandle`,
`CodeSha256`, and `PositiveU64` keep their handle fields private, but each also
has a public generated constructor taking `UniffiWithHandle` and an arbitrary
`Long`; the public `NoHandle` singleton is another test-only construction path.
`javap` on the installed cohort confirms these constructors. Therefore an
external Kotlin caller can forge a wrapper value even though using it should
fail when it reaches Rust. This candidate does not claim unforgeable native
handles.

The distinction is observable without modifying generated code:

```kotlin
val forged = ActorId(UniffiWithHandle, 1L)       // currently compiles
val wrong = SubscriptionStart.Cursor(true)      // currently rejected: expects ULong
```

The first line is the release-blocking negative result; the second confirms
that the enum payload type itself is nominally checked by Kotlin.

The bounded follow-up is a Rust-owned target facade or generator option that
removes raw-handle and `NoHandle` constructors from the public package surface,
followed by an external negative compilation probe. That follow-up must be
implemented in Rust-owned metadata or maintained generator templates; editing
the generated Kotlin file is excluded.

## Reproducible generation plan

1. Read the package name and version with `cargo metadata --no-deps` from the
   standalone manifest and refuse a hand-authored Maven version.
2. Verify the domain, Cargo lockfile, UniFFI configuration, bindgen, and
   generated-source hashes in the derived evidence JSON.
3. Run the pinned UniFFI 0.31.0 generator from Rust-owned source metadata with
   immutable records enabled. Keep the generated Kotlin source unmodified.
4. Package native outputs at JNA's standard resource prefixes and verify each
   resource hash. Do not introduce a loader or native search-path property.
5. Generate the Maven POM from the candidate data, including the Cargo-derived
   version and pinned Kotlin/coroutines/JNA dependencies. The current
   `0.0.0-qualification` POM remains a qualification artifact only.
6. Run constructor, all-eight operation, service-error, and pending-cancellation
   probes on Linux x86_64, Windows x86_64, and macOS arm64. Keep the existing
   three-platform receipts attached to the same source and native hashes.
7. Before any release decision, require the nominal-handle gate above to pass;
   otherwise report the package as operationally qualified but typing-blocked.

This plan keeps package identity and release version in Rust-owned generation
data while preserving the current maintained UniFFI/JNA behavior and its
observed platform receipts.
