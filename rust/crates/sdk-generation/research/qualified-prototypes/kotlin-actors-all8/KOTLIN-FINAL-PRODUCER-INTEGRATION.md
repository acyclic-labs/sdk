# Kotlin/JVM integration at the final Rust semantic producer

This note defines the smallest maintained integration for the Actors Kotlin/JVM
package after the single-domain cutover. The semantic producer is
`acyclic-actors::domain`; the UniFFI crate is a foreign-function facade and the
Maven project is a packaging consumer. The generated Kotlin binding stays a
derived artifact, and the package remains a qualification artifact until a
release lane is explicitly adopted.

## Current cutover evidence

The inspected producer checkout is
`Q:\sdk\work\sdkgen-main-actual03bb` at revision
`03bbf867c32ab61dfb262ab20fdf9d31a4e6dae1`. Its working tree is dirty because
the single-domain migration is in progress. The producer has no
`rust/crates/actors-uniffi` directory yet, so the exact qualified JAR remains a
task-local artifact from the earlier facade checkout.

The producer-owned extraction points are already clear:

- `rust/crates/actors/src/domain.rs` has the `export_roots!` list used by
  `export_typescript`; it currently names 19 semantic roots, including the
  eight request and eight response types plus `PositiveU64`, `ErrorCode`, and
  `ServiceError`.
- `rust/crates/actors/src/client.rs` has eight `operation!` declarations:
  create, update, inspect, add subscription, remove subscription, resume
  subscription, checkpoint, and invoke.
- `rust/crates/actors/src/lib.rs` has the eight canonical HTTP route names.

The source hashes and exact JAR/resource presence are captured by
`audit/kotlin-final-producer-source.receipt.json` and the raw terminal output
in `audit/kotlin-final-producer-source.raw.txt`. The inspected JAR is
`acyclic-actors-uniffi-kotlin-0.2.0.jar`, SHA-256
`9877BE2BB3FF38964F05044A1EBA1101B0F70595D3956BCE84F6866E43DB7AE3`; all
three standard JNA resource prefixes are present.

## Maintained dependency graph

The final producer owns semantic types, validation, wire conversion, transport
operations, and cancellation behavior. A maintained `acyclic-actors-uniffi`
facade should depend on `acyclic-actors` by path and contain only the UniFFI
surface: opaque wrappers, FFI records/enums, error conversion, and the client
entrypoints. It must not copy validators, transport code, or semantic request
definitions.

The generation driver then resolves one exact source closure:

1. the root and Actors Cargo manifests and lockfile;
2. `actors/src/domain.rs`, `client.rs`, `lib.rs`, `contract.rs`,
   `contract_definitions.rs`, `codegen.rs`, and `build.rs`;
3. the UniFFI facade manifest, `src/lib.rs`, `uniffi.toml`, and maintained
   Kotlin template patch;
4. the pinned UniFFI 0.31.0 dependency and Rust toolchain; and
5. the Kotlin/JVM package inputs: Kotlin 1.9.21, coroutines 1.8.0, JNA 5.15.0,
   and JVM target 17.

The driver runs `cargo metadata` from the facade checkout, derives the Maven
version from the facade Cargo package version, applies the local generator
patch to the pinned bindgen source, and packages native libraries beneath
JNA's `Platform.RESOURCE_PREFIX` directories. It must refuse a generated
package when any source or lock hash differs from the hashes used to build the
native resources. No registry or package publication is part of this lane.

## Source metadata changes required for regeneration

The current producer exposes enough information for a proof-of-concept
extractor, but the long-term generator should consume explicit Rust metadata
rather than parse source text. The required small changes are:

1. Move the `export_roots!` type list into a public producer-owned constant or
   metadata function and have `export_typescript` consume it. This gives every
   language generator the same semantic roots and prevents a Kotlin-only list
   from drifting.
2. Give the `operation!` declarations a producer-owned operation descriptor
   table containing the Rust method, wire RPC, request type, response type, and
   cancellation support. Have the macro and the Kotlin adapter generator
   consume that table. The existing `HTTP_ROUTES` table should use the same
   descriptor source instead of being a second operation list.
3. Expose a small `SemanticProducerMetadata` value containing the schema
   revision, package name/version, semantic root names, operation descriptors,
   and source-closure paths. This is metadata only; it must not contain a
   second wire schema or transport implementation.
4. Make the facade build step derive source and lock hashes from that closure.
   `kotlin_generation_metadata.rs` should become generated build metadata or a
   thin view over the producer metadata, rather than hard-coded domain,
   generated-Kotlin, and native hashes.
5. Generate the Kotlin cancellation adapter from operation descriptors. The
   handwritten portion then contains only the generic `null`-handle overload
   pattern and imports; request/response names and the eight operation list are
   producer-derived.

These changes preserve the existing UniFFI boundary. UniFFI annotations remain
on the facade because making the core semantic crate depend on UniFFI would
couple the Rust domain to one foreign-language generator. The facade may wrap
producer values, but it must call producer constructors and accessors rather
than reimplementing their rules.

## Local qualification lane

Run the source and package audit with:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File `
  .\qualify-final-producer.ps1
```

The script extracts the producer roots, eight operation declarations, and eight
routes; records the source commit, dirty-state, source-closure hashes, exact
JAR hash, and standard JNA resource roots; and writes both JSON and raw text
receipts under `audit/`. `-SourceRoot` and `-Jar` can point at a different
task-local checkout and exact package without modifying production files.

The script deliberately does not publish, install globally, alter a Cargo
registry, or claim that an older native resource belongs to a new source lock.
When the facade integration lands in the production cutover, the same receipt
is the admission input for the full bindgen, native rebuild, Maven package,
external-negative compile, all-eight runtime, and pending-cancellation lanes.
