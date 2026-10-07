# Maintained Dart generator integration recipe — 2026-10-07

## Upstream identity and license

- Source: `Q:\sdk\work\uniffi-bindgen-dart-013-source`
- Base revision: `c2a2ee53cc2dc1ef2d6e8e8f41168d71e3098c6f`
- Package version: `0.1.3`; UniFFI compatibility: `0.31.0`
- License: MIT; license SHA-256 `6018AC579E866FCF510135FD6352F5DC421B73C6FED258DAD56110DAFA006FB4`
- Focused source patch: `generator-source-nominal-validator-current-main-20261007.patch`
- Source-plus-golden patch: `generator-source-and-goldens-nominal-validator-current-main-20261007.patch`

The focused patch changes only generic Dart generator code under `crates/ubdg_bindgen/src/dart`. It adds BigInt public UInt64 mapping with signed FFI bit-pattern conversion, generic recursive readonly collection wrapping, generic nominal custom wrappers, optional Rust validator calls, external record-only codec support, and FFI-buffer lowering. It contains no Actors names, operation list, field model, or copied Rust semantic predicate. `safe_value` is a Rust-owned configuration marker; the only byte-specific local action is defensive copying before the configured validator call.

## Rebuild the maintained generator

From a clean checkout of the base revision, apply `generator-source-nominal-validator-current-main-20261007.patch` for the production generator. For the exact test checkout, apply `generator-source-and-goldens-nominal-validator-current-main-20261007.patch` instead; that combined patch already contains the source diff and the maintained fixture snapshots. Do not apply both patches to the same checkout.

```powershell
cargo test --manifest-path Q:\sdk\work\uniffi-bindgen-dart-013-source\Cargo.toml --workspace
cargo build --manifest-path Q:\sdk\work\uniffi-bindgen-dart-013-source\Cargo.toml --workspace
```

The qualification run produced 225 unit-test passes and 21 golden-test passes. The executable used for generation was `target-local2\debug\uniffi-bindgen-dart.exe`; its SHA-256 and size are in the current-main receipt.

## Reproduce the installed package

Use the frozen current producer receipt and its source-only FFI-buffer patch. Build `acyclic-actors-uniffi` from `Q:\sdk\work\sdkgen-actors-c8-minimal` with UniFFI 0.31.0 and `scaffolding-ffi-buffer-fns`. Generate the domain component with `dart-domain.toml`, then generate the facade component with `dart-facade.toml` and the external package mapping to `acyclic_actors.dart`. Copy the generated files and the matching native library into one clean package with the recorded `pubspec.yaml` and `pubspec.lock`.

```powershell
& Q:\sdk\work\uniffi-bindgen-dart-013-source\target-local2\debug\uniffi-bindgen-dart.exe generate `
  --crate acyclic_actors --config dart-domain.toml --out-dir generated `
  Q:\sdk\work\sdkgen-actors-c8-minimal\target-dart-ffi-buffer-c8-final-20261007\debug\acyclic_actors_uniffi.dll

& Q:\sdk\work\uniffi-bindgen-dart-013-source\target-local2\debug\uniffi-bindgen-dart.exe generate `
  --crate acyclic_actors_uniffi --config dart-facade.toml --out-dir generated-facade `
  Q:\sdk\work\sdkgen-actors-c8-minimal\target-dart-ffi-buffer-c8-final-20261007\debug\acyclic_actors_uniffi.dll

C:\flutter\flutter\bin\dart.bat pub get
C:\flutter\flutter\bin\dart.bat analyze probe-all8.dart probe-cancel.dart generated\acyclic_actors.dart generated\acyclic_actors_uniffi.dart
```

The exact clean archive is `Q:\sdk\work\actors-uniffi-dart-production-c8-current-main-v3-20261007.zip`. It is a package artifact only; no registry publication is part of this recipe.

## Architecture audit

- Rust domain types and constructors remain the sole authority for ActorId, CodeSha256, PositiveU64, and CurrentHeadMarker admission.
- The generator consumes generic custom-type metadata and emits wrappers/factory calls; it does not know the Actors model or operation set.
- The facade uses external-package imports and generated codec bridges; no handwritten domain fields are mirrored.
- Cancellation remains a Rust-exported UniFFI handle passed to generated async methods. The Dart generator does not implement cancellation or server cleanup logic.
- CurrentHead true-only semantics are supplied by the producer's Rust custom type and validator configuration, not by a Dart predicate.

## Next-family streaming gap inventory

The qualified Actors producer has subscription-shaped records (`SubscriptionSpec`, `SubscriptionStart`, `SubscriptionObservation`) and eight unary async operations. It does not export a live server-streaming UniFFI operation or a Dart `Stream<T>` surface. The cancellation probe proves one in-flight unary request and server abort cleanup only.

A next-family streaming qualification therefore still needs a concrete Rust producer with an actual event source and generated boundary, plus an installed Dart package that demonstrates: ordered events, cursor/current-head start behavior, reconnect or terminal error semantics, caller cancellation while events are active, server-side abort cleanup, backpressure or bounded buffering, typed stream errors, and active-resource zero after shutdown on Windows, Linux, and macOS. Until that producer and package exist, this remains an open transport/streaming research item rather than a Dart capability claim.

