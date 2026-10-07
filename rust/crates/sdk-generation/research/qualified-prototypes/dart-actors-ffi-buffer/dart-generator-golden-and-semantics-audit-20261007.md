# Dart generator golden and semantic audit (2026-10-07)

## Reproducible golden maintenance

The maintained generator source is `Q:\sdk\work\uniffi-bindgen-dart-013-source` at base revision `c2a2ee53cc2dc1ef2d6e8e8f41168d71e3098c6f`, with the source-only Dart BigInt/FFI-buffer patch already recorded in `generator-source-production-current.patch`.

All 19 non-library golden roots were regenerated with the maintained `target-local2\debug\uniffi-bindgen-dart.exe` from their checked-in UDL/config inputs and copied into the corresponding `fixtures/*/expected/*.dart` files:

- compound-demo, coverall-demo, custom-types-demo, docstrings-demo, error-types-demo, ext-types-demo, futures-stress, keywords-demo, model-types-demo, non-exhaustive-demo
- regressions/async-object-lift-demo, regressions/callback-custom-async-demo, regressions/custom-shadow-demo, regressions/defaults-demo, regressions/forward-refs-demo
- rename-demo, simple-fns, trait-demo, type-limits-demo

The golden suite now passes **21/21**. Workspace unit tests pass **225/225**. The combined source plus golden patch is `generator-source-and-goldens-production-current.patch`, 145,253 bytes, SHA-256 `10F0B21729CE1E1FE7CDCAB0B73CEF8559E4D7C4BA3766B97C5C73D013FF7B38`. The source-only patch remains 36,271 bytes, SHA-256 `9877C9AC1D69E0E6D072861E66B6C67ACFA6F9E68E0A4927CBEDA9CBD4C2B343`.

## Actual Actors source semantics versus Dart surface

Source evidence is in `Q:\sdk\work\sdkgen-main-port-current\rust\crates\actors\src\domain.rs` and the generated canonical file `generated-production-current-20261007/acyclic_actors.dart`.

- `ActorId` is a Rust validated newtype over `String` (empty/whitespace rejected), but Dart emits `typedef ActorId = String`; nominal validation is erased at compile time.
- `CodeSha256` is a Rust validated 32-byte newtype, but Dart emits `typedef CodeSha256 = Uint8List`; length validation is not a Dart nominal type guarantee.
- `PositiveU64` is Rust `NonZeroU64` and rejects zero. Dart emits `typedef PositiveU64 = BigInt`; this preserves the full 64-bit numeric range but permits `BigInt.zero` statically. The production probe intentionally demonstrates zero/high/max values through the public BigInt fields, so this is a semantic nominal-type gap, not a width loss.
- Generated record and oneof model fields are `final`; constructors preserve required versus optional fields. Collection fields remain mutable Dart `List`/`Map` values, with no readonly/unmodifiable wrapper policy currently applied.
- Optional fields preserve null/presence (`BigInt?`, nullable records, sentinel-backed `copyWith`). `SubscriptionStart.start` is required and rendered as sealed `Start` with `StartCursor` and `StartCurrentHead`; no oneof omission is silently collapsed.
- The canonical package has 24 records, 3 enums, and the 2-variant `Start` oneof. All eight caller-supplied operation roots are covered by the installed runtime probe.

The nominal custom-type wrappers, readonly collection policy, and shared metadata needed to align Dart with JVM/Swift should remain an explicit cross-language policy decision. This audit records the current maintained Dart behavior without adding domain-field mirrors or handwritten validation algorithms.
The full test transcript is preserved as dart-generator-tests-20261007.out (225 unit tests and 21 golden tests passed). Golden hashes are recorded in dart-golden-snapshot-manifest-20261007.json.
