# Dart maintained nominal qualification (2026-10-07)

Producer source is `sdkgen-main-port-current` at `0093da12d7d4205d56db1c25fb75d50b59de11d6`, with the uncommitted shared metadata file `rust/crates/actors-uniffi/uniffi.nominal.toml`. The maintained generator source is `uniffi-bindgen-dart-013-source` at base `c2a2ee53cc2dc1ef2d6e8e8f41168d71e3098c6f`; the exact source-only and source+goldens patches are preserved beside this audit.

The generator consumes Dart binding metadata for nominal `ActorId`, `CodeSha256`, and `PositiveU64`, plus `readonly_collections = true`. Public Dart values are wrappers (`String`, `Uint8List`, and `BigInt` respectively); UInt64 values preserve zero, `2^63`, and `2^64-1` as `BigInt`, while FFI remains signed bit-pattern `int`. Record collection fields are generated with typed `UnmodifiableListView`/`UnmodifiableMapView` values, and mutation attempts fail with `UnsupportedError`.

Rust remains the validation authority. The `safe_value` markers document the Rust custom-type invariant; generated Dart constructors do not reproduce `non-empty-string`, `non-zero-u64`, or fixed-width predicates. Trusted Rust output uses a private wrapper constructor. The current UniFFI metadata parser exposes only `connect_actors` and `connect_actors_with_ca` for this producer, so the Rust validator helper exports are not callable through generated Dart. This receipt therefore records canonical Rust validation ownership rather than a duplicated Dart predicate.

Validation completed:

- Generator: 225 unit tests and 21 golden tests passed.
- Windows stable Dart: analyzer clean; all 8 remote operations passed; typed errors, zero/high/max UInt64, nullable presence, oneof, readonly mutation, and in-flight cancellation/server abort passed.
- WSL Linux Dart beta (`/tmp/dart-sdk-beta-20261007`): same analyzer, all 8, and cancellation checks passed.
- macOS arm64 over authorized `ssh ivar`, Dart `3.7.0-209.1.beta`: same analyzer, all 8, and cancellation checks passed against a live TLS fixture; pending marker ended `active=0` after server abort.
- Static typing negative probe fails at the intended call site because `int` cannot be passed as `PositiveU64`.

The reproducible external package archive is `actors-uniffi-dart-production-nominal-20261007.zip`; it contains the same `pubspec.yaml`/lockfile, generated sources, probes, and platform native artifacts and excludes `.dart_tool`/build output.
