# Acyclic Dart transport package

This is the transport-only Dart prototype for the Rust-owned Actors v1 and
Stream v2 contracts. It contains no handwritten shared models or service
behavior. `tool/generate.dart` invokes the pinned official Dart protobuf
plugin against the canonical protobuf files.

On a Dart 3.8+ toolchain:

```powershell
dart pub get
dart pub global activate protoc_plugin 25.1.0
dart run tool/generate.dart
dart test
```

The qualification run also works with an unpacked SDK. Set `PUB_CACHE` to a
package-local cache and point `PROTOC`/`PROTOC_GEN_DART` at the pinned compiler
and plugin; the generator records the source revision in
`lib/src/generated/provenance.json`.

The generated files are intentionally build artifacts. The generator requires
`protoc` and `protoc-gen-dart` on `PATH` (or `PROTOC` and `PROTOC_GEN_DART`);
CI should install exact versions and record their checksums in the provenance
file. The gRPC plugin emits `Future` unary methods and `Stream` server-streaming
methods, which covers Actors and Stream's Read/Follow/Children RPCs.
Generated provenance contains SHA-256 content hashes for the lock file, each
schema input, and the optional Rust authority manifest.

The repository includes no SDK binaries. A clean qualification run used the
official Dart 3.8.3 stable Windows archive in an ignored local toolchain and
passed generation, analysis, and all transport tests. CI must repeat those
checks with a fresh supported SDK rather than relying on that local cache.

The package also exposes a thin `RemoteClient` facade. Its automatic resolver
selects the native policy on Dart VM and the browser policy on Dart web;
callers may request a compatible transport override before invocation. The wire
adapter is injected, so this facade does not add a handwritten HTTP encoder or
retry/recovery policy. Bearer credentials follow Rust's `bearer-no-crlf` rule.

The package license is Apache-2.0; dependency license evidence is tracked in
`LICENSE-THIRD-PARTY.md`.

`RemoteClient` delegates transport selection and bearer validation to the
Rust-emitted `lib/src/generated_remote_policy.dart` snapshot. Refresh that
snapshot with the `sdk-contract-wire generate-products` command whenever the
Rust transport policy changes.
