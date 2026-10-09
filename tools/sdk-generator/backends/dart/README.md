# Dart descriptor producer

This backend generates maintained protobuf and gRPC Dart bindings directly from
the accepted Rust descriptor exports. Generated source belongs in an explicit
output directory, outside this tooling tree.

```text
src/                  generation and qualification tooling
tests/                offline staging controls
tests/fixtures/       installed-consumer controls
templates/package/    generated package metadata
toolchains/           exact SDK, plugin and dependency admission
```

Run the producer from the repository root:

```sh
node tools/sdk-generator/backends/dart/src/generate.mjs \
  --source-root /accepted-source --authority /rust-export \
  --protoc /protoc --dart-plugin /protoc-gen-dart \
  --output /new-package
node --test --test-concurrency=1 tools/sdk-generator/backends/dart/tests/*.test.mjs
```

Output must be absent, have an existing parent, and be disjoint from protected
inputs. The producer validates immutable Rust authority, every attested input,
compiler identity and plugin identity before creating output. Protoc receives
only temporary verified descriptor snapshots, without a source include root.
Missing family output and path collisions fail without a success receipt.

Protoc 28.3 pins are shared in `../../shared/protoc.json`. The admitted Windows
x64 plugin is protoc_plugin 25.1.0 compiled with official Dart SDK 3.13.5.
`toolchains/plugin-pubspec.yaml` and its lock record the exact dependency closure;
`dependencies.json` records all 33 published archive digests, and `sdk-files.json`
records the official SDK archive's extracted files. These files are hashes and
metadata, not vendored package implementations. Other hosts need their own
verified maintained-plugin build before admission.

To reproduce the plugin, resolve the locked toolchain project in an owned cache
and compile the unmodified published `bin/protoc_plugin.dart` with:

```sh
dart compile exe --packages=/toolchain-project/.dart_tool/package_config.json \
  /owned-cache/hosted/pub.dev/protoc_plugin-25.1.0/bin/protoc_plugin.dart \
  -o /protoc-gen-dart
```

The package pins fixnum 1.1.1, protobuf 6.1.0 and grpc 5.1.0. Two actual reusable
producer runs for accepted foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe` emit identical payloads. Maintained
bsdtar packaging with sorted files, fixed ownership/time and
`--options gzip:!timestamp` produces archive SHA-256
`224dd1312302bbcae17a8c583cac163fd91b99ff4b398177903194ac0788bfd8`.

The exact archive passes the reusable installed qualifier with a fresh
offline dependency cache: all message/enum descriptors and nested contents,
binary bytes, unsigned integer bits, optional zero presence, both oneof branches,
and gRPC method/message/streaming shapes. Three independent static type controls
reject invalid Actor bytes, Worker bytes and an optional Stream integer.
The gRPC plugin does not export complete file descriptors; the qualifier does
not claim complete file-level metadata equality. Rust-backed RPC, TLS/authentication, cancellation/recovery,
remaining families/platforms and embedded runtime are still outstanding.

Run installed qualification after preparing the pinned SDK and raw dependency
archives from their official URLs:

```sh
node tools/sdk-generator/backends/dart/src/qualify.mjs \
  --package /new-package --authority /rust-export --dart-home /dart-sdk \
  --archiver /tar.exe --cache /verified-pub-archives \
  --output /new-qualification
```

The qualifier checks the entire SDK file inventory, every dependency archive,
package metadata and all receipt payload hashes before creating output. It pins
the admitted Windows bsdtar binary and version, validates archive member paths,
and checks the built archive and complete installed payload. Each dependency is
extracted from its verified raw archive into a new owned pub cache; old extracted
packages are unused. Dart resolves the pinned consumer lock with `--offline
--enforce-lockfile`. Every package configuration entry must resolve to the new
SDK, new consumer or exact new dependency directory. Homes and Dart/pub settings
are isolated. Runtime and installed payloads are checked again after controls.

Compilation, explicit completion output and all intended analyzer diagnostics
must pass before a success receipt is written. A nonzero analyzer status alone
is insufficient. Failures preserve partial output and logs. Receipts retain code,
tool, input, package, project, dependency and log identities. Inputs and tools
must remain exclusively owned during execution.

Routine CI runs only the lightweight offline Node staging controls. It downloads
no Dart SDK, plugin or dependency archives. Registered backends are listed in
`.github/sdk-generator-backends.json`; that list also drives qualification input
scoping so later backend admissions need no planner code edits.
