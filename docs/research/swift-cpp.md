# Swift and C++ SDK qualification

Status: transport-only qualification lanes added; Swift and C++ packages are
not installable or publishable yet. Research and local toolchain audit:
2026-10-03.

## Boundary

The Rust-owned contract must emit a `FileDescriptorSet` and protobuf sources
before either language is generated. Existing protocol identity is protected by
`compatibility/manifest.json` and `compatibility/public-rpc-matrix.json`.
Generation must preserve field numbers, enum values, oneof and optional
presence, custom validation options, HTTP route metadata, RPC streaming kind,
idempotency, and descriptor digests. Standard language generators do not
interpret Acyclic's validation options, so those options and the common
conformance vectors remain Rust-owned metadata.

Remote SDKs and embedded bindings are separate qualifications:

| surface | mechanism | result | current status |
| --- | --- | --- | --- |
| Swift remote | SwiftProtobuf + gRPC Swift 2.4.1 | generated message/service transport | generated package builds on Windows; NIO transport gate pending |
| C++ remote | Protobuf C++ + gRPC C++ | generated message/service transport | source-bound generation passes; matching runtime consumer remains pending |
| Swift embedded | UniFFI 0.32.1 or the versioned C ABI | Rust behavior through a stable boundary | requires dedicated façade and ABI vectors |
| C/C++ embedded | cbindgen 0.29.4 over explicit C ABI | C header plus C++ wrapper | local prototype qualified; release ABI remains gated |

Neither generated wire code nor generated headers are complete SDKs. A
qualified package must also provide the Rust-owned capability/error metadata,
transport configuration, installation test, and common Actors/Stream/Objects/
Filesystem vectors.

## Swift evidence

- [gRPC Swift 2](https://github.com/grpc/grpc-swift-2), pinned to the 2.4.1
  release, uses the Swift 6.1
  toolchain line and publishes the `GRPCCore`/code-generation stack.
- [grpc-swift-protobuf](https://github.com/grpc/grpc-swift-protobuf) connects
  gRPC Swift 2 to SwiftProtobuf message types.
- [SwiftProtobuf releases](https://github.com/apple/swift-protobuf/releases)
  provide the pinned `1.38.1` message generator/runtime.
- The older [gRPC Swift 1 plugin guide](https://github.com/grpc/grpc-swift/blob/release/1.x/docs/plugin.md)
  documents the `protoc-gen-swift` and `protoc-gen-grpc-swift` model. The v2
  release's exact plugin invocation must be taken from the pinned release in
  macOS CI rather than copied from the maintenance-mode v1 line.

The new `swift/Generate.ps1` fails closed unless pinned `protoc`, SwiftProtobuf,
and gRPC Swift plugins are supplied. With `-WritePackageManifest` it emits a
source-bound SPM package that depends on portable `GRPCCore` and `GRPCProtobuf`
products. Swift.org's official Windows instructions provide a native Swift
toolchain, so this lane is not macOS-only. The pinned 6.4.0 Burn installer
completed successfully, and its installed tree was copied to
`Q:/sdk/build/swift-toolchain-6.4.0/installed`. With the bundled Windows SDK
and a sanitized child environment, `swiftc hello.swift -sdk <Q-installed SDK>`
compiled and ran a Windows executable with output `swift-portable-hello`. The
source-bound receipt is
`Q:/sdk/build/swift-toolchain-6.4.0/swift-windows-portable-receipt.json`.
The remaining Swift gate is the generated gRPC package: pinned `protoc`,
`protoc-gen-swift`, and `protoc-gen-grpc-swift-2` executables and an SPM build.
The Windows route is concrete: Swift.org specifies MSVC v143, a Windows SDK,
Python 3.10.x and Git, and publishes an x86_64 `.exe` installer plus a WinGet
package.

The transport split is evidence-based. The gRPC Swift 2 package itself has no
Apple-only platform declaration and its generated core can be built without a
network transport. The NIO transport package has a POSIX implementation and a
separate Network.framework Transport Services implementation. The latter uses
`#if canImport(Network)` and Apple availability annotations, and its package
declares Apple platforms for its Foundation compatibility dependency; it is
not a Windows transport. A Windows qualification must use and test the NIO
HTTP/2 path. This is a platform-specific restriction on one transport module,
not a blanket exclusion of Swift on Windows.
The official Swift 6.4.0 x86_64 Windows installer used by the receipt is a
2,097,914,680-byte `.exe` (`application/octet-stream`, HTTP 200, last modified
2026-09-15). Swift's manual instructions specify a per-user `%LocalAppData%`
installation and do not document archive extraction. The Burn installer uses
its documented per-user root and C: package cache; no machine-wide installation
was performed.

## C++ evidence

- [Protocol Buffers C++ build documentation](https://github.com/protocolbuffers/protobuf/blob/main/src/README.md)
  requires matching `protoc`/gencode and warns that C++ runtime ABI
  compatibility is not guaranteed across versions.
- [Protocol Buffers releases](https://github.com/protocolbuffers/protobuf/releases)
  provide the `36.2` pin selected for this lane.
- [gRPC C++ build documentation](https://github.com/grpc/grpc/blob/master/src/cpp/README.md)
  uses `grpc_cpp_plugin` with `--grpc_out`; the plugin and runtime must come
  from the same gRPC release.
- [gRPC C++ 1.80.0](https://github.com/grpc/grpc/releases/tag/v1.80.0) is the
  pinned source release for this lane (verified commit `f5e2d6e`); the cached
  source probe at `1.56.2` cannot qualify it.

The new `cpp/Generate.ps1` requires Protobuf 36.2 and gRPC C++ 1.80.0, then
emits only raw `.pb.*` and `.grpc.pb.*` transport bindings. `cpp/CMakeLists.txt`
requires matching `Protobuf::libprotobuf` and `gRPC::grpc++` packages and fails
if generated sources are absent. There is no C++ facade or package claim yet.

The host has Clang 17, MSVC Build Tools 14.44 and CMake 3.26. The pinned
executables are retained outside the repository at
`Q:/sdk/build/protobuf-36.2/bin/protoc.exe` and
`Q:/sdk/build/grpc-install-1.80.0-vs-clean/bin/grpc_cpp_plugin.exe`.
`cpp/Generate.ps1` ran against the current Rust-owned `proto/` tree and emitted
10 families and a source-bound receipt under
`sdk/build/sdk-cpp-generated-current`. This supersedes the earlier PATH audit;
the cached Cargo protoc 31.1 and gRPC 1.56.2 probe remain excluded.

The fresh generated tree was then compiled with the pinned gRPC package. The
consumer exposed a real toolchain closure issue: the available gRPC 1.80.0
MSVC runtime was built against Protobuf 6.31.1 (`PROTOBUF_VERSION 6031001`),
while the selected Rust-bound generator emits Protobuf C++ 7.36.2
(`PROTOBUF_VERSION 7036002`). The generated headers fail their exact runtime
guard when paired with that runtime. The separate Protobuf 36.2 runtime is
built `/MT` while the available gRPC libraries are `/MD`, so the attempted
consumer also reports the MSVC runtime-library mismatch. The remote package
remains pending until gRPC 1.80.0 is rebuilt against the same Protobuf 36.2
and MSVC runtime mode.

A bounded source probe also inspected the locally cached gRPC C++ source bundled
by `grpcio-sys` (`1.56.2`, not the pinned `1.80.0`). CMake/Clang reached the
Windows host feature-detection phase but did not complete within the bounded
probe; no plugin or runtime artifact was accepted from that older tree. The
probe cannot qualify the remote lane because both the version and runtime
provenance differ from the selected pins.

The independent embedded C++ smoke is qualified locally. It links
`cpp/embedded-consumer` against the real release `cdylib` and generated
cbindgen header from `rust/crates/sdk-embedded-prototype` with Clang 17 and
CMake/Ninja. CTest covers ABI version, opaque handles, copied input, owned
output, explicit release, close, stale handles, cancellation, and duplicate
release. A second clean-prefix CMake consumer proves the thin RAII facade,
generated header, import library, and DLL install together. The C++ consumer
imports the C header inside `extern "C"`; it does not add shared behavior or
alter the ABI. The generated header in this run has SHA-256
`18B07471660357296969E9B32C2891B5367949D21F5F736957D3BDF65197D677`; the
DLL and import-library hashes are recorded in
`cpp/embedded-consumer/package-manifest.json`. The manifest marks this as a
working-tree local install, never a registry or production artifact.

## Embedded plan

For C and C++, add a Rust façade crate that exports only opaque handles,
caller-independent owned buffers, explicit release functions, and callback or
poll completion. Run [cbindgen](https://github.com/mozilla/cbindgen) `0.29.4`
over that façade and put any ergonomic C++ API above the C header. Never expose
Rust layouts, protobuf C++ objects, references, futures, or panics.

For Swift, [UniFFI](https://github.com/mozilla/uniffi-rs) `0.32.1` is suitable
for an embedded façade with records, enums, errors, bytes, callbacks, and async
methods. UniFFI is a binding generator, not a remote gRPC generator; its
generated ownership and async behavior must be checked against
`ffi/abi-policy.md`. If that check fails, Swift should import the same C ABI
instead.

## Reproduction commands

With pinned C++ tools installed:

```powershell
.\cpp\Generate.ps1 `
  -Protoc C:\toolchains\protobuf-36.2\bin\protoc.exe `
  -GrpcCppPlugin C:\toolchains\grpc-1.80.0\bin\grpc_cpp_plugin.exe
cmake -S cpp -B build/sdk-cpp -DACYCLIC_CPP_GENERATED_DIR=$PWD/build/sdk-cpp/generated
cmake --build build/sdk-cpp --config Release
```

On Windows, Linux, or macOS with the pinned Swift tools:

```text
./swift/Generate.ps1 \
  -Protoc /opt/toolchains/protobuf-36.2/bin/protoc \
  -SwiftPlugin /opt/toolchains/swift-protobuf-1.38.1/bin/protoc-gen-swift \
  -GrpcSwiftPlugin /opt/toolchains/grpc-swift-protobuf-2.4.1/bin/protoc-gen-grpc-swift-2 \
  -WritePackageManifest
swift build -c release --package-path build/sdk-swift/generated
swift test --package-path build/sdk-swift/generated
```

These commands intentionally stop before packaging when the generator,
runtime, or platform gate is missing. That distinction keeps a generated type
projection from being mistaken for an installable, tested SDK.


## Current source-bound receipts

- Swift generator output: `Q:/sdk/build/sdk-swift-generated-241`; exact source tags are SwiftProtobuf 1.38.1, gRPC Swift 2.4.1, and gRPC Swift Protobuf 2.4.1. The package release build passed with the official Swift 6.4 Windows toolchain.
- Swift NIO transport source: `grpc-swift-nio-transport` tag 2.4.1 (`1f247d35f305ef3c21d9ebc1dd2dfcfee64260d8`). The Windows probe used the POSIX HTTP/2 product with local pinned checkouts. The receipt records the concrete swift-nio-ssl unsupported-Windows failure; no Windows NIO fixture call is claimed.
- C++ remote remains pending: current Rust-bound generation and the pinned
  plugin pass, but the available gRPC 1.80.0 installation is coupled to
  Protobuf 6.31.1 and `/MD`, which cannot consume the Protobuf 36.2 `/MT`
  runtime. A matching gRPC 1.80.0 rebuild is required before a remote package
  receipt can qualify. Cached gRPC 1.56.2 remains excluded from evidence. The
  embedded C++ consumer has an independent local CMake/CTest receipt.
## Windows NIO transport gate (2026-10-03)

The pinned gRPC Swift NIO 2.4.1 POSIX product was compiled with the official Swift 6.4 Windows toolchain. Its dependency graph unconditionally includes swift-nio-ssl; NIOSSL rejects Windows with a source #error("unsupported os") and unresolved POSIX symbols (inet_ntop, AF_INET, socklen_t). The alternative Transport Services product requires Apple Network.framework. Therefore the generated GRPCCore/GRPCProtobuf package is Windows-buildable, but the native Windows fixture transport remains pending an upstream Windows transport or a source-bound Linux/macOS CI consumer. This is a transport-module restriction, not a blanket Swift Windows exclusion.


## Windows protobuf-only consumer (2026-10-03)

The generated package was consumed by an installed Swift 6.4 Windows executable at `Q:/sdk/build/swift-build/swift-protobuf-consumer-241/out/Products/Release-windows-x86_64/swift-protobuf-consumer.exe`. The source-bound receipt is `Q:/sdk/build/swift-protobuf-consumer-241-receipt.json`. It round-tripped an Actors `CreateActorRequest` with `memoryBytes=9007199254740993`, `cursor=18446744073709551000`, optional `limits` and `start` presence, and the `SubscriptionStart.cursor` oneof; output was `swift-protobuf-only=passed bytes=122 uint64=passed presence=passed oneof=passed`. This qualifies generated protobuf encoding and decoding on Windows independently of network transport. It does not qualify a gRPC fixture call.

A local Linux qualification path was checked on this host, but WSL enumeration returned `Wsl/EnumerateDistros/Service/E_ACCESSDENIED`; no Linux toolchain or fixture result is inferred from that host limitation. Swift remains a viable target for a maintained Windows transport or source-bound Linux/macOS CI job.

## Supported-platform Swift qualification recipe (2026-10-03)

Swift remains a viable target on maintained Linux and macOS runners. The
following source-bound CI job is the next qualification path; it is a recipe,
not a passing receipt from this Windows host. The matrix must use the official
Swift 6.4 toolchain for each runner and retain the exact generator/runtime
pins below:

- SwiftProtobuf 1.38.1, commit `55d7a1cc5666b85c13464aea1c4b4a90feccb4c8`.
- gRPC Swift 2.4.1, commit `21fe69ab7ce0e87ac089534733c52f037e74a3eb`.
- gRPC Swift Protobuf 2.4.1, commit `176c5a434fd76f6f479848d1a8f7d44967534168`.
- gRPC Swift NIO transport 2.4.1, commit `1f247d35f305ef3c21d9ebc1dd2dfcfee64260d8`.
- `protoc` 36.2, SHA-256
  `F0C128DC0D8492ECEECE83BB459A4C0E316764B929FFBF1AA416357FD644EDD3`.

Each Linux and macOS job should:

1. Fetch the pins above and verify commits, the `protoc` digest, and the
   generator versions before generation. Set `SWIFTCI_USE_LOCAL_DEPS=1` so the
   package resolves to the checked-out pins rather than an unrecorded registry
   revision.
2. Run `swift/Generate.ps1` (or its equivalent shell wrapper on the runner)
   with `protoc-gen-swift` and `protoc-gen-grpc-swift-2` from those checkouts
   against all nine schema families. Record the generated file list and hashes.
3. Build and test the generated package with `swift build -c release` and
   `swift test -c release`. Build the installed consumer from the generated
   package, recording the toolchain version, platform triple, package manifest
   hash, and executable hash in the receipt.
4. Start the source-bound Rust fixture and invoke it through generated gRPC
   NIO transport. The consumer must check a uint64 value above 2^53, optional
   field presence, a oneof branch, cancellation, and recovery on a fresh
   request after cancellation. Record the fixture revision, endpoint, request
   results, and transport product in the receipt.
5. Mark the target qualified only when generation, package tests, installed
   consumer, and all fixture checks pass on the same runner. A package-only
   build or a Windows protobuf-only run remains a partial receipt.

The Windows evidence currently qualifies the generated protobuf package and
its uint64/presence/oneof consumer only. The pinned NIO transport imports
NIOSSL, which rejects Windows with an unsupported-OS guard and POSIX socket
symbols; Transport Services is Apple-only. No maintained Windows gRPC Swift
transport was found in the pinned package graph, so a Windows fixture receipt
must wait for an upstream-supported adapter. A Rust C ABI remote client is
also not an automatic substitute: it may qualify only after the repository
provides an actual installed adapter exposing the Rust wire model and the
receipt proves the same fixture checks through that adapter. No translated
shared algorithm or invented ABI is accepted as transport evidence.
