# Swift SDK generation lane

This directory is the owned qualification lane for the Swift **remote** SDK.
Generated output is source-bound and can be built by Swift Package Manager on
Windows, Linux, or Apple hosts. Generated code will be checked in or packaged
only after the pinned plugins have produced it, a supported host has built it,
and the shared conformance vectors have passed.

The Rust contract emitter supplies the protobuf descriptor/proto tree. The
Swift lane then uses Apple SwiftProtobuf for message types and gRPC Swift 2 for
service transport. No Swift copy of the Rust behavior is authored here.

## Pinned toolchain

| component | pin | role |
| --- | --- | --- |
| SwiftProtobuf | `1.38.1` | protobuf message generator/runtime |
| gRPC Swift | `2.4.1` line | Swift service transport and code generation |
| Swift tools | `6.1` | package/toolchain baseline used by gRPC Swift 2; Swift.org currently provides a Windows 6.4.0 installer |

The exact plugin executable paths are supplied by the host's pinned toolchain. The
script refuses to run without `protoc`, `protoc-gen-swift`, and
`protoc-gen-grpc-swift`; it does not fall back to handwritten types.

## Generation

On Windows, invoke Swift through scripts/run-swift-sanitized.py. It removes
case-insensitive duplicate PATH/Path entries from the child environment before
starting Swift, which keeps the official compiler usable when the host process
environment contains both spellings.

```powershell
./swift/Generate.ps1 `
  -Protoc /opt/toolchains/protobuf-36.2/bin/protoc `
  -SwiftPlugin /opt/toolchains/swift-protobuf-1.38.1/bin/protoc-gen-swift `
  -GrpcSwiftPlugin /opt/toolchains/grpc-swift-protobuf-2.4.1/bin/protoc-gen-grpc-swift-2 `
  -ProtoRoot target/sdk-generation/wire
```

The ProtoRoot must be the Rust contract-wire export; the generator rejects an
authored proto directory without rust-authority.json. The output is a
transport-only build input under
`build/sdk-swift/generated`. Pass `-WritePackageManifest` to emit a package
manifest alongside the generated files. That manifest depends exactly on
gRPC Swift 2.4.1 and `grpc-swift-protobuf` 2.4.1 and uses their portable
`GRPCCore`/`GRPCProtobuf` products, so generated transport types can be
compiled on Windows with the official Swift toolchain. Swift generated types
alone do not provide authentication, retries, capability checks, error mapping,
or embedded Rust behavior. The same directory receives a
`generation-receipt.json` with all input proto and generated Swift hashes and
the three pinned generator versions.

The official Windows compiler gate is now qualified. The pinned 6.4.0 Burn
installer completed successfully, and its installed tree was copied to
`Q:/sdk/build/swift-toolchain-6.4.0/installed` for a portable test. Using the
bundled Windows SDK and a sanitized child environment, `swiftc hello.swift
-sdk <Q-installed Windows.sdk>` compiled and ran a Windows executable with
output `swift-portable-hello`. The source-bound receipt is
`Q:/sdk/build/swift-toolchain-6.4.0/swift-windows-portable-receipt.json`.
This qualifies the official compiler/toolchain on Windows. The pinned SwiftProtobuf 1.38.1 and gRPC Swift Protobuf 2.4.1 plugins were built from source, and all ten Rust proto families generated 20 Swift sources into `Q:/sdk/build/sdk-swift-generated-241`. The generated SwiftPM package completes a Swift 6.4 Windows release build; its clean installed NIO consumer gate remains pending. Swift.org documents a
native Windows installation with MSVC v143, the Windows SDK, Python 3.10.x and
Git, and publishes both an x86_64 `.exe` installer and a WinGet package. The Windows generation/build lane is source-qualified through generation and package compilation, rather than excluded. The generated package
leaves network transport selection to the consuming app: gRPC Swift's NIO
transport package has a POSIX path and a separate Apple Network.framework
path, while Transport Services is Apple-platform-only. A Windows lane must
select and test the NIO path and must not claim Transport Services support.
The official 6.4.0 x86_64 installer used by the receipt is a 2,097,914,680-byte executable
(`application/octet-stream`, HTTP 200, last modified 2026-09-15). The manual
instructions document a per-user install under `%LocalAppData%` and do not
document archive extraction. The installer metadata and receipt make the
compiler gate reproducible without silently replacing a global toolchain. The
Burn installer uses its documented per-user root and C: package cache; no
machine-wide installation was performed.

## Qualification gates

The Windows, Linux, or macOS lane must build the generated output with SPM and test Actors unary,
Stream server streams, Objects client streams, and Filesystem import/export.
The vectors must cover deadline/cancellation, recovery and idempotency,
oneof/optional presence, bytes, unknown fields, descriptor digest, and clean
consumer installation. The package must be tied to the exact Rust SDK revision
that produced its descriptor set.

Embedded Swift is a separate track. It will use UniFFI or the versioned C ABI
from `ffi/`, subject to the opaque-handle, ownership, release, and async
requirements in `ffi/abi-policy.md`.

## Windows transport qualification gate (2026-10-03)

The official Swift 6.4 Windows toolchain and the generated SwiftProtobuf/gRPC Swift package compile on Windows. The pinned `grpc-swift-nio-transport` 2.4.1 POSIX product cannot currently compile there because its unconditional `swift-nio-ssl` dependency reaches NIOSSL's `#error("unsupported os")` and POSIX-only symbols; the Transport Services product requires Apple Network.framework. The Windows fixture consumer therefore remains pending a maintained Windows NIO transport or a source-bound Linux/macOS CI job. This platform transport result does not exclude Swift code generation or the portable GRPCCore/GRPCProtobuf package.
