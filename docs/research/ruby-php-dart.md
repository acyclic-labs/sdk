# Ruby, PHP, and Dart transport qualification

Status: Ruby, PHP, and Dart package generation and dependency qualification
passed in isolated portable caches. PHP unsigned values above `PHP_INT_MAX`
use the package's exact decimal `UInt64` wrapper on the pure-PHP runtime.

The three targets can consume the Rust-owned protobuf descriptor through their
official protobuf/gRPC toolchains. They should begin as generated transport
packages. Shared authentication, retry/recovery, typed error, hosted JSON/SSE,
and embedded behavior must remain Rust-owned integration work rather than being
re-authored in each language.

## Selected toolchains

| Target | Generator | Runtime | License | Initial stream coverage |
| --- | --- | --- | --- | --- |
| Ruby | `grpc-tools` 1.82.0 | `grpc` 1.82.0, `google-protobuf` >=3.25,<5 | Apache-2.0 | unary, server-stream, client-stream, bidi |
| PHP | `grpc_php_plugin` 1.82.0 | `grpc/grpc` 1.82.0, `google/protobuf` 5.36.2 | Apache-2.0 / BSD-3-Clause | unary, server-stream, client-stream, bidi client |
| Dart | `protoc_plugin` 25.1.0 | `grpc` 5.1.0, `protobuf` 6.1.0, `fixnum` 1.1.1 | BSD-3-Clause / Apache-2.0 | `Future` unary and `Stream` server-stream |

The exact versions are recorded in `ruby/generator.lock.json`,
`php/generator.lock.json`, and `dart/generator.lock.yaml`. CI must install
these versions (and pin `protoc`/`grpc_php_plugin` checksums where the package
manager does not provide them), regenerate from a clean checkout, and fail on
uncommitted generated changes.

Primary documentation and package sources:

- [Ruby gRPC basics](https://grpc.io/docs/languages/ruby/basics/),
  [`grpc`](https://rubygems.org/gems/grpc), and
  [`grpc-tools`](https://rubygems.org/gems/grpc-tools).
- [PHP gRPC basics](https://grpc.io/docs/languages/php/basics/),
  [`grpc/grpc`](https://packagist.org/packages/grpc/grpc), and
  [`google/protobuf`](https://packagist.org/packages/google/protobuf).
- [Dart gRPC basics](https://grpc.io/docs/languages/dart/basics/),
  [`protoc_plugin`](https://pub.dev/packages/protoc_plugin),
  [`grpc`](https://pub.dev/packages/grpc), and
  [`protobuf`](https://pub.dev/packages/protobuf).

## Contract mapping

Actors v1 is unary and includes `uint64`, `bytes`, optional values, enums, and
oneofs. Stream v2 adds server-streaming `Read`, `Follow`, and `Children` plus
the same scalar/presence shapes. Objects v2, when included, adds client-stream
and server-stream methods. Official gRPC generators preserve method identity,
field numbers, binary protobuf wire encoding, and streaming direction. The
generated files are therefore the right transport projection of the Rust
contract.

OpenAPI Generator is not sufficient as the binary source: its Ruby, PHP, and
Dart generators document no protobuf wire support and do not fully preserve
oneof/presence or gRPC streaming. It can remain a secondary HTTP client target
after Rust emits OpenAPI, with a thin adapter for decimal 64-bit values,
base64 bytes, SSE, and the typed error envelope.

## Conformance gates

Every language package must run the shared Actors/Stream vectors after
generation and install from its produced artifact in an empty directory.

1. Descriptor and service identity match the Rust source revision, including
   complete RPC paths and stream direction.
2. Exercise `uint64`/`int64` values `0`, `2^32-1`, `2^53-1`, `2^53`,
   `2^63-1`, and `2^64-1` without rounding.
3. Exercise empty, NUL-containing, all-`0xff`, 64 KiB, and over-limit `bytes`.
4. Distinguish absent optional fields from explicit zero/empty values and test
   each oneof branch and enum value.
5. Run every Actors unary method with valid, unauthenticated, invalid,
   not-found, conflict, deadline, and retry cases.
6. Run Stream unary methods plus Read/Follow/Children with ordering, EOF,
   cancellation, reconnect/recovery, and terminal semantic errors.
7. If Objects is enabled, test client-streaming writes and server-streaming
   reads, including cancellation before publication.
8. Test TLS/private CA, bearer `authorization` metadata, metadata redaction,
   and rejection of plaintext except explicitly permitted loopback.

Language-specific assertions are required: Ruby `Integer` and Enumerator/
Operation cancellation; PHP 64-bit integer behavior and iterator status mapping;
Dart `Int64` (including an explicit unsigned-64 representation policy),
`Future`/`Stream` cancellation, deadlines, and `GrpcError` mapping.

## Embedded boundary

No target is embedded-qualified. The repository's `ffi/` policy requires a
versioned C ABI with opaque handles, owned buffers, explicit releases,
callback/poll completion, and panic isolation; that ABI is not implemented.
Ruby Fiddle/FFI and PHP FFI are optional runtime extensions, and Dart
`dart:ffi` does not cover Flutter Web. Remote generated clients are therefore
the initial supported surface. Native/WASM qualification begins only after a
Rust-owned ABI artifact and its conformance harness exist.

## Local verification record

The isolated qualification caches used RubyInstaller 3.2.11 x64, PHP 8.2.34
NTS x64, Composer 2.10.3, and Dart 3.8.3. Ruby generated from the authority
manifest, generated all nine exported families plus the validation options
dependency, passed all three transport tests (9 assertions), built a gem, and
loaded Actors, Objects, and Machines messages from a clean installed consumer.
Ruby also passed real Rust fixture gRPC append/read and server-stream
cancellation checks. Dart generated all nine families with `protoc_plugin`
25.1.0, passed analysis and all package tests, then passed its opt-in HTTP
protobuf append/read and response-subscription cancellation tests plus
generated gRPC server-stream cancellation against the Rust fixture server via
`FIXTURE_GRPC_ADDRESS`. PHP `composer validate` and `composer install` both
passed with the
exact `grpc/grpc` 1.82.0 and `google/protobuf` 5.36.2 pins. The official gRPC
v1.82.0 source built `grpc_php_plugin.exe` with the isolated Strawberry MinGW
toolchain; generation then emitted 479 PHP files including the checked-in exact
`UInt64` runtime wrapper across all nine families plus
validation options. The PHP package now autoloads `GPBMetadata\\` and
`Inference\\` alongside `Acyclic\\`.

The Ruby and PHP package metadata now pins every direct runtime version that
the package controls: Ruby uses `grpc`/`grpc-tools` 1.82.0 and
`google-protobuf` 4.33.0 exactly; PHP uses `grpc/grpc` 1.82.0 and
`google/protobuf` 5.36.2 exactly. Their generators accept the Rust authority
root and manifest and emit SHA-256 hashes for the lock file, every schema
input, and the authority manifest. Each package also carries a third-party
license table. The Dart package has exact pub lock hashes and an opt-in
fixture test that posts generated protobuf bytes to the Rust loopback server,
verifies append/read receipts, and cancels its response subscription. A second
opt-in assertion cancels the generated gRPC `ResponseStream` against the
Rust fixture gRPC endpoint.

The authority manifest’s ten schema source/descriptor hashes (nine exported
families plus the validation-options dependency) were independently recomputed
and all matched. Ruby, PHP, and Dart negative runs against a copied
tampered Actors schema all rejected `schema hash mismatch` before accepting
generated output. The PHP generator now rejects values above `PHP_INT_MAX`
before the runtime can narrow them; the transport test therefore exercises
`Acyclic\\Runtime\\UInt64` for exact decimal JSON and protobuf varint
round-trips, while generated scalar accessors cover the signed 64-bit range.
The official PECL protobuf 5.36.2 PHP 8.2 NTS x64 DLL also loaded
successfully. The generated scalar policy rejects the unsigned maximum on both
available runtimes, and the decimal wrapper is the qualified exact path.
The native qualification also decoded and re-encoded a Rust-generated
`ActorLimits.memoryBytes = u64::MAX` message, preserving wire
`10ffffffffffffffffff01` and JSON `{"memoryBytes":"18446744073709551615"}`;
the result was parsed through the exact `UInt64` facade.
The pure-PHP runtime remains limited to the wrapper path for values above
`PHP_INT_MAX`; its generated-message decode is rejected rather than allowed to
silently narrow the field.
The system PATH still lacks Ruby, PHP, Composer, and `grpc_php_plugin`; all
successful checks above used isolated portable archives and caches. Git/MSYS
submodule operations still report `CreateFileMapping ... Win32 error 5`, but
the already-populated official source tree builds successfully through CMake
and Strawberry MinGW.

## Windows toolchain follow-up

Ruby has a bounded portable route through the self-contained
[RubyInstaller](https://rubyinstaller.org/) archives (Ruby 3.2.x x64 is the
minimum line selected by the current `grpc` gem). The official
[`ruby/setup-ruby` Windows version map](https://github.com/ruby/setup-ruby/blob/master/windows-versions.json)
provides immutable archive URLs. The `grpc` gem publishes native Windows
variants, so the first CI check should use the RubyInstaller archive plus
Bundler before attempting a DevKit build.

PHP has an official x64 NTS zip at the
[PHP Windows downloads](https://www.php.net/downloads.php?os=windows) and a
matching prebuilt gRPC extension from
[PECL](https://pecl.php.net/package/gRPC). The PHP client generator requires
`grpc_php_plugin`; the official gRPC v1.82.0 source now builds that plugin in
the isolated portable GCC/CMake lane. The official PECL protobuf 5.36.2
PHP 8.2 NTS x64 DLL was loaded in the same cache. Generated scalar assignment
above `PHP_INT_MAX` is rejected by the package policy, and exact unsigned
values use the checked-in decimal wrapper.

## Authority families and capability matrix

Generators read the manifest `families` list and recursively include its proto
dependencies, so newly exported Rust families are included without editing
each language script.

| Authority family | Current schema | Ruby/PHP/Dart wire gate | Rust fixture gate |
| --- | --- | --- | --- |
| Actors | `actors/v1/actors.proto` | unary, uint64/bytes, presence/oneof | create unary |
| Stream | `stream/v2/stream.proto` | unary plus server-stream methods and cancellation | append/read HTTP protobuf plus gRPC cancellation |
| Objects | `objects/v2/objects.proto` | generated all-family stubs; stream vectors pending | fixture endpoint required |
| Workers | `workers/v1/workers.proto` | generated all-family stubs; RPC vectors pending | fixture endpoint required |
| Filesystem | `filesystem/v2/filesystem.proto` | generated all-family stubs; RPC vectors pending | fixture endpoint required |
| Harness | `harness/v2/harness.proto` | generated all-family stubs; RPC vectors pending | fixture endpoint required |
| Protocol | `protocol/v1/protocol.proto` | generated dependency stubs; facade vectors pending | fixture endpoint required |
| Inference | `inference/v1/inference.proto` | generated all-family stubs; RPC vectors pending | fixture endpoint required |
| Machines | `machines/v1/machines.proto` | generated all-family stubs; RPC vectors pending | fixture endpoint required |

## Fresh PHP native transport receipt

The official PECL Windows lane is now qualified on PHP 8.2 NTS x64. `php_grpc-1.82.0-8.2-nts-vs16-x64.zip` and `php_protobuf-5.36.2-8.2-nts-vs16-x64.zip` are loaded together in the isolated consumer. The Rust fixture server accepted a generated PHP unary actor request, a stream append, and a server stream whose call was cancelled after its first response. The exact runtime, package closure, authority closure, source closure, fixture-server source, output, and exit-code hashes are in `php/tests/fixtures/native-qualification.receipt.json`.

The pure-PHP uint64 limitation remains scoped to that runtime: generated scalar values above `PHP_INT_MAX` are rejected rather than clamped, while the native protobuf extension preserves the eight Rust `u64::MAX` family vectors exactly. PHP is therefore not excluded as a language; the native PECL lane qualifies unary, streaming, and cancellation transport behavior for this Windows target.
