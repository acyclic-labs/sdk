# JVM and .NET binding research

## Decision

The Rust client and contract remain the only semantic implementation. JVM and
.NET packages may contain generated ABI declarations plus thin naming,
lifecycle, and package-loading adapters. They must not regenerate the wire
model, transport selection, retry/recovery policy, or stream state machine.

For the JVM, the strongest maintained candidate is the Kotlin backend in
Mozilla UniFFI. The current C generation record uses **UniFFI 0.31.0**; keep
that exact cohort pin until a new generated package is qualified. Upstream has
since published 0.32.1, but a version change is a binding-cohort migration,
not a safe lockfile-only upgrade: UniFFI records backend and metadata changes
in its release notes. UniFFI is MPL-2.0 and its maintained language set
includes Kotlin. It can generate records, enums, objects, errors, custom
newtypes, and async functions, but its async layer does not provide general
cancellation. Cancellation must therefore be a Rust-owned operation method or
handle exposed through the generated API.

For .NET, **csbindgen 1.9.8** is the maintained Rust-to-C# code generator
candidate. It is MIT licensed and generates C# `DllImport` declarations from
Rust `extern "C"` functions (and can generate callback declarations). It is a
mechanical C ABI generator, not a Rust semantic-model generator: unsupported
signatures can be skipped, byte buffers require explicit ownership, and it has
no built-in async, cancellation, stream, error, or unknown-enum policy. The
official .NET `LibraryImport` source generator is available in .NET 7 and
later and is a useful alternative for the same narrow C ABI surface, but it
also has no knowledge of the Rust contract. A future .NET package should use
one of these for declarations only and keep the semantic wrapper Rust-owned.

`jni-rs` 0.22.4 is a maintained, dual MIT/Apache-2.0 JNI runtime binding and
has useful non-null Java-reference checks, but it is not a binding generator.
It is a lower-level fallback for a deliberately small JNI facade, not a
second JVM contract.

## Evidence and exact upstream references

| Candidate | Exact upstream evidence | License | What it proves | What it does not prove |
| --- | --- | --- | --- | --- |
| UniFFI 0.31.0 | [release v0.31.0](https://github.com/mozilla/uniffi-rs/releases/tag/v0.31.0); [repository](https://github.com/mozilla/uniffi-rs) | MPL-2.0 | Maintained Kotlin generation, records, enums, objects, errors, custom types, and Rust async futures | General cancellation, stream protocol, packaging, or this SDK's remote behavior |
| UniFFI 0.32.1 | [release v0.32.1](https://github.com/mozilla/uniffi-rs/releases/tag/v0.32.1); [async guide](https://mozilla.github.io/uniffi-rs/next/futures.html) | MPL-2.0 | Current upstream release and explicit library-specific cancellation requirement | Compatibility with the current 0.31.0 cohort or a qualified SDK package |
| jni-rs 0.22.4 | [latest release](https://github.com/jni-rs/jni-rs/releases); [repository](https://github.com/jni-rs/jni-rs) | MIT or Apache-2.0 | JNI invocation and non-null reference checks | Generated Kotlin API, immutable records, stream/cancellation semantics |
| csbindgen 1.9.8 | [release v1.9.8](https://github.com/Cysharp/csbindgen/releases/tag/1.9.8); [README](https://github.com/Cysharp/csbindgen) | MIT | Rust `extern "C"` to C# P/Invoke generation, Cdecl and callback templates | Safe Rust types, semantic errors, async/cancellation, or stream behavior |
| .NET LibraryImport | [P/Invoke source generation](https://learn.microsoft.com/en-us/dotnet/standard/native-interop/pinvoke-source-generation); [ABI support](https://learn.microsoft.com/en-us/dotnet/standard/native-interop/abi-support) | .NET runtime license | Compile-time C ABI marshalling in .NET 7+ and NativeAOT-friendly declarations | Rust contract awareness or a generated remote client |

The current repository contains no JVM or .NET package that calls the C
authority. These rows are candidates and evidence, not qualification receipts.

## Type fidelity at the boundary

The Rust facade should expose a small, stable semantic surface and make the
foreign type system carry only what it can represent safely:

* **Nominal and nonzero values.** Keep `ActorId`, hashes, and positive numeric
  values as Rust-owned newtypes with checked constructors. A Kotlin custom
  type or a C# wrapper may improve ergonomics, but a Kotlin `ULong` or C#
  `ulong` does not enforce nonzero. Constructors must return the canonical
  Rust error for zero, overflow, malformed bytes, and wrong length.
* **Immutability.** Enable UniFFI's `generate_immutable_records` and keep any
  mutable-record allowlist empty unless the Rust API explicitly models a
  mutable object. For .NET, expose immutable managed records or read-only
  wrappers around opaque handles; do not project Rust layout structs directly
  into C#.
* **Nullability.** `Option<T>` is nullable only where the Rust contract says
  the value is optional. Required handles and buffers need validated
  constructors and explicit ownership functions. Kotlin nullability and C#
  nullable-reference annotations are useful diagnostics but do not replace
  runtime validation at the Rust boundary.
* **Unknown enums.** A forward-compatible wire enum must not become an
  exhaustive Kotlin or C# enum at the FFI edge. Decode into the Rust model
  that retains the raw numeric value and known/unknown distinction, then
  expose an explicit `Unknown(raw)` case or a tagged value. Foreign `when`
  expressions and C# casts must never be the place where unknown values are
  discarded.
* **Errors and bytes.** Return the Rust error category, operation identity,
  and service detail through one generated error surface. Keep byte encoding,
  lengths, and release functions in Rust; memory allocated by Rust is released
  by Rust. The csbindgen examples themselves require manual buffer ownership,
  which is evidence that a generated declaration alone is insufficient.

## Cancellation and streams

UniFFI's async guide explicitly says that it does not directly support
general cancellation and recommends a library-specific cancellation channel.
The Rust facade should therefore export an operation handle with an
idempotent `cancel`, a bounded `next`/poll operation for streams, and an
explicit close or terminal result. Kotlin coroutine cancellation should call
that Rust cancellation method; dropping a coroutine or relying on a foreign
future destructor is not sufficient evidence. The stream tests must cover
cancellation while the operation is in flight, cancellation after a terminal
result, exactly-once close, ordered events, and terminal error preservation.

The .NET C ABI should expose the same Rust-owned handle operations. A C# thin
adapter may register a `CancellationToken` callback and implement
`IAsyncEnumerable<T>`, but those adapters may only forward calls. They must
not retry, reconnect, reorder, or infer terminal state. `LibraryImport` and
csbindgen provide no stream or cancellation implementation themselves.

## Cross-platform package obligations

The JVM artifact needs generated Kotlin/JVM sources, its pinned JNA and
coroutines dependencies, and native libraries for every declared OS and
architecture. Android ABIs, desktop JVM targets, and their Gradle/Maven
coordinates must be tested separately; a successful host JVM load does not
qualify an Android package.

The .NET artifact needs one managed API assembly and RID-specific native
assets under `runtimes/{rid}/native/`. NuGet's documented native asset layout
and portable RIDs should drive the package matrix: [multi-targeting and
architecture-specific assets](https://learn.microsoft.com/en-us/nuget/create-packages/supporting-multiple-target-frameworks)
and [.NET RID catalog](https://learn.microsoft.com/en-us/dotnet/core/rid-catalog).
NativeAOT should be an explicit test target because it is one reason to prefer
source-generated `LibraryImport` declarations over runtime-generated
`DllImport` stubs.

## Qualification gate

Neither candidate is qualified for the SDK until a clean generation run from
the current Rust revision records the exact tool pins, generated-source hash,
native binary hashes, package metadata, and supported target matrix. The
minimum semantic matrix is:

1. nominal IDs, hashes, nonzero values, bytes, optional values, unknown enum
   values, and rich errors round-trip through the Rust codec;
2. unary and streaming operations preserve operation identity, ordering,
   terminal errors, and service details;
3. cancellation is observed while work is in flight and after an observer is
   dropped, with no lost handle or retained task;
4. reconnect/recovery and uncertain acknowledgements use the Rust policy and
   never originate in foreign wrappers; and
5. clean Gradle/Maven and NuGet installs load each supported native asset on
   the declared OS/architecture set, including a .NET NativeAOT smoke test.

Until those artifacts and tests exist, record JVM and .NET as candidates rather
than shipping generated packages or claiming cross-platform support.
