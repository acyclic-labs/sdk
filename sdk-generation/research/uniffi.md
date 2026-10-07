# UniFFI and C ABI research

Status: architecture research only. No generator or package qualification is claimed by this note.

Date: 2026-10-07

Rust source snapshot reviewed: `371bb4170e16aca973176b6756a261ee5add7297`.

## Decision boundary

The Rust crate remains the only owner of public semantics, wire identity, validation, transport selection, and error meaning. A foreign-language binding may add an ABI adapter and packaging metadata, but it must call the Rust implementation. It must not reimplement the protocol, validation, recovery, or shared behavior.

UniFFI is the best maintained OSS fit for first-class Kotlin, Swift, Python, and Ruby bindings. The recommended pin for a first qualification cohort is **UniFFI 0.32.1**, released 2026-09-08. The project is MPL-2.0 licensed. This is a research pin; it has not been downloaded or built as part of this note.

The stable route is UniFFI proc-macro metadata embedded in a compiled Rust library, followed by `uniffi-bindgen`. UniFFI also documents UDL as an interface-description route. UDL would be a second contract, so it is not suitable for the source-of-truth architecture. The newer `uniffi_parse_rs` work described in the current changelog parses Rust sources directly, but it is not a released, stable pin yet and should not be made a production dependency until it is released and qualified.

The current Actors crate is not directly bindable by adding a generator command. Its domain types currently use `ts-rs`, and the async `Client` surface uses Tokio and typed Rust errors. The smallest source-owned addition is a UniFFI adapter module whose records, enums, errors, opaque client object, cancellation handle, and stream handle delegate to the existing domain and client code. The adapter is metadata and ABI glue; it must not own a second schema or transport implementation.

## Evidence from maintained projects

- [UniFFI README](https://github.com/mozilla/uniffi-rs/blob/main/README.md?plain=1): built-in bindings are Kotlin, Swift, Python, and Ruby; C#, Go, Dart, Java, Node, Haskell, and others are listed as third-party integrations. The README also describes UniFFI as pre-1.0, with breaking changes possible in advanced areas.
- [UniFFI proc-macro guide](https://mozilla.github.io/uniffi-rs/latest/proc_macro/index.html): `#[uniffi::export]`, records, enums, interfaces, and errors are declared on Rust items, with `uniffi::setup_scaffolding!()` in the binding crate.
- [UniFFI bindings IR](https://mozilla.github.io/uniffi-rs/next/internals/bindings_ir.html): metadata is emitted by proc macros or UDL, stored in the library, and consumed by language-specific bindgen stages. Kotlin, Swift, Python, and Ruby have built-in language IR; C# and Dart are not built-in backends.
- [UniFFI changelog](https://github.com/mozilla/uniffi-rs/blob/main/CHANGELOG.md): 0.32.0 includes breaking changes around async constructors and Ruby enum constructors; 0.32.1 contains backend fixes. This reinforces pinning both the generator and backend crates.
- [UniFFI futures guide](https://mozilla.github.io/uniffi-rs/next/futures.html): Rust async functions map to native foreign futures, but UniFFI has no built-in cancellation protocol. The library must expose cancellation itself.
- [UniFFI custom types](https://mozilla.github.io/uniffi-rs/latest/types/custom_types.html): custom types can bridge a Rust type to a foreign representation, but conversion failures need an explicit error path and must be tested.
- [cbindgen](https://docs.rs/crate/cbindgen/latest): cbindgen 0.29.4 generates C and C++ headers from an existing public C ABI. It does not turn ordinary Rust APIs, async methods, or Rust ownership into a complete high-level SDK.

## Binding architecture for the Actors crate

1. Keep `rust/crates/actors/src/domain.rs` authoritative for nominal identifiers, limits, validation, enum meaning, and error payloads.
2. Keep `rust/crates/actors/src/client.rs` authoritative for the remote client and the native-versus-WASM transport choice. The foreign facade calls these semantic methods and never calls generated wire code directly.
3. Add a Rust-owned binding crate or module with UniFFI attributes. It should expose only stable semantic operations and records, and should use the existing domain constructors for all admission checks.
4. Put `uniffi::setup_scaffolding!()` in that binding crate, generate metadata from the compiled Rust library, and record the exact UniFFI version, Rust revision, library hash, generated source hashes, package hashes, and target triple.
5. Generate each language from the same metadata. Language-specific package manifests, build scripts, and thin ergonomic adapters are allowed; a second shared contract or behavior implementation is not.
6. Keep transport selection inside Rust. Consumers should construct one facade and should not select native gRPC, gRPC-web, TLS, or feature flags themselves.

This architecture preserves explicit wire identities and makes generated language declarations a derived view of Rust. It also makes the current gap visible: the domain and client types do not yet carry UniFFI metadata, and the existing `Client` operations do not expose a public cancellation parameter or portable stream object.

## Type fidelity and semantic gaps

The following rules are required for the qualification cohort:

- `ActorId`, `CodeSha256`, and `PositiveU64` remain nominal Rust-owned types. A binding may expose a language-native wrapper, but it must call the Rust constructor. It must not silently replace these with unvalidated strings or integers.
- `ActorLimits` remains a Rust-owned record whose constructor applies the existing Rust invariant. Generated constructors must preserve the distinction between invalid and valid values.
- `u64` values require language-specific static and runtime tests. Python integers are unbounded, Swift has `UInt64`, and other languages have different signedness and range rules; every generated package must prove that values at both boundaries do not narrow or round.
- Byte fields must preserve exact bytes, including the distinction between absent and empty values. The generated representation may be `bytes`, `Data`, `ByteArray`, or an equivalent, but the package test must exercise round trips.
- Exhaustive foreign enums are unsafe when the Rust protocol can receive an unknown numeric value. The Rust facade must preserve an explicit unknown value or typed error path for such cases. It must not let a generated exhaustive enum discard future values.
- `client::Error` and domain errors need UniFFI-compatible error declarations. The current service error contains a nested wire error; the facade should expose a Rust-owned error record or enum payload rather than leaking transport implementation types.
- Option and presence rules must be tested separately from empty values and defaults. A generated language type that collapses these cases is not a qualifying binding.

UniFFI's custom-type support is useful for these rules, but it does not prove them. Constructors, conversion errors, unknown values, ranges, and presence semantics require generated-package tests and Rust conformance tests.

## Async, cancellation, lifecycle, and streams

UniFFI can map Rust async functions to Kotlin suspend functions, Swift async functions, Python awaitables, and Ruby's supported async mechanism. It does not provide a universal cancellation contract. The current Rust client has `run_with_cancellation`, so the facade should expose a Rust-owned operation or cancellation handle that calls this existing mechanism and maps cancellation to the existing `Cancelled` error. Consumer code should not need to know how the transport implements cancellation.

An opaque `Client` should be backed by Rust ownership (`Arc` where required) and exposed through UniFFI interfaces. Kotlin wrappers have explicit `close()`/`AutoCloseable` lifecycle behavior; Swift, Python, and Ruby destruction timing differs and must be covered by package tests. Avoid callback cycles and retain deterministic close methods for resources that hold a connection or stream.

Async futures are not a portable streaming API. A streaming capability needs a Rust-owned stream object with `next`, `cancel`, and close semantics, or a carefully constrained callback interface. The stream object must delegate to the canonical Rust client and preserve recovery and error behavior. No language should be marked streaming-qualified merely because unary async generation succeeds.

## Language qualification matrix

| Language | UniFFI status | Type and metadata coverage | Async, errors, and lifecycle | Current status and minimum proof |
| --- | --- | --- | --- | --- |
| Kotlin/JVM | Built-in, maintained | Records, enums, custom types, nullable values, and opaque objects are available. Verify exact unsigned and byte mappings. | `suspend` support and typed exceptions are available; objects expose explicit disposal. | Candidate for first cohort. Generate a real package, compile static positive/negative examples, test JVM installation, cancellation, and close. Android is a separate target. |
| Swift | Built-in, maintained | Records, enums, `Data`, optionals, `UInt64`, and typed throws are available. | Async generation is available; Swift 6 `Sendable` integration remains a documented rough edge. | Candidate for first cohort. Qualify SwiftPM/XCFramework packaging separately and test concurrency diagnostics and resource close behavior. |
| Python | Built-in, maintained | Records, enums, bytes, custom conversions, and exception classes are available. Static typing needs generated stubs or a verified `py.typed` strategy. | Awaitables are supported; event-loop behavior and cancellation must be tested. | Candidate for first cohort. Use a pinned packaging tool and run mypy/pyright plus installed wheel tests; do not infer typing quality from runtime generation. |
| Ruby | Built-in, maintained, lower feature investment | Core records/enums/custom types are available, but feature parity and generated documentation are weaker. | Async work uses Ruby's supported thread/fiber mechanisms rather than native async/await; lifecycle is runtime-dependent. | Research candidate, not first-cohort qualified. Prove current generator coverage, error fields, cancellation, and gem installation before promising it. |
| C# | Third-party backend listed by Mozilla | Not a core UniFFI backend; metadata and type mapping must be checked against the pinned backend. | Async, exception, disposal, and stream behavior are backend-specific. | Candidate only after pinning a maintained backend and license, then running an installed .NET package against the Rust facade. |
| Dart | Third-party integration listed by Mozilla | Not a core UniFFI backend; FFI representation and nominal wrappers require qualification. | Future, error, isolate, and disposal behavior are backend-specific. | Candidate only after a pinned maintained backend and Flutter/Dart package proof. |
| C/C++ | C ABI route, not UniFFI high-level output | cbindgen emits headers for an explicitly authored `extern "C"` API; Rust types and ownership are not inferred into a safe SDK. | Handles, allocation/free, status payloads, polling/callbacks, and cancellation must be authored and tested. | Useful low-level escape hatch, not the preferred high-level SDK pipeline. Keep it below the Rust facade if needed. |

The README also lists Go, Java, Node, Haskell, and React Native integrations. They remain inventory items, not qualified targets. Each needs a pinned maintained backend, an installed package test, and the same type, error, cancellation, and lifecycle evidence.

## UniFFI versus cbindgen

UniFFI has the right abstraction for high-level generated SDKs because its metadata model understands records, enums, errors, interfaces, async functions, and language-specific ownership wrappers. It still requires a Rust-defined binding surface and explicit tests for semantics outside its common type model.

cbindgen is valuable when a C ABI is itself a product requirement. It is not a replacement for the high-level pipeline: an ABI author must design every `extern "C"` function, opaque handle, free function, status/error representation, stream protocol, and cancellation mechanism. That surface also tends to weaken nominal types and documentation fidelity. Use cbindgen only as a narrow low-level compatibility layer whose implementation delegates to the same Rust facade.

## What may be custom

Custom work is justified only for demonstrated gaps:

- UniFFI annotations and a Rust-owned adapter over the existing domain and client.
- Nominal type conversions that call the existing Rust constructors.
- Error records that preserve the current service and semantic payloads.
- A Rust-owned cancellation handle and stream handle over the existing Tokio client.
- Package integration, generated typing files where a backend does not emit sufficient static information, and reproducible artifact receipts.
- Drift checks proving the generated metadata and packages correspond to the Rust revision.

Custom work is not justified for a second protobuf contract, a foreign validation implementation, a foreign transport client, or independently authored shared behavior. Those would make the generated SDK diverge from Rust and defeat the source-of-truth requirement.

## Qualification gates

Before a language is called supported, the generated package must be installed from the produced artifact and must pass the relevant tests for:

- nominal constructors, invalid values, ranges, bytes, presence, enum unknown values, and error payloads;
- unary transport, default transport selection, TLS/auth configuration, cancellation, recovery, and streaming where applicable;
- resource lifetime and deterministic close behavior;
- static type checks in the strongest practical mode for the language;
- documentation and examples generated from the same Rust revision;
- package hash and source-revision receipts.

The generator itself must be pinned and reproducible. A successful bindgen invocation is evidence that metadata was consumable; it is not evidence that the foreign API preserves all Rust semantics. That distinction is required in the qualification record.


