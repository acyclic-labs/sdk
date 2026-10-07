# Remote generator qualification research

This note compares OpenAPI Generator with Protocol Buffers and gRPC for remote SDKs. It records upstream capabilities and the qualification gate for this repository; it does not make OpenAPI or protobuf files an independently authored contract.

## Upstream evidence and pins

- OpenAPI Generator currently documents stable client generators for C#, C++, Dart, Go, Java, Kotlin, PHP, Python, Ruby, Rust, Swift, and several TypeScript variants. It also labels individual generators as beta, experimental, or deprecated, so the repository must pin a named generator and library rather than treating the project as one uniform backend. See the [official generator list](https://openapi-generator.tech/docs/generators/).
- The upstream release page identifies `v7.25.0` as the current stable release at the time of this review, commit `ef964b0`. Pin that tag and the exact CLI artifact digest when using it. The project is Apache-2.0 licensed ([license](https://github.com/OpenAPITools/openapi-generator/blob/master/LICENSE)).
- Protobuf’s official documentation lists direct `protoc` generation for C++, C#, Java, Kotlin, Objective-C, PHP, Python, and Ruby, with Dart and Go supplied by plugins; Rust and other languages are covered by additional plugins. See the [official overview](https://protobuf.dev/overview/) and [language guide](https://protobuf.dev/programming-guides/proto2/).
- gRPC’s supported-language page lists C#/.NET, C++, Dart, Go, Java, Kotlin, Node, Objective-C, PHP, Python, Ruby, Rust, and Swift. The runtime and plugin are language-specific even though the RPC contract is shared ([supported languages](https://grpc.io/docs/languages/)). gRPC is Apache-2.0 licensed ([license](https://github.com/grpc/grpc/blob/master/LICENSE)); protobuf is Apache-2.0 licensed ([license](https://github.com/protocolbuffers/protobuf/blob/main/LICENSE)).
- The repository already pins `tonic-prost-build = 0.14.6`, `protoc-bin-vendored = 3.2.0`, and the Rust contract tooling. Those are the Rust-side baseline. Every foreign generator must add an exact generator/plugin version, package-manager lockfile, source revision, and binary/container SHA-256 to the generation receipt. Floating `latest`, remote URLs, and mutable container tags fail reproducibility.

## Contract and behavior comparison

| Concern | OpenAPI Generator | Protobuf + gRPC | Decision for the Rust-owned pipeline |
| --- | --- | --- | --- |
| Input | OpenAPI 2/3 documents are normalized into an internal API model before templates run. | `.proto` or descriptor input preserves protobuf declarations, services, options, and descriptor identities. | Generate any OpenAPI or protobuf intermediate from Rust metadata; never author either as a second contract. |
| Type fidelity | Good JSON model coverage, but `oneOf`, `anyOf`, nullable values, formats, maps, and additional properties are interpreted by each generator/template. The upstream docs explicitly describe generator-specific transformations and custom templates. | Strong wire fidelity for field numbers, enum values, oneof, maps, repeated fields, services, and descriptors. Presence still depends on the source declaration; protobuf recommends explicit `optional` presence for proto3 scalars. | Use protobuf/gRPC for the remote contract. Treat OpenAPI as a derived HTTP/documentation view only. |
| Unary RPC | Broad HTTP client coverage. | Native in every gRPC target with a maintained runtime. | Both can cover unary; protobuf/gRPC has the stronger shared semantics. |
| Streaming | No uniform streaming contract across generators. SSE/WebSocket support depends on the input shape and individual template/library. | Unary, server-streaming, client-streaming, and bidirectional streaming are first-class RPC forms; target support still needs runtime qualification. | Use gRPC for stream/recovery families and qualify each runtime with executable fixtures. |
| Cancellation/deadlines | Runtime-specific cancellation APIs and generated behavior; no cross-language guarantee from OpenAPI alone. | Cancellation and deadline semantics are part of gRPC’s model, but APIs differ by language. The official guide documents automatic outgoing cancellation for Java, Go, and C++; other targets require direct tests. | Put cancellation/recovery in the shared Rust-owned scenario suite and expose idiomatic target APIs through thin facades. |
| Default transport | Each generator chooses a default HTTP library and configuration; templates expose different knobs. | Each target binds to its gRPC runtime; browser/Node, native, mobile, and WASM transports differ. | The generated facade selects the best transport for the platform. Consumers do not select feature flags. |
| Documentation | Can generate OpenAPI, Markdown, HTML, and client README material, but content is derived from the OpenAPI model and templates. | Protobuf descriptors can drive references and reflection, but gRPC itself is not a complete website documentation generator. | Generate the website and Rustdoc inputs from Rust comments/examples; use protocol artifacts as machine-readable references. |
| Reproducibility | Pin CLI JAR/container, generator name/library, template revision, config, input digest, and output normalization. Custom templates are an additional source of drift. | Pin `protoc`, each `protoc-gen-*` plugin, runtime package locks, descriptor digest, and plugin parameters. | Protobuf/gRPC has the smaller reproducible contract surface; record all tool hashes in one receipt. |
| Licensing | OpenAPI Generator is Apache-2.0; each generated package also inherits its runtime dependencies’ licenses. | Protobuf and gRPC are Apache-2.0; each language runtime/plugin must still be inventoried. | Keep SPDX/license manifests per generated package and generator image. |
| Maintenance | Very broad target count, but support quality varies by generator status and template/library. | Fewer core generators, with mature official runtimes and language-specific plugins. | Prefer protobuf/gRPC for remote clients; use OpenAPI only where an HTTP/JSON consumer explicitly needs it. |

## Target qualification matrix

The status below is an architecture qualification, not a claim that the repository has already passed the target tests. “Candidate” means the upstream stack has a plausible maintained path; “fixture required” means it cannot be called supported until the shared executable scenarios pass.

| Target | OpenAPI Generator candidate | Protobuf/gRPC candidate | Streaming | Cancellation/recovery | Type-strength risk | Qualification direction |
| --- | --- | --- | --- | --- | --- | --- |
| TypeScript | `typescript-fetch`, `typescript-axios`, and related clients are listed; template/library behavior differs. | `grpc-web`/Node runtimes plus a TypeScript protobuf plugin such as `ts-proto`; browser transport is distinct from native Node. | grpc-web capabilities are narrower than native gRPC and must be tested. | Abort/cancellation is runtime-specific. | High risk of JSON nullability and union drift. | Keep a thin TypeScript presentation/transport adapter over Rust-owned metadata; qualify browser and Node separately. |
| Python | Stable `python` and Pydantic variants are listed. | Official protobuf Python runtime and gRPC Python plugin. | Native gRPC supports streaming. | Context/deadline and cancellation fixtures required. | Strong generated classes, but Python remains runtime-typed. | High-priority candidate. |
| Go | Stable `go` client is listed. | Official `protoc-gen-go` plus `protoc-gen-go-grpc`; context cancellation is idiomatic. | Native gRPC supports all RPC stream shapes. | Context propagation and recovery fixtures required. | Strong static types; optional/presence mapping must be checked. | High-priority candidate. |
| Java | Stable Java clients and multiple HTTP libraries. | Official protobuf Java plus gRPC Java; Kotlin uses its own API layer. | Native gRPC supports all stream shapes. | Official cancellation guidance covers Java. | Strong static types, but nullability and oneof idioms vary. | High-priority candidate; pin Java/runtime toolchain. |
| Kotlin | Stable Kotlin client and Kotlin Spring variants. | gRPC Java/Kotlin ecosystem with protobuf Java/Kotlin generated APIs. | Native JVM streaming. | Deadline/cancellation fixture required for coroutine and blocking APIs. | Strong types; nullable/presence mapping needs golden tests. | Candidate after JVM fixture. |
| C#/.NET | Stable C# client and ASP.NET variants. | Official protobuf C# plus grpc-dotnet/Grpc.Tools. | Native gRPC streaming. | CancellationToken/deadline mapping is a clear target fixture. | Strong static types; nullable reference and oneof mappings need checks. | High-priority candidate. |
| C++ | Stable and beta HTTP client variants. | Official protobuf C++ plus gRPC C++. | Native gRPC streaming. | Official cancellation model; test call lifetime and status propagation. | Strong wire types; ownership/lifetime APIs are the main risk. | High-priority candidate. |
| Swift | `swift6` is listed as a client generator; other variants may be deprecated. | grpc-swift/protobuf Swift plugins are separate maintained inputs. | Requires native/mobile stream qualification. | Task cancellation and call cancellation need direct tests. | Strong types, but optional/oneof and async API mappings vary. | Candidate only after a pinned grpc-swift prototype. |
| Ruby | Stable Ruby client is listed. | Official protobuf Ruby and gRPC Ruby runtimes/plugins. | Native gRPC supports streaming. | Call cancellation/status mapping fixture required. | Runtime typing and nil/presence distinctions need tests. | Candidate. |
| PHP | Stable PHP and beta/next-generation variants are listed. | Official protobuf PHP and gRPC PHP plugin/runtime. | Runtime support must be verified for each stream shape. | Cancellation and long-lived stream behavior require direct tests. | PHP nullable/union behavior is a high risk. | Candidate with a stricter fixture gate. |
| Dart | Stable Dart and Dart Dio clients are listed. | Dart protobuf plugin and gRPC Dart runtime. | Native Dart streaming; browser/mobile split must be tested. | Future/cancellation behavior requires direct tests. | Strong declared types, but JSON/OpenAPI nullability can drift. | High-priority mobile/web candidate. |
| Rust | OpenAPI Rust generator exists, but would duplicate the Rust contract. | Existing tonic/prost path is maintained and already pinned in this repository. | Native tonic supports all four RPC shapes. | Rust cancellation and recovery tests already exist in the foundation work. | Highest source-language fidelity. | Source-of-truth implementation and reference behavior. |

## Qualification gate

For every target that is promoted beyond candidate status, generate from the same Rust-owned descriptor and run one scenario bundle covering: field numbers and enum values, explicit presence, oneof last-value behavior, JSON mappings, unknown fields, unary calls, all supported stream shapes, cancellation before and during a call, deadlines, retry/recovery status and raw details, authentication metadata, default transport selection, package installation, and documentation snippets. Compare serialized golden bytes and descriptor hashes before comparing idiomatic APIs.

OpenAPI Generator can remain a derived compatibility output for HTTP/JSON consumers, but it does not satisfy the remote source-of-truth requirement by itself. The default architecture is therefore:

1. Rust declarations and Rustdoc are authoritative.
2. Rust emits descriptors and deterministic protobuf service inputs.
3. Pinned protobuf/gRPC plugins generate wire/client layers per language.
4. Small Rust-owned facades provide platform-default transport, cancellation, recovery, packaging, and docs examples.
5. OpenAPI is emitted only as a derived HTTP/documentation artifact and is never edited independently.

## Sources

- [OpenAPI Generator generators](https://openapi-generator.tech/docs/generators/)
- [OpenAPI Generator customization and transformations](https://openapi-generator.tech/docs/templating/)
- [OpenAPI Generator v7.25.0 release](https://github.com/OpenAPITools/openapi-generator/releases/tag/v7.25.0)
- [OpenAPI Generator Apache-2.0 license](https://github.com/OpenAPITools/openapi-generator/blob/master/LICENSE)
- [Protocol Buffers overview](https://protobuf.dev/overview/)
- [Protocol Buffers field presence](https://protobuf.dev/programming-guides/field_presence/)
- [Protocol Buffers version support](https://protobuf.dev/support/version-support/)
- [gRPC supported languages](https://grpc.io/docs/languages/)
- [gRPC core concepts](https://grpc.io/docs/what-is-grpc/core-concepts/)
- [gRPC cancellation](https://grpc.io/docs/guides/cancellation/)
- [gRPC Apache-2.0 license](https://github.com/grpc/grpc/blob/master/LICENSE)
- [Protocol Buffers Apache-2.0 license](https://github.com/protocolbuffers/protobuf/blob/main/LICENSE)
