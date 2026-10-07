# Go bindings: Rust-owned SDK qualification

**Research date:** 2026-10-07  
**Scope:** Go remote and embedded bindings for the Rust source-of-truth SDK.  
**Decision boundary:** the Go package must call Rust-owned behavior for both remote services and embedded execution. A generated Go wire client may be used as a low-level transport layer only when validation, default transport selection, retries/recovery, capability checks, and error semantics remain in Rust.

## Required Go surface

The generated package must preserve Rust contract identity and Go’s strongest useful type information:

- named structs and enums for public records and discriminated values;
- explicit presence for optional and `oneof` fields;
- `uint64` without conversion through `int` or floating point;
- `[]byte` for bytes and typed wrappers for Rust newtypes where the contract gives them identity;
- typed nominal errors with machine-readable variants and fields;
- `context.Context` cancellation and deadlines;
- typed unary and streaming operations with deterministic end-of-stream and cancellation behavior;
- unknown-enum and unknown-field forward compatibility;
- one generated package version tied to the Rust contract revision and native artifact hashes.

A Go implementation that translates the shared behavior into an independent runtime fails this boundary even if its wire types are excellent.

## Maintained OSS candidates

| Stack and pinned input | What it proves | Type, stream, and cancellation result | Packaging and license | Qualification result |
| --- | --- | --- | --- | --- |
| [Mozilla UniFFI](https://github.com/mozilla/uniffi-rs), paired with [NordSecurity `uniffi-bindgen-go` v0.7.1 + UniFFI 0.31.0](https://github.com/NordSecurity/uniffi-bindgen-go) | Rust owns the implementation. The Go generator consumes the Rust UDL and emits a high-level Go wrapper over a compiled Rust library. The Go project explicitly pairs generator tags with UniFFI versions and documents the `v0.7.1+v0.31.0` pairing. | The generator maps Rust records, enums, objects, optionals, maps, bytes, and `u64` into named Go representations. Objects preserve Rust-owned state. The public Go documentation does not establish a transport, typed remote stream, or cancellation/recovery model; those must be designed in the Rust FFI surface and proven by executable tests. | Requires a compiled native Rust library and cgo/shared-library loading (`LD_LIBRARY_PATH` is documented). UniFFI is MPL-2.0; the Go generator is also MPL-2.0. Both projects describe the Go generator as a separate, young 0.x project rather than a built-in UniFFI target. | **Only acceptable architecture for a full Rust-owned Go SDK, pending qualification.** The pinned local prototype exposed an upstream generator metadata-recursion/compile failure. Do not fork it or add a handwritten Go runtime; either qualify a maintained upstream-compatible revision or exclude Go until that blocker is resolved. |
| [Official gRPC-Go](https://grpc.io/docs/languages/go/generated-code/) with [protobuf-go](https://github.com/protocolbuffers/protobuf-go) and the pinned repository toolchain (Go 1.27.1, protoc 36.2, `protoc-gen-go` 1.36.10, `protoc-gen-go-grpc` 1.5.1) | Generates strongly typed Go messages and client interfaces from protobuf. The official generated-code reference documents generic typed streaming clients, and every RPC receives `context.Context`. | Excellent Go wire typing: named messages/enums, optional/oneof presence, bytes, `uint64`, typed status plumbing, and native stream `Recv`/`Send` plus context cancellation. It does not make Rust own validation, default transport selection, retries/recovery, or embedded behavior. Those semantics would otherwise be duplicated in Go. | Mature Go module and protoc plugin ecosystem; the grpc-go repository is Apache-2.0. Distribution is pure Go for remote clients, but it still requires pinned protoc/plugin inputs at generation time. | **Transport primitive only.** Keep generated protobuf and gRPC code behind a Rust-owned facade if a proof shows all semantics remain in Rust. Do not present direct generated gRPC as the canonical Go SDK under the current boundary. |
| [OpenAPI Generator](https://github.com/OpenAPITools/openapi-generator) 7.25.0 CLI family | Broad Go and multi-language client coverage from OpenAPI 2/3. The project explicitly generates API clients, server stubs, and documentation. | Can generate idiomatic Go request/response types and HTTP calls, but the OpenAPI contract has no automatic preservation of Rust nominal identities, Rust error behavior, streaming protocol semantics, or embedded execution. Cancellation/retry behavior is generator/runtime behavior unless delegated to Rust. | Apache-2.0. Broad template maintenance is useful for REST transport coverage, but every template is another language runtime surface to qualify. | **Reject for the canonical Go SDK.** It is useful only as a raw REST transport layer behind Rust-owned behavior or as a later target if the architecture explicitly permits language-owned remote semantics. |
| [cbindgen](https://github.com/mozilla/cbindgen) plus handwritten cgo wrappers | Generates C headers from Rust declarations and can expose a stable C ABI. | The C ABI can preserve Rust ownership and nominal handles, but cbindgen does not generate the required Go package, typed streaming facade, cancellation model, or error mapping. Handwritten cgo wrappers would become an independently maintained shared runtime. | MPL-2.0. Native packaging and cross-compilation remain the consumer’s responsibility. | **Exclude as a Go generator.** It is an ABI building block only; it does not close the demonstrated gap without the custom binding runtime that this architecture forbids. |
| [BoltFFI](https://github.com/DioxusLabs/bolt-ffi) and similar multi-language FFI projects | Some projects offer generated bindings for several languages. | The maintained target inventory does not provide a qualified Go path for this contract; the upstream language matrix marks Go as planned. | License and release support vary by project and are insufficient evidence for this SDK. | **Exclude until an actual maintained Go generator and artifact test exist.** Do not count planned support as coverage. |

## Why direct gRPC is not the final answer

The official Go gRPC generator is the strongest option for Go-level remote typing. Its generated stream APIs and `context.Context` signatures give the consumer the right Go shape for cancellation and streaming. That strength is precisely at the wire/client layer: the generated Go client still owns the call path that the consumer invokes. It cannot, by itself, guarantee Rust-owned validation, default transport choice, retry/recovery, capability selection, or the same behavior when the operation is embedded.

The acceptable use is therefore narrow: generate protobuf messages and transport stubs from the Rust-owned descriptor, then expose them only through a Rust-owned facade whose operations return the same typed contract for remote and embedded execution. The facade must be the only public SDK entry point. If that wrapper cannot be generated and tested from the maintained UniFFI Go path, direct grpc-go does not satisfy the source-of-truth goal.

OpenAPI has the same architectural issue with a weaker type identity story. It remains a useful REST transport generator, not the authority for the SDK contract.

## Qualification gate

A Go candidate qualifies only after a clean-checkout run proves all of the following:

1. The Rust contract and Rust documentation are the only authored source for public types, operation identity, validation metadata, errors, and examples. Generation records the Rust revision, generator revisions, toolchain versions, and output hashes.
2. A fresh generation produces the same Go source and native artifact manifest byte-for-byte. Drift mode fails after any generated edit or stale artifact.
3. Go compile-time assertions cover named records/enums, field identity, optional and `oneof` presence, bytes, `uint64`, unknown enum values, and typed error fields.
4. Remote tests cover unary calls, stream send/receive, `context` cancellation and deadlines, recovery/retry, default transport selection, and service-availability errors. The assertions must demonstrate that the behavior is executed by Rust, rather than reimplemented in Go.
5. Embedded tests run the same scenarios through the native Rust library, including Actor, Stream, Filesystem, and Harness behavior where supported. Remote and embedded results must match the Rust conformance vectors.
6. Native packages install on every supported OS/architecture with no consumer-managed feature flags. The package test must exercise the produced artifact, not a developer checkout or an unpinned system library.
7. Snippets compile against the exact generated package and native artifact revision used by the documentation bundle.

The current local UniFFI Go prototype does not pass the generator/compile portion of this gate. This excludes the **native UniFFI/cgo binding route for the current cohort**. It does not exclude the recorded remote Go wire prototype; the full semantic Go SDK remains unqualified until a Rust-owned facade demonstrates remote and embedded behavior parity. This is not a reason to introduce a custom cgo facade.

### Maintained upstream blockers and their scope

The current maintained upstream record gives three separate blockers; they must not be collapsed into a claim that Go as a language is excluded:

- [uniffi-bindgen-go#94](https://github.com/NordSecurity/uniffi-bindgen-go/issues/94) reports that installing the pinned `v0.7.1+v0.31.0` generator requires about 11 GB of memory. This is a generator resource/qualification blocker, not evidence against the remote protobuf transport.
- [uniffi-bindgen-go#53](https://github.com/NordSecurity/uniffi-bindgen-go/issues/53) records that `Async` and `WithForeign` are unsupported. The current Rust facade exports asynchronous Tokio operations, so this prevents claiming generated Go async/cancellation parity until upstream support exists.
- [uniffi-bindgen-go#97](https://github.com/NordSecurity/uniffi-bindgen-go/issues/97) shows `Option` values in Go error variants rendering as pointers. The current facade has an optional service error code; this is a typed-error correctness blocker even when generation completes.

These are maintained upstream issues against the external generator. The local prototype's metadata/compile failure should be recorded as the observed local result and linked to these relevant upstream constraints, rather than naming an unverified issue as its root cause.

### Bounded C ABI fallback architecture

If the maintained Go generator remains blocked, a Rust-owned C ABI is the only bounded fallback that preserves the semantic boundary, but it is an architecture proposal rather than a qualification. The existing `actors-uniffi` Rust facade already has the right ownership shape: constructors for nominal values (`ActorId`, `CodeSha256`, `PositiveU64`, `Binding`, `ActorLimits`, and `SubscriptionSpec`), opaque validated request objects, a Rust-owned client, typed errors, and a cancellation handle. A C ABI target could expose that same facade through Rust-owned opaque handles with explicit retain/release and Rust-owned buffers; a Go wrapper would hold only those handles and call exported Rust constructors/operations.

The Go side may generate descriptor-backed wire structs and thin nominal wrapper declarations, but constructors must call the Rust ABI. It must not reimplement domain validation, endpoint or credential checks, transport choice, retries/recovery, capability checks, error classification, or remote/embedded dispatch. Cancellation would be a generated `context.Context` adapter that invokes the Rust cancellation handle; streaming would be a Rust-owned stream handle with explicit next/cancel/close operations. The same Rust operation must serve remote and embedded modes.

This fallback still needs a maintained generator or a reviewed, generated C header/wrapper pipeline. `cbindgen` alone only emits declarations; handwritten cgo runtime code would recreate the unsupported language-owned behavior and fails the source-of-truth boundary. Qualification therefore requires generated wrappers from the current Rust facade, artifact and ABI version pinning, constructor/error/cancellation conformance, and remote/embedded parity. Until that toolchain exists, the C ABI path is feasible design work, not a Go SDK claim.

### Historical remote Go evidence

A recorded external remote probe exists at `Q:\sdk\work\go-typed-consumer-probe`. Its `generation-receipt.json` identifies Rust authority and records source/model digest `fa78445a9203c4c69dff2d3baa6ff7c11e5301e51cefff3b73b1fd8b27bffdd6`, Go `go1.27.1`, `libprotoc 36.2`, `protoc-gen-go v1.36.10`, and `protoc-gen-go-grpc 1.5.1`. The receipt hashes `gen/actors/v1/actors.pb.go` as `56e7d46acfbb9cd58fddbefba1d91782d1abd2a06d8e8b3a8e463ae81759cc3d` and `gen/actors/v1/actors_grpc.pb.go` as `482e20bdd07853779065bc61ada4d09df670cadad313c46ef224cc931d5712f1`.

Its `typed-consumer-receipt.json` records source digest and Rust-model digest equal to `fa78445a9203c4c69dff2d3baa6ff7c11e5301e51cefff3b73b1fd8b27bffdd6`, generated-consumer SHA-256 `88d76adaad6d0344ed98ebd54923ee8d7cc82df630a5946cea9e2c3c165d1ffb`, and command exit code `0`. The source-bound assertions for field identity, optional/`oneof` presence, bytes, `uint64`, enums, and RPC stream signatures all passed. This is historical evidence of a working Rust-authority remote protobuf/gRPC internal primitive, not evidence of a qualified public semantic Go SDK. The recorded `client.go` facade is generated by obsolete `acyclic-sdk-contract-wire` and is not current maintained-binding evidence.

## Recommendation for the SDK plan

Keep **UniFFI Go v0.7.1 paired with UniFFI 0.31.0** as the sole full-SDK candidate. Give it one bounded upstream-compatible qualification attempt against the existing Rust-owned facade. If the pinned maintained generator still fails its metadata recursion or cannot express typed stream/cancellation objects without a custom Go runtime, mark the **native UniFFI/embedded route** excluded for this cohort. Retain the historical remote protobuf/gRPC result as an internal transport prototype; keep the full semantic Go SDK unqualified pending a maintained Rust-owned facade and remote/embedded conformance.

Use **grpc-go/protobuf-go only beneath the Rust facade** where the service wire protocol needs Go transport primitives. Use OpenAPI Generator only for a similarly hidden REST transport layer. Neither tool should generate the public Go SDK facade or independently authored Go behavior.

This keeps strong Go types where the language can provide them, while keeping Rust as the single behavioral source of truth. It also gives the later language matrix a concrete evidence rule: a language is qualified only by a working Rust-owned artifact and executable conformance, never by a generator’s claimed template coverage.
