# Additional language target inventory

Status: research and prototype evidence, 2026-10-03.

The machine-readable inventory is [`languages/generation-targets.json`](../../languages/generation-targets.json), validated by [`generation-targets.schema.json`](../../compatibility/schemas/generation-targets.schema.json). It includes the current primary targets and practical targets outside TypeScript, Python, Go, Java/JVM, C#/.NET, Swift, C++, Ruby, PHP and Dart. Every entry separates remote transport capability from embedded capability and records whether the evidence is unqualified, a prototype, qualified, or blocked.

## Contract gates

The target cannot become a full SDK merely because a generator emits files. It must consume the Rust-owned descriptor and preserve:

- proto3 optional presence, oneofs, maps, repeated values, bytes, timestamps, enum values and reserved ranges;
- custom validation extensions `51001` through `51012`;
- complete RPC identities and unary, client-streaming, server-streaming and future bidi directions;
- descriptor/capability handshake, metadata, deadlines, cancellation, idempotency and recovery behavior; and
- package installation from a local artifact plus the shared conformance vectors.

Filesystem and Harness behavior is embedded Rust behavior. A foreign target receives it through an explicitly qualified native or WASM ABI; it does not reimplement those algorithms. JSON/OpenAPI targets therefore carry `http-projection` capability and cannot claim protobuf/gRPC parity.

## Inventory audit

The current `generation-targets.json` contains 34 entries: 22 entries have a
producer recipe and 12 are inventory-only. A recipe proves that a Rust-owned
projection can be requested; it does not prove that the generated package is
typed, installable, source-bound, or transport-qualified.

The strongest current evidence is a generated, source-bound consumer that
compiles against the target package and exercises actual generated types. The
ScalaPB receipt (`research/additional-languages/scala-receipt.json`) covers
Rust Actors and Stream protobuf inputs, bearer metadata, bytes, uint64,
optional presence, server streaming, and append/read against the Rust fixture;
custom options, cancellation, recovery, and full conformance remain open. The
Haskell prototype provenance (`research/additional-languages/haskell-grapesy-prototype/provenance.json`)
records a pinned proto-lens/grapesy toolchain and a passed typed surface for
106 RPCs across 18 services, while remote fixture qualification remains
pending. The JVM/.NET receipt (`docs/research/jvm-dotnet-receipt.json`)
records installed Java, Kotlin, and .NET artifacts, nine-family golden
serialization, and bounded Actors/Stream consumers; its current renderer and
fixture source closures differ, so current-source qualification is not claimed.
The Julia, Bash, Perl, and PowerShell manifests provide bounded installed
HTTP projection evidence only.

The practical full-gRPC OSS set is therefore the primary Rust, TypeScript, and
Python targets plus the generated recipes for Go, Java, C#/.NET, Swift, C++,
Ruby, PHP, and Dart, the Kotlin and Scala JVM recipes, and the Haskell
proto-lens/grapesy prototype. Elixir, Ballerina, and
Objective-C have credible maintained OSS runtimes and remain achievable
no-recipe candidates, but have no local generated-consumer receipt. Erlang,
OCaml, and Common Lisp remain experimental until release provenance, package
reproducibility, custom-option handling, and descriptor compatibility are
demonstrated.

Lua is explicitly excluded from full SDK coverage because the available
`lua-protobuf` path provides serialization without a maintained generated
gRPC runtime; its OpenAPI output is beta HTTP only. Ada, C, Clojure, Crystal,
Elm, GDScript, Julia, Nim, Perl, PowerShell, R, and Bash remain HTTP-only
projections. C's Rust ABI and its generated libcurl client are separate
surfaces. Documentation, k6, JMeter, and Terraform outputs are adapter
artifacts, not language targets.

For every target, type evidence must include generated field numbers,
proto3 presence and oneof behavior, bytes and uint64 representation, enum
values, and the generated unary/client/server stream signatures. File
existence, route-name checks, generic JSON tables, or nominal upstream
support do not satisfy this gate.

## Strong additional candidates

Kotlin, Scala, Elixir, Ballerina and Objective-C have maintained OSS generation/runtime paths with package ecosystems and the streaming shapes required by the active protocol surface. Kotlin uses [grpc-kotlin](https://github.com/grpc/grpc-kotlin); Scala uses [ScalaPB](https://github.com/scalapb/ScalaPB) plus `scalapb-grpc`; Elixir uses [elixir-grpc](https://github.com/elixir-grpc/grpc) and [protobuf](https://github.com/elixir-protobuf/protobuf); Ballerina uses its official [`bal grpc`](https://ballerina.io/spec/grpc/) tool; Objective-C uses the official [gRPC Objective-C plugin](https://github.com/grpc/grpc/tree/master/src/objective-c).

The Scala HTTP projection is retained as bounded HTTP evidence. The current
ScalaPB gRPC prototype and receipt are the stronger typed evidence; neither
promotes Scala to complete SDK coverage.

The independent protobuf/gRPC lane now also has a real ScalaPB build:
[`scala-grpc-prototype.ps1`](../../research/additional-languages/scala-grpc-prototype.ps1)
uses ScalaPB `0.11.17`, `sbt-protoc` `1.0.7`, and pinned `protoc` `3.25.5` to
generate and compile 70 Scala sources from the Rust Actors and Stream protobuf
fixtures. It packages and publishes
`dev.acyclic:acyclic-sdk-scala-grpc-prototype_2.13:0.1.0` to an isolated Ivy
cache, then consumes that published artifact from a separate resolver against
the Rust tonic fixture. The installed consumer passes bearer metadata, bytes,
uint64 and Stream append/read vectors. [`scala-receipt.json`](../../research/additional-languages/scala-receipt.json)
binds the artifact, generated source hashes, consumer source and Rust-owned
protobuf inputs. Full SDK qualification still requires shared conformance,
custom-option, descriptor-digest, recovery and cancellation tests.

Erlang (`grpcbox`), OCaml (`ocaml-grpc`) and Common Lisp (`ag-gRPC`) remain experimental. Their upstreams demonstrate useful generated or streaming behavior, but release cadence, package reproducibility, custom-option handling and descriptor compatibility still need evidence. Haskell is a typed full-gRPC prototype with remote fixture qualification pending; Lua remains blocked because no maintained generated gRPC runtime is available.

## HTTP and tooling projections

[OpenAPI Generator](https://openapi-generator.tech/docs/generators/) `7.25.0` supplies practical HTTP projections for Ada, C, Clojure, Crystal, Elm, GDScript, Julia, Nim, Perl, PowerShell, R and Bash, in addition to the primary languages. These are deliberately represented as HTTP-only in the inventory. The C template uses libcurl and is beta; Perl and R document JSON/XML-oriented support without protobuf; Bash has a local installable five-family HTTP projection bundle generated from the Rust OpenAPI documents, PowerShell has five locally installed Rust-derived HTTP modules with a bounded consumer receipt, and Perl has a locally installed five-family HTTP projection bundle with a bounded consumer receipt. The target receipts record artifact hashes, runtime and dependency license scopes, fixture checks, and limits. Stream coverage is the polling projection plus explicit recovery error handling; the Perl generated client has no operation cancellation primitive. These projections remain HTTP-only and do not claim protobuf/gRPC, automatic retry, cancellation, or embedded behavior. The Perl receipt separately records 70 installed CPAN metadata records; that closure is not a redistribution license grant.

The same generator lists k6, JMeter, Terraform provider and documentation outputs. These are execution/documentation artifacts, not languages, and are grouped under `docs-and-execution-targets` so they cannot be mistaken for package coverage.

Cloudflare [Forge](https://github.com/cloudflare/forge) remains a downstream OpenAPI/TypeScript/docs experiment. Its current input is OpenAPI and its future-input roadmap mentions Protobuf; it does not currently preserve this repository's descriptor options, gRPC stream semantics or handshake policy. Forge therefore does not appear as the canonical generator in this inventory.

## Promotion order

1. Finish ScalaPB and grpc-kotlin descriptor/service prototypes, then qualify Elixir, Ballerina and Objective-C against the same vectors.
2. Run Erlang and OCaml in isolated OTP/opam environments; promote only after package and stream/recovery evidence is retained.
3. Generate HTTP-only packages and snippets from the Rust OpenAPI projection, with explicit route and streaming limitations.
4. Keep each target's generator/runtime lock, source revision, artifact hash, install receipt and conformance report beside the generated documentation bundle.
