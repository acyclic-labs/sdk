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

## Strong additional candidates

Kotlin, Scala, Elixir, Ballerina and Objective-C have maintained OSS generation/runtime paths with package ecosystems and the streaming shapes required by the active protocol surface. Kotlin uses [grpc-kotlin](https://github.com/grpc/grpc-kotlin); Scala uses [ScalaPB](https://github.com/scalapb/ScalaPB) plus `scalapb-grpc`; Elixir uses [elixir-grpc](https://github.com/elixir-grpc/grpc) and [protobuf](https://github.com/elixir-protobuf/protobuf); Ballerina uses its official [`bal grpc`](https://ballerina.io/spec/grpc/) tool; Objective-C uses the official [gRPC Objective-C plugin](https://github.com/grpc/grpc/tree/master/src/objective-c).

The Scala HTTP projection is the first executable prototype. [`scala-prototype.ps1`](../../research/additional-languages/scala-prototype.ps1) runs the Rust Actors projection, OpenAPI Generator `7.25.0`, sbt `1.10.11`, compiles an independent generated consumer, performs a local HTTP loopback that checks the generated route and bearer header, then runs `package` and isolated `publishLocal`. The generated `org.openapitools:acyclic-actors_2.13:0.1.0` artifact compiled successfully and reproduced identical binary/source hashes across two runs. [`scala-prototype.md`](../../research/additional-languages/scala-prototype.md) records hashes and its fidelity boundary.

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

Erlang (`grpcbox`), OCaml (`ocaml-grpc`) and Common Lisp (`ag-gRPC`) remain experimental. Their upstreams demonstrate useful generated or streaming behavior, but release cadence, package reproducibility, custom-option handling and descriptor compatibility still need evidence. Haskell and Lua remain blocked for full SDK status: the available gRPC or protobuf paths are explicitly incomplete or lack a maintained generated gRPC runtime.

## HTTP and tooling projections

[OpenAPI Generator](https://openapi-generator.tech/docs/generators/) `7.25.0` supplies practical HTTP projections for Ada, C, Clojure, Crystal, Elm, GDScript, Julia, Nim, Perl, PowerShell, R and Bash, in addition to the primary languages. These are deliberately represented as HTTP-only in the inventory. The C template uses libcurl and is beta; Perl and R document JSON/XML-oriented support without protobuf; Bash has a local installable five-family HTTP projection bundle generated from the Rust OpenAPI documents, PowerShell has five locally installed Rust-derived HTTP modules with a bounded consumer receipt, and Perl has a locally installed five-family HTTP projection bundle with a bounded consumer receipt. The target receipts record artifact hashes, runtime and dependency license scopes, fixture checks, and limits. Stream coverage is the polling projection plus explicit recovery error handling; the Perl generated client has no operation cancellation primitive. These projections remain HTTP-only and do not claim protobuf/gRPC, automatic retry, cancellation, or embedded behavior. The Perl receipt separately records 70 installed CPAN metadata records; that closure is not a redistribution license grant.

The same generator lists k6, JMeter, Terraform provider and documentation outputs. These are execution/documentation artifacts, not languages, and are grouped under `docs-and-execution-targets` so they cannot be mistaken for package coverage.

Cloudflare [Forge](https://github.com/cloudflare/forge) remains a downstream OpenAPI/TypeScript/docs experiment. Its current input is OpenAPI and its future-input roadmap mentions Protobuf; it does not currently preserve this repository's descriptor options, gRPC stream semantics or handshake policy. Forge therefore does not appear as the canonical generator in this inventory.

## Promotion order

1. Finish ScalaPB and grpc-kotlin descriptor/service prototypes, then qualify Elixir, Ballerina and Objective-C against the same vectors.
2. Run Erlang and OCaml in isolated OTP/opam environments; promote only after package and stream/recovery evidence is retained.
3. Generate HTTP-only packages and snippets from the Rust OpenAPI projection, with explicit route and streaming limitations.
4. Keep each target's generator/runtime lock, source revision, artifact hash, install receipt and conformance report beside the generated documentation bundle.
