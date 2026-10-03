# OpenAPI and remote SDK generation research

Status: pinned research and Actors projection prototype, 2026-10-03

This report records the evidence used for the remote SDK boundary. The
prototype in `rust/crates/sdk-openapi-prototype` consumes the checked-in Actors
descriptor (`acyclic_actors::FILE_DESCRIPTOR_SET`) and the Rust-owned
`acyclic_actors::HTTP_ROUTES` table. It emits a derived OpenAPI 3.0.3 document;
no OpenAPI contract is authored independently.

## Decision

Keep Rust and protobuf descriptors authoritative. Use Buf/protoc and gRPC
plugins for remote clients and native streaming. Generate OpenAPI only as an
HTTP/JSON projection for operations whose semantics fit that transport, then
qualify OpenAPI Generator (OAG) language templates against the emitted
document. Cloudflare Forge is a promising later pipeline for derived OpenAPI
SDK/docs surfaces, but it is not mature enough to become the contract engine.

The projection is appropriate for Actors' eight unary operations and similar
unary endpoints. It is not a replacement for generated protobuf/gRPC or a
Rust-owned transport facade for Stream `Read`/`Follow`, Inference `Watch`,
Objects streaming uploads/downloads, Filesystem import/export, or Harness
replay. Those contracts carry cancellation, framing, recovery, CAS and
idempotency semantics which OpenAPI does not model portably.

## Pinned evidence

### OpenAPI Generator

- Repository: <https://github.com/OpenAPITools/openapi-generator>
- Release: `v7.25.0`
- Release tag object observed at `ef964b04480889ef86b56cfae84ade8ad4c91c41`
- Generator matrix: <https://openapi-generator.tech/docs/generators/>
- Installation/pinning: <https://openapi-generator.tech/docs/installation/>
- Usage and `version --sha`: <https://openapi-generator.tech/docs/usage/>
- Rust generator: <https://openapi-generator.tech/docs/generators/rust/>
- Python generator: <https://openapi-generator.tech/docs/generators/python/>

OAG has the broadest HTTP client template inventory tested in this research:
TypeScript/Node, Python, Go, Java/Kotlin, C#/.NET, Swift, C++, Rust, Ruby,
PHP, Dart, Objective-C, Scala, Elixir, Perl, Julia, R and others. It supports
package-oriented output and template/configuration overrides, and its CLI can
report the executable commit SHA for provenance.

The Rust template supports async reqwest/hyper clients, middleware and bearer,
API-key and basic auth. The official feature table marks protobuf wire format,
callbacks, links, multiserver, parameter styling and most OAuth grant flows as
unsupported. Python has asyncio/httpx/urllib3 libraries and package build
configuration, but also marks protobuf wire format and several union/schema
features unsupported. These limitations make OAG useful for a derived JSON
surface, not for preserving the canonical wire contract.

OAG has no portable model for gRPC server/client/bidirectional streams, NDJSON
event framing, resumable follow, CAS preconditions, idempotency replay, or
service-specific recovery. A generated client can expose an HTTP response
body, but stream and recovery behavior would still require a Rust-owned
adapter and a language-specific facade. Pagination and error handling are
generator/template dependent and require explicit qualification rather than
being assumed from an OpenAPI document.

### Cloudflare Forge

- Repository: <https://github.com/cloudflare/forge>
- Main revision checked: `86cb1ef3047abc7441d96c894e8cd35e826fa8e5`
- Announcement: <https://blog.cloudflare.com/forge-open-source-generation-pipeline/>

Forge describes itself as an early-days, pluggable OpenAPI generation and
surface-tooling pipeline. Its current focus is the Cloudflare `cf` CLI. The
announcement explicitly says the current input is OpenAPI and lists AsyncAPI,
GraphQL, Cap'n Proto and Protobuf as future input directions. The repository's
package metadata currently reports `0.1.0`, Node `>=22`, pnpm `10.27.0`, and
TypeScript `6.0.3`.

Forge is worth a pinned experiment for a later derived TypeScript/docs path,
especially where chained plugins and preview surfaces are useful. It does not
yet supply the protobuf-first contract or verified multi-language coverage
needed here, so selecting it as the source-of-truth generator would violate
the contract boundary.

### Buf, Protobuf and gRPC

- Buf generation: <https://buf.build/docs/generate/>
- Buf Generated SDKs: <https://buf.build/docs/bsr/generated-sdks/>
- Generated SDK quickstart/version identity: <https://buf.build/docs/bsr/generated-sdks/quickstart/>
- Cargo registry: <https://buf.build/docs/bsr/generated-sdks/cargo/>
- Protobuf language support: <https://protobuf.dev/>
- gRPC language support: <https://grpc.io/docs/languages/>
- gRPC metadata: <https://grpc.io/docs/guides/metadata/>

Buf runs pinned local or remote plugins and can generate Go, TypeScript,
Java/Kotlin, Python, C++, Rust and other targets from the descriptor. Buf's
Generated SDK model ties a module revision to a plugin revision and exposes
native install paths for Go, JS/TS, Java/Kotlin, Swift, Python, Rust/Cargo,
C#/.NET and C++; unsupported registries can use reproducible archive artifacts.
This is the correct base for field numbers, enum values, oneof/presence,
protobuf JSON and native streaming.

The language inventory should therefore start with TypeScript/Node, Python,
Go, Java/Kotlin, C#/.NET, Swift, C++, Rust, then qualify Ruby, PHP, Dart and
Objective-C. Each generated package needs auth/interceptor, canonical error,
retry, cancellation, streaming and recovery tests in that language. gRPC
metadata provides transport headers, but authorization and retry policy remain
language-specific facade behavior.

## Prototype contract and fidelity

The Actors prototype derives every path by joining the service methods in the
descriptor to the Rust-owned `HTTP_ROUTES` operation names. It emits:

- all eight unary POST routes;
- operation IDs and fully qualified protobuf RPC identities;
- request/response `$ref` schemas for descriptor messages;
- protobuf JSON decimal-string schemas for `int64`/`uint64`;
- base64 byte schemas;
- enum names and values from the descriptor;
- `x-protobuf-presence` for proto3 optional fields;
- `x-protobuf-oneof` and `x-protobuf-oneofs` metadata;
- a descriptor SHA-256 and route-source provenance block;
- bearer authentication and a canonical error response reference.

The implementation intentionally does not invent `required` properties: proto3
absence and Rust admission validation are separate concerns. A future
projection must import validation options and Rust-owned validation metadata
before marking fields required. It must also add explicit path parameter
projection, pagination metadata and documented error status mapping when those
contracts appear.

## Reproducibility and generation experiment

From this crate directory, run:

```text
cargo test --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- target/openapi/actors.json
```

The generated file is a local qualification artifact. The prototype crate is
deliberately outside the root workspace, so this experiment does not edit the
root `Cargo.toml` or `Cargo.lock`. OAG 7.25.0 can then be run against that
file with a pinned Java 11 runtime, for example:

```text
java -jar openapi-generator-cli-7.25.0.jar generate \
  -i target/openapi/actors.json -g python \
  -o target/openapi/python --package-name acyclic_actors
```

The output must be treated as an HTTP client candidate until package install,
serialization, auth, canonical error, cancellation and service conformance
checks pass. No package is published by this loop.

In the pinned experiment, Java reported OAG commit `ef964b0` and the Python
generator completed successfully for all eight routes. A local wheel was built
and installed with `pip install --no-deps` into the ignored target directory;
the generated package imported and accepted the descriptor's decimal-string
form using the JSON alias (`memoryBytes="18446744073709551615"`). The generated
package correctly declares runtime dependencies (`urllib3`, `pydantic`,
`python-dateutil`, and `typing-extensions`), so the no-dependencies install is
only a packaging smoke test. The available environment had Pydantic 2.10 while
the generated package requests >=2.11; this is an environment qualification
failure to resolve before publishing, not a contract change.

## Consequences for migration

Use the Rust descriptor and Buf plugin lock as the single generation input.
Emit OpenAPI and docs as derived artifacts with source revision and hashes.
Keep OAG/Forge outputs behind a qualification matrix; remove handwritten
TypeScript contracts only after generated replacements pass the same conformance
tests. Retain thin TypeScript/Svelte/React adapters at the presentation edge.

The open gaps are deliberate: route metadata for Actors is currently Rust
table data rather than a protobuf HTTP option, error-to-status mapping is not
yet a descriptor-owned option, and streaming APIs need an explicit derived
SSE/NDJSON/WebSocket schema or native gRPC package. Those gaps are inputs to
the next bounded migration milestone, not reasons to introduce a second
handwritten contract.
