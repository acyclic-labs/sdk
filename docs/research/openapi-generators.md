# OpenAPI and remote SDK generation research

Status: pinned research and Rust-model HTTP projection prototype, 2026-10-03

This report records the evidence used for the remote SDK boundary. The
prototype in `rust/crates/sdk-openapi-prototype` consumes the Rust-authored
`acyclic_sdk_contract_wire` models (`ACTORS`, `WORKERS`, `OBJECTS_V2`, and
`STREAM`), including each `ContractSpec.routes` table and `MethodSpec.docs`.
It emits derived OpenAPI 3.0.3 documents; no OpenAPI contract is authored
independently.

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

The Actors prototype iterates `ContractSpec.routes` and verifies each route
against its modeled service method and explicit RPC identity. It emits:

- all eight unary POST routes;
- operation IDs and fully qualified protobuf RPC identities;
- request/response `$ref` schemas for Rust-model messages;
- protobuf JSON decimal-string schemas and explicit uint64 range metadata;
- uint32 maximum metadata (`4294967295`);
- base64 byte schemas;
- enum names and values from the descriptor;
- `x-protobuf-presence` for proto3 optional fields;
- `x-protobuf-oneof` and `x-protobuf-oneofs` metadata;
- known enum names plus an integer branch for unknown numeric enum values;
- operation and schema descriptions from Rust model comments and method docs;
- a complete Rust-model SHA-256 and contract-route provenance block;
- bearer authentication and a canonical error response reference.

The same generic projection now qualifies the exported Workers model: all
seven unary routes, exact route RPC identities, Rust-owned descriptions, and
the two explicit path parameters (`sha256hex` and `alias`) are covered by a
golden test. Stream's ten modeled routes are also emitted when the explicit
`Polling` projection is selected. Unary calls retain ordinary JSON response
semantics; server-streaming `Read`, `Follow`, and `Children` operations are
represented as one `ReadResponse`/`ChildrenResponse` per repeated HTTP request
with a caller-supplied cursor. The OpenAPI operation carries the modeled
`x-protobuf-streaming` direction and an `x-acyclic-http-polling` extension
which states `application/json` framing and explicitly marks SSE and NDJSON
false. No event-stream schema or SSE equivalence is inferred from a gRPC
server-streaming flag.

The Objects v2 model exercises the multi-service path as well: all thirteen
Rust-owned routes from `BucketsService`, `ObjectsService`, and
`MultipartService` are emitted with their exact RPC identities and docs.
Imported protobuf `google.protobuf.Timestamp` values retain their RFC3339
protobuf-JSON representation, map fields retain `x-protobuf-map-entry`, and
client/server streaming directions remain explicit extensions without an
invented event framing protocol.

Inference now has a projection entry point and CLI family selector for its
fourteen Rust-owned routes. Its `INFERENCE_OPTIONS` table is carried as the
`x-acyclic-validation-options` extension, including HTTP path options and
uint64 limits. `RunsService.Watch` retains a generic streaming extension with
SSE and NDJSON explicitly false because its option table does not define an
HTTP polling policy. Filesystem and Harness remain descriptor-only until their
Rust models expose actual HTTP route policy. Inference message and field prose
currently use structural labels because wire-core has not published comment
tables for that package; the receipt records this fidelity limit.

The pinned OAG Python run installed and imported the derived Inference package
after validation reported no issues and produced matching normalized wheels
(`d267a3436826492d61e1e6ba527a70bf71737bba2fad1890cdb20de554551565`). The
Inference route table now uses stable service-qualified operation IDs such as
`runsInspect` and `evaluationsInspect`; the original fully qualified protobuf
service/RPC identity remains in `x-protobuf-rpc`. OAG still reports only the
known free-form and nullable-model warnings for this structural documentation
fallback.

The implementation intentionally does not invent `required` properties: proto3
absence and Rust admission validation are separate concerns. A future
projection must import validation options and Rust-owned validation metadata
before marking fields required. It must also add explicit path parameter
projection, pagination metadata and documented error status mapping when those
contracts appear.

The document omits `servers`; deployment endpoints are environment
configuration and must not be invented by generation. It also leaves
`additionalProperties` unspecified until the canonical protobuf JSON parser's
unknown-field policy is explicitly part of the Rust model.

## Reproducibility and generation experiment

From this crate directory, run:

```text
cargo test --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- target/openapi/actors.json
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- --contract workers target/openapi/workers.json
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- --contract objects target/openapi/objects.json
cargo run --manifest-path rust/crates/sdk-openapi-prototype/Cargo.toml -- --contract stream target/openapi/stream-polling.json
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

For the Rust-model checkout, the derived JSON artifact hashed to
`250bb82cc0a34e1edf433a9d9ffc35f6ddf70ea4e5032d86aba28ed9c9223160`.
Unnormalized wheel builds differed (`4bc178e3…` versus `d0d617f7…`). Setting
`SOURCE_DATE_EPOCH=0` before both builds produced identical wheels with SHA-256
`6e5b2890b5e00dcc1fa5c851174d88209e89628cb9bf57d89b7e41ac8481cf4e` for the
earlier generated package. Repeating the same qualification against the
Rust-model-generated Python package produced identical wheels with SHA-256
`8369fcedf724f0624bded9704d2238cf8c8f63a2c37d49ba82960d0e55e24104`.
The generation loop must require this environment setting (or an equivalent
reproducible packager) before claiming byte-identical SDK artifacts.

The same pinned OAG run generated and installed isolated Python prototypes for
Workers, Objects, and Stream. With `SOURCE_DATE_EPOCH=0` and
`PYTHONHASHSEED=0`, two wheel passes matched byte-for-byte: Workers
`5ae9cd0d6b6294f9ae5f7c155a644956585ff531c75cdbae6adf79ad72eb02ac`, Objects
`8565ff707240b5c43618b622fcf2bcea7434cc2743f773fe228b04e1e07410f3`, and
Stream `1fc214b16eba0b0a407d0146b6d0bf6a6d48a8530cb76ca2d1a87b0bfceb3af1`.
The installed Workers client completed a request against a Rust loopback
fixture at `/v1/workers/deployments/prod/invoke`, and the installed Stream
client completed one JSON polling request at `/v1/stream/read`. These fixtures
verify the route and body shape; the generated Stream client still exposes a
plain JSON response method because OAG does not model the Rust streaming or
continuation semantics.

The same pinned Workers artifact also generated OAG's Rust client template.
After isolating the generated crate as its own workspace, `cargo check`
completed with the declared reqwest/serde dependencies. The generated crate
contains 67 files (manifest SHA-256
`6d2ec25442d1583adbd4eb74d55546125fb2c2a2c3a333c0d9ff8a4296e9b9c3`) and its
resolved `Cargo.lock` is recorded in the receipt. This is compile evidence for
the derived HTTP client template; it does not qualify native gRPC behavior or
the Rust-owned validation and retry policy.

The additional target receipt at
`research/additional-languages/openapi-targets/receipt.json` records the same
Workers artifact qualified through OAG's Rust and PowerShell templates. The
Rust package uses a metadata-only Apache-2.0 overlay over the Rust-owned
artifact; its two standalone `cargo package --offline` archives match
byte-for-byte, and an isolated consumer completed a loopback request with the
protobuf JSON bytes round trip. The PowerShell module uses fixed package and
Apache license metadata plus an anchor-checked Workers adaptation: callers
pass `byte[]` directly, the generated request JSON carries canonical base64,
and response bytes decode back to `byte[]`. A second generation plus
adaptation matched all compared files. Both results remain HTTP projections;
the receipt does not claim native gRPC streaming, SSE, or recovery behavior.
The unified `sdk-generation` stage emits the five Rust projections and the
Rust-owned adapter together, then writes `openapi/stage-receipt.json`. Its
artifact set binds the 7.25.0 OAG pin, Apache-2.0 metadata-overlay scope, and
adapter anchor report to the generated bytes, so the same `check` operation
detects drift across the projection and target adaptation.
The PowerShell receipt now also binds the exact `WORKERS` artifact hash, the
Rust adaptation authority, a local module package archive, and a live fixture
executable hash. Its Apache-2.0 evidence is scoped per target: Perl's OAG
template has no package license field, so the Workers metadata overlay is not
reported as a global Perl package license.

The prototype's seventeen Rust tests include strict Rust-comment description
coverage probes for Actors, Workers, Objects, and Stream, an Objects route,
timestamp, and streaming-direction fidelity probe, a stream polling fidelity
probe, and real loopback HTTP exchanges for Actors, Workers, and Stream. The
fixture sends a protobuf-JSON-shaped Actors request containing a maximum uint64
as a decimal string, base64 bytes, an explicit `currentHead` oneof member, and
an unknown field; it receives an HTTP 409 canonical error body containing an
unknown numeric enum value and asserts that the number is preserved. Source
model mutation tests change a field's JSON name and a route path and verify
both the generated schema/paths and model digest change; deleting a modeled
RPC fails generation. Path parameter names are checked in source order so URL
encoding remains a transport concern of the generated HTTP client.

## Consequences for migration

Use the Rust descriptor and Buf plugin lock as the single generation input.
Emit OpenAPI and docs as derived artifacts with source revision and hashes.
Keep OAG/Forge outputs behind a qualification matrix; remove handwritten
TypeScript contracts only after generated replacements pass the same conformance
tests. Retain thin TypeScript/Svelte/React adapters at the presentation edge.

The remaining open gaps are deliberate: error-to-status mapping is not yet a
descriptor-owned option, and streaming APIs need an explicit derived
SSE/NDJSON/WebSocket schema or native gRPC package. Those gaps are inputs to
the next bounded migration milestone, not reasons to introduce a second
handwritten contract.
