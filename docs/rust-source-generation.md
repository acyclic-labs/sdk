# Rust-owned SDK generation

Public executable Rust declarations own wire identities, validation, client
behavior, binding projections, examples, and documentation. Generated contracts
are outputs. A generator must not read an independently authored contract to
reconstruct those declarations.

## Generator selection

| Responsibility | Pinned candidate | Working evidence | Remaining acceptance |
| --- | --- | --- | --- |
| Rust declarations to protobuf | Protify 0.1.4, Prost 0.14.4 | Rust-owned Actors contract plus shared `build.rs`/`codegen.rs`; fresh descriptor structural golden match recorded below. `sdk-generation` renders the maintained proto through `acyclic_actors::contract` and stages the descriptor embedded by the Actors build | Full release drift gate over descriptor, options, streaming signatures, and the Actors-owned tonic build |
| TypeScript static types | ts-rs 12.0.1 | Semantic export roots and `ts-rs` metadata live in `actors/src/domain.rs`; the Actors package consumes generated proto types through its HTTP and gRPC wrappers | Connect the Rust semantic export step to the standalone generation bundle, then qualify installed native/browser clients and complete operation coverage |
| Rust documentation input | rustdoc-types 0.60.0, Rustdoc 1.98.1, format 60 | Real public/re-export/private-path fixtures; pinned typed input with docs and spans available by item identity | A typed Rustdoc-to-public-API join is not claimed until its provenance, source coverage and immutable bundle checks pass |
| API signature formatting | public-api 0.52.2 | Integrated checked format adapter; real generic methods, repeated aliases, enum fields and associated items; custom formatter removed | Full SDK source coverage |
| Shared language bindings | UniFFI 0.31.0 | Historical external Stream prototype: an installed Windows Python wheel exercised canonical Rust Stream memory behavior, typed errors, finite reads and cancellation; Swift generated-source type checks | Current Actors remote backend, installed target packages and complete capability coverage |
| Browser gRPC | tonic-web-wasm-client 0.9.2, tonic 0.14.6 | Generic Actors client completed all eight unary operations against a local gRPC-Web service in headless Chrome | Production Rust-source package and public gRPC-Web ingress |

Cloudflare Forge and OpenAPI Generator operate downstream of API contracts.
They do not extract executable Rust behavior. Independently generated foreign
transport runtimes would create another behavioral authority; public SDKs instead
call the canonical Rust runtime through maintained bindings.

The Actors generation boundary is intentionally a thin physical build step.
`rust/crates/actors/build.rs` declares the contract inputs and delegates to the
shared `src/codegen.rs`; that code renders the Rust-owned contract through
`src/contract.rs`, then invokes the maintained prost/tonic builder for the proto,
descriptor set, and transport facade. The standalone `sdk-generation` launcher
uses the public `acyclic_actors::contract::render_proto_files` API for the
intermediate proto and writes `acyclic_actors::FILE_DESCRIPTOR_SET`, which was
compiled by that Actors build. It does not compile a second contract or emit a
second tonic Rust facade. A fresh structural golden comparison passed on
2026-10-06: the fresh descriptor SHA-256 was
`c565b7d1fa43b3e96fc068ec7db687e504603a377e9e592d15033f15cd02e9e9`, compared
with baseline `70720491f34232b4b7e424a17f8383ad5a69b1018460e8fff7a62600fb6ec16c`.
The comparator reported `descriptor structural golden match`; it compares the
descriptor's files, options, messages, fields, enums, services, and RPC shape,
so this check does not depend on protobuf byte ordering.

The compatibility manifest keeps the Actors `descriptorDigest` at the
archived baseline value. It identifies the immutable descriptor used for
compatibility comparison, not the byte hash of the newly generated descriptor.
The fresh descriptor hash is recorded by `sdk-generation` in its
`generation-manifest.json` artifact list. Other families' compatibility
manifest descriptor fields retain their existing generated-artifact meaning.

## Current migration inventory

The Actors Rust bootstrap is now explicit and bounded. The Rust-owned contract
is declared in `rust/crates/actors/src/contract.rs` and `domain.rs`; the thin
`build.rs` invokes the shared `src/codegen.rs`, which renders the intermediate
proto and descriptor before the tonic facade is generated. The standalone
`sdk-generation` launcher calls the contract module's public proto renderer,
stages that proto and the canonical embedded descriptor under its bundle, and
then passes typed Rustdoc JSON to `sdk-docs`. It does not regenerate tonic Rust
source. Generated proto and descriptor files remain artifacts and are not source
inputs; the compiled-generator source guard continues to hash the Actors
`build.rs`, `codegen.rs`, contract definitions, manifest, and library sources
that produce the embedded descriptor.

The TypeScript wiring is a separate current path: `ts-rs` export roots are
defined in `actors/src/domain.rs`, while
`typescript/packages/actors/src/index.ts`, `http.ts`, and `grpc.ts` consume the
checked-in generated proto package. No caller currently connects
`export_typescript` to the standalone Rust generation launcher. That connection,
followed by installed-client qualification, is the remaining Actors TypeScript
migration work; this document does not treat existing generated files as proof
that it is complete.

The Rust docs slice currently ends at generated data. `sdk-docs` emits the
versioned `sdk-docs-data.v1` projection, schema, and release or preview index;
`sdk-generation` records their source and artifact digests in its manifest.
There is no website renderer, route registration, or deployment step in this
Rust source foundation. Website consumption and presentation remain a later
handoff from the generated data contract.

Protify's proxied conversion generates infallible `From` implementations and can
normalize unknown enums. Validated domain values therefore use raw executable
wire types followed by explicit Rust `TryFrom` admission. Rejected input must
return a typed error, rather than panic or become a default value.

Protify 0.1.4 does not capture Rust comments in its service/schema model. A
narrow typed Rustdoc join by canonical Rust path and field name is the selected
research direction, but it is not an implemented generation claim in this
source-selection document. It must supply documentation without redefining
fields, tags, or transport behavior when its provenance checks are accepted.

The public-api dependency accepts format 59. Format 60 adds nullable unstable
default metadata to functions and associated constants/types. The adapter accepts
only normal public format-60 input with that metadata absent, removes those null
fields from its formatter view, and rejects any unsupported case. The original
typed format-60 input remains the documentation authority.

## Runtime and publication

Native clients select Rust gRPC automatically. Browser clients select the
maintained Rust/WASM transport. Generic connection, authentication, TLS, timeout
and application errors do not cause fallback or replay. The current public Edge
is raw HTTP/2 gRPC: browser Actors qualification requires a separate ingress
change. Standard gRPC-Web also cannot represent Objects client-streaming uploads.

Versions cover SDK packages, executable snippets and documentation together.
Release assets contain generated SDKs, native binaries, docs and hashes; Git
contains source, pins and verification. Published version bundles are immutable;
the catalog selects the latest stable version semantically and retains previous
versions. Branch previews have separate identities. Website presentation and
deployment are outside this change.

## Acceptance

Each replacement PR includes generation, installation, applicable behavior tests
and deletion of the replaced implementation. Rust/TypeScript qualification comes
first. A language becomes supported only after its installed package passes the
applicable remote and embedded cases; generator availability alone is insufficient.

Normal CI verifies source and generated drift with bounded Rust/TypeScript checks.
Broad package/platform checks run locally or on release/manual qualification.
Initial native targets are Windows x64, Linux x64 GNU and macOS arm64. Production
deployment, registry publication and auto-merge are separate operations.
